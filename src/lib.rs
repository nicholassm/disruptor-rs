//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # Examples
//! ```
//! use disruptor::Builder;
//! use disruptor::BusySpin;
//!
//! // The data entity on the ring buffer.
//! struct Event {
//! 	price: f64
//! }
//!
//! // Create a factory for populating the ring buffer with events.
//! let factory = || { Event { price: 0.0 }};
//!
//! // Create a closure for processing events. A thread, controlled by the disruptor, will run this
//! // processor each time an event is published.
//! let processor = |e: &Event| {
//! 	// Process e.
//! };
//!
//! // Create a Disruptor by using a `disruptor::Builder`, In this example, the ring buffer has
//! // size 8 and the `BusySpin` wait strategy. Finally, the Disruptor is built by specifying that
//! // only a single thread will publish into the Disruptor (via a `Publisher` handle).
//! let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
//! // Publish into the Disruptor.
//! for i in 0..10 {
//! 	producer.publish(|e| {
//! 		e.price = i as f64;
//! 	});
//! }
//! // At this point, the processor thread processes all published events and then stops as
//! // the Publisher instance goes out of scope and the publisher and Disruptor are dropped.
//! ```

pub use wait_strategies::BusySpin;

pub mod wait_strategies;
pub mod producer;
mod consumer;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use crossbeam_utils::CachePadded;
use crate::consumer::Receiver;
use crate::producer::{ProducerBarrier, Producer, SingleProducerBarrier};
use crate::wait_strategies::WaitStrategy;

pub(crate) struct Disruptor<E, P: ProducerBarrier> {
	producer_barrier: P,
	consumer_barrier: CachePadded<AtomicI64>,
	shutting_down:    AtomicBool,
	index_mask:       i64,
	ring_buffer_size: i64,
	ring_buffer:      Box<[Slot<E>]>
}

pub struct Builder<E, P, W> {
	ring_buffer:      Box<[Slot<E>]>,
	consumer_barrier: CachePadded<AtomicI64>,
	shutting_down:    AtomicBool,
	index_mask:       i64,
	ring_buffer_size: i64,
	processor:        P,
	wait_strategy:    W
}

fn is_pow_of_2(num: usize) -> bool {
	return num != 0 && (num & (num - 1) == 0)
}

impl<E, P, W> Builder<E, P, W>
	where E: 'static,
		  P: Send + FnMut(&E) -> () + 'static,
		  W: WaitStrategy + 'static {

	/// Creates a Builder for a Disruptor.
	///
	/// The `size` must be a power of 2.
	///
	/// The `event_factory` is used for populating the initial values in the ring buffer and
	/// the `processor`closure will be invoked on each available event `E`.
	/// The `wait_strategy` determines what to do when there are no available events yet.
	/// (See module [wait_strategies] for the available options.)
	///
	/// # Panics
	///
	/// Panics if the `size` is not a power of 2.
	///
	/// # Examples
	///
	/// ```
	/// // The data entity on the ring buffer.
	/// use disruptor::wait_strategies::BusySpin;
	///
	/// struct Event {
	/// 	price: f64
	/// }
	///
	/// // Create a factory for populating the ring buffer with events.
	/// let factory = || { Event { price: 0.0 }};
	///
	/// // Create a closure for processing events. A thread, controlled by the disruptor, will run this
	/// // processor each time an event is published.
	/// let processor = |e: &Event| {
	/// 	// Process e.
	/// };
	///
	/// // Create a Disruptor by using a `disruptor::Builder`, In this example, the ring buffer has
	/// // size 8 and the `BusySpin` wait strategy. Finally, the Disruptor is built by specifying that
	/// // only a single thread will publish into the Disruptor (via a `Producer` handle).
	/// let mut publisher = disruptor::Builder::new(8, factory, processor, BusySpin);
	/// ```
	pub fn new<F>(size: usize, mut event_factory: F, processor: P, wait_strategy: W) -> Builder<E, P, W>
		where F: FnMut() -> E
	{
		if !is_pow_of_2(size) { panic!("Size must be power of 2.") }

		let ring_buffer: Box<[Slot<E>]> = (0..size)
			.map(|_i| {
				Slot { event: UnsafeCell::new(event_factory()) }
			}).collect();
		let index_mask       = (size - 1) as i64;
		let ring_buffer_size = size as i64;
		let consumer_barrier = CachePadded::new(AtomicI64::new(0));
		let shutting_down    = AtomicBool::new(false);

		Builder {
			ring_buffer,
			consumer_barrier,
			shutting_down,
			index_mask,
			ring_buffer_size,
			processor,
			wait_strategy
		}
	}

	/// Creates the Disruptor and returns a [Producer<E>] used for publishing into the Disruptor (single
	/// thread).
	pub fn create_with_single_producer(self) -> Producer<E> {
		let producer_barrier = SingleProducerBarrier::new();
		let disruptor        = Box::into_raw(
			Box::new(
				Disruptor {
					producer_barrier,
					shutting_down:    self.shutting_down,
					consumer_barrier: self.consumer_barrier,
					ring_buffer_size: self.ring_buffer_size,
					ring_buffer:      self.ring_buffer,
					index_mask:       self.index_mask
				}
			)
		);

		let wrapper  = DisruptorWrapper { disruptor };
		let receiver = Receiver::new(wrapper, self.processor, self.wait_strategy);
		Producer::new(disruptor, receiver, self.ring_buffer_size - 1)
	}
}

struct Slot<E> {
	event: UnsafeCell<E>
}

struct DisruptorWrapper<T, P: ProducerBarrier> {
	disruptor: *mut Disruptor<T, P>
}

unsafe impl<E, P: ProducerBarrier> Send for DisruptorWrapper<E, P> {}

impl<T, P: ProducerBarrier> DisruptorWrapper<T, P> {
	#[inline]
	fn unwrap(&self) -> &Disruptor<T, P> {
		unsafe { &*self.disruptor }
	}
}

impl<E, P: ProducerBarrier> Disruptor<E, P> {
	fn shut_down(&self) {
		self.shutting_down.store(true, Ordering::Release);
	}

	#[inline]
	fn is_shutting_down(&self) -> bool {
		self.shutting_down.load(Ordering::Acquire)
	}

	#[inline]
	fn is_published(&self, sequence: i64) -> bool {
		self.producer_barrier.is_published(sequence)
	}

	#[inline]
	fn wrap_point(&self, sequence: i64) -> i64 {
		sequence - self.ring_buffer_size
	}

	#[inline]
	fn get(&self, sequence: i64) -> *mut E {
		let index = (sequence & self.index_mask) as usize;
		self.ring_buffer[index].event.get()
	}
}

#[cfg(test)]
mod tests {
	use std::thread;
	use crate::BusySpin;
	use std::sync::mpsc;
	use super::*;

	#[test]
	#[should_panic(expected = "Size must be power of 2.")]
	fn test_size_not_a_factor_of_2() {
		std::panic::set_hook(Box::new(|_| {})); // To avoid backtrace in console.
		Builder::new(3, || { 0 }, |_i| {}, BusySpin);
	}

	#[derive(Debug)]
	struct Event {
		price: i64,
		size:  i64,
		data:  Data
	}

	#[derive(Debug)]
	struct Data {
		data: String
	}

	#[test]
	fn test_single_producer() {
		let factory     = || { Event { price: 0, size: 0, data: Data { data: "".to_owned() } }};
		let (s, r)      = mpsc::channel();
		let processor   = move |e: &Event| {
			s.send(e.price*e.size).expect("Should be able to send.");
		};

		let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
		let producer_thread = thread::spawn(move || {
			for i in 0..10 {
				producer.publish(|e| {
					e.price    = i as i64;
					e.size     = i as i64;
					e.data     = Data { data: i.to_string() }
				});
			}
		});
		producer_thread.join().unwrap();

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
	}

	#[test]
	fn test_pipeline_of_two_disruptors() {
		let factory   = || { Event { price: 0, size: 0, data: Data { data: "".to_owned() } } };
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event| {
			s.send(e.price*e.size).expect("Should be able to send.");
		};

		// Last Disruptor.
		let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
		let processor = move |e: &Event| {
			producer.publish(|e2| {
				e2.price    = e.price*2;
				e2.size     = e.size*2;
				e2.data     = Data { data: e.data.data.clone() };
			});
		};

		// First Disruptor.
		let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
		let input = thread::spawn(move || {
			for i in 0..10 {
				producer.publish(|e| {
					e.price    = i as i64;
					e.size     = i as i64;
					e.data     = Data { data: i.to_string() }
				});
			}
		});
		input.join().unwrap();

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 4, 16, 36, 64, 100, 144, 196, 256, 324]);
	}
}
