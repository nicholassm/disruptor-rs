//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # General Usage
//!
//! The Disruptor in this library can only be used once. I.e. it cannot be rewound and restarted.
//! It also owns and manages the processing thread(s) for the convenience of the library users.
//!
//! When the Disruptor is created, you choose whether publication to the Disruptor will happen from
//! one or multiple threads via **Producer** handles.
//! In any case, when the last Producer goes out of scope, all events published are processed and
//! then the processing thread(s) will be stopped and the entire Disruptor will be dropped.
//!
//! # Examples
//! ```
//! use disruptor::Builder;
//! use disruptor::BusySpin;
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Define a factory for populating the ring buffer with events.
//! let factory = || { Event { price: 0.0 }};
//!
//! // Define a closure for processing events. A thread, controlled by the disruptor, will run this
//! // processor closure each time an event is published.
//! let processor = |e: &Event, sequence: i64, end_of_batch: bool| {
//!     // Process the Event `e` published at `sequence`.
//!     // If `end_of_batch` is false, you can batch up events until it's invoked with
//!     // `end_of_batch` being true.
//! };
//!
//! // Create a Disruptor by using a `disruptor::Builder`, In this example, the ring buffer has a
//! // size 8 and uses the `BusySpin` wait strategy.
//! // Finally, the Disruptor is built by specifying that only a single thread will publish into
//! // the Disruptor (via a `Producer` handle). There's also a `create_with_multi_producer()` for
//! // publication from multiple threads.
//! let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
//! // Publish into the Disruptor.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! // At this point, the processor thread processes all published events and then stops as
//! // the Producer instance goes out of scope and the Disruptor (and the Producer) are dropped.
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub use wait_strategies::BusySpin;
pub use wait_strategies::BusySpinWithSpinLoopHint;

pub mod wait_strategies;
pub mod producer;
mod consumer;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use core_affinity::CoreId;
use crossbeam_utils::CachePadded;
use crate::consumer::Consumer;
use crate::producer::{ProducerBarrier, Producer, SingleProducerBarrier, MultiProducer, MultiProducerBarrier};
use crate::wait_strategies::WaitStrategy;

pub(crate) struct Disruptor<E, P: ProducerBarrier> {
	producer_barrier: P,
	consumer_barrier: CachePadded<AtomicI64>,
	shutting_down:    AtomicBool,
	index_mask:       i64,
	ring_buffer_size: i64,
	ring_buffer:      Box<[UnsafeCell<E>]>
}

/// Builder used for configuring and constructing a Disruptor.
pub struct Builder<E, P, W> {
	ring_buffer:           Box<[UnsafeCell<E>]>,
	consumer_barrier:      CachePadded<AtomicI64>,
	shutting_down:         AtomicBool,
	index_mask:            i64,
	ring_buffer_size:      i64,
	processor:             P,
	processor_affinity:    Option<CoreId>,
	processor_thread_name: &'static str,
	wait_strategy:         W
}

fn is_pow_of_2(num: usize) -> bool {
	num != 0 && (num & (num - 1) == 0)
}

impl<E, P, W> Builder<E, P, W> where
		E: 'static,
		P: Send + FnMut(&E, i64, bool) + 'static,
		W: WaitStrategy + 'static {

	/// Creates a Builder for a Disruptor.
	///
	/// The required parameters are:
	/// - The `size` of the ring buffer. Must be a power of 2. It's recommended to make the ring
	///   buffer as small as possible for cache coherency while big enough to cope with bursty input
	///   being published to the Disruptor at high ingestion rates.
	/// - The `event_factory` is used for populating the initial values in the ring buffer.
	/// - The `processor` closure which will be invoked on each available event `E`.
	/// - The `wait_strategy` determines what to do when there are no available events yet.
	///   (See module [`wait_strategies`] for the available options.)
	///
	/// # Panics
	///
	/// Panics if the `size` is not a power of 2.
	///
	/// # Examples
	///
	/// ```
	/// use disruptor::wait_strategies::BusySpin;
	///
	/// // The data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	///
	/// // Define a factory for populating the ring buffer with events.
	/// let factory = || { Event { price: 0.0 }};
	///
	/// // Define a closure for processing events. A thread, controlled by the disruptor, will run
	/// // this processor each time an event is published.
	/// let processor = |e: &Event, sequence: i64, end_of_batch: bool| {
	///     // Process e.
	/// };
	///
	/// // Create a Disruptor by using a `disruptor::Builder`, In this example, the ring buffer has
	/// // size 8 and the `BusySpin` wait strategy. Finally, the Disruptor is built by specifying that
	/// // only a single thread will publish into the Disruptor (via a `Producer` handle).
	/// let mut publisher = disruptor::Builder::new(8, factory, processor, BusySpin)
	///     .create_with_single_producer();
	/// ```
	pub fn new<F>(size: usize, mut event_factory: F, processor: P, wait_strategy: W) -> Builder<E, P, W> where
		F: FnMut() -> E
	{
		if !is_pow_of_2(size) { panic!("Size must be power of 2.") }

		let ring_buffer: Box<[UnsafeCell<E>]> = (0..size)
			.map(|_i| UnsafeCell::new(event_factory()) )
			.collect();
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
			processor_affinity: None,
			processor_thread_name: "processor",
			wait_strategy
		}
	}

	/// Set the core that the processor thread should be pinned to.
	/// Note, core numbering typically starts with id=0.
	///
	/// # Panics
	///
	/// Panics if the core with `id` is not available.
	pub fn pin_processor_to_core(mut self, id: usize) -> Self {
		let available: Vec<usize> = core_affinity::get_core_ids().unwrap().iter()
			.map(|core_id| core_id.id)
			.collect();

		if !available.contains(&id) {
			panic!("No core with ID={} is available.", id);
		}

		self.processor_affinity = Some(CoreId { id });
		self
	}

	/// Creates the Disruptor and returns a [`Producer<E>`] used for publishing into the Disruptor
	/// (single thread).
	///
	/// # Panics
	///
	/// If thread affinity has been set for the processor thread and it cannot be pinned by the
	/// operating system.
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

		let wrapper  = DisruptorWrapper(disruptor);
		let consumer = Consumer::new(
			wrapper,
			self.processor_thread_name,
			self.processor,
			self.processor_affinity,
			self.wait_strategy);
		Producer::new(disruptor, consumer, self.ring_buffer_size - 1)
	}

	/// Creates the Disruptor and returns a [`MultiProducer<E>`] used for publishing into the Disruptor
	/// (multiple threads).
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::Builder;
	///# use disruptor::BusySpin;
	///# use disruptor::producer::RingBufferFull;
	///# use std::thread;
	/// // The example data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	///
	/// let factory = || { Event { price: 0.0 }};
	/// let processor = |e: &Event, _, _| {};
	/// let mut producer1 = Builder::new(8, factory, processor, BusySpin).create_with_multi_producer();
	/// let mut producer2 = producer1.clone();
	/// thread::scope(|s| {
	///     s.spawn(move || {
	///         producer1.publish(|e| { e.price = 24.0; });
	///     });
	///     s.spawn(move || {
	///         producer2.publish(|e| { e.price = 42.0; });
	///     });
	/// });
	/// ```
	///
	/// # Panics
	///
	/// If thread affinity has been set for the processor thread and it cannot be pinned by the
	/// operating system.
	pub fn create_with_multi_producer(self) -> MultiProducer<E> {
		let producer_barrier = MultiProducerBarrier::new(self.ring_buffer_size as usize);
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

		let wrapper  = DisruptorWrapper(disruptor);
		let consumer = Consumer::new(
			wrapper,
			self.processor_thread_name,
			self.processor,
			self.processor_affinity,
			self.wait_strategy);
		MultiProducer::new(disruptor, consumer)
	}
}

/// Needed for providing a [`Disruptor`] reference to the Consumer thread.
struct DisruptorWrapper<T, P: ProducerBarrier> (*mut Disruptor<T, P>);

unsafe impl<E, P: ProducerBarrier> Send for DisruptorWrapper<E, P> {}

impl<E, P: ProducerBarrier> DisruptorWrapper<E, P> {
	fn unwrap(&self) -> &Disruptor<E, P> {
		unsafe { &*self.0 }
	}
}

impl<E, P: ProducerBarrier> Disruptor<E, P> {
	fn shut_down(&self) {
		self.shutting_down.store(true, Ordering::Relaxed);
	}

	#[inline]
	fn is_shutting_down(&self) -> bool {
		self.shutting_down.load(Ordering::Relaxed)
	}

	#[inline]
	fn get_highest_published(&self, lower_bound: i64) -> i64 {
		self.producer_barrier.get_highest_available(lower_bound)
	}

	#[inline]
	fn wrap_point(&self, sequence: i64) -> i64 {
		sequence - self.ring_buffer_size
	}

	#[inline]
	fn get(&self, sequence: i64) -> *mut E {
		let index = (sequence & self.index_mask) as usize;
		self.ring_buffer[index].get()
	}
}

#[cfg(test)]
mod tests {
	use std::thread;
	use crate::BusySpin;
	use std::sync::mpsc;
	use crate::wait_strategies::BusySpinWithSpinLoopHint;
	use super::*;

	#[test]
	#[should_panic(expected = "Size must be power of 2.")]
	fn test_size_not_a_factor_of_2() {
		std::panic::set_hook(Box::new(|_| {})); // To avoid backtrace in console.
		Builder::new(3, || { 0 }, |_i, _, _| {}, BusySpin);
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
		let factory   = || { Event { price: 0, size: 0, data: Data { data: "".to_owned() } }};
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.price*e.size).expect("Should be able to send.");
		};

		let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
		thread::scope(|s| {
			s.spawn(move || {
				for i in 0..10 {
					producer.publish(|e| {
						e.price = i as i64;
						e.size  = i as i64;
						e.data  = Data { data: i.to_string() }
					});
				}
			});
		});

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
	}

	#[test]
	fn test_pipeline_of_two_disruptors() {
		let factory   = || { Event { price: 0, size: 0, data: Data { data: "".to_owned() } } };
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.price*e.size).expect("Should be able to send.");
		};

		// Last Disruptor.
		let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
		let processor = move |e: &Event, _, _| {
			producer.publish(|e2| {
				e2.price = e.price*2;
				e2.size  = e.size*2;
				e2.data  = Data { data: e.data.data.clone() };
			});
		};

		// First Disruptor.
		let mut producer = Builder::new(8, factory, processor, BusySpin)
			.create_with_single_producer();
		thread::scope(|s| {
			s.spawn(move || {
				for i in 0..10 {
					producer.publish(|e| {
						e.price = i as i64;
						e.size  = i as i64;
						e.data  = Data { data: i.to_string() }
					});
				}
			});
		});

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 4, 16, 36, 64, 100, 144, 196, 256, 324]);
	}

	#[test]
	fn test_multi_publisher_disruptor() {
		let factory   = || { Event { price: 0, size: 0, data: Data { data: "".to_owned() } } };
		let num_items = 100;
		let (s, r)    = mpsc::channel();

		let processor = move |e: &Event, _, _| {
			s.send(e.price).expect("Should be able to send.");
		};

		let mut producer1 = Builder::new(8, factory, processor, BusySpinWithSpinLoopHint).create_with_multi_producer();
		let mut producer2 = producer1.clone();

		thread::scope(|s| {
			s.spawn(move || {
				for i in 0..num_items/2 {
					producer1.publish(|e| {
						e.price = i as i64;
						e.size  = i as i64;
						e.data  = Data { data: i.to_string() }
					});
				}
			});

			s.spawn(move || {
				for i in (num_items/2)..num_items {
					producer2.publish(|e| {
						e.price = i as i64;
						e.size  = i as i64;
						e.data  = Data { data: i.to_string() }
					});
				}
			});
		});

		let mut result: Vec<_> = r.iter().collect();
		result.sort();
		let expected: Vec<i64> = (0..num_items).map(|n| { n as i64 }).collect();
		assert_eq!(result, expected);
	}
}
