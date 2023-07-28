//! This module contains different producer handles for publishing into the Disruptor.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};
use crate::consumer::Receiver;
use crate::Disruptor;

pub(crate) trait ProducerBarrier {
	fn publish(&self, sequence: i64);
	fn get_highest_available(&self) -> i64;
}

pub(crate) struct SingleProducerBarrier {
	cursor: CachePadded<AtomicI64>
}

impl SingleProducerBarrier {
	pub(crate) fn new() -> SingleProducerBarrier {
		let cursor = CachePadded::new(AtomicI64::new(-1));
		SingleProducerBarrier { cursor }
	}
}

impl ProducerBarrier for SingleProducerBarrier {
	#[inline]
	fn publish(&self, sequence: i64) {
		self.cursor.store(sequence, Ordering::Release);
	}

	#[inline]
	fn get_highest_available(&self) -> i64 {
		self.cursor.load(Ordering::Acquire)
	}
}

/// Producer for publishing to the Disruptor from a single thread.
pub struct Producer<E> {
	disruptor: *mut Disruptor<E, SingleProducerBarrier>,
	receiver: Receiver,
	available_publisher_sequence: i64,
}

unsafe impl<E: Send> Send for Producer<E> {}

/// Error indicating that the ring buffer is full.
///
/// Client code can then take appropriate action, e.g. discard data or even panic as this indicates
/// that the consumers cannot keep up - i.e. latency.
#[derive(Debug)]
pub struct RingBufferFull;

impl<E> Producer<E> {
	pub(crate) fn new(
		disruptor: *mut Disruptor<E, SingleProducerBarrier>,
		receiver: Receiver,
		available_publisher_sequence: i64
	) -> Self {
		Producer {
			disruptor,
			receiver,
			available_publisher_sequence
		}
	}

	#[inline]
	fn disruptor(&self) -> &Disruptor<E, SingleProducerBarrier> {
		unsafe { &*self.disruptor }
	}

	/// Publish an Event into the Disruptor.
	///
	/// Returns a `Result` with the published sequence number or a [RingBufferFull] in case the
	/// ring buffer is full.
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::Builder;
	///# use disruptor::BusySpin;
	///# use disruptor::producer::RingBufferFull;
	///#
	/// // The example data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	///# fn main() -> Result<(), RingBufferFull> {
	/// let factory = || { Event { price: 0.0 }};
	///# let processor = |e: &Event, _, _| {};
	///# let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
	/// producer.try_publish(|e| { e.price = 42.0; })?;
	///# Ok(())
	///# }
	/// ```
	///
	/// See also [`Self::publish`].
	#[inline]
	pub fn try_publish<F>(&mut self, update: F) -> Result<i64, RingBufferFull> where F: FnOnce(&mut E)
	{
		let sequence  = self.next_sequence()?;
		self.apply_update(sequence, update);
		Ok(sequence)
	}

	/// Publish an Event into the Disruptor.
	///
	/// Blocks until there is an available slot in case the ring buffer is full.
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::Builder;
	///# use disruptor::BusySpin;
	///#
	/// // The example data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	///# let factory      = || { Event { price: 0.0 }};
	///# let processor    = |e: &Event, _, _| {};
	///# let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
	/// producer.publish(|e| { e.price = 42.0; });
	/// ```
	///
	/// See also [`Self::try_publish`].
	#[inline]
	pub fn publish<F>(&mut self, update: F) where F: FnOnce(&mut E) {
		let sequence =
			if let Ok(sequence) = self.next_sequence() { // Optimize for the common case.
				sequence
			}
			else {
				loop {
					break match self.next_sequence() {
						Ok(sequence)        => sequence,
						Err(RingBufferFull) => continue
					};
				}
			};

		self.apply_update(sequence, update);
	}

	#[inline]
	fn next_sequence(&mut self) -> Result<i64, RingBufferFull> {
		let disruptor        = self.disruptor();
		// Only one producer can publish so we can load it once.
		let last_produce_seq = disruptor.producer_barrier.cursor.load(Ordering::Relaxed);
		let sequence         = last_produce_seq + 1;

		if self.available_publisher_sequence < sequence {
			// We have to check where the consumer is in case we're about to
			// publish into the slot currently being read by the consumer.
			// (Consumer is an entire ring buffer behind the publisher).
			let wrap_point        = disruptor.wrap_point(sequence);
			let consumer_sequence = disruptor.consumer_barrier.load(Ordering::Acquire);
			if consumer_sequence == wrap_point {
				return Err(RingBufferFull);
			}

			// We can now continue until we get right behind the consumer's current
			// position without checking where it actually is.
			self.available_publisher_sequence = consumer_sequence + disruptor.ring_buffer_size - 1;
		}

		Ok(sequence)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, sequence: i64, update: F) where F: FnOnce(&mut E) {
		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let disruptor = self.disruptor(); // Re-borrow as mutated above.
		unsafe {
			let element = &mut *disruptor.get(sequence);
			update(element);
		}
		// Publish by publishing `sequence`.
		disruptor.producer_barrier.publish(sequence);
	}
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E> Drop for Producer<E> {
	fn drop(&mut self) {
		self.disruptor().shut_down();
		self.receiver.join();

		// Safety: Both publishers and receivers are done accessing the Disruptor.
		unsafe {
			drop(Box::from_raw(self.disruptor));
		}
	}
}