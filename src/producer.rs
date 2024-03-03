//! Module with different producer handles for publishing into the Disruptor.
//!
//! Both publishing from a single thread (fastest) and multiple threads is supported.
//!
//! A `Producer` has two methods for publication:
//! 1. `try_publish` and
//! 2. `publish`
//!
//! It is recommended to use `try_publish` and handle the [`RingBufferFull`] error as appropriate in
//! the application.
//!
//! Note, that a [`RingBufferFull`] error indicates that the consumer logic cannot keep up with the
//! data ingestion rate and that latency is increasing. Therefore, the safe route is to panic the
//! application instead of sending latent data out. (Of course appropriate action should be taken to
//! make e.g. prices indicative in a price engine or cancel all open orders in a trading
//! application before panicking.)

use std::process;
use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use crate::consumer::Consumer;
use crate::{Disruptor, Sequence};

pub(crate) trait ProducerBarrier {
	/// Publishes the sequence number that is now available for being read by consumers.
	/// (The sequence number is stored with [`Ordering::Release`] semantics.)
	fn publish(&self, sequence: Sequence);
	/// Gets the highest available sequence number with relaxed memory ordering.
	///
	/// Note, to establish proper happens-before relationships (and thus proper synchronization),
	/// the caller must issue a [`std::sync::atomic::fence`] with [`Ordering::Acquire`].
	fn get_highest_available_relaxed(&self, lower_bound: Sequence) -> Sequence;
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
	fn publish(&self, sequence: Sequence) {
		self.cursor.store(sequence, Ordering::Release);
	}

	#[inline]
	fn get_highest_available_relaxed(&self, _lower_bound: Sequence) -> Sequence {
		self.cursor.load(Ordering::Relaxed)
	}
}

/// Producer for publishing to the Disruptor from a single thread.
///
/// See also [`MultiProducer`] for multi-threaded publication.
pub struct Producer<E> {
	disruptor: *mut Disruptor<E, SingleProducerBarrier>,
	consumer: Consumer,
	/// Next sequence to be published.
	sequence: Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
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
		consumer: Consumer,
		sequence_clear_of_consumers: Sequence
	) -> Self {
		Producer {
			disruptor,
			consumer,
			sequence: 0,
			sequence_clear_of_consumers,
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
	pub fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull> where F: FnOnce(&mut E) {
		self.next_sequence()?;
		self.apply_update(update)
	}

	/// Publish an Event into the Disruptor.
	///
	/// Spins until there is an available slot in case the ring buffer is full.
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::{Builder, BusySpin, Sequence};
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
		while let Err(RingBufferFull) = self.next_sequence() { /* Empty. */ }
		self.apply_update(update).expect("Ringbuffer should not be full.");
	}

	#[inline]
	fn next_sequence(&mut self) -> Result<Sequence, RingBufferFull> {
		let disruptor = self.disruptor();
		let sequence  = self.sequence;

		if self.sequence_clear_of_consumers < sequence {
			// We have to check where the consumer is in case we're about to
			// publish into the slot currently being read by the consumer.
			// (Consumer is an entire ring buffer behind the producer).
			let wrap_point        = disruptor.wrap_point(sequence);
			let consumer_sequence = disruptor.consumer_barrier.load(Ordering::Acquire);
			if consumer_sequence == wrap_point {
				return Err(RingBufferFull);
			}

			// We can now continue until we get right behind the consumer's current
			// position without checking where it actually is.
			self.sequence_clear_of_consumers = consumer_sequence + disruptor.ring_buffer_size - 1;
		}

		Ok(sequence)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull> where F: FnOnce(&mut E) {
		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let sequence  = self.sequence;
		let disruptor = self.disruptor();
		unsafe {
			let element = &mut *disruptor.get(sequence);
			update(element);
		}
		// Publish by publishing `sequence`.
		disruptor.producer_barrier.publish(sequence);
		// Update sequence that will be published the next time.
		self.sequence += 1;
		Ok(sequence)
	}
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E> Drop for Producer<E> {
	fn drop(&mut self) {
		self.disruptor().shut_down();
		self.consumer.join();

		// SAFETY: Both publishers and receivers are done accessing the Disruptor.
		unsafe {
			drop(Box::from_raw(self.disruptor));
		}
	}
}

pub(crate) struct MultiProducerBarrier {
	cursor:      CachePadded<AtomicI64>,
	available:   Box<[AtomicI32]>,
	index_mask:  usize,
	index_shift: usize,
}

impl MultiProducerBarrier {
	pub(crate) fn new(size: usize) -> MultiProducerBarrier {
		let cursor      = CachePadded::new(AtomicI64::new(-1));
		let available   = (0..size).map(|_i| { AtomicI32::new(-1) }).collect();
		let index_mask  = size - 1;
		let index_shift = Self::log2(size);

		MultiProducerBarrier { cursor, available, index_mask, index_shift }
	}

	fn log2(i: usize) -> usize {
		std::mem::size_of::<usize>()*8 - (i.leading_zeros() as usize) - 1
	}

	#[inline]
	fn next(&self) -> Sequence {
		self.cursor.fetch_add(1, Ordering::AcqRel) + 1
	}

	#[inline]
	fn calculate_availability_index(&self, sequence: Sequence) -> usize {
		sequence as usize & self.index_mask
	}

	#[inline]
	fn calculate_availability_flag(&self, sequence: Sequence) -> i32 {
		(sequence >> self.index_shift) as i32
	}

	#[inline]
	fn get_availability(&self, sequence: Sequence) -> &AtomicI32 {
		let availability_index = self.calculate_availability_index(sequence);
		unsafe {
			self.available.get_unchecked(availability_index)
		}
	}

	#[inline]
	fn is_published(&self, sequence: Sequence) -> bool {
		let availability      = self.get_availability(sequence);
		let availability_flag = self.calculate_availability_flag(sequence);
		availability.load(Ordering::Relaxed) == availability_flag
	}
}

impl ProducerBarrier for MultiProducerBarrier {
	#[inline]
	fn publish(&self, sequence: Sequence) {
		let availability      = self.get_availability(sequence);
		let availability_flag = self.calculate_availability_flag(sequence);
		availability.store(availability_flag, Ordering::Release);
	}

	/// Returns highest available sequence number for consumption.
	/// `lower_bound - 1` must have been previously available.
	#[inline]
	fn get_highest_available_relaxed(&self, lower_bound: Sequence) -> Sequence {
		let mut highest_available = lower_bound;
		loop {
			if ! self.is_published(highest_available) {
				return highest_available - 1;
			}
			highest_available += 1;
		}
	}
}

struct SharedProducer {
	consumer: Consumer,
	counter:  AtomicI64,
}

/// Producer for publishing to the Disruptor from one of many threads.
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
/// See also [`Producer`] for single-threaded publication.
pub struct MultiProducer<E> {
	disruptor:                   *mut Disruptor<E, MultiProducerBarrier>,
	shared_producer:             *mut SharedProducer,
	/// Next sequence number for the MultiProducer to publish.
	claimed_sequence:            Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
}

/// Indicates no sequence number has been claimed (yet).
const NONE: Sequence = -1;

unsafe impl<E: Send> Send for MultiProducer<E> {}

impl<E> Clone for MultiProducer<E> {
	fn clone(&self) -> Self {
		let count = self.shared().counter.fetch_add(1, Ordering::AcqRel);

		// Cloning publishers and calling `mem::forget` on the clones could potentially overflow the
		// counter. It's very difficult to recover sensibly from such degenerate scenarios so we
		// just abort when the count becomes very large.
		if count > i64::MAX/2 {
			process::abort();
		}

		// Known to be available initially as consumers start at index 0.
		let sequence_clear_of_consumers = unsafe { (*self.disruptor).ring_buffer_size - 1 };

		MultiProducer {
			disruptor:        self.disruptor,
			shared_producer:  self.shared_producer,
			claimed_sequence: NONE,
			sequence_clear_of_consumers
		}
	}
}

impl<E> Drop for MultiProducer<E> {
	fn drop(&mut self) {
		let old_count = self.shared().counter.fetch_sub(1, Ordering::AcqRel);
		if old_count == 1 {
			self.disruptor().shut_down();

			// SAFETY: Both producers and consumers are done accessing the Disruptor and
			// the shared_producer.
			unsafe {
				(*self.shared_producer).consumer.join();

				drop(Box::from_raw(self.disruptor));
				drop(Box::from_raw(self.shared_producer));
			}
		}
	}
}

impl<E> MultiProducer<E> {
	pub(crate) fn new(
		disruptor: *mut Disruptor<E, MultiProducerBarrier>,
		consumer: Consumer) -> Self {
		let shared_producer = Box::into_raw(
			Box::new(
				SharedProducer {
					consumer,
					counter: AtomicI64::new(1)
				}
			)
		);
		// Known to be available initially as consumers start at index 0.
		let sequence_clear_of_consumers = unsafe { (*disruptor).ring_buffer_size - 1 };
		MultiProducer {
			disruptor,
			shared_producer,
			claimed_sequence: NONE,
			sequence_clear_of_consumers
		}
	}

	#[inline]
	fn disruptor(&self) -> &Disruptor<E, MultiProducerBarrier> {
		unsafe { &*self.disruptor }
	}

	#[inline]
	fn shared(&self) -> &SharedProducer {
		unsafe { & *self.shared_producer }
	}

	#[inline]
	fn claim_next_sequence(&mut self) -> Result<Sequence, RingBufferFull> {
		let disruptor = self.disruptor();
		// We get the last produced sequence number and increment it for the next publisher.
		// `sequence` is now exclusive for this producer.
		// We need to store it, because the ring buffer could be full (and the producer barrier has
		// already increased its publication counter so we *must* eventually use it for publication).
		if self.claimed_sequence == NONE {
			let next_sequence     = disruptor.producer_barrier.next();
			self.claimed_sequence = next_sequence;
		}
		let disruptor = self.disruptor();
		let sequence  = self.claimed_sequence;

		if self.sequence_clear_of_consumers < sequence {
			// We have to check where the consumer is in case we're about to
			// publish into the slot currently being read by the consumer.
			// (Consumer is an entire ring buffer behind the producer).
			let wrap_point        = disruptor.wrap_point(sequence);
			let consumer_sequence = disruptor.consumer_barrier.load(Ordering::Acquire);
			// `<=` because a producer can claim a sequence number that a consumer is still using
			// before the wrap_point. (Compare with the single-threaded Producer that cannot claim
			// a sequence number beyond the wrap_point).
			if consumer_sequence <= wrap_point {
				return Err(RingBufferFull);
			}

			// We can now continue until we get right behind the consumer's current
			// position without checking where it actually is.
			self.sequence_clear_of_consumers = consumer_sequence + disruptor.ring_buffer_size - 1;
		}

		Ok(sequence)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull> where F: FnOnce(&mut E) {
		let sequence  = self.claimed_sequence;
		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let disruptor = self.disruptor();
		unsafe {
			let element = &mut *disruptor.get(sequence);
			update(element);
		}
		// Make publication available by publishing `sequence`.
		disruptor.producer_barrier.publish(sequence);
		// sequence is now used - replace it with None.
		self.claimed_sequence = NONE;
		Ok(sequence)
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
	///# let factory = || { Event { price: 0.0 }};
	///# let processor = |e: &Event, _, _| {};
	///# let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_multi_producer();
	/// producer.try_publish(|e| { e.price = 42.0; })?;
	///# Ok(())
	///# }
	/// ```
	///
	/// See also [`Self::publish`].
	#[inline]
	pub fn try_publish<F: FnOnce(&mut E)>(&mut self, update: F) -> Result<Sequence, RingBufferFull> {
		self.claim_next_sequence()?;
		self.apply_update(update)
	}

	/// Publish an Event into the Disruptor.
	///
	/// Spins until there is an available slot in case the ring buffer is full.
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
	///# let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_multi_producer();
	/// producer.publish(|e| { e.price = 42.0; });
	/// ```
	///
	/// See also [`Self::try_publish`].
	#[inline]
	pub fn publish<F: FnOnce(&mut E)>(&mut self, update: F) {
		while let Err(RingBufferFull) = self.claim_next_sequence() { /* Empty. */ }
		self.apply_update(update).expect("Ringbuffer should not be full.");
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_log2() {
		assert_eq!(1, MultiProducerBarrier::log2(2));
		assert_eq!(1, MultiProducerBarrier::log2(3));
		assert_eq!(3, MultiProducerBarrier::log2(8));
		assert_eq!(3, MultiProducerBarrier::log2(9));
		assert_eq!(3, MultiProducerBarrier::log2(10));
		assert_eq!(3, MultiProducerBarrier::log2(11));
	}
}
