use std::{process, sync::{atomic::{fence, AtomicI32, AtomicI64, Ordering}, Arc, Mutex}};
use crossbeam_utils::CachePadded;
use crate::{barrier::{Barrier, NONE}, consumer::Consumer, producer::ProducerBarrier, ringbuffer::RingBuffer, producer::{Producer, RingBufferFull}, Sequence};
use crate::cursor::Cursor;

use super::{MissingFreeSlots, MutBatchIter};

struct SharedProducer {
	consumers: Vec<Consumer>,
	counter:   AtomicI64,
}

/// Producer for publishing to the Disruptor from multiple threads.
///
/// See also [SingleProducer](crate::SingleProducer) for single-threaded publication and
/// [`Producer`] for how to use a Producer.
pub struct MultiProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 Arc<RingBuffer<E>>,
	shared_producer:             Arc<Mutex<SharedProducer>>,
	producer_barrier:            Arc<MultiProducerBarrier>,
	consumer_barrier:            Arc<C>,
	/// Next sequence number for this MultiProducer to publish.
	claimed_sequence:            Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
}

impl<E, C> Producer<E> for MultiProducer<E, C>
where
	C: Barrier
{
	#[inline]
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
	where
		F: FnOnce(&mut E)
	{
		self.next_sequences(1).map_err(|_| RingBufferFull)?;
		let sequence = self.apply_update(update);
		Ok(sequence)
	}

	#[inline]
	fn publish<F>(&mut self, update: F)
	where
		F: FnOnce(&mut E)
	{
		while self.next_sequences(1).is_err() { /* Empty. */ }
		self.apply_update(update);
	}

	#[inline]
	fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F) -> Result<Sequence, MissingFreeSlots>
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>),
	{
		self.next_sequences(n)?;
		let sequence = self.apply_updates(n, update);
		Ok(sequence)
	}

	#[inline]
	fn batch_publish<'a, F>(&'a mut self, n: usize, update: F)
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
	{
		while self.next_sequences(n).is_err() { /* Empty. */ }
		self.apply_updates(n, update);
	}
}

impl<E, C> Clone for MultiProducer<E, C> {
	fn clone(&self) -> Self {
		let shared = self.shared_producer.lock().unwrap();
		let count  = shared.counter.fetch_add(1, Ordering::AcqRel);

		// Cloning publishers and calling `mem::forget` on the clones could potentially overflow the
		// counter. It's very difficult to recover sensibly from such degenerate scenarios so we
		// just abort when the count becomes very large.
		if count > i64::MAX/2 {
			process::abort();
		}

		let shutdown_at_sequence = Arc::clone(&self.shutdown_at_sequence);
		let producer_barrier     = Arc::clone(&self.producer_barrier);
		let shared_producer      = Arc::clone(&self.shared_producer);
		let consumer_barrier     = Arc::clone(&self.consumer_barrier);
		let ring_buffer          = Arc::clone(&self.ring_buffer);

		MultiProducer {
			shutdown_at_sequence,
			ring_buffer,
			shared_producer,
			producer_barrier,
			consumer_barrier,
			claimed_sequence: NONE,
			sequence_clear_of_consumers: 0
		}
	}
}

impl<E, C> Drop for MultiProducer<E, C> {
	fn drop(&mut self) {
		let mut shared = self.shared_producer.lock().unwrap();
		let old_count  = shared.counter.fetch_sub(1, Ordering::AcqRel);
		if old_count == 1 {
			// All consumers are waiting to read the next sequence.
			let sequence = self.producer_barrier.current() + 1;
			self.shutdown_at_sequence.store(sequence, Ordering::Relaxed);
			shared.consumers.iter_mut().for_each(|c| { c.join(); });
		}
	}
}

impl<E, C> MultiProducer<E, C>
where
	C: Barrier
{
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          Arc<RingBuffer<E>>,
		producer_barrier:     Arc<MultiProducerBarrier>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     C,
	) -> Self
	{
		let shared_producer = Arc::new(
			Mutex::new(
				SharedProducer {
					consumers,
					counter: AtomicI64::new(1)
				}
			)
		);
		let consumer_barrier            = Arc::new(consumer_barrier);
		// Known to be available initially as consumers start at index 0.
		let sequence_clear_of_consumers = ring_buffer.size() - 1;
		MultiProducer {
			shutdown_at_sequence,
			ring_buffer,
			shared_producer,
			producer_barrier,
			consumer_barrier,
			claimed_sequence: NONE,
			sequence_clear_of_consumers
		}
	}

	#[inline]
	fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
		let n           = n as i64;
		// We get the last produced sequence number and try and increment it.
		let mut current = self.producer_barrier.current();
		let mut n_next  = current + n;

		loop {
			if self.sequence_clear_of_consumers < n_next {
				// We have to check where the rear consumer is in case we're about to
				// publish into the slot currently being read by the slowest consumer.
				// (Consumer is too far behind the producer to publish next n events).
				let rear_sequence_read = self.consumer_barrier.get_after(current);
				let free_slots         = self.ring_buffer.free_slots(current, rear_sequence_read);
				if free_slots < n {
					return Err(MissingFreeSlots((n - free_slots) as u64));
				}
				fence(Ordering::Acquire);

				// We now know how far we can continue until we get right behind the slowest consumers'
				// current position without checking where they actually are.
				self.sequence_clear_of_consumers = current + free_slots;
			}

			match self.producer_barrier.compare_exchange(current, n_next) {
				Ok(_) => {
					// The sequence interval `]current; n_next] is now exclusive for this producer.
					self.claimed_sequence = n_next;
					break;
				}
				Err(new_current) => {
					current = new_current;
					n_next  = current + n;
				}
			}
		}

		Ok(n_next)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Sequence
	where
		F: FnOnce(&mut E)
	{
		let sequence  = self.claimed_sequence;
		// SAFETY: Now, we have exclusive access to the event at `sequence` and a producer
		// can now update the data.
		let event_ptr = self.ring_buffer.get(sequence);
		let event     = unsafe { &mut *event_ptr };
		update(event);
		// Make publication available by publishing `sequence`.
		self.producer_barrier.publish(sequence);
		sequence
	}

	/// Precondition: `sequence` and previous `n - 1` sequences are available for publication.
	#[inline]
	fn apply_updates<'a, F>(&'a mut self, n: usize, updates: F) -> Sequence
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
	{
		let n      = n as i64;
		let upper  = self.claimed_sequence;
		let lower  = upper - n + 1;
		// SAFETY: Now, we have exclusive access to the event at `sequence` and a producer
		// can now update the data.
		let iter   = MutBatchIter::new(lower, upper, &self.ring_buffer);
		updates(iter);
		// Make publications available by publishing all the sequences in the interval [lower; upper].
		for sequence in lower..=upper {
			self.producer_barrier.publish(sequence);
		}
		upper
	}
}

/// Barrier for multiple producers.
#[doc(hidden)]
pub struct MultiProducerBarrier {
	cursor:      Cursor,
	/// For each slot, an `AtomicI32` tracks its availability.
	/// The value encodes what "round" the event is available in to avoid producers having to
	/// coordinate directly (with the added overhead).
	/// Note, producers can never "overtake" each other and overwrite the availability of an event
	/// in the "previous" round as all producers must wait for the consumer furtherst behind (which
	/// is again blocked by the slowest producer).
	available:   Box<[CachePadded<AtomicI32>]>,
	index_mask:  usize,
	index_shift: usize,
}

impl MultiProducerBarrier {
	pub(crate) fn new(size: usize) -> Self {
		let cursor      = Cursor::new(-1);
		let available   = (0..size).map(|_i| { CachePadded::new(AtomicI32::new(-1)) }).collect();
		let index_mask  = size - 1;
		let index_shift = Self::log2(size);

		Self { cursor, available, index_mask, index_shift }
	}

	fn log2(i: usize) -> usize {
		std::mem::size_of::<usize>()*8 - (i.leading_zeros() as usize) - 1
	}

	#[inline]
	fn current(&self) -> Sequence {
		self.cursor.relaxed_value()
	}

	#[inline]
	fn compare_exchange(&self, current: Sequence, next: Sequence) -> Result<i64, i64> {
		self.cursor.compare_exchange(current, next)
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

impl Barrier for MultiProducerBarrier {
	#[inline]
	fn get_after(&self, lower_bound: Sequence) -> Sequence {
		let mut highest_available = lower_bound;
		loop {
			if ! self.is_published(highest_available) {
				return highest_available - 1;
			}
			highest_available += 1;
		}
	}
}

impl ProducerBarrier for MultiProducerBarrier {
	#[inline]
	fn publish(&self, sequence: Sequence) {
		let availability      = self.get_availability(sequence);
		let availability_flag = self.calculate_availability_flag(sequence);
		availability.store(availability_flag, Ordering::Release);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn log2() {
		assert_eq!(1, MultiProducerBarrier::log2(2));
		assert_eq!(1, MultiProducerBarrier::log2(3));
		assert_eq!(3, MultiProducerBarrier::log2(8));
		assert_eq!(3, MultiProducerBarrier::log2(9));
		assert_eq!(3, MultiProducerBarrier::log2(10));
		assert_eq!(3, MultiProducerBarrier::log2(11));
	}

	#[test]
	fn publication_of_single_event_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 1);
		// Verify published:
		assert_eq!(barrier.get_after(0), 0);
	}

	#[test]
	fn publication_of_range_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 10);
		// Verify published:
		assert_eq!(barrier.get_after(0), 9);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 50);
		// Verify published:
		assert_eq!(barrier.get_after(0), 49);

		barrier.publish_range_relaxed(50, 50);
		// Verify published:
		assert_eq!(barrier.get_after(49), 99);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_barrier() {
		let barrier = MultiProducerBarrier::new(128);

		barrier.publish_range_relaxed(0, 100);
		// Verify published:
		assert_eq!(barrier.get_after(0), 99);
		// Verify not published:

		barrier.publish_range_relaxed(100, 100);
		// Verify published:
		assert_eq!(barrier.get_after(99), 199);
	}
}
