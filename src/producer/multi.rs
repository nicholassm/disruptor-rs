use std::{process, sync::{atomic::{fence, AtomicI32, AtomicI64, Ordering}, Arc, Mutex}};
use crossbeam_utils::CachePadded;
use crate::{barrier::{Barrier, NONE}, consumer::Consumer, producer::ProducerBarrier, ringbuffer::RingBuffer, producer::{Producer, RingBufferFull}, Sequence};
use crate::cursor::Cursor;

struct SharedProducer {
	consumers: Vec<Consumer>,
	counter:   AtomicI64,
}

/// Producer for publishing to the Disruptor from multiple threads.
///
/// See also [SingleProducer](crate::single_producer::SingleProducer) for single-threaded publication and
/// [`Producer`] for how to use a Producer.
pub struct MultiProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 *mut RingBuffer<E>,
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
		self.next_sequence()?;
		self.apply_update(update)
	}

	#[inline]
	fn publish<F>(&mut self, update: F)
	where
		F: FnOnce(&mut E)
	{
		while let Err(RingBufferFull) = self.next_sequence() { /* Empty. */ }
		self.apply_update(update).expect("Ringbuffer should not be full.");
	}
}

unsafe impl<E: Send, C> Send for MultiProducer<E, C> {}

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

		MultiProducer {
			shutdown_at_sequence,
			ring_buffer: self.ring_buffer,
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
			// Next is the sequence that all consumers are waiting to read.
			let sequence = self.producer_barrier.next();
			self.shutdown_at_sequence.store(sequence, Ordering::Relaxed);
			shared.consumers.iter_mut().for_each(|c| { c.join(); });

			// SAFETY: Both producers and consumers are done accessing the RingBuffer.
			unsafe {
				drop(Box::from_raw(self.ring_buffer));
			}
		}
	}
}

impl<E, C> MultiProducer<E, C>
where
	C: Barrier
{
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          *mut RingBuffer<E>,
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
		let consumer_barrier = Arc::new(consumer_barrier);
		// Known to be available initially as consumers start at index 0.
		let sequence_clear_of_consumers = unsafe { (*ring_buffer).size() - 1 };
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
	fn next_sequence(&mut self) -> Result<Sequence, RingBufferFull> {
		// We get the last produced sequence number and increment it for the next publisher.
		// `sequence` is now exclusive for this producer.
		// We need to store it, because the ring buffer could be full (and the producer barrier has
		// already increased its publication counter so we *must* eventually use it for publication).
		if self.claimed_sequence == NONE {
			let next_sequence     = self.producer_barrier.next();
			self.claimed_sequence = next_sequence;
		}

		let sequence = self.claimed_sequence;
		if self.sequence_clear_of_consumers < sequence {
			let ring_buffer = self.ring_buffer();
			// We have to check where the consumer is in case we're about to
			// publish into the slot currently being read by the consumer.
			// (Consumer is an entire ring buffer behind the producer).
			let wrap_point                 = ring_buffer.wrap_point(sequence);
			let lowest_sequence_being_read = self.consumer_barrier.get_after(sequence) + 1;
			// `<=` because a producer can claim a sequence number that a consumer is still using
			// before the wrap_point. (Compare with the single-threaded Producer that cannot claim
			// a sequence number beyond the wrap_point).
			if lowest_sequence_being_read <= wrap_point {
				return Err(RingBufferFull);
			}
			fence(Ordering::Acquire);

			// We can now continue until we get right behind the consumer's current
			// position without checking where it actually is.
			self.sequence_clear_of_consumers = lowest_sequence_being_read + ring_buffer.size() - 1;
		}

		Ok(sequence)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
	where
		F: FnOnce(&mut E)
	{
		let sequence  = self.claimed_sequence;
		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let ring_buffer = self.ring_buffer();
		unsafe {
			let element = &mut *ring_buffer.get(sequence);
			update(element);
		}
		// Make publication available by publishing `sequence`.
		self.producer_barrier.publish(sequence);
		// sequence is now used - replace it with None.
		self.claimed_sequence = NONE;
		Ok(sequence)
	}

	#[inline]
	fn ring_buffer(&self) -> &RingBuffer<E> {
		unsafe { &*self.ring_buffer }
	}
}

/// Barrier for multiple producers.
#[doc(hidden)]
pub struct MultiProducerBarrier {
	cursor:      Cursor,
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
	fn next(&self) -> Sequence {
		self.cursor.next()
	}

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
	fn test_log2() {
		assert_eq!(1, MultiProducerBarrier::log2(2));
		assert_eq!(1, MultiProducerBarrier::log2(3));
		assert_eq!(3, MultiProducerBarrier::log2(8));
		assert_eq!(3, MultiProducerBarrier::log2(9));
		assert_eq!(3, MultiProducerBarrier::log2(10));
		assert_eq!(3, MultiProducerBarrier::log2(11));
	}
}
