use std::sync::atomic::{fence, Ordering};
use crate::{barrier::{Barrier, NONE}, cursor::Cursor, producer::ProducerBarrier};
use crossbeam_utils::CachePadded;
use crate::{consumer::Consumer, ringbuffer::RingBuffer, Sequence};
use super::*;

/// Producer for publishing to the Disruptor from a single thread.
///
/// See also [MultiProducer](crate::MultiProducer) for multi-threaded publication and
/// [`Producer`] for how to use a Producer.
pub struct SingleProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 Arc<RingBuffer<E>>,
	producer_barrier:            Arc<SingleProducerBarrier>,
	consumers:                   Vec<Consumer>,
	consumer_barrier:            C,
	/// Next sequence to be published.
	sequence:                    Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
}

impl<E, C> Producer<E> for SingleProducer<E, C>
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

impl<E, C> SingleProducer<E, C>
where
	C: Barrier
{
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          Arc<RingBuffer<E>>,
		producer_barrier:     Arc<SingleProducerBarrier>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     C,
	) -> Self
	{
		let sequence_clear_of_consumers = ring_buffer.size() - 1;
		Self {
			shutdown_at_sequence,
			ring_buffer,
			producer_barrier,
			consumers,
			consumer_barrier,
			sequence: 0,
			sequence_clear_of_consumers,
		}
	}

	#[inline]
	fn next_sequence(&mut self) -> Result<Sequence, RingBufferFull> {
		let sequence = self.sequence;

		if self.sequence_clear_of_consumers < sequence {
			// We have to check where the consumers are in case we're about to overwrite a slot
			// which is still being read.
			// (The slowest consumer is an entire ring buffer behind the producer).
			let wrap_point                 = self.ring_buffer.wrap_point(sequence);
			let lowest_sequence_being_read = self.consumer_barrier.get_after(sequence) + 1;
			if lowest_sequence_being_read == wrap_point {
				return Err(RingBufferFull);
			}
			fence(Ordering::Acquire);

			// We can now continue until we get right behind the slowest consumer's current
			// position without checking where it actually is.
			self.sequence_clear_of_consumers = lowest_sequence_being_read + self.ring_buffer.size() - 1;
		}

		Ok(sequence)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
	where
		F: FnOnce(&mut E)
	{
		let sequence  = self.sequence;
		// SAFETY: Now, we have exclusive access to the event at `sequence` and a producer
		// can now update the data.
		let event_ptr = self.ring_buffer.get(sequence);
		let event     = unsafe { &mut *event_ptr };
		update(event);
		// Publish by publishing `sequence`.
		self.producer_barrier.publish(sequence);
		// Update sequence that will be published the next time.
		self.sequence += 1;
		Ok(sequence)
	}
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E, C> Drop for SingleProducer<E, C> {
	fn drop(&mut self) {
		self.shutdown_at_sequence.store(self.sequence, Ordering::Relaxed);
		self.consumers.iter_mut().for_each(|c| { c.join(); });
	}
}

/// Barrier for a single producer.
#[doc(hidden)]
pub struct SingleProducerBarrier {
	cursor: Cursor
}

impl SingleProducerBarrier {
	pub(crate) fn new() -> Self {
		Self {
			cursor: Cursor::new(NONE)
		}
	}
}

impl Barrier for SingleProducerBarrier {
	/// Gets the `Sequence` of the last published event.
	#[inline]
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursor.relaxed_value()
	}
}

impl ProducerBarrier for SingleProducerBarrier {
	#[inline]
	fn next(&self) -> Sequence {
		self.cursor.next()
	}

	#[inline]
	fn publish(&self, sequence: Sequence) {
		self.cursor.store(sequence);
	}
}
