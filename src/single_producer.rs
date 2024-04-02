//! Module with a producer for single threaded publication into the Disruptor.

use std::sync::{atomic::{fence, AtomicI64, Ordering}, Arc};
use crossbeam_utils::CachePadded;

use crate::{barrier::{Barrier, NONE}, consumer::{Consumer, ConsumerBarrier}, cursor::Cursor, producer::{Producer, ProducerBarrier, ProducerImpl, RingBufferFull}, ringbuffer::RingBuffer, Sequence};

/// Producer for publishing to the Disruptor from a single thread.
///
/// See also [`crate::multi_producer::MultiProducer`] for multi-threaded publication.
pub struct SingleProducer<E, P> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 *mut RingBuffer<E>,
	producer_barrier:            Arc<P>,
	consumers:                   Vec<Consumer>,
	consumer_barrier:            ConsumerBarrier,
	/// Next sequence to be published.
	sequence:                    Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
}

unsafe impl<E: Send, P> Send for SingleProducer<E, P> {}

impl<E, P: ProducerBarrier> Producer<E, P> for SingleProducer<E, P> {}

impl<E, P: ProducerBarrier> ProducerImpl<E, P> for SingleProducer<E, P> {
	fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          *mut RingBuffer<E>,
		producer_barrier:     Arc<P>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     ConsumerBarrier,
	) -> Self {
		SingleProducer::new(
			shutdown_at_sequence,
			ring_buffer,
			producer_barrier,
			consumers,
			consumer_barrier)
	}

	#[inline]
	fn next_sequence(&mut self) -> Result<Sequence, RingBufferFull> {
		let sequence = self.sequence;

		if self.sequence_clear_of_consumers < sequence {
			// We have to check where the consumers are in case we're about to overwrite a slot
			// which is still being read.
			// (The slowest consumer is an entire ring buffer behind the producer).
			let ring_buffer                = self.ring_buffer();
			let wrap_point                 = ring_buffer.wrap_point(sequence);
			// TODO: Change interface so we don't need sequence.
			let lowest_sequence_being_read = self.consumer_barrier.get_relaxed(sequence) + 1;
			if lowest_sequence_being_read == wrap_point {
				return Err(RingBufferFull);
			}
			fence(Ordering::Acquire);

			// We can now continue until we get right behind the slowest consumer's current
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
		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let sequence    = self.sequence;
		let ring_buffer = self.ring_buffer();
		unsafe {
			let element = &mut *ring_buffer.get(sequence);
			update(element);
		}
		// Publish by publishing `sequence`.
		self.producer_barrier.publish(sequence);
		// Update sequence that will be published the next time.
		self.sequence += 1;
		Ok(sequence)
	}
}

impl<E, P: ProducerBarrier> SingleProducer<E, P> {
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          *mut RingBuffer<E>,
		producer_barrier:     Arc<P>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     ConsumerBarrier,
	) -> Self
	{
		let sequence_clear_of_consumers = unsafe { (*ring_buffer).size() - 1};
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
	fn ring_buffer(&self) -> &RingBuffer<E> {
		unsafe { &*self.ring_buffer }
	}
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E, P> Drop for SingleProducer<E, P> {
	fn drop(&mut self) {
		self.shutdown_at_sequence.store(self.sequence, Ordering::Relaxed);
		self.consumers.iter_mut().for_each(|c| { c.join(); });

		// SAFETY: Both publishers and receivers are done accessing the RingBuffer.
		unsafe {
			drop(Box::from_raw(self.ring_buffer));
		}
	}
}

/// Barrier for a single producer.
pub struct SingleProducerBarrier {
	cursor: Cursor
}

impl Barrier for SingleProducerBarrier {
	fn new(_size: usize) -> Self {
		SingleProducerBarrier {
			cursor: Cursor::new(NONE)
		}
	}

	/// Gets the `Sequence` of the last published event.
	fn get_relaxed(&self, _lower_bound: Sequence) -> Sequence {
		self.cursor.relaxed_value()
	}
}

impl ProducerBarrier for SingleProducerBarrier {
	fn next(&self) -> Sequence {
		self.cursor.next()
	}

	#[inline]
	fn publish(&self, sequence: Sequence) {
		self.cursor.store(sequence);
	}
}
