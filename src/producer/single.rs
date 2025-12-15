use super::*;
use crate::{
    barrier::{Barrier, NONE},
    consumer::Consumer,
    cursor::Cursor,
    producer::ProducerBarrier,
    ringbuffer::RingBuffer,
    wait_strategies::{WaitStrategy, WakeupNotifier},
    Sequence,
};
use crossbeam_utils::CachePadded;
use std::sync::atomic::{fence, Ordering};

/// Producer for publishing to the Disruptor from a single thread.
///
/// See also [MultiProducer](crate::MultiProducer) for multi-threaded publication and
/// [`Producer`] for how to use a Producer.
pub struct SingleProducer<E, C, W>
where
    C: Barrier,
    W: WaitStrategy,
{
    shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
    ring_buffer: Arc<RingBuffer<E>>,
    producer_barrier: Arc<SingleProducerBarrier>,
    consumers: Vec<Consumer>,
    consumer_barrier: C,
    notifier: W::Notifier,
    /// Next sequence to be published.
    sequence: Sequence,
    /// Highest sequence available for publication because the Consumers are "enough" behind
    /// to not interfere.
    sequence_clear_of_consumers: Sequence,
}

impl<E, C, W> Producer<E> for SingleProducer<E, C, W>
where
    C: Barrier,
    W: WaitStrategy,
{
    #[inline]
    fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
    where
        F: FnOnce(&mut E),
    {
        self.next_sequences(1).map_err(|_| RingBufferFull)?;
        let sequence = self.apply_update(update);
        Ok(sequence)
    }

    #[inline]
    fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut E),
    {
        while self.next_sequences(1).is_err() { /* Empty. */ }
        self.apply_update(update);
    }

    #[inline]
    fn try_batch_publish<'a, F>(
        &'a mut self,
        n: usize,
        update: F,
    ) -> Result<Sequence, MissingFreeSlots>
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
        F: FnOnce(MutBatchIter<'a, E>),
    {
        while self.next_sequences(n).is_err() { /* Empty. */ }
        self.apply_updates(n, update);
    }
}

impl<E, C, W> SingleProducer<E, C, W>
where
    C: Barrier,
    W: WaitStrategy,
{
    pub(crate) fn new(
        shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
        ring_buffer: Arc<RingBuffer<E>>,
        producer_barrier: Arc<SingleProducerBarrier>,
        consumers: Vec<Consumer>,
        consumer_barrier: C,
        notifier: W::Notifier,
    ) -> Self {
        let sequence_clear_of_consumers = ring_buffer.size() - 1;
        Self {
            shutdown_at_sequence,
            ring_buffer,
            producer_barrier,
            consumers,
            consumer_barrier,
            notifier,
            sequence: 0,
            sequence_clear_of_consumers,
        }
    }

    #[inline]
    fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
        let n = n as i64;
        let n_next = self.sequence - 1 + n;

        if self.sequence_clear_of_consumers < n_next {
            // We have to check where the consumers are in case we're about to overwrite a slot
            // which is still being read.
            // (The slowest consumer is too far behind the producer to publish next n events).
            let last_published = self.sequence - 1;
            let rear_sequence_read = self.consumer_barrier.get_after(last_published);
            let free_slots = self
                .ring_buffer
                .free_slots(last_published, rear_sequence_read);
            if free_slots < n {
                return Err(MissingFreeSlots((n - free_slots) as u64));
            }
            fence(Ordering::Acquire);

            // We can now continue until we get right behind the slowest consumer's current
            // position without checking where it actually is.
            self.sequence_clear_of_consumers = last_published + free_slots;
        }

        Ok(n_next)
    }

    /// Precondition: `sequence` is available for publication.
    #[inline]
    fn apply_update<F>(&mut self, update: F) -> Sequence
    where
        F: FnOnce(&mut E),
    {
        let sequence = self.sequence;
        // SAFETY: Now, we have exclusive access to the event at `sequence` and a producer
        // can now update the data.
        let event_ptr = self.ring_buffer.get(sequence);
        let event = unsafe { &mut *event_ptr };
        update(event);
        // Publish by publishing `sequence`.
        self.producer_barrier.publish(sequence);
        self.notifier.wake();
        // Update sequence that will be published the next time.
        self.sequence += 1;
        sequence
    }

    /// Precondition: `sequence` and next `n - 1` sequences are available for publication.
    #[inline]
    fn apply_updates<'a, F>(&'a mut self, n: usize, updates: F) -> Sequence
    where
        E: 'a,
        F: FnOnce(MutBatchIter<'a, E>),
    {
        let n = n as i64;
        let lower = self.sequence;
        let upper = lower + n - 1;
        // SAFETY: Now, we have exclusive access to the events between `lower` and `upper` and
        // a producer can update the data.
        let iter = MutBatchIter::new(lower, upper, &self.ring_buffer);
        updates(iter);
        // Publish batch by publishing `upper`.
        self.producer_barrier.publish(upper);
        self.notifier.wake();
        // Update sequence that will be published the next time.
        self.sequence += n;
        upper
    }
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E, C, W> Drop for SingleProducer<E, C, W>
where
    C: Barrier,
    W: WaitStrategy,
{
    fn drop(&mut self) {
        self.shutdown_at_sequence
            .store(self.sequence, Ordering::Relaxed);
        self.consumers.iter_mut().for_each(|c| c.join());
    }
}

/// Barrier for a single producer.
pub struct SingleProducerBarrier {
    cursor: Cursor,
}

impl SingleProducerBarrier {
    pub(crate) fn new() -> Self {
        Self {
            cursor: Cursor::new(NONE),
        }
    }
}

impl Barrier for SingleProducerBarrier {
    /// Gets the `Sequence` of the last published event.
    #[inline]
    fn get_after(&self, _prev: Sequence) -> Sequence {
        self.cursor.relaxed_value()
    }
}

impl ProducerBarrier for SingleProducerBarrier {
    #[inline]
    fn publish(&self, sequence: Sequence) {
        self.cursor.store(sequence);
    }
}
