use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};

use crate::{
    affinity::set_affinity_if_defined,
    barrier::Barrier,
    builder::Shared,
    cursor::Cursor,
    sequence::DependentSequence,
    wait_strategies::{WaitOutcome, WaitStrategy, Waiter},
    Sequence,
};

pub mod event_poller;

#[doc(hidden)]
pub struct Consumer {
    join_handle: Option<JoinHandle<()>>,
}

impl Consumer {
    pub(crate) fn new(join_handle: JoinHandle<()>) -> Self {
        Self {
            join_handle: Some(join_handle),
        }
    }

    pub(crate) fn join(&mut self) {
        if let Some(h) = self.join_handle.take() {
            h.join().expect("Consumer should not panic.")
        }
    }
}

/// Barrier tracking a single consumer.
pub struct SingleConsumerBarrier {
    cursor: Arc<Cursor>,
}

/// Barrier tracking the minimum sequence of a group of consumers.
pub struct MultiConsumerBarrier {
    cursors: Vec<Arc<Cursor>>,
}

/// Barrier that tracks minimum of a group of consumers plus optional external gating sequences.
pub struct MultiConsumerDependentsBarrier {
    cursors: Vec<Arc<Cursor>>,
    dependent_sequences: Vec<Arc<DependentSequence>>,
}

impl SingleConsumerBarrier {
    pub(crate) fn new(cursor: Arc<Cursor>) -> Self {
        Self { cursor }
    }
}

impl Barrier for SingleConsumerBarrier {
    #[inline]
    fn get_after(&self, _lower_bound: Sequence) -> Sequence {
        self.cursor.relaxed_value()
    }
}

impl MultiConsumerBarrier {
    pub(crate) fn new(cursors: Vec<Arc<Cursor>>) -> Self {
        Self { cursors }
    }
}

impl Barrier for MultiConsumerBarrier {
    /// Gets the available `Sequence` of the slowest consumer.
    #[inline]
    fn get_after(&self, _lower_bound: Sequence) -> Sequence {
        self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
            let sequence = cursor.relaxed_value();
            std::cmp::min(sequence, min_sequence)
        })
    }
}

impl MultiConsumerDependentsBarrier {
    pub(crate) fn new(
        cursors: Vec<Arc<Cursor>>,
        dependent_sequences: Vec<Arc<DependentSequence>>,
    ) -> Self {
        Self {
            cursors,
            dependent_sequences,
        }
    }
}

impl Barrier for MultiConsumerDependentsBarrier {
    #[inline]
    fn get_after(&self, _lower_bound: Sequence) -> Sequence {
        let consumer_min = self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
            let sequence = cursor.relaxed_value();
            std::cmp::min(sequence, min_sequence)
        });
        self.dependent_sequences
            .iter()
            .fold(consumer_min, |min_sequence, seq| {
                let sequence = seq.get();
                std::cmp::min(sequence, min_sequence)
            })
    }
}

pub(crate) fn start_processor<E, EP, W, B>(
    mut event_handler: EP,
    builder: &mut Shared<E, W>,
    barrier: Arc<B>,
) -> (Arc<Cursor>, Consumer)
where
    E: 'static + Send + Sync,
    EP: 'static + Send + FnMut(&E, Sequence, bool),
    W: 'static + WaitStrategy,
    B: 'static + Barrier + Send + Sync,
{
    let consumer_cursor = Arc::new(Cursor::new(-1)); // Initially, the consumer has not read slot 0 yet.
    let wait_strategy = builder.wait_strategy.clone();
    let ring_buffer = Arc::clone(&builder.ring_buffer);
    let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
    let thread_name = builder.thread_context.name();
    let affinity = builder.thread_context.affinity();
    let thread_builder = thread::Builder::new().name(thread_name.clone());
    let join_handle = {
        let consumer_cursor = Arc::clone(&consumer_cursor);
        thread_builder
            .spawn(move || {
                set_affinity_if_defined(affinity, thread_name.as_str());
                let mut waiter = wait_strategy.new_waiter();
                let mut sequence = 0;
                loop {
                    let available = match waiter.wait_for(
                        sequence,
                        barrier.as_ref(),
                        shutdown_at_sequence.as_ref(),
                    ) {
                        WaitOutcome::Available { upper } => upper,
                        WaitOutcome::Shutdown => break,
                        WaitOutcome::Timeout => continue,
                    };
                    while available >= sequence {
                        // Potential batch processing.
                        let end_of_batch = available == sequence;
                        // SAFETY: Now, we have (shared) read access to the event at `sequence`.
                        let event_ptr = ring_buffer.get(sequence);
                        let event = unsafe { &*event_ptr };
                        event_handler(event, sequence, end_of_batch);
                        // Update next sequence to read.
                        sequence += 1;
                    }
                    // Signal to producers or later consumers that we're done processing `sequence - 1`.
                    consumer_cursor.store(sequence - 1);
                }
            })
            .expect("Should spawn thread.")
    };

    let consumer = Consumer::new(join_handle);
    (consumer_cursor, consumer)
}

pub(crate) fn start_processor_with_state<E, EP, W, B, S, IS>(
    mut event_handler: EP,
    builder: &mut Shared<E, W>,
    barrier: Arc<B>,
    initialize_state: IS,
) -> (Arc<Cursor>, Consumer)
where
    E: 'static + Send + Sync,
    IS: 'static + Send + FnOnce() -> S,
    EP: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
    W: 'static + WaitStrategy,
    B: 'static + Barrier + Send + Sync,
{
    let consumer_cursor = Arc::new(Cursor::new(-1)); // Initially, the consumer has not read slot 0 yet.
    let wait_strategy = builder.wait_strategy.clone();
    let ring_buffer = Arc::clone(&builder.ring_buffer);
    let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
    let thread_name = builder.thread_context.name();
    let affinity = builder.thread_context.affinity();
    let thread_builder = thread::Builder::new().name(thread_name.clone());
    let join_handle = {
        let consumer_cursor = Arc::clone(&consumer_cursor);
        thread_builder
            .spawn(move || {
                set_affinity_if_defined(affinity, thread_name.as_str());
                let mut waiter = wait_strategy.new_waiter();
                let mut sequence = 0;
                let mut state = initialize_state();
                loop {
                    let available_sequence = match waiter.wait_for(
                        sequence,
                        barrier.as_ref(),
                        shutdown_at_sequence.as_ref(),
                    ) {
                        WaitOutcome::Available { upper } => upper,
                        WaitOutcome::Shutdown => break,
                        WaitOutcome::Timeout => continue,
                    };
                    while available_sequence >= sequence {
                        // Potential batch processing.
                        let end_of_batch = available_sequence == sequence;
                        // SAFETY: Now, we have (shared) read access to the event at `sequence`.
                        let event_ptr = ring_buffer.get(sequence);
                        let event = unsafe { &*event_ptr };
                        event_handler(&mut state, event, sequence, end_of_batch);
                        // Update next sequence to read.
                        sequence += 1;
                    }
                    // Signal to producers or later consumers that we're done processing `sequence - 1`.
                    consumer_cursor.store(sequence - 1);
                }
            })
            .expect("Should spawn thread.")
    };

    let consumer = Consumer::new(join_handle);
    (consumer_cursor, consumer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_consumer_gating_barrier_respects_external_gate() {
        let c1 = Arc::new(Cursor::new(-1));
        let c2 = Arc::new(Cursor::new(-1));
        c1.store(10);
        c2.store(12);

        let gating_low = Arc::new(DependentSequence::with_value(5));
        let gating_high = Arc::new(DependentSequence::with_value(20));

        let barrier =
            MultiConsumerDependentsBarrier::new(vec![c1.clone(), c2.clone()], vec![gating_low]);
        assert_eq!(5, barrier.get_after(0));

        let barrier = MultiConsumerDependentsBarrier::new(vec![c1, c2], vec![gating_high]);
        assert_eq!(10, barrier.get_after(0));
    }

    #[test]
    fn dependents_barrier_clamps_when_dependent_moves_backwards() {
        let c1 = Arc::new(Cursor::new(-1));
        let c2 = Arc::new(Cursor::new(-1));
        c1.store(8);
        c2.store(9);

        let dep = Arc::new(DependentSequence::with_value(7));
        let barrier = MultiConsumerDependentsBarrier::new(vec![c1.clone(), c2.clone()], vec![dep.clone()]);
        assert_eq!(7, barrier.get_after(0));

        // Move the dependent sequence backwards; barrier should clamp to the new minimum.
        dep.set(3);
        assert_eq!(3, barrier.get_after(0));
    }
}
