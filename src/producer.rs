//! Module with different producer handles for publishing into the Disruptor.
use thiserror::Error;

pub mod multi;
pub mod single;

use crate::{barrier::Barrier, ringbuffer::RingBuffer, Sequence};
use std::sync::{atomic::AtomicI64, Arc};

/// Barrier for producers.
#[doc(hidden)]
pub trait ProducerBarrier: Barrier {
    /// Publishes the sequence number that is now available for being read by consumers.
    /// (The sequence number is stored with [`std::sync::atomic::Ordering::Release`] semantics.)
    fn publish(&self, sequence: Sequence);
}

/// Error indicating that the ring buffer is full.
///
/// Client code can then take appropriate action, e.g. discard data or even panic as this indicates
/// that the consumers cannot keep up - i.e. latency.
#[derive(Debug, Error, PartialEq)]
#[error("Ring Buffer is full.")]
pub struct RingBufferFull;

/// The Ring Buffer was missing a number of free slots for doing the batch publication.
#[derive(Debug, Error, PartialEq)]
#[error("Missing free slots in Ring Buffer: {0}")]
pub struct MissingFreeSlots(pub u64);

/// Iterator for events that can be batch updated.
pub struct MutBatchIter<'a, E> {
    ring_buffer: &'a RingBuffer<E>,
    current: Sequence, // Inclusive.
    last: Sequence,    // Inclusive.
}

impl<'a, E> MutBatchIter<'a, E> {
    fn new(start: Sequence, end: Sequence, ring_buffer: &'a RingBuffer<E>) -> Self {
        Self {
            ring_buffer,
            current: start,
            last: end,
        }
    }

    fn remaining(&self) -> usize {
        (self.last - self.current + 1) as usize
    }
}

impl<'a, E> Iterator for MutBatchIter<'a, E> {
    type Item = &'a mut E;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current > self.last {
            None
        } else {
            let event_ptr = self.ring_buffer.get(self.current);
            // Safety: Iterator has exclusive access to event.
            let event = unsafe { &mut *event_ptr };
            self.current += 1;
            Some(event)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.remaining()
    }
}

/// Producer used for publishing into the Disruptor.
///
/// A `Producer` has two pairs of methods for publication:
/// 1. **Single publication:** [try_publish](Self::try_publish) and [publish](Self::publish),
/// 2. **Batch publication:** [try_batch_publish](Self::try_batch_publish) and [batch_publish](Self::batch_publish).
///
/// It is recommended to use the `try_` variants and handle the error as appropriate in the application.
///
/// There are two different error types: [try_publish](Self::try_publish) can return a [`RingBufferFull`] error and
/// [try_batch_publish](Self::try_batch_publish) can return a [`MissingFreeSlots`] error with the number of missing
/// slots for publication of a batch.
///
/// Note, that a [`RingBufferFull`] error indicates that the consumer logic cannot keep up with the
/// data ingestion rate and that latency is increasing. Therefore, the safe route is to panic the
/// application instead of sending latent data out. (Of course appropriate action should be taken to
/// make e.g. prices indicative in a price engine or cancel all open orders in a trading
/// application before panicking.)
/// This could also be the case for a [`MissingFreeSlots`] error but not necessarily. That depends on
/// the application.
///
/// Note also, that there is a (very high) limit to how many events can be published into the Disruptor: 2^63 - 1.
/// It is the client code's responsibility to ensure this limit it never breached as it will result in undefined
/// behaviour. (There is no guard in the library against breaching the limit due to performance considerations.)
/// The limit can be avoided by e.g. dropping the Disruptor and creating a new if you have a use case where you can
/// actually reach the limit in practice.
pub trait Producer<E> {
    /// Publish an Event into the Disruptor.
    /// Returns a `Result` with the published sequence number or a [RingBufferFull] in case the
    /// ring buffer is full.
    ///
    /// # Examples
    ///
    /// ```
    ///# use disruptor::*;
    ///#
    /// // The example data entity on the ring buffer.
    /// struct Event {
    ///     price: f64
    /// }
    ///# fn main() -> Result<(), RingBufferFull> {
    /// let factory = || { Event { price: 0.0 }};
    ///# let processor = |e: &Event, _, _| {};
    ///# let mut producer = build_single_producer(8, factory, BusySpin)
    ///#     .handle_events_with(processor)
    ///#     .build();
    /// producer.try_publish(|e| { e.price = 42.0; })?;
    ///# Ok(())
    ///# }
    /// ```
    ///
    /// See also [`Self::publish`] and [`Self::try_batch_publish`].
    fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
    where
        F: FnOnce(&mut E);

    /// Publish a batch of Events into the Disruptor.
    /// Returns a `Result` with the upper published sequence number or a [MissingFreeSlots] in case the
    /// ring buffer did not have enough available slots for the batch.
    ///
    /// Note, publishing a batch of zero elements is a no-op (only wasting cycles).
    ///
    /// # Examples
    ///
    /// ```
    ///# use disruptor::*;
    ///#
    /// // The example data entity on the ring buffer.
    /// struct Event {
    ///     price: f64
    /// }
    ///# fn main() -> Result<(), MissingFreeSlots> {
    /// let factory = || { Event { price: 0.0 }};
    ///# let processor = |e: &Event, _, _| {};
    ///# let mut producer = build_single_producer(8, factory, BusySpin)
    ///#     .handle_events_with(processor)
    ///#     .build();
    /// producer.try_batch_publish(3, |iter| {
    ///     for e in iter { // `iter` is guaranteed to yield 3 events.
    ///         e.price = 42.0;
    ///     }
    /// })?;
    ///# Ok(())
    ///# }
    /// ```
    ///
    /// See also [`Self::batch_publish`] and [`Self::try_publish`].
    fn try_batch_publish<'a, F>(
        &'a mut self,
        n: usize,
        update: F,
    ) -> Result<Sequence, MissingFreeSlots>
    where
        E: 'a,
        F: FnOnce(MutBatchIter<'a, E>);

    /// Publish an Event into the Disruptor.
    ///
    /// Spins until there is an available slot in case the ring buffer is full.
    ///
    /// # Examples
    ///
    /// ```
    ///# use disruptor::*;
    ///#
    /// // The example data entity on the ring buffer.
    /// struct Event {
    ///     price: f64
    /// }
    /// let factory = || { Event { price: 0.0 }};
    ///# let processor = |e: &Event, _, _| {};
    ///# let mut producer = build_single_producer(8, factory, BusySpin)
    ///#     .handle_events_with(processor)
    ///#     .build();
    /// producer.publish(|e| { e.price = 42.0; });
    /// ```
    ///
    /// See also [`Self::try_publish`] and [`Self::batch_publish`].
    fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut E);

    /// Publish a batch of Events into the Disruptor.
    ///
    /// Spins until there are enough available slots in the ring buffer.
    ///
    /// Note, publishing a batch of zero elements is a no-op (only wasting cycles).
    ///
    /// # Examples
    ///
    /// ```
    ///# use disruptor::*;
    ///#
    /// // The example data entity on the ring buffer.
    /// struct Event {
    ///     price: f64
    /// }
    /// let factory = || { Event { price: 0.0 }};
    ///# let processor = |e: &Event, _, _| {};
    ///# let mut producer = build_single_producer(8, factory, BusySpin)
    ///#     .handle_events_with(processor)
    ///#     .build();
    /// producer.batch_publish(3, |iter| {
    ///     for e in iter { // `iter` is guaranteed to yield 3 events.
    ///         e.price = 42.0;
    ///     }
    /// })
    /// ```
    ///
    /// See also [`Self::try_batch_publish`] and [`Self::publish`].
    fn batch_publish<'a, F>(&'a mut self, n: usize, update: F)
    where
        E: 'a,
        F: FnOnce(MutBatchIter<'a, E>);
}
