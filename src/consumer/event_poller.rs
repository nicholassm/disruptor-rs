use crate::{barrier::Barrier, cursor::Cursor, ringbuffer::RingBuffer, Sequence};
use crossbeam_utils::CachePadded;
use std::sync::{
    atomic::{fence, AtomicI64, Ordering},
    Arc,
};
use thiserror::Error;

/// Represents an EventPoller that can be used to poll events from the ring buffer.
/// Use the EventPoller when you want to control your own thread
/// (instead of letting the Disruptor manage it).
/// The EventPoller supports batch reading and can be used to process events in a loop.
///
/// ```
///# use disruptor::*;
///#
///# #[derive(Debug)]
///# struct Event {
///#     price: f64
///# }
///# let factory = || { Event { price: 0.0 }};
///# let builder = build_single_producer(8, factory, BusySpin);
///# let (mut event_poller, builder) = builder.event_poller();
///# let mut producer = builder.build();
///# producer.publish(|e| { e.price = 42.0; });
///# drop(producer);
/// loop {
///     match event_poller.poll() {
///         Ok(mut events) => {
///             // Batch process events if efficient in your use case.
///             let batch_size = (&mut events).len();
///             // Read events with an iterator.
///             for event in &mut events {
///                 println!("Processing event: {:?}", event);
///             }
///         },// At this point the EventGuard (here named `events`) is dropped,
///           // signaling the Disruptor that the events have been processed.
///         Err(Polling::NoEvents) => { /* Do other work or try again. */ },
///         Err(Polling::Shutdown) => { break; }, // Exit the loop if the Disruptor is shut down.
///     }
/// }
/// ```
pub struct EventPoller<E, B> {
    ring_buffer: Arc<RingBuffer<E>>,
    dependent_barrier: Arc<B>,
    shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
    cursor: Arc<Cursor>,
}

/// Error types that can occur if polling is unsuccessful.
#[derive(Debug, Error, PartialEq)]
pub enum Polling {
    /// Indicates that there are no events available to process.
    #[error("No available events.")]
    NoEvents,
    /// Indicates that the Disruptor has been shut down.
    #[error("Disruptor is shut down.")]
    Shutdown,
}

/// Guards the available events that can be processed when using the EventPoller API.
/// It can be used as an iterator to read the published events and the dropping of the `EventGuard`
/// will signal to the `Disruptor` that the reading is completed. This will allow other consumers or
/// producers to advance.
pub struct EventGuard<'p, E, B> {
    parent: &'p mut EventPoller<E, B>,
    sequence: Sequence,
    available: Sequence,
}

/// The Iterator is implemented for the `&mut EventGuard` to bind the returned
/// events' lifetime to the lifetime (`'g`) of the `EventGuard`.
/// (And not the lifetime of the `EventPoller`. The latter would be catastrophic because a client
/// could hold on to a reference to a en Event after the drop method was run for the
/// `EventGuard` and a publisher could write to the immutable reference - UB.)
impl<'g, E, B> Iterator for &'g mut EventGuard<'_, E, B> {
    type Item = &'g E;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sequence > self.available {
            return None;
        }

        // SAFETY: The Guard is authorized to read up to and including `available` sequence.
        let event_ptr = self.parent.ring_buffer.get(self.sequence);
        let event = unsafe { &*event_ptr };
        self.sequence += 1;
        Some(event)
    }
}

impl<E, B> ExactSizeIterator for &mut EventGuard<'_, E, B> {
    /// Returns the number of events available to read.
    fn len(&self) -> usize {
        (self.available - self.sequence + 1) as usize
    }
}

impl<E, B> Drop for EventGuard<'_, E, B> {
    fn drop(&mut self) {
        // Signal to producers or later consumers that we're done processing `available` sequence.
        // Note, not all events in the range have to have been read.
        // (I.e. client code can skip reading any number events.)
        self.parent.cursor.store(self.available);
    }
}

impl<E, B> EventPoller<E, B>
where
    B: Barrier,
{
    pub(crate) fn new(
        ring_buffer: Arc<RingBuffer<E>>,
        dependent_barrier: Arc<B>,
        shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
        cursor: Arc<Cursor>,
    ) -> Self {
        Self {
            ring_buffer,
            dependent_barrier,
            shutdown_at_sequence,
            cursor,
        }
    }

    /// Returns the next available event or an error if no events are available or the Disruptor is shut down.
    /// This method does not block; it will return immediately.
    pub fn poll(&mut self) -> Result<EventGuard<'_, E, B>, Polling> {
        let sequence = self.cursor.relaxed_value() + 1;

        if sequence == self.shutdown_at_sequence.load(Ordering::Relaxed) {
            return Err(Polling::Shutdown);
        }

        let available = self.dependent_barrier.get_after(sequence);
        if available < sequence {
            return Err(Polling::NoEvents);
        }
        fence(Ordering::Acquire);

        Ok(EventGuard {
            parent: self,
            sequence,
            available,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::SingleConsumerBarrier;
    use crate::ringbuffer::RingBuffer;

    #[test]
    fn poller_observes_manual_shutdown_without_producer_drop() {
        #[derive(Debug)]
        struct Event;

        let ring_buffer = Arc::new(RingBuffer::new(2, || Event));
        let cursor = Arc::new(Cursor::new(-1));
        let shutdown = Arc::new(CachePadded::new(AtomicI64::new(-1)));
        let barrier = Arc::new(SingleConsumerBarrier::new(Arc::clone(&cursor)));

        let mut poller = EventPoller::new(
            ring_buffer,
            barrier,
            Arc::clone(&shutdown),
            Arc::clone(&cursor),
        );

        // Signal shutdown as if producer had advanced to sequence 0.
        shutdown.store(0, Ordering::Relaxed);
        assert_eq!(poller.poll().err(), Some(Polling::Shutdown));
    }

    #[test]
    fn poller_exits_on_shutdown_race() {
        #[derive(Debug)]
        struct Event;

        let ring_buffer = Arc::new(RingBuffer::new(2, || Event));
        let cursor = Arc::new(Cursor::new(-1));
        let shutdown = Arc::new(CachePadded::new(AtomicI64::new(-1)));
        let barrier = Arc::new(SingleConsumerBarrier::new(Arc::clone(&cursor)));

        let mut poller = EventPoller::new(
            ring_buffer,
            barrier,
            Arc::clone(&shutdown),
            Arc::clone(&cursor),
        );

        let shutdown_ref = shutdown.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            shutdown_ref.store(0, Ordering::Relaxed);
        });

        // Spin poll until shutdown is observed.
        let mut saw_shutdown = false;
        for _ in 0..10 {
            if matches!(poller.poll(), Err(Polling::Shutdown)) {
                saw_shutdown = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        handle.join().unwrap();
        assert!(saw_shutdown, "poller should see shutdown promptly");
    }
}
