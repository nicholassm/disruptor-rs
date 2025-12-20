use std::{sync::{atomic::{fence, AtomicI64, Ordering}, Arc}};
use crossbeam_utils::CachePadded;
use thiserror::Error;
use crate::{cursor::Cursor, barrier::Barrier, ringbuffer::RingBuffer, Sequence};

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
///# let factory = || Event { price: 0.0 };
///# let builder = build_single_producer(8, factory, BusySpin);
///# let (mut event_poller, builder) = builder.event_poller();
///# let mut producer = builder.build();
///# producer.publish(|e| { e.price = 42.0; });
///# drop(producer);
/// loop {
///     // 1. Either poll all available events:
///     match event_poller.poll() {
///         Ok(mut events) => {
///             // Batch process events if efficient in your use case.
///             let batch_size = (&mut events).len();
///             // The guard named `events` is an `Iterator` so you can read events by iterating.
///             for event in &mut events {
///                 println!("Processing event: {:?}", event);
///             }
///         },// At this point the EventGuard (here named `events`) is dropped,
///           // signaling the Disruptor that the events have been processed.
///         Err(Polling::NoEvents) => { /* Do other work or try again. */ },
///         Err(Polling::Shutdown) => { break; }, // Exit the loop if the Disruptor is shut down.
///     }
///     // 2. Or limit the number of events per poll:
///     match event_poller.poll_take(64) {
///         Ok(mut guard) => {
///             // Process events same as above but yielding at most 64 events.
///             for event in &mut guard {
///                 println!("Processing event: {:?}", event);
///             }
///         },
///         Err(Polling::NoEvents) => {},
///         Err(Polling::Shutdown) => { break; },
///     }
/// }
/// ```
pub struct EventPoller<E, B> {
	ring_buffer:          Arc<RingBuffer<E>>,
	dependent_barrier:    Arc<B>,
	shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	cursor:               Arc<Cursor>,
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
	parent:    &'p mut EventPoller<E, B>,
	sequence:  Sequence,
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
		let event     = unsafe { &*event_ptr };
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
		ring_buffer:          Arc<RingBuffer<E>>,
		dependent_barrier:    Arc<B>,
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		cursor:               Arc<Cursor>,
	) -> Self {
		Self {
			ring_buffer,
			dependent_barrier,
			shutdown_at_sequence,
			cursor,
		}
	}

	/// Polls the ring buffer and returns an [`EventGuard`] if any events are available.
	/// The guard can be used like an iterator and yields all available events at the time of polling.
	/// Dropping the guard will signal to the Disruptor that the events have been processed.
	///
	/// This method does not block; it will return immediately.
	///
	/// This method is equivalent to calling [`EventPoller::poll_take`] with `u64::MAX` as the limit.
	///
	/// # Errors
	///
	/// This method returns an error if:
	/// 1. No events are available: [`Polling::NoEvents`]
	/// 2. The Disruptor is shut down: [`Polling::Shutdown`]
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::*;
	///#
	///# #[derive(Debug)]
	///# struct Event {
	///#     price: f64
	///# }
	///# let factory = || Event { price: 0.0 };
	///# let builder = build_single_producer(8, factory, BusySpin);
	///# let (mut event_poller, builder) = builder.event_poller();
	///# let mut producer = builder.build();
	///# producer.publish(|e| { e.price = 42.0; });
	///# drop(producer);
	/// match event_poller.poll() {
	///     Ok(mut events) => {
	///         for event in &mut events {
	///             // ...
	///         }
	///     },
	///     Err(Polling::NoEvents) => { /* ... */ },
	///     Err(Polling::Shutdown) => { /* ... */ },
	/// };
	/// ```
	pub fn poll(&mut self) -> Result<EventGuard<'_, E, B>, Polling> {
		self.poll_take(u64::MAX)
	}

	/// Polls for available events, yielding at most `limit` events.
	///
	/// This method behaves like [`EventPoller::poll`], but caps the number of events yielded by the returned
	/// [`EventGuard`]. Fewer events may be yielded if less are available at the time of polling.
	///
	/// Note: A `limit` of zero returns an [`EventGuard`] that yields no events (and not an [Polling::NoEvents] error).
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::*;
	///#
	///# #[derive(Debug)]
	///# struct Event {
	///#     price: f64
	///# }
	///# let factory = || Event { price: 0.0 };
	///# let builder = build_single_producer(8, factory, BusySpin);
	///# let (mut event_poller, builder) = builder.event_poller();
	///# let mut producer = builder.build();
	///# producer.publish(|e| { e.price = 42.0; });
	///# drop(producer);
	/// match event_poller.poll_take(64) {
	///     Ok(mut events) => {
	///         // Process events same as above but yielding at most 64 events.
	///         for event in &mut events {
	///             // ...
	///         }
	///     },
	///     Err(Polling::NoEvents) => { /* ... */ },
	///     Err(Polling::Shutdown) => { /* ... */ },
	/// };
	/// ```
	pub fn poll_take(&mut self, limit: u64) -> Result<EventGuard<'_, E, B>, Polling> {
		let cursor_at = self.cursor.relaxed_value();
		let sequence  = cursor_at + 1;

		if sequence == self.shutdown_at_sequence.load(Ordering::Relaxed) {
			return Err(Polling::Shutdown);
		}

		let available = self.dependent_barrier.get_after(sequence);
		if available < sequence {
			return Err(Polling::NoEvents);
		}
		fence(Ordering::Acquire);

		let max_sequence = (cursor_at).saturating_add_unsigned(limit);
		let available    = std::cmp::min(available, max_sequence);

		Ok(EventGuard {
			parent: self,
			sequence,
			available,
		})
	}
}
