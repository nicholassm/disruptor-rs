//! Module with different producer handles for publishing into the Disruptor.
//!
//! Both publishing from a single thread (fastest) and from multiple threads is supported.

pub mod single;
pub mod multi;

use std::sync::{atomic::AtomicI64, Arc};
use crossbeam_utils::CachePadded;
use crate::{barrier::Barrier, consumer::{Consumer, ConsumerBarrier}, ringbuffer::RingBuffer, Sequence};

/// Barrier for producers.
#[doc(hidden)]
pub trait ProducerBarrier : Barrier {
	/// Claim the next Sequence (for publication).
	fn next(&self) -> Sequence;

	/// Publishes the sequence number that is now available for being read by consumers.
	/// (The sequence number is stored with [`std::sync::atomic::Ordering::Release`] semantics.)
	fn publish(&self, sequence: Sequence);
}

/// Error indicating that the ring buffer is full.
///
/// Client code can then take appropriate action, e.g. discard data or even panic as this indicates
/// that the consumers cannot keep up - i.e. latency.
#[derive(Debug)]
pub struct RingBufferFull;

/// Producer used for publishing into the Disruptor.
///
/// A `Producer` has two methods for publication:
/// 1. `try_publish` and
/// 2. `publish`
///
/// It is recommended to use `try_publish` and handle the [`RingBufferFull`] error as appropriate in
/// the application.
///
/// Note, that a [`RingBufferFull`] error indicates that the consumer logic cannot keep up with the
/// data ingestion rate and that latency is increasing. Therefore, the safe route is to panic the
/// application instead of sending latent data out. (Of course appropriate action should be taken to
/// make e.g. prices indicative in a price engine or cancel all open orders in a trading
/// application before panicking.)
pub trait Producer<E, P> {
	#[doc(hidden)]
	fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          *mut RingBuffer<E>,
		producer_barrier:     Arc<P>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     ConsumerBarrier,
	) -> Self;

	/// Publish an Event into the Disruptor.
	///
	/// Returns a `Result` with the published sequence number or a [RingBufferFull] in case the
	/// ring buffer is full.
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::Producer;
	///# use disruptor::BusySpin;
	///# use disruptor::RingBufferFull;
	///#
	/// // The example data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	///# fn main() -> Result<(), RingBufferFull> {
	/// let factory = || { Event { price: 0.0 }};
	///# let processor = |e: &Event, _, _| {};
	///# let mut builder = disruptor::build_single_producer(8, factory, BusySpin);
	///# let mut producer = builder.handle_events_with(processor).build();
	/// producer.try_publish(|e| { e.price = 42.0; })?;
	///# Ok(())
	///# }
	/// ```
	///
	/// See also [`Self::publish`].
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
	where F: FnOnce(&mut E);

	/// Publish an Event into the Disruptor.
	///
	/// Spins until there is an available slot in case the ring buffer is full.
	///
	/// # Examples
	///
	/// ```
	///# use disruptor::Producer;
	///# use disruptor::BusySpin;
	///# use disruptor::RingBufferFull;
	///#
	/// // The example data entity on the ring buffer.
	/// struct Event {
	///     price: f64
	/// }
	/// let factory = || { Event { price: 0.0 }};
	///# let processor = |e: &Event, _, _| {};
	///# let mut builder = disruptor::build_single_producer(8, factory, BusySpin);
	///# let mut producer = builder.handle_events_with(processor).build();
	/// producer.publish(|e| { e.price = 42.0; });
	/// ```
	///
	/// See also [`Self::try_publish`].
	fn publish<F>(&mut self, update: F)
	where F: FnOnce(&mut E);
}
