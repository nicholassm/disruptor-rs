//! Module for building the Disruptor and adding event handlers.
//!
//! Use [build_single_producer] or [build_multi_producer] to get started.

use std::sync::{atomic::AtomicI64, Arc};
use core_affinity::CoreId;
use crossbeam_utils::CachePadded;
use crate::{affinity::cpu_has_core_else_panic, barrier::{Barrier, NONE}, consumer::{start_processor, start_processor_with_state, event_poller::EventPoller}, Sequence};
use crate::consumer::Consumer;
use crate::cursor::Cursor;
use crate::producer::{single::SingleProducerBarrier, multi::MultiProducerBarrier};
use crate::ringbuffer::RingBuffer;
use crate::wait_strategies::WaitStrategy;

use self::{multi::MPBuilder, single::SPBuilder};

pub mod single;
pub mod multi;

/// State: No consumers (yet).
pub struct NC;
/// State: Single consumer.
pub struct SC;
/// State: Multiple consumers.
pub struct MC;

/// Build a single producer Disruptor. Use this if you only need to publish events from one thread.
///
/// For using a producer see [Producer](crate::producer::Producer).
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
///# let processor1 = |e: &Event, _, _| {};
///# let processor2 = |e: &Event, _, _| {};
///# let processor3 = |e: &Event, _, _| {};
/// let mut producer = build_single_producer(8, factory, BusySpin)
///    .handle_events_with(processor1)
///    .handle_events_with(processor2)
///    .and_then()
///        // `processor3` only reads events after the other two processors are done reading.
///        .handle_events_with(processor3)
///    .build();
///
/// // Now use the `producer` to publish events.
/// ```
pub fn build_single_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> SPBuilder<NC, E, W, SingleProducerBarrier>
where
	F: FnMut() -> E,
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
{
	let producer_barrier  = Arc::new(SingleProducerBarrier::new());
	let dependent_barrier = Arc::clone(&producer_barrier);
	SPBuilder::new(size, event_factory, wait_strategy, producer_barrier, dependent_barrier)
}

/// Build a multi producer Disruptor. Use this if you need to publish events from many threads.
///
/// For using a producer see [Producer](crate::producer::Producer).
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
///# let processor1 = |e: &Event, _, _| {};
///# let processor2 = |e: &Event, _, _| {};
///# let processor3 = |e: &Event, _, _| {};
/// let mut producer1 = build_multi_producer(64, factory, BusySpin)
///    .handle_events_with(processor1)
///    .handle_events_with(processor2)
///    .and_then()
///        // `processor3` only reads events after the other two processors are done reading.
///        .handle_events_with(processor3)
///    .build();
///
/// let mut producer2 = producer1.clone();
///
/// // Now two threads can get a Producer each.
/// ```
pub fn build_multi_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> MPBuilder<NC, E, W, MultiProducerBarrier>
where
	F: FnMut() -> E,
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
{
	let producer_barrier  = Arc::new(MultiProducerBarrier::new(size));
	let dependent_barrier = Arc::clone(&producer_barrier);
	MPBuilder::new(size, event_factory, wait_strategy, producer_barrier, dependent_barrier)
}

/// The processor's thread name and CPU affinity can be set via the builders that implement this trait.
///
/// # Examples
///
/// ```
///# #[cfg(miri)] fn main() {}
///# #[cfg(not(miri))]
///# fn main() {
///# use disruptor::*;
///#
/// // The example data entity on the ring buffer.
/// struct Event {
///     price: f64
/// }
/// let factory = || { Event { price: 0.0 }};
///# let processor1 = |e: &Event, _, _| {};
///# let processor2 = |e: &Event, _, _| {};
///# let processor3 = |e: &Event, _, _| {};
/// let mut producer = build_single_producer(8, factory, BusySpin)
///    // Processor 1 is pinned and has a custom name.
///    .pin_at_core(1).thread_name("my_processor").handle_events_with(processor1)
///    // Processor 2 is not pinned and gets a generic name.
///    .handle_events_with(processor2)
///    // Processor 3 is pinned and gets a generic name.
///    .pin_at_core(2).handle_events_with(processor3)
///    .build();
///# }
/// ```
pub trait ProcessorSettings<E, W>: Sized {
	#[doc(hidden)]
	fn shared(&mut self) -> &mut Shared<E, W>;

	/// Pin processor thread on the core with `id` for the next added event handler.
	/// Outputs an error on stderr if the thread could not be pinned.
	fn pin_at_core(mut self, id: usize) -> Self {
		self.shared().pin_at_core(id);
		self
	}

	/// Set a name for the processor thread for the next added event handler.
	fn thread_name(mut self, name: &'static str) -> Self {
		self.shared().thread_named(name);
		self
	}
}

trait Builder<E, W, B>: ProcessorSettings<E, W>
where
	E: 'static + Send + Sync,
	B: 'static + Barrier,
	W: 'static + WaitStrategy,
{
	fn add_event_handler<EH>(&mut self, event_handler: EH)
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		let barrier            = self.dependent_barrier();
		let (cursor, consumer) = start_processor(event_handler, self.shared(), barrier);
		self.shared().add_consumer_and_cursor(consumer, cursor);
	}

	fn get_event_poller(&mut self) -> EventPoller<E, B> {
		let cursor = Arc::new(Cursor::new(-1)); // Initially, the consumer has not read slot 0 yet.
		self.shared().add_cursor(Arc::clone(&cursor));

		EventPoller::new(
			Arc::clone(&self.shared().ring_buffer),
			Arc::clone(&self.dependent_barrier()),
			Arc::clone(&self.shared().shutdown_at_sequence),
			cursor,
		)
	}

	fn add_event_handler_with_state<EH, S, IS>(&mut self, event_handler: EH, initialize_state: IS)
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S,
	{
		let barrier            = self.dependent_barrier();
		let (cursor, consumer) = start_processor_with_state(event_handler, self.shared(), barrier, initialize_state);
		self.shared().add_consumer_and_cursor(consumer, cursor);
	}

	fn dependent_barrier(&self) -> Arc<B>;
}

#[doc(hidden)]
pub struct Shared<E, W> {
	pub(crate) shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	pub(crate) ring_buffer:          Arc<RingBuffer<E>>,
	pub(crate) consumers:            Vec<Consumer>,
	current_consumer_cursors:        Option<Vec<Arc<Cursor>>>,
	pub(crate) wait_strategy:        W,
	pub(crate) thread_context:       ThreadContext,
}

impl <E, W> Shared<E, W> {
	fn new<F>(size: usize, event_factory: F, wait_strategy: W) -> Self
	where
		F: FnMut() -> E
	{
		let ring_buffer              = Arc::new(RingBuffer::new(size, event_factory));
		let shutdown_at_sequence     = Arc::new(CachePadded::new(AtomicI64::new(NONE)));
		let current_consumer_cursors = Some(vec![]);

		Self {
			ring_buffer,
			wait_strategy,
			shutdown_at_sequence,
			current_consumer_cursors,
			consumers: vec![],
			thread_context: ThreadContext::default(),
		}
	}

	fn add_consumer_and_cursor(&mut self, consumer: Consumer, cursor: Arc<Cursor>) {
		self.consumers.push(consumer);
		self.add_cursor(cursor);
	}

	fn add_cursor(&mut self, cursor: Arc<Cursor>) {
		self.current_consumer_cursors.as_mut().unwrap().push(cursor);
	}

	fn pin_at_core(&mut self, id: usize) {
		cpu_has_core_else_panic(id);
		self.thread_context.affinity = Some(CoreId { id } );
	}

	fn thread_named(&mut self, name: &'static str) {
		self.thread_context.name = Some(name.to_owned());
	}
}

#[derive(Default)]
pub(crate) struct ThreadContext {
	affinity: Option<CoreId>,
	name:     Option<String>,
	id:       usize,
}

impl ThreadContext {
	pub(crate) fn name(&mut self) -> String {
		self.name.take().or_else(|| {
			self.id += 1;
			Some(format!("processor-{}", self.id))
		}).unwrap()
	}

	pub(crate) fn affinity(&mut self) -> Option<CoreId> {
		self.affinity.take()
	}
}
