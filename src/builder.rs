//! Module for building the Disruptor and adding event handlers.

use std::{sync::{atomic::{fence, AtomicI64, Ordering}, Arc}, thread};
use core_affinity::CoreId;
use crossbeam_utils::CachePadded;
use crate::{affinity::{cpu_has_core_else_panic, set_affinity_if_defined}, barrier::{Barrier, NONE}, Sequence};
use crate::consumer::Consumer;
use crate::cursor::Cursor;
use crate::producer::{single::SingleProducerBarrier, multi::MultiProducerBarrier};
use crate::ringbuffer::RingBuffer;
use crate::wait_strategies::WaitStrategy;

use self::{multi::MPBuilder, single::SPBuilder};

pub mod single;
pub mod multi;

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
-> SPBuilder<E, W, SingleProducerBarrier>
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
/// let mut producer1 = build_multi_producer(8, factory, BusySpin)
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
-> MPBuilder<E, W, MultiProducerBarrier>
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
///    // Processor 1 is pined and has a custom name.
///    .pined_at_core(1).thread_named("my_processor").handle_events_with(processor1)
///    // Processor 2 is not pined and gets a generic name.
///    .handle_events_with(processor2)
///    // Processor 3 is pined and gets a generic name.
///    .pined_at_core(2).handle_events_with(processor3)
///    .build();
///# }
/// ```
pub trait ProcessorSettings<E, W>: Sized {
	#[doc(hidden)]
	fn shared(&mut self) -> &mut Shared<E, W>;

	/// Pin processor thread on the core with `id` for the next added event handler.
	/// Outputs an error on stderr if the thread could not be pinned.
	fn pined_at_core(mut self, id: usize) -> Self {
		self.shared().pined_at_core(id);
		self
	}

	/// Set a name for the processor thread for the next added event handler.
	fn thread_named(mut self, name: &'static str) -> Self {
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
		self.shared().add_consumer(consumer, cursor);
	}

	fn dependent_barrier(&self) -> Arc<B>;
}

#[doc(hidden)]
pub struct Shared<E, W> {
	pub(crate) shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	pub(crate) ring_buffer:          Arc<RingBuffer<E>>,
	pub(crate) consumers:            Vec<Consumer>,
	current_consumer_cursors:        Option<Vec<Arc<Cursor>>>,
	wait_strategy:                   W,
	thread_context:                  ThreadContext,
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

	fn add_consumer(&mut self, consumer: Consumer, cursor: Arc<Cursor>) {
		self.consumers.push(consumer);
		self.current_consumer_cursors.as_mut().unwrap().push(cursor);
	}

	fn pined_at_core(&mut self, id: usize) {
		cpu_has_core_else_panic(id);
		self.thread_context.affinity = Some(CoreId { id } );
	}

	fn thread_named(&mut self, name: &'static str) {
		self.thread_context.name = Some(name.to_owned());
	}
}

#[derive(Default)]
struct ThreadContext {
	affinity: Option<CoreId>,
	name:     Option<String>,
	id:       usize,
}

impl ThreadContext {
	fn name(&mut self) -> String {
		self.name.take().or_else(|| {
			self.id += 1;
			Some(format!("processor-{}", self.id))
		}).unwrap()
	}

	fn affinity(&mut self) -> Option<CoreId> {
		self.affinity.take()
	}
}

fn start_processor<E, EP, W, B> (
	mut event_handler: EP,
	builder:           &mut Shared<E, W>,
	barrier:           Arc<B>)
-> (Arc<Cursor>, Consumer)
where
	E:  'static + Send + Sync,
	EP: 'static + Send + FnMut(&E, Sequence, bool),
	W:  'static + WaitStrategy,
	B:  'static + Barrier + Send + Sync,
{
	let consumer_cursor      = Arc::new(Cursor::new(-1));// Initially, the consumer has not read slot 0 yet.
	let wait_strategy        = builder.wait_strategy;
	let ring_buffer          = Arc::clone(&builder.ring_buffer);
	let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
	let thread_name          = builder.thread_context.name();
	let affinity             = builder.thread_context.affinity();
	let thread_builder       = thread::Builder::new().name(thread_name.clone());
	let join_handle          = {
		let consumer_cursor = Arc::clone(&consumer_cursor);
		thread_builder.spawn(move || {
			set_affinity_if_defined(affinity, thread_name.as_str());
			let mut sequence = 0;
			loop {
				let mut available = barrier.get_after(sequence);
				while available < sequence {
					// If publisher(s) are done publishing events we're done.
					if shutdown_at_sequence.load(Ordering::Relaxed) == sequence {
						return;
					}
					wait_strategy.wait_for(sequence);
					available = barrier.get_after(sequence);
				}
				fence(Ordering::Acquire);

				while available >= sequence {
					let end_of_batch = available == sequence;
					// SAFETY: Now, we have (shared) read access to the event at `sequence`.
					let event_ptr    = ring_buffer.get(sequence);
					unsafe {
						event_handler(& *event_ptr, sequence, end_of_batch);
					}
					// Signal to producers or later consumers that we're done processing `sequence`.
					consumer_cursor.store(sequence);
					// Update next sequence to read.
					sequence += 1;
				}
			}
		}).expect("Should spawn thread.")
	};

	let consumer = Consumer::new(join_handle);
	(consumer_cursor, consumer)
}
