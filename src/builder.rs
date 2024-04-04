//! Module for building the Disruptor and adding event handlers.
//!
//! # Examples
//!
//! ```
//!# use disruptor::build_single_producer;
//!# use disruptor::Producer;
//!# use disruptor::BusySpin;
//!# use disruptor::RingBufferFull;
//!#
//! // The example data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//! let factory = || { Event { price: 0.0 }};
//!# let processor1 = |e: &Event, _, _| {};
//!# let processor2 = |e: &Event, _, _| {};
//!# let processor3 = |e: &Event, _, _| {};
//! let mut producer = disruptor::build_single_producer(8, factory, BusySpin)
//!    .pined_at_core(1).thread_named("my_processor").handle_events_with(processor1)
//!    .handle_events_with(processor2) // Not pinned and getting a generic name.
//!    .and_then()
//!        .pined_at_core(2).handle_events_with(processor3) // Pined but with a generic name.
//!    .build();
//! ```

use std::{marker::PhantomData, sync::{atomic::{fence, AtomicI64, Ordering}, Arc}, thread};
use core_affinity::CoreId;
use crossbeam_utils::CachePadded;
use crate::{affinity::{cpu_has_core_else_panic, set_affinity_if_defined}, barrier::{Barrier, NONE}, Sequence};
use crate::consumer::{Consumer, ConsumerBarrier};
use crate::cursor::Cursor;
use crate::producer::{Producer, single::{SingleProducer, SingleProducerBarrier}, multi::{MultiProducer, MultiProducerBarrier}, ProducerBarrier};
use crate::ringbuffer::RingBuffer;
use crate::wait_strategies::WaitStrategy;

/// Build a single producer Disruptor. Use this if you only need to publish events from one thread.
///
/// For using a producer see [`Producer`].
pub fn build_single_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> Builder<E, W, SingleProducerBarrier, SingleProducer<E, SingleProducerBarrier>>
where
	F: FnMut() -> E,
	E: 'static,
	W: 'static + WaitStrategy,
{
	let producer_barrier = SingleProducerBarrier::new();
	Builder::new(size, event_factory, wait_strategy, producer_barrier)
}

/// Build a multi producer Disruptor. Use this if you need to publish events from many threads.
///
/// For using a producer see [`Producer`].
pub fn build_multi_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> Builder<E, W, MultiProducerBarrier, MultiProducer<E, MultiProducerBarrier>>
where
	F: FnMut() -> E,
	E: 'static,
	W: 'static + WaitStrategy,
{
	let producer_barrier = MultiProducerBarrier::new(size);
	Builder::new(size, event_factory, wait_strategy, producer_barrier)
}

/// Adds a dependency on all previously added event handlers.
///
/// See [`Builder`] for examples of usage (they have the same methods).
pub struct DependencyChain<E, W, P, PR>
where
	PR: Producer<E, P>
{
	builder:           Builder<E, W, P, PR>,
	dependent_barrier: Arc<ConsumerBarrier>,
	consumer_barrier:  Option<ConsumerBarrier>,
}

/// Builder used for configuring and constructing a Disruptor.
///
/// # Examples
///
/// ```
///# use disruptor::build_single_producer;
///# use disruptor::Producer;
///# use disruptor::BusySpin;
///# use disruptor::RingBufferFull;
///#
/// // The example data entity on the ring buffer.
/// struct Event {
///     price: f64
/// }
/// let factory = || { Event { price: 0.0 }};
///# let processor1 = |e: &Event, _, _| {};
///# let processor2 = |e: &Event, _, _| {};
///# let processor3 = |e: &Event, _, _| {};
/// let mut producer = disruptor::build_single_producer(8, factory, BusySpin)
///    .pined_at_core(1).thread_named("my_processor").handle_events_with(processor1)
///    .handle_events_with(processor2) // Not pinned and getting a generic name.
///    .and_then()
///        .pined_at_core(2).handle_events_with(processor3) // Pined but with a generic name.
///    .build();
/// ```
pub struct Builder<E, W, P, PR>
where
	PR: Producer<E, P>
{
	pub(crate) shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	pub(crate) ring_buffer:          *mut RingBuffer<E>,
	pub(crate) producer_barrier:     Arc<P>,
	pub(crate) consumers:            Vec<Consumer>,
	phantom_data:                    PhantomData<PR>,
	wait_strategy:                   W,
	consumer_barrier:                Option<ConsumerBarrier>,
	thread_context:                  ThreadContext,
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

struct RingBufferWrapper<E>(*mut RingBuffer<E>);
unsafe impl<E> Send for RingBufferWrapper<E> {}

impl<E> RingBufferWrapper<E> {
	fn unwrap(&self) -> &RingBuffer<E> {
		unsafe { &*self.0 }
	}
}

impl <E, W, P, PR> Builder<E, W, P, PR>
where
	E:  'static,
	P:  'static + Send + Sync + ProducerBarrier + Barrier,
	W:  'static + WaitStrategy,
	PR: Producer<E, P>,
{
	fn new<F>(size: usize, event_factory: F, wait_strategy: W, producer_barrier: P) -> Self
	where
		F: FnMut() -> E
	{
		let ring_buffer          = Box::into_raw(Box::new(RingBuffer::new(size, event_factory)));
		let producer_barrier     = Arc::new(producer_barrier);
		let shutdown_at_sequence = Arc::new(CachePadded::new(AtomicI64::new(NONE)));
		let consumer_barrier     = Some(ConsumerBarrier::new());

		Builder {
			ring_buffer,
			wait_strategy,
			shutdown_at_sequence,
			producer_barrier,
			consumer_barrier,
			consumers: vec![],
			phantom_data: PhantomData,
			thread_context: ThreadContext::default(),
		}
	}

	/// Pin processor thread on the core with `id`.
	/// Outputs an error on stderr if the thread could not be pinned.
	pub fn pined_at_core(mut self, id: usize) -> Self {
		cpu_has_core_else_panic(id);
		self.thread_context.affinity = Some(CoreId { id } );
		self
	}

	/// Set a name for the processor thread.
	pub fn thread_named(mut self, name: &'static str) -> Self {
		self.thread_context.name = Some(name.to_owned());
		self
	}

	/// Add an event handler.
	pub fn handle_events_with<EP>(mut self, event_handler: EP) -> Self
	where
		EP: 'static + Send + FnMut(&E, Sequence, bool)
	{
		let barrier            = Arc::clone(&self.producer_barrier);
		let (cursor, consumer) = start_processor(event_handler, &mut self, barrier);
		self.consumers.push(consumer);
		self.consumer_barrier.as_mut().unwrap().add(cursor);
		self
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> DependencyChain<E, W, P, PR> {
		let dependent_barrier = Arc::new(self.consumer_barrier.take().unwrap());
		let consumer_barrier  = Some(ConsumerBarrier::new());
		DependencyChain {
			builder: self,
			dependent_barrier,
			consumer_barrier
		}
	}

	/// Finish the build and get the producer used for publication.
	pub fn build(mut self) -> PR {
		let consumer_barrier = self.consumer_barrier.take().unwrap();
		PR::new(
			self.shutdown_at_sequence,
			self.ring_buffer,
			self.producer_barrier,
			self.consumers,
			consumer_barrier)
	}
}

impl <E, W, P, PR> DependencyChain<E, W, P, PR>
where
	E:  'static,
	P:  'static + Send + Sync + ProducerBarrier + Barrier,
	W:  'static + WaitStrategy,
	PR: Producer<E, P>,
{
	/// Add an event handler.
	pub fn handle_events_with<EP>(mut self, event_handler: EP) -> Self
	where
		EP: 'static + Send + FnMut(&E, Sequence, bool)
	{
		let barrier            = Arc::clone(&self.dependent_barrier);
		let (cursor, consumer) = start_processor(event_handler, &mut self.builder, barrier);
		self.builder.consumers.push(consumer);
		self.consumer_barrier.as_mut().unwrap().add(cursor);
		self
	}

	/// Pin processor thread on the core with `id`.
	/// Outputs an error on stderr if the thread could not be pinned.
	pub fn pined_at_core(mut self, id: usize) -> Self {
		self.builder = self.builder.pined_at_core(id);
		self
	}

	/// Set a name for the processor thread.
	pub fn thread_named(mut self, name: &'static str) -> Self {
		self.builder = self.builder.thread_named(name);
		self
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> DependencyChain<E, W, P, PR> {
		let dependent_barrier = Arc::new(self.consumer_barrier.take().unwrap());
		let consumer_barrier  = Some(ConsumerBarrier::new());
		DependencyChain {
			builder: self.builder,
			dependent_barrier,
			consumer_barrier
		}
	}

	/// Finish the build and get the producer used for publication.
	pub fn build(mut self) -> PR {
		let consumer_barrier = self.consumer_barrier.take().unwrap();
		PR::new(
			self.builder.shutdown_at_sequence,
			self.builder.ring_buffer,
			self.builder.producer_barrier,
			self.builder.consumers,
			consumer_barrier)
	}
}

fn start_processor<E, EP, W, B, P, PR: Producer<E, P>> (
	mut event_handler: EP,
	builder:           &mut Builder<E, W, P, PR>,
	barrier:           Arc<B>)
-> (Arc<Cursor>, Consumer)
where
	E:  'static,
	EP: 'static + Send + FnMut(&E, Sequence, bool),
	W:  'static + WaitStrategy,
	B:  'static + Barrier + Send + Sync,
{
	let consumer_cursor      = Arc::new(Cursor::new(-1));// Initially, the consumer has not read slot 0 yet.
	let wait_strategy        = builder.wait_strategy;
	let wrapper              = RingBufferWrapper(builder.ring_buffer);
	let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
	let thread_name          = builder.thread_context.name();
	let affinity             = builder.thread_context.affinity();
	let thread_builder       = thread::Builder::new().name(thread_name.clone());
	let join_handle          = {
		let consumer_cursor = Arc::clone(&consumer_cursor);
		thread_builder.spawn(move || {
			set_affinity_if_defined(affinity, thread_name.as_str());
			let ring_buffer  = wrapper.unwrap();
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
					// SAFETY: Now, we have (shared) read access to the element at `sequence`.
					let mut_element  = ring_buffer.get(sequence);
					unsafe {
						let element: &E = &*mut_element;
						event_handler(element, sequence, end_of_batch);
					}
					// Signal to producers that we're done processing `sequence`.
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
