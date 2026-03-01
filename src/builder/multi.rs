//! Module for structs for building a Multi Producer Disruptor in a type safe way.
//!
//! To get started building a Multi Producer Disruptor, invoke [super::build_multi_producer].

use std::{marker::PhantomData, sync::Arc};

use crate::{barrier::Barrier, builder::ProcessorSettings, consumer::{event_poller::{BranchPoller, EventPoller}, start_processor, start_processor_with_state, MultiConsumerBarrier, SingleConsumerBarrier}, cursor::{BranchJoinHandle, Cursor, CursorHandle}, producer::multi::{MultiProducer, MultiProducerBarrier}, wait_strategies::WaitStrategy, Sequence};

use super::{Builder, Shared, MC, NC, SC};

/// First step in building a Disruptor with a [MultiProducer].
pub struct MPBuilder<State, E, W, B> {
	state:             PhantomData<State>,
	shared:            Shared<E, W>,
	producer_barrier:  Arc<MultiProducerBarrier>,
	dependent_barrier: Arc<B>,
}

impl<S, E, W, B> ProcessorSettings<E, W> for MPBuilder<S, E, W, B> {
	fn shared(&mut self) -> &mut Shared<E, W> {
		&mut self.shared
	}
}

impl<S, E, W, B> MPBuilder<S, E, W, B>
where
	E: 'static + Send + Sync,
	B: 'static + Barrier,
{
	/// Create an out-of-band [`EventPoller`] that depends on the current barrier.
	///
	/// This is intended for building DAG topologies where a branch runs in parallel with multiple
	/// stages, and is only joined back at a later point (use [`and_then_joining`](Self::and_then_joining)).
	/// Use [`BranchPoller::join_handle`] to obtain a handle that can be joined downstream.
	///
	/// This poller participates in producer back-pressure, but does not affect the builder's
	/// dependency chain unless it is explicitly joined.
	///
	/// Note: Dropping the returned poller without advancing it can cause producers to eventually
	/// stall when the ring buffer wraps, since it participates in producer back-pressure.
	pub fn branch_poller(&mut self) -> BranchPoller<E, B> {
		let cursor = Arc::new(Cursor::new(-1));
		self.shared.add_producer_gating_cursor(Arc::clone(&cursor));
		let poller = EventPoller::new(
			Arc::clone(&self.shared.ring_buffer),
			Arc::clone(&self.dependent_barrier),
			Arc::clone(&self.shared.shutdown_at_sequence),
			Arc::clone(&cursor));
		BranchPoller::new(poller, BranchJoinHandle(cursor))
	}
}

impl<S, E, W, B> MPBuilder<S, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Create an out-of-band processor that depends on the current barrier.
	///
	/// Like [`branch_poller`](Self::branch_poller), this is intended for wiring DAG topologies where
	/// a branch runs in parallel with multiple stages and is only joined back later.
	///
	/// Returns a join handle that can be moved into [`and_then_joining`](Self::and_then_joining).
	pub fn branch_handle_events_with<EH>(&mut self, event_handler: EH) -> BranchJoinHandle
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool),
	{
		let barrier            = Arc::clone(&self.dependent_barrier);
		let (cursor, consumer) = start_processor(event_handler, &mut self.shared, barrier);
		self.shared.add_branch_consumer_and_cursor(consumer, Arc::clone(&cursor));
		BranchJoinHandle(cursor)
	}

	/// Like [`branch_handle_events_with`](Self::branch_handle_events_with), but with state.
	pub fn branch_handle_events_and_state_with<EH, S2, IS>(&mut self, event_handler: EH, initialize_state: IS) -> BranchJoinHandle
	where
		EH: 'static + Send + FnMut(&mut S2, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S2,
	{
		let barrier            = Arc::clone(&self.dependent_barrier);
		let (cursor, consumer) = start_processor_with_state(event_handler, &mut self.shared, barrier, initialize_state);
		self.shared.add_branch_consumer_and_cursor(consumer, Arc::clone(&cursor));
		BranchJoinHandle(cursor)
	}
}

impl<S, E, W, B> Builder<E, W, B> for MPBuilder<S, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	fn dependent_barrier(&self) -> Arc<B> {
		Arc::clone(&self.dependent_barrier)
	}
}

impl <E, W, B> MPBuilder<NC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	pub(super) fn new<F>(size: usize, event_factory: F, wait_strategy: W, producer_barrier: Arc<MultiProducerBarrier>, dependent_barrier: Arc<B>) -> Self
	where
		F: FnMut() -> E
	{
		let shared = Shared::new(size, event_factory, wait_strategy);
		Self {
			state: PhantomData,
			shared,
			producer_barrier,
			dependent_barrier,
		}
	}

	/// Get an EventPoller.
	pub fn event_poller(mut self) -> (EventPoller<E, B>, MPBuilder<SC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> MPBuilder<SC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initialize_state: IS) -> MPBuilder<SC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S
	{
		self.add_event_handler_with_state(event_handler, initialize_state);
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}
}

impl <E, W, B> MPBuilder<SC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Get an EventPoller.
	pub fn event_poller(mut self) -> (EventPoller<E, B>, MPBuilder<MC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> MPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initialize_state: IS) -> MPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S
	{
		self.add_event_handler_with_state(event_handler, initialize_state);
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> MPBuilder<NC, E, W, SingleConsumerBarrier> {
		// Guaranteed to be present by construction.
		let consumer_cursors  = self.shared().current_consumer_cursors.as_mut().unwrap();
		let dependent_barrier = Arc::new(SingleConsumerBarrier::new(consumer_cursors.remove(0)));

		MPBuilder {
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
			dependent_barrier,
		}
	}

	/// Like [`and_then`](Self::and_then) but the resulting barrier also waits for extra cursors
	/// from other branches (e.g. created via [`branch_poller`](Self::branch_poller)).
	pub fn and_then_joining(mut self, extra_cursors: Vec<CursorHandle>) -> MPBuilder<NC, E, W, MultiConsumerBarrier> {
		// Guaranteed to be present by construction.
		let consumer_cursors  = self.shared().current_consumer_cursors.as_mut().unwrap();
		let mut all_cursors   = Vec::with_capacity(1 + extra_cursors.len());
		all_cursors.push(consumer_cursors.remove(0));
		all_cursors.extend(extra_cursors.into_iter().map(|c| c.0));
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(all_cursors));

		MPBuilder {
			dependent_barrier,
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
		}
	}

	/// Finish the build and get a [`MultiProducer`].
	pub fn build(mut self) -> MultiProducer<E, SingleConsumerBarrier> {
		let mut consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		let producer_gating_cursors = std::mem::take(&mut self.shared.producer_gating_cursors);
		// Guaranteed to be present by construction.
		let consumer_barrier     = SingleConsumerBarrier::new(consumer_cursors.remove(0));
		MultiProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
			consumer_barrier,
			producer_gating_cursors)
	}
}

impl <E, W, B> MPBuilder<MC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Get an EventPoller.
	pub fn event_poller(mut self) -> (EventPoller<E, B>, MPBuilder<MC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		MPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> MPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		self
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initialize_state: IS) -> MPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S
	{
		self.add_event_handler_with_state(event_handler, initialize_state);
		self
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> MPBuilder<NC, E, W, MultiConsumerBarrier> {
		let consumer_cursors  = self.shared().current_consumer_cursors.replace(vec![]).unwrap();
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(consumer_cursors));

		MPBuilder {
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
			dependent_barrier,
		}
	}

	/// Like [`and_then`](Self::and_then) but the resulting barrier also waits for extra cursors
	/// from other branches (e.g. created via [`branch_poller`](Self::branch_poller)).
	pub fn and_then_joining(mut self, extra_cursors: Vec<CursorHandle>) -> MPBuilder<NC, E, W, MultiConsumerBarrier> {
		let consumer_cursors  = self.shared().current_consumer_cursors.replace(vec![]).unwrap();
		let mut all_cursors   = Vec::with_capacity(consumer_cursors.len() + extra_cursors.len());
		all_cursors.extend(consumer_cursors);
		all_cursors.extend(extra_cursors.into_iter().map(|c| c.0));
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(all_cursors));

		MPBuilder {
			dependent_barrier,
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
		}
	}

	/// Finish the build and get a [`MultiProducer`].
	pub fn build(mut self) -> MultiProducer<E, MultiConsumerBarrier> {
		let mut consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		consumer_cursors.extend(std::mem::take(&mut self.shared.producer_gating_cursors));
		let consumer_barrier = MultiConsumerBarrier::new(consumer_cursors);
		MultiProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
			consumer_barrier,
			vec![])
	}
}
