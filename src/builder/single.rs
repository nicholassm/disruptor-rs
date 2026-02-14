//! Module for structs for building a Single Producer Disruptor in a type safe way.
//!
//! To get started building a Single Producer Disruptor, invoke [super::build_single_producer].

use std::{marker::PhantomData, sync::Arc};

use crate::{barrier::Barrier, builder::ProcessorSettings, consumer::{event_poller::EventPoller, MultiConsumerBarrier, SingleConsumerBarrier}, cursor::Cursor, producer::single::{SingleProducer, SingleProducerBarrier}, wait_strategies::WaitStrategy, Sequence};

use super::{Builder, Shared, MC, NC, SC};

/// First step in building a Disruptor with a [SingleProducer].
pub struct SPBuilder<State, E, W, B> {
	state:             PhantomData<State>,
	shared:            Shared<E, W>,
	producer_barrier:  Arc<SingleProducerBarrier>,
	dependent_barrier: Arc<B>,
}

impl<E, W, B, S> ProcessorSettings<E, W> for SPBuilder<S, E, W, B> {
	fn shared(&mut self) -> &mut Shared<E, W> {
		&mut self.shared
	}
}

impl<E, W, B, S> SPBuilder<S, E, W, B>
where
	E: 'static + Send + Sync,
	B: 'static + Barrier,
{
	/// Create an out-of-band [`EventPoller`] that depends on an earlier consumer's cursor.
	///
	/// This is intended for building diamond/DAG topologies where a downstream consumer must wait
	/// for multiple independent branches to complete (use `and_then_joining(...)`).
	///
	/// Warning: If this poller is not joined into a downstream dependency chain (or otherwise included
	/// in producer back-pressure), producers may overwrite slots before this poller has read them.
	pub fn branch_poller(&mut self, depends_on: Arc<Cursor>) -> EventPoller<E, SingleConsumerBarrier> {
		let barrier = Arc::new(SingleConsumerBarrier::new(depends_on));
		let cursor  = Arc::new(Cursor::new(-1));
		EventPoller::new(
			Arc::clone(&self.shared.ring_buffer),
			barrier,
			Arc::clone(&self.shared.shutdown_at_sequence),
			cursor)
	}
}

impl<E, W, B, S> Builder<E, W, B> for SPBuilder<S, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	fn dependent_barrier(&self) -> Arc<B> {
		Arc::clone(&self.dependent_barrier)
	}
}

impl <E, W, B> SPBuilder<NC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	pub(super) fn new<F>(size: usize, event_factory: F, wait_strategy: W, producer_barrier: Arc<SingleProducerBarrier>, dependent_barrier: Arc<B>) -> Self
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
	pub fn event_poller(mut self) -> (EventPoller<E, B>, SPBuilder<SC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPBuilder<SC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initialize_state: IS) -> SPBuilder<SC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S,
	{
		self.add_event_handler_with_state(event_handler, initialize_state);
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}
}

impl <E, W, B> SPBuilder<SC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Finish the build and get a [`SingleProducer`].
	pub fn build(mut self) -> SingleProducer<E, SingleConsumerBarrier> {
		let mut consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		// Guaranteed to be present by construction.
		let consumer_barrier     = SingleConsumerBarrier::new(consumer_cursors.remove(0));
		SingleProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
		consumer_barrier)
	}

	/// Get an EventPoller.
	pub fn event_poller(mut self) -> (EventPoller<E, B>, SPBuilder<MC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> SPBuilder<NC, E, W, SingleConsumerBarrier> {
		// Guaranteed to be present by construction.
		let consumer_cursors  = self.shared().current_consumer_cursors.as_mut().unwrap();
		let dependent_barrier = Arc::new(SingleConsumerBarrier::new(consumer_cursors.remove(0)));

		SPBuilder {
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
			dependent_barrier,
		}
	}

	/// Like [`and_then`](Self::and_then) but the resulting barrier also waits for extra cursors
	/// from other branches (e.g. created via [`branch_poller`](Self::branch_poller)).
	pub fn and_then_joining(mut self, extra_cursors: Vec<Arc<Cursor>>) -> SPBuilder<NC, E, W, MultiConsumerBarrier> {
		// Guaranteed to be present by construction.
		let consumer_cursors  = self.shared().current_consumer_cursors.as_mut().unwrap();
		let mut all_cursors   = Vec::with_capacity(1 + extra_cursors.len());
		all_cursors.push(consumer_cursors.remove(0));
		all_cursors.extend(extra_cursors);
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(all_cursors));

		SPBuilder {
			dependent_barrier,
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
		}
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initalize_state: IS) -> SPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S,
	{
		self.add_event_handler_with_state(event_handler, initalize_state);
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		}
	}
}

impl <E, W, B> SPBuilder<MC, E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Get an EventPoller.
	pub fn event_poller(mut self) -> (EventPoller<E, B>, SPBuilder<MC, E, W, B>) {
		let event_poller = self.get_event_poller();

		(event_poller,
		SPBuilder {
			state:             PhantomData,
			shared:            self.shared,
			producer_barrier:  self.producer_barrier,
			dependent_barrier: self.dependent_barrier,
		})
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		self
	}

	/// Add an event handler with state.
	pub fn handle_events_and_state_with<EH, S, IS>(mut self, event_handler: EH, initialize_state: IS) -> SPBuilder<MC, E, W, B>
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S,
	{
		self.add_event_handler_with_state(event_handler, initialize_state);
		self
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> SPBuilder<NC, E, W, MultiConsumerBarrier> {
		let consumer_cursors  = self.shared().current_consumer_cursors.replace(vec![]).unwrap();
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(consumer_cursors));

		SPBuilder {
			dependent_barrier,
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
		}
	}

	/// Like [`and_then`](Self::and_then) but the resulting barrier also waits for extra cursors
	/// from other branches (e.g. created via [`branch_poller`](Self::branch_poller)).
	pub fn and_then_joining(mut self, extra_cursors: Vec<Arc<Cursor>>) -> SPBuilder<NC, E, W, MultiConsumerBarrier> {
		let consumer_cursors  = self.shared().current_consumer_cursors.replace(vec![]).unwrap();
		let mut all_cursors   = Vec::with_capacity(consumer_cursors.len() + extra_cursors.len());
		all_cursors.extend(consumer_cursors);
		all_cursors.extend(extra_cursors);
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(all_cursors));

		SPBuilder {
			dependent_barrier,
			state:            PhantomData,
			shared:           self.shared,
			producer_barrier: self.producer_barrier,
		}
	}

	/// Finish the build and get a [`SingleProducer`].
	pub fn build(mut self) -> SingleProducer<E, MultiConsumerBarrier> {
		let consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		let consumer_barrier = MultiConsumerBarrier::new(consumer_cursors);
		SingleProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
			consumer_barrier)
	}
}
