//! Module for structs for building a Multi Producer Disruptor in a type safe way.
//!
//! To get started building a Multi Producer Disruptor, invoke [super::build_multi_producer].

use std::{marker::PhantomData, sync::Arc};

use crate::{Sequence, barrier::Barrier, builder::ProcessorSettings, consumer::{MultiConsumerBarrier, SingleConsumerBarrier, event_poller::{EventPoller, JoinPromise}}, cursor::Cursor, producer::multi::{MultiProducer, MultiProducerBarrier}, wait_strategies::WaitStrategy};

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
	pub fn branch_poller(&mut self) -> JoinPromise<E, B> {
		let cursor = Arc::new(Cursor::new(-1));
		let poller = EventPoller::new(
			Arc::clone(&self.shared.ring_buffer),
			Arc::clone(&self.dependent_barrier),
			Arc::clone(&self.shared.shutdown_at_sequence),
			Arc::clone(&cursor));
		JoinPromise::new(poller)
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

	/// Join a branched `EventPoller` back into the main flow.
	pub fn join<B2>(mut self, join_promise: JoinPromise<E, B2>) -> (EventPoller<E, B2>, MPBuilder<SC, E, W, B>) {
		self.add_cursor_from_branched_poller(&join_promise);

		(
			join_promise.into_poller(),
			MPBuilder {
				state:             PhantomData,
				shared:            self.shared,
				producer_barrier:  self.producer_barrier,
				dependent_barrier: self.dependent_barrier,
			}
		)
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

	/// Join a branched `EventPoller` back into the main flow.
	pub fn join<B2>(mut self, join_promise: JoinPromise<E, B2>) -> (EventPoller<E, B2>, MPBuilder<MC, E, W, B>) {
		self.add_cursor_from_branched_poller(&join_promise);
		
		(
			join_promise.into_poller(),
			MPBuilder {
				state:             PhantomData,
				shared:            self.shared,
				producer_barrier:  self.producer_barrier,
				dependent_barrier: self.dependent_barrier,
			}
		)
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

	/// Finish the build and get a [`MultiProducer`].
	pub fn build(mut self) -> MultiProducer<E, SingleConsumerBarrier> {
		let mut consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		// Guaranteed to be present by construction.
		let consumer_barrier     = SingleConsumerBarrier::new(consumer_cursors.remove(0));
		MultiProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
			consumer_barrier)
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

	/// Join a branched `EventPoller` back into the main flow.
	pub fn join<B2>(mut self, join_promise: JoinPromise<E, B2>) -> (EventPoller<E, B2>, MPBuilder<MC, E, W, B>) {
		self.add_cursor_from_branched_poller(&join_promise);

		(
			join_promise.into_poller(),
			MPBuilder {
				state:             PhantomData,
				shared:            self.shared,
				producer_barrier:  self.producer_barrier,
				dependent_barrier: self.dependent_barrier,
			}
		)
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

	/// Finish the build and get a [`MultiProducer`].
	pub fn build(mut self) -> MultiProducer<E, MultiConsumerBarrier> {
		let consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		let consumer_barrier = MultiConsumerBarrier::new(consumer_cursors);
		MultiProducer::new(
			self.shared.shutdown_at_sequence,
			self.shared.ring_buffer,
			self.producer_barrier,
			self.shared.consumers,
			consumer_barrier)
	}
}
