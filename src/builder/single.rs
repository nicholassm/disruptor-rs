use std::sync::Arc;

use crate::{barrier::Barrier, consumer::{MultiConsumerBarrier, SingleConsumerBarrier}, producer::single::{SingleProducer, SingleProducerBarrier}, wait_strategies::WaitStrategy, builder::ProcessorSettings, Sequence};

use super::{Builder, Shared};

pub struct SPBuilder<E, W, B> {
	shared:            Shared<E, W>,
	producer_barrier:  Arc<SingleProducerBarrier>,
	dependent_barrier: Arc<B>,
}

pub struct SPSCBuilder<E, W, B> {
	parent: SPBuilder<E, W, B>,
}

pub struct SPMCBuilder<E, W, B> {
	parent: SPBuilder<E, W, B>,
}

impl<E, W, B> ProcessorSettings<E, W> for SPBuilder<E, W, B> {
	fn shared(&mut self) -> &mut Shared<E, W> {
		&mut self.shared
	}
}

impl<E, W, B> ProcessorSettings<E, W> for SPSCBuilder<E, W, B> {
	fn shared(&mut self) -> &mut Shared<E, W> {
		self.parent.shared()
	}
}

impl<E, W, B> ProcessorSettings<E, W> for SPMCBuilder<E, W, B> {
	fn shared(&mut self) -> &mut Shared<E, W> {
		self.parent.shared()
	}
}

impl<E, W, B> Builder<E, W, B> for SPBuilder<E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	fn dependent_barrier(&self) -> Arc<B> {
		Arc::clone(&self.dependent_barrier)
	}
}

impl<E, W, B> Builder<E, W, B> for SPSCBuilder<E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	fn dependent_barrier(&self) -> Arc<B> {
		self.parent.dependent_barrier()
	}
}

impl<E, W, B> Builder<E, W, B> for SPMCBuilder<E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	fn dependent_barrier(&self) -> Arc<B> {
		self.parent.dependent_barrier()
	}
}

impl <E, W, B> SPBuilder<E, W, B>
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
			shared,
			producer_barrier,
			dependent_barrier,
		}
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPSCBuilder<E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		SPSCBuilder { parent: self }
	}
}

impl <E, W, B> SPSCBuilder<E, W, B>
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
			self.parent.shared.shutdown_at_sequence,
			self.parent.shared.ring_buffer,
			self.parent.producer_barrier,
			self.parent.shared.consumers,
		consumer_barrier)
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> SPBuilder<E, W, SingleConsumerBarrier> {
		// Guaranteed to be present by construction.
		let consumer_cursors  = self.shared().current_consumer_cursors.as_mut().unwrap();
		let dependent_barrier = Arc::new(SingleConsumerBarrier::new(consumer_cursors.remove(0)));

		SPBuilder {
			shared: self.parent.shared,
			producer_barrier: self.parent.producer_barrier,
			dependent_barrier,
		}
	}

	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPMCBuilder<E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		SPMCBuilder { parent: self.parent }
	}
}

impl <E, W, B> SPMCBuilder<E, W, B>
where
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
	B: 'static + Barrier,
{
	/// Add an event handler.
	pub fn handle_events_with<EH>(mut self, event_handler: EH) -> SPMCBuilder<E, W, B>
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		self.add_event_handler(event_handler);
		self
	}

	/// Complete the (concurrent) consumption of events so far and let new consumers process
	/// events after all previous consumers have read them.
	pub fn and_then(mut self) -> SPBuilder<E, W, MultiConsumerBarrier> {
		let consumer_cursors  = self.shared().current_consumer_cursors.replace(vec![]).unwrap();
		let dependent_barrier = Arc::new(MultiConsumerBarrier::new(consumer_cursors));

		SPBuilder {
			shared: self.parent.shared,
			producer_barrier: self.parent.producer_barrier,
			dependent_barrier,
		}
	}

	/// Finish the build and get a [`SingleProducer`].
	pub fn build(mut self) -> SingleProducer<E, MultiConsumerBarrier> {
		let consumer_cursors = self.shared().current_consumer_cursors.take().unwrap();
		let consumer_barrier = MultiConsumerBarrier::new(consumer_cursors);
		SingleProducer::new(
			self.parent.shared.shutdown_at_sequence,
			self.parent.shared.ring_buffer,
			self.parent.producer_barrier,
			self.parent.shared.consumers,
			consumer_barrier)
	}
}
