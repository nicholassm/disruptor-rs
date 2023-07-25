use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use crate::DisruptorWrapper;
use crate::producer::ProducerBarrier;
use crate::wait_strategies::WaitStrategy;

pub(crate) struct Receiver {
	join_handle: Option<JoinHandle<()>>
}

impl Receiver {
	pub(crate) fn new<E, F, W, P>(wrapper: DisruptorWrapper<E, P>, mut handle: F, wait_strategy: W) -> Receiver
		where F: Send + FnMut(&E) -> () + 'static,
			  E: 'static,
			  W: WaitStrategy + Send + 'static,
			  P: ProducerBarrier + 'static
	{
		let join_handle: JoinHandle<()> = thread::spawn(move || {
			let disruptor = wrapper.unwrap();
			loop {
				let sequence = disruptor.consumer_barrier.load(Ordering::Relaxed);

				while !disruptor.is_published(sequence) {
					if disruptor.is_shutting_down() {
						// Recheck that no new published events are present.
						if !disruptor.is_published(sequence) { return }
					}
					wait_strategy.wait_for(sequence);
				}
				// SAFETY: Now, we have exclusive access to the element.
				let mut_element = disruptor.get(sequence);
				unsafe {
					let element: &E = &*mut_element;
					handle(element);
				}
				// Signal to producers that we're done processing `sequence`.
				disruptor.consumer_barrier.store(sequence + 1, Ordering::Release);
			}
		});

		Receiver { join_handle: Some(join_handle) }
	}

	pub(crate) fn join(&mut self) {
		self.join_handle.take().map(|h| { h.join().expect("Receiver should be stopped.")});
	}
}