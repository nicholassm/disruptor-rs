use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use crate::DisruptorWrapper;
use crate::producer::ProducerBarrier;
use crate::wait_strategies::WaitStrategy;

pub(crate) struct Consumer {
	join_handle: Option<JoinHandle<()>>
}

impl Consumer {
	pub(crate) fn new<E, F, W, P>(wrapper: DisruptorWrapper<E, P>, mut process: F, wait_strategy: W) -> Consumer where
		F: Send + FnMut(&E, i64, bool) + 'static,
		E: 'static,
		W: WaitStrategy + 'static,
		P: ProducerBarrier + 'static
	{
		let join_handle: JoinHandle<()> = thread::spawn(move || {
			let disruptor    = wrapper.unwrap();
			let mut sequence = 0i64;
			loop {
				let mut available = disruptor.get_highest_published(sequence);

				while available < sequence {
					if disruptor.is_shutting_down() {
						// Recheck that no new published events are present.
						if disruptor.get_highest_published(sequence) < sequence { return }
					}
					wait_strategy.wait_for(sequence);
					available = disruptor.get_highest_published(sequence);
				}

				while available >= sequence {
					let end_of_batch = available == sequence;
					// SAFETY: Now, we have exclusive access to the element at `sequence`.
					let mut_element  = disruptor.get(sequence);
					unsafe {
						let element: &E = &*mut_element;
						process(element, sequence, end_of_batch);
					}
					// Signal to producers that we're done processing `sequence`.
					sequence += 1;
					disruptor.consumer_barrier.store(sequence, Ordering::Release);
				}
			}
		});

		Consumer { join_handle: Some(join_handle) }
	}

	pub(crate) fn join(&mut self) {
		if let Some(h) = self.join_handle.take() { h.join().expect("Receiver should be stopped.") }
	}
}
