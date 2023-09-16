use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::thread;
use std::thread::JoinHandle;
use core_affinity::CoreId;
use crate::DisruptorWrapper;
use crate::producer::ProducerBarrier;
use crate::wait_strategies::WaitStrategy;

pub(crate) struct Consumer {
	join_handle: Option<JoinHandle<()>>
}

/// Error indicating that the thread affinity could not be set.
struct ThreadAffinityError(String);

fn set_affinity_if_defined(core_affinity: Option<CoreId>, thread_name: &str) -> Result<(), ThreadAffinityError> {
	if let Some(core_id) = core_affinity {
		let got_pinned = core_affinity::set_for_current(core_id);
		if !got_pinned {
			let error = format!("Could not pin processor thread '{}' to {:?}", thread_name, core_id);
			return Err(ThreadAffinityError(error));
		}
	}
	Ok(())
}

impl Consumer {
	pub(crate) fn new<E, F, W, P>(
		wrapper: DisruptorWrapper<E, P>,
		processor_thread_name: &'static str,
		mut processor: F,
		processor_affinity: Option<CoreId>,
		wait_strategy: W) -> Consumer
		where F: Send + FnMut(&E, i64, bool) + 'static,
		      E: 'static,
		      W: WaitStrategy + 'static,
		      P: ProducerBarrier + 'static
	{
		// Channel is used for communicating the result of setting the thread affinity.
		let (sender, receiver)          = channel();
		let thread_name                 = processor_thread_name.to_owned();
		let thread_builder              = thread::Builder::new().name(thread_name);

		let join_handle: JoinHandle<()> = thread_builder.spawn(move || {
			let result = set_affinity_if_defined(processor_affinity, processor_thread_name);
			// Transmit result to calling thread to enable that thread to panic on errors setting
			// the thread affinity.
			sender.send(result).expect("Should be able to send.");
			drop(sender);

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
						processor(element, sequence, end_of_batch);
					}
					// Signal to producers that we're done processing `sequence`.
					sequence += 1;
					disruptor.consumer_barrier.store(sequence, Ordering::Release);
				}
			}
		}).expect("Should spawn thread.");

		let set_affinity_result = receiver.recv().expect("Should receive affinity result");
		if let Err(affinity_error) = set_affinity_result {
			eprintln!("Thread affinity error: {}", affinity_error.0);
		}

		Consumer { join_handle: Some(join_handle) }
	}

	pub(crate) fn join(&mut self) {
		if let Some(h) = self.join_handle.take() { h.join().expect("Consumer should not panic.") }
	}
}
