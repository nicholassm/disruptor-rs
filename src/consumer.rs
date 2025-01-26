use std::{sync::{atomic::{fence, AtomicI64, Ordering}, Arc}, thread::{self, JoinHandle}};
use crossbeam_utils::CachePadded;

use crate::{affinity::set_affinity_if_defined, barrier::Barrier, builder::Shared, cursor::Cursor, wait_strategies::WaitStrategy, Sequence};

#[doc(hidden)]
pub struct Consumer {
	join_handle: Option<JoinHandle<()>>,
}

impl Consumer {
	pub(crate) fn new(join_handle: JoinHandle<()>) -> Self {
		Self {
			join_handle: Some(join_handle),
		}
	}

	pub(crate) fn join(&mut self) {
		if let Some(h) = self.join_handle.take() { h.join().expect("Consumer should not panic.") }
	}
}

#[doc(hidden)]
pub struct SingleConsumerBarrier {
	cursor: Arc<Cursor>
}

#[doc(hidden)]
pub struct MultiConsumerBarrier {
	cursors: Vec<Arc<Cursor>>
}

impl SingleConsumerBarrier {
	pub(crate) fn new(cursor: Arc<Cursor>) -> Self {
		Self {
			cursor
		}
	}
}

impl Barrier for SingleConsumerBarrier {
	#[inline]
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursor.relaxed_value()
	}
}

impl MultiConsumerBarrier {
	pub(crate) fn new(cursors: Vec<Arc<Cursor>>) -> Self {
		Self {
			cursors
		}
	}
}

impl Barrier for MultiConsumerBarrier {
	/// Gets the available `Sequence` of the slowest consumer.
	#[inline]
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
			let sequence = cursor.relaxed_value();
			std::cmp::min(sequence, min_sequence)
		})
	}
}

pub(crate) fn start_processor<E, EP, W, B> (
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
			while let Some(available) = wait_for_events(sequence, &shutdown_at_sequence, barrier.as_ref(), &wait_strategy) {
				while available >= sequence { // Potentiel batch processing.
					let end_of_batch = available == sequence;
					// SAFETY: Now, we have (shared) read access to the event at `sequence`.
					let event_ptr    = ring_buffer.get(sequence);
					let event        = unsafe { & *event_ptr };
					event_handler(event, sequence, end_of_batch);
					// Update next sequence to read.
					sequence += 1;
				}
				// Signal to producers or later consumers that we're done processing `sequence - 1`.
				consumer_cursor.store(sequence - 1);
			}
		}).expect("Should spawn thread.")
	};

	let consumer = Consumer::new(join_handle);
	(consumer_cursor, consumer)
}

pub(crate) fn start_processor_with_state<E, EP, W, B, S, IS> (
	mut event_handler: EP,
	builder:           &mut Shared<E, W>,
	barrier:           Arc<B>,
	initialize_state:  IS)
-> (Arc<Cursor>, Consumer)
where
	E:  'static + Send + Sync,
	IS: 'static + Send + FnOnce() -> S,
	EP: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
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
			let mut state    = initialize_state();
			while let Some(available_sequence) = wait_for_events(sequence, &shutdown_at_sequence, barrier.as_ref(), &wait_strategy) {
				while available_sequence >= sequence { // Potentiel batch processing.
					let end_of_batch = available_sequence == sequence;
					// SAFETY: Now, we have (shared) read access to the event at `sequence`.
					let event_ptr    = ring_buffer.get(sequence);
					let event        = unsafe { & *event_ptr };
					event_handler(&mut state, event, sequence, end_of_batch);
					// Update next sequence to read.
					sequence += 1;
				}
				// Signal to producers or later consumers that we're done processing `sequence - 1`.
				consumer_cursor.store(sequence - 1);
			}
		}).expect("Should spawn thread.")
	};

	let consumer = Consumer::new(join_handle);
	(consumer_cursor, consumer)
}

#[inline]
fn wait_for_events<B, W>(
	sequence:             Sequence,
	shutdown_at_sequence: &CachePadded<AtomicI64>,
	barrier:              &B,
	wait_strategy:        &W
)
-> Option<Sequence>
where
	B: Barrier + Send + Sync,
	W: WaitStrategy,
{
	let mut available = barrier.get_after(sequence);
	while available < sequence {
		// If publisher(s) are done publishing events we're done when we've seen the last event.
		if shutdown_at_sequence.load(Ordering::Relaxed) == sequence {
			return None;
		}
		wait_strategy.wait_for(sequence);
		available = barrier.get_after(sequence);
	}
	fence(Ordering::Acquire);
	Some(available)
}
