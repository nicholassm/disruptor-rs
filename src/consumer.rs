use std::{sync::Arc, thread::JoinHandle};
use crate::{barrier::Barrier, cursor::Cursor, Sequence};

pub struct ConsumerBarrier {
	cursors: Vec<Arc<Cursor>>
}

impl ConsumerBarrier {
	pub(crate) fn add(&mut self, cursor: Arc<Cursor>) {
		self.cursors.push(cursor);
	}
}

impl Barrier for ConsumerBarrier {
	fn new(_size: usize) -> Self {
		Self {
			cursors: vec![]
		}
	}

	/// Gets the available `Sequence` of the slowest consumer.
	fn get_relaxed(&self, _lower_bound: Sequence) -> Sequence {
		self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
			let sequence = cursor.relaxed_value();
			std::cmp::min(sequence, min_sequence)
		})
	}
}

pub(crate) struct Consumer {
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
