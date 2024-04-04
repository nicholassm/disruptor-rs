use std::{sync::Arc, thread::JoinHandle};
use crate::{barrier::Barrier, cursor::Cursor, Sequence};

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
pub struct ConsumerBarrier {
	cursors: Vec<Arc<Cursor>>
}

impl ConsumerBarrier {
	pub(crate) fn new() -> Self {
		Self {
			cursors: vec![]
		}
	}

	pub(crate) fn add(&mut self, cursor: Arc<Cursor>) {
		self.cursors.push(cursor);
	}

	/// Gets the available `Sequence` of the slowest consumer.
	///
	/// Note, to establish proper happens-before relationships (and thus proper synchronization),
	/// the caller must issue a [`std::sync::atomic::fence`] with
	/// [`Ordering::Acquire`](std::sync::atomic::Ordering::Acquire).
	pub(crate) fn get(&self) -> Sequence {
		self.get_after(0)
	}
}

impl Barrier for ConsumerBarrier {
	/// Gets the available `Sequence` of the slowest consumer.
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
			let sequence = cursor.relaxed_value();
			std::cmp::min(sequence, min_sequence)
		})
	}
}
