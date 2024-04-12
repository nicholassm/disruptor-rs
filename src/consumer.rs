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
