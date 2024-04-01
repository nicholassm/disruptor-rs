use std::sync::atomic::{AtomicI64, Ordering};
use crossbeam_utils::CachePadded;

use crate::Sequence;

pub(crate) struct Cursor {
	counter: CachePadded<AtomicI64>
}

impl Cursor {
	pub(crate) fn new(start_value: i64) -> Self {
		Self {
			counter: CachePadded::new(AtomicI64::new(start_value))
		}
	}

	pub(crate) fn next(&self) -> Sequence {
		self.counter.fetch_add(1, Ordering::AcqRel) + 1
	}

	pub(crate) fn store(&self, sequence: Sequence) {
		self.counter.store(sequence, Ordering::Release);
	}

	pub(crate) fn relaxed_value(&self) -> Sequence {
		self.counter.load(Ordering::Relaxed)
	}
}
