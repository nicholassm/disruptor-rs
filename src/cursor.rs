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

	#[inline]
	pub(crate) fn next(&self) -> Sequence {
		self.counter.fetch_add(1, Ordering::AcqRel) + 1
	}

	#[inline]
	pub(crate) fn store(&self, sequence: Sequence) {
		self.counter.store(sequence, Ordering::Release);
	}

	#[inline]
	pub(crate) fn relaxed_value(&self) -> Sequence {
		self.counter.load(Ordering::Relaxed)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cursor_operations() {
		let cursor = Cursor::new(-1);

		assert_eq!(cursor.next(), 0);
		assert_eq!(cursor.next(), 1);

		cursor.store(100);
		assert_eq!(cursor.relaxed_value(), 100);
	}
}
