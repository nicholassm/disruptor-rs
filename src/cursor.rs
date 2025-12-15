use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::Sequence;

pub(crate) struct Cursor {
    counter: CachePadded<AtomicI64>,
}

impl Cursor {
    pub(crate) fn new(start_value: i64) -> Self {
        Self {
            counter: CachePadded::new(AtomicI64::new(start_value)),
        }
    }

    #[inline]
    pub(crate) fn compare_exchange(&self, current: Sequence, next: Sequence) -> Result<i64, i64> {
        self.counter
            .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed)
    }

    /// Stores `sequence` to the cursor with `Ordering::Release` semantics.
    #[inline]
    pub(crate) fn store(&self, sequence: Sequence) {
        self.counter.store(sequence, Ordering::Release);
    }

    /// Retrieves the cursor value with `Ordering::Relaxed` semantics.
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

        assert_eq!(cursor.compare_exchange(-1, 0).ok().unwrap(), -1);
        assert_eq!(cursor.compare_exchange(0, 1).ok().unwrap(), 0);
        // Simulate other thread having updated the cursor.
        assert_eq!(cursor.compare_exchange(0, 1).err().unwrap(), 1);

        cursor.store(100);
        assert_eq!(cursor.relaxed_value(), 100);
    }
}
