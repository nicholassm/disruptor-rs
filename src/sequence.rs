use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};

/// A sequence that can be advanced by external actors (e.g. fsync flush markers).
///
/// Defaults to `-1`, matching the initial cursor used internally so a gated stage
/// is blocked until explicitly advanced. Cache-line padded to reduce false sharing.
#[repr(align(64))]
pub struct DependentSequence(CachePadded<AtomicI64>);

impl DependentSequence {
    /// Create a new sequence initialized to `-1`.
    pub fn new() -> Self {
        Self::with_value(-1)
    }

    /// Create a sequence with the given initial value (power user escape hatch).
    pub fn with_value(v: i64) -> Self {
        Self(CachePadded::new(AtomicI64::new(v)))
    }

    /// Load the current value with `Acquire` ordering.
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    /// Store a new value with `Release` ordering.
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Release);
    }

    /// Compare and set the value with `AcqRel`/`Acquire` ordering.
    pub fn compare_and_set(&self, current: i64, new: i64) -> Result<i64, i64> {
        self.0
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

impl Default for DependentSequence {
    fn default() -> Self {
        Self::new()
    }
}
