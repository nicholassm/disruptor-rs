use crate::Sequence;

/// Indicates no sequence number has been claimed (yet).
pub const NONE: Sequence = -1;

#[doc(hidden)]
pub trait Barrier: Send + Sync {
    /// Gets the sequence number of the barrier with relaxed memory ordering.
    /// `prev` must be the last sequence returned from this barrier.
    ///
    /// Note, to establish proper happens-before relationships (and thus proper synchronization),
    /// the caller must issue a [`std::sync::atomic::fence`] with [`Ordering::Acquire`].
    fn get_after(&self, prev: Sequence) -> Sequence;
}
