use crate::Sequence;

/// Indicates no sequence number has been claimed (yet).
pub const NONE: Sequence = -1;

pub trait Barrier {
	/// Creates a new barrier of the specified size.
	fn new(size: usize) -> Self;

	/// Gets the sequence number of the barrier with relaxed memory ordering.
	///
	/// Note, to establish proper happens-before relationships (and thus proper synchronization),
	/// the caller must issue a [`std::sync::atomic::fence`] with [`Ordering::Acquire`].
	fn get_relaxed(&self, lower_bound: Sequence) -> Sequence;
}
