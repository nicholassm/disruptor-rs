use std::cell::UnsafeCell;

use crate::Sequence;

unsafe impl<E> Sync for RingBuffer<E> {}

#[doc(hidden)]
pub struct RingBuffer<E> {
	slots:      Box<[UnsafeCell<E>]>,
	index_mask: i64,
}

fn is_pow_of_2(num: usize) -> bool {
	num != 0 && (num & (num - 1) == 0)
}

impl <E> RingBuffer<E> {
	pub(crate) fn new<F>(size: usize, mut event_factory: F)
	-> Self
	where
		F: FnMut() -> E
	{
		if !is_pow_of_2(size) { panic!("Size must be power of 2.") }

		let slots: Box<[UnsafeCell<E>]> = (0..size)
			.map(|_i| UnsafeCell::new(event_factory()) )
			.collect();
		let index_mask = (size - 1) as i64;

		RingBuffer {
			slots,
			index_mask,
		}
	}

	#[inline]
	pub(crate) fn wrap_point(&self, sequence: Sequence) -> Sequence {
		sequence - self.size()
	}

	/// Callers must ensure that only a single mutable reference or multiple immutable references
	/// exist at any point in time.
	#[inline]
	pub(crate) fn get(&self, sequence: Sequence) -> *mut E {
		let index = (sequence & self.index_mask) as usize;
		// SAFETY: Index is within bounds - guaranteed by invariant and index mask.
		let slot  = unsafe { self.slots.get_unchecked(index) };
		slot.get()
	}

	#[inline]
	pub(crate) fn size(&self) -> i64 {
		self.slots.len() as i64
	}
}
