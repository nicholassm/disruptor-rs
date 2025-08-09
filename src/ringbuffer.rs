use std::cell::UnsafeCell;

use crate::Sequence;

unsafe impl<E> Sync for RingBuffer<E> {}

#[doc(hidden)]
pub struct RingBuffer<E> {
	slots:      Box<[UnsafeCell<E>]>,
	index_mask: i64,
}

impl <E> RingBuffer<E> {
	pub(crate) fn new<F>(size: usize, mut event_factory: F)
	-> Self
	where
		F: FnMut() -> E
	{
		if !size.is_power_of_two() { panic!("Size must be power of 2.") }

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
	pub(crate) fn free_slots(&self, producer_sequence: Sequence, highest_read_by_consumers: Sequence) -> i64 {
		// The producer is at `producer_sequence`. The slowest consumer is at
		// `highest_read_by_consumers`.
		// The number of items in flight is `producer_sequence - highest_read_by_consumers`.
		// The number of free slots is `size - in_flight`.
		//
		// Example: size=8, producer=3, consumer=0.
		// In flight: 3 - 0 = 3.
		// Free: 8 - 3 = 5.
		// Note: `producer_sequence` is the *next* sequence to be written, so sequences 0, 1, 2
		// are in flight. The item at sequence 0 is available to be overwritten by the producer
		// when `highest_read_by_consumers` becomes 0.
		// The number of occupied slots is `producer_sequence - highest_read_by_consumers`.
		// So, `free_slots` is `self.size() - (producer_sequence - highest_read_by_consumers)`.
		//
		// The producer is not allowed to lap the slowest consumer.
		// `producer_sequence` must not be more than `size` ahead of `highest_read_by_consumers`.
		// `producer_sequence` - `highest_read_by_consumers` <= `size`.
		self.size() - (producer_sequence - highest_read_by_consumers)
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


#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn free_slots() {
		let ring_buffer = RingBuffer::new(8, || { 0 });

		// Round 1:
		// producer_sequence: 7, highest_read_by_consumers: 0
		assert_eq!(1, ring_buffer.free_slots(7, 0));
		// producer_sequence: 8, highest_read_by_consumers: 0
		assert_eq!(0, ring_buffer.free_slots(8, 0));
		// producer_sequence: 0, highest_read_by_consumers: 0
		assert_eq!(8, ring_buffer.free_slots(0, 0));
		// producer_sequence: 3, highest_read_by_consumers: -1 (consumer hasn't read item 0 yet)
		assert_eq!(4, ring_buffer.free_slots(3, -1));
		// producer_sequence: 7, highest_read_by_consumers: -1
		assert_eq!(0, ring_buffer.free_slots(7, -1));
		// producer_sequence: 7, highest_read_by_consumers: 0
		assert_eq!(1, ring_buffer.free_slots(7, 0));
		// producer_sequence: 5, highest_read_by_consumers: 2
		assert_eq!(5, ring_buffer.free_slots(5, 2));
		// producer_sequence: 5, highest_read_by_consumers: 3
		assert_eq!(6, ring_buffer.free_slots(5, 3));
		// producer_sequence: 5, highest_read_by_consumers: 4
		assert_eq!(7, ring_buffer.free_slots(5, 4));
		// producer_sequence: 5, highest_read_by_consumers: 5
		assert_eq!(8, ring_buffer.free_slots(5, 5));

		// Round 2:
		// producer_sequence: 11, highest_read_by_consumers: 9
		assert_eq!(6, ring_buffer.free_slots(11, 9));
		// producer_sequence: 12, highest_read_by_consumers: 12
		assert_eq!(8, ring_buffer.free_slots(12, 12));
		// producer_sequence: 15, highest_read_by_consumers: 7
		assert_eq!(0, ring_buffer.free_slots(15, 7));
	}
}
