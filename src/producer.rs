use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};
use crate::consumer::Receiver;
use crate::Disruptor;

pub(crate) trait ProducerBarrier {
	fn publish(&self, sequence: i64);
	fn is_published(&self, sequence: i64) -> bool;
}

pub(crate) struct SingleProducerBarrier {
	cursor: CachePadded<AtomicI64>
}

impl SingleProducerBarrier {
	pub(crate) fn new() -> SingleProducerBarrier {
		let cursor = CachePadded::new(AtomicI64::new(-1));
		SingleProducerBarrier { cursor }
	}
}

impl ProducerBarrier for SingleProducerBarrier {
	#[inline]
	fn publish(&self, sequence: i64) {
		self.cursor.store(sequence, Ordering::Release);
	}

	#[inline]
	fn is_published(&self, sequence: i64) -> bool {
		self.cursor.load(Ordering::Acquire) >= sequence
	}
}

pub struct Producer<E> {
	disruptor: *mut Disruptor<E, SingleProducerBarrier>,
	receiver: Receiver,
	available_publisher_sequence: i64,
}

unsafe impl<E: Send> Send for Producer<E> {}

impl<E> Producer<E> {
	pub(crate) fn new(
		disruptor: *mut Disruptor<E, SingleProducerBarrier>,
		receiver: Receiver,
		available_publisher_sequence: i64
	) -> Self {
		Producer {
			disruptor,
			receiver,
			available_publisher_sequence
		}
	}

	fn disruptor(&self) -> &Disruptor<E, SingleProducerBarrier> {
		unsafe { &*self.disruptor }
	}

	/// Publish an Event into the Disruptor.
	#[inline]
	pub fn publish<F: FnOnce(&mut E) -> ()>(&mut self, update: F) {
		let disruptor = self.disruptor();
		// Only one producer can publish so we can load it once.
		let last_produce_seq = disruptor.producer_barrier.cursor.load(Ordering::Relaxed);
		let sequence = last_produce_seq + 1;

		if self.available_publisher_sequence < sequence {
			// We have to check where the consumer is and potential wait for it if we're about to
			// publish into the slot currently being read by the consumer.
			// (Consumer is an entire ring buffer behind the publisher).
			let wrap_point            = disruptor.wrap_point(sequence);
			let mut consumer_sequence = disruptor.consumer_barrier.load(Ordering::Acquire);
			while consumer_sequence <= wrap_point {
				// TODO: Removed wait strategy here. This should return Result<i64, FullRingBuffer> instead.
				consumer_sequence = disruptor.consumer_barrier.load(Ordering::Acquire);
			}
			// We can now continue a full round until we get right behind the consumer's current
			// position without checking where it actually is.
			self.available_publisher_sequence = consumer_sequence + disruptor.ring_buffer_size - 1;
		}

		// SAFETY: Now, we have exclusive access to the element at `sequence` and a producer
		// can now update the data.
		let disruptor = self.disruptor(); // Re-borrow as mutated above.
		unsafe {
			let element = &mut *disruptor.get(sequence);
			update(element);
		}
		// Publish by publishing `sequence`.
		disruptor.producer_barrier.publish(sequence);
	}
}

/// Stops the processor thread and drops the Disruptor, the processor thread and the [Producer].
impl<E> Drop for Producer<E> {
	fn drop(&mut self) {
		self.disruptor().shut_down();
		self.receiver.join();

		// Safety: Both publishers and receivers are done accessing the Disruptor.
		unsafe {
			drop(Box::from_raw(self.disruptor));
		}
	}
}