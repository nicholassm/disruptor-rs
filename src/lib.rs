//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # General Usage
//!
//! The usage can be divided into three stages:
//! 1. **Setup:** Build the Disruptor and setup consumers including any interdependencies.
//! 2. **Publish:** Publish into the Disruptor.
//! 3. **Shutdown:** Stop all consumer threads and drop the Disruptor and the consumer(s).
//!
//! The Disruptor in this library can only be used once. I.e. it cannot be rewound and restarted.
//!
//! It also owns and manages the processing thread(s) for the convenience of the library users.
//!
//! ## Setup
//!
//! When the Disruptor is created, you choose whether publication to the Disruptor will happen from
//! one or multiple threads via [`Producer`] handles. The size of the
//! RingBuffer is also specified and you have to provide a "factory" closure for initializing the
//! events inside the RingBufffer. Then all consumers are added.
//!
//! Use either [`build_single_producer`] or [`build_multi_producer`] to get started.
//!
//! ## Publish
//!
//! Once the Disruptor is built, you have a [`Producer`] "handle" which can be used to publish into
//! the Disruptor. In case of a multi producer Disruptor, the [`Producer`] can be cloned so that
//! each publishing thread has its own handle.
//!
//! ## Shutdown
//!
//! Finally, when there's no more events to publish and the last Producer goes out of scope, all events
//! published are processed and then the processing thread(s) will be stopped and the entire Disruptor
//! will be dropped including consumers.
//!
//! # Examples
//!
//! ### Basic Usage:
//!
//! ```
//! use disruptor::*;
//!
//! // *** Phase SETUP ***
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Define a factory for populating the ring buffer with events.
//! let factory = || { Event { price: 0.0 }};
//!
//! // Define a closure for processing events. A thread, controlled by the disruptor, will run this
//! // processor closure each time an event is published.
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Process the Event `e` published at `sequence`.
//!     // If `end_of_batch` is false, you can batch up events until it's invoked with
//!     // `end_of_batch` being true.
//! };
//!
//! // Create a Disruptor with a ring buffer of size 8 and use the `BusySpin` wait strategy.
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // *** Phase PUBLISH ***
//!
//! // Publish into the Disruptor.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! // *** Phase SHUTDOWN ***
//!
//! // The Producer instance goes out of scope and the Disruptor, the processor (consumer) and
//! // the Producer are dropped.
//! ```
//!
//! ### Batch Publication:
//!
//! ```
//! use disruptor::*;
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! let factory = || { Event { price: 0.0 }};
//!
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Processing logic.
//! };
//!
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // Batch publish into the Disruptor - 5 events at a time.
//! for i in 0..10 {
//!     producer.batch_publish(5, |iter| {
//!         // `iter` is guaranteed to yield 5 events.
//!         for e in iter {
//!             e.price = i as f64;
//!         }
//!     });
//! }
//! ```
//!
//! ###  Multiple Producers and Multiple, Pined Consumers:
//! ```
//! use disruptor::*;
//! use std::thread;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! # #[cfg(miri)] fn main() {}
//! # #[cfg(not(miri))]
//! fn main() {
//!     // Factory closure for initializing events in the Ring Buffer.
//!     let factory = || { Event { price: 0.0 }};
//!
//!     // Closure for processing events.
//!     let h1 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Processing logic here.
//!     };
//!     let h2 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Some processing logic here.
//!     };
//!     let h3 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // More processing logic here.
//!     };
//!
//!     let mut producer1 = disruptor::build_multi_producer(64, factory, BusySpin)
//!         // `h2` handles events concurrently with `h1`.
//!         .pined_at_core(1).handle_events_with(h1)
//!         .pined_at_core(2).handle_events_with(h2)
//!             .and_then()
//!             // `h3` handles events after `h1` and `h2`.
//!             .pined_at_core(3).handle_events_with(h3)
//!         .build();
//!
//!     // Create another producer.
//!     let mut producer2 = producer1.clone();
//!
//!     // Publish into the Disruptor.
//!     thread::scope(|s| {
//!         s.spawn(move || {
//!             for i in 0..10 {
//!                 producer1.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!         s.spawn(move || {
//!             for i in 10..20 {
//!                 producer2.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!     });
//! }// At this point, the Producers instances go out of scope and when the
//!  // processors are done handling all events then the Disruptor is dropped
//!  // as well.
//! ```
//!
//! ### Adding Custom State That is Neither `Send` Nor `Sync`:
//!
//! ```
//! use std::{cell::RefCell, rc::Rc};
//! use disruptor::*;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Your custom state.
//! #[derive(Default)]
//! struct State {
//!     data: Rc<RefCell<i32>>
//! }
//!
//! let factory = || { Event { price: 0.0 }};
//! let initial_state = || { State::default() };
//!
//! // Closure for processing events *with* state.
//! let processor = |s: &mut State, e: &Event, _: Sequence, _: bool| {
//!     // Mutate your custom state:
//!     *s.data.borrow_mut() += 1;
//! };
//!
//! let size = 64;
//! let mut producer = disruptor::build_single_producer(size, factory, BusySpin)
//!     .handle_events_and_state_with(processor, initial_state)
//!     .build();
//!
//! // Publish into the Disruptor via the `Producer` handle.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

/// The type for Sequence numbers in the Ring Buffer ([`i64`]).
pub type Sequence = i64;

pub mod wait_strategies;
mod affinity;
mod barrier;
mod consumer;
mod cursor;
mod ringbuffer;
mod producer;
mod builder;

pub use crate::builder::{build_single_producer, build_multi_producer, ProcessorSettings};
pub use crate::producer::{Producer, RingBufferFull, MissingFreeSlots};
pub use crate::wait_strategies::{BusySpin, BusySpinWithSpinLoopHint};
pub use crate::producer::{single::SingleProducer, multi::MultiProducer};
pub use crate::consumer::{SingleConsumerBarrier, MultiConsumerBarrier};

#[cfg(test)]
mod tests {
	use std::rc::Rc;
	use std::cell::RefCell;
	use std::collections::HashSet;
	use std::sync::atomic::AtomicBool;
	use std::sync::atomic::Ordering::Relaxed;
	use std::sync::{mpsc, Arc};
	use std::thread;

	use producer::MissingFreeSlots;

	use super::*;

	#[derive(Debug)]
	struct Event {
		num: i64,
	}

	fn factory() -> impl Fn() -> Event {
		|| { Event { num: -1 }}
	}

	#[test]
	#[should_panic(expected = "Size must be power of 2.")]
	fn size_not_a_factor_of_2() {
		build_single_producer(3, || { 0 }, BusySpin);
	}

	#[test]
	fn full_ringbuffer() {
		let (s, r)    = mpsc::channel();
		let barrier   = Arc::new(AtomicBool::new(true));
		let processor = {
			let barrier = Arc::clone(&barrier);
			move |e: &Event, _, _| {
				while barrier.load(Relaxed) { /* Wait. */ }
				s.send(e.num).expect("Should be able to send.");
			}
		};
		let mut producer = build_single_producer(4, factory(), BusySpinWithSpinLoopHint)
			.handle_events_with(processor)
			.build();

		for i in 0..4 {
			producer.try_publish(|e| e.num = i).expect("Should publish");
		}
		// Now ring buffer is full.
		assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 4).err().unwrap());
		// And it stays full.
		assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 4).err().unwrap());
		// Until the processor continues reading events.
		barrier.store(false, Relaxed);
		producer.publish(|e| e.num = 4);

		drop(producer);
		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 2, 3, 4]);
	}

	#[test]
	fn insufficient_space_for_batch_publication() {
		let (s, r)    = mpsc::channel();
		let barrier   = Arc::new(AtomicBool::new(true));
		let processor = {
			let barrier = Arc::clone(&barrier);
			move |e: &Event, _, _| {
				while barrier.load(Relaxed) { /* Wait. */ }
				s.send(e.num).expect("Should be able to send.");
			}
		};
		let mut producer = build_single_producer(4, factory(), BusySpin)
			.handle_events_with(processor)
			.build();

		for i in 0..2 {
			producer.publish(|e| e.num = i);
		}
		assert_eq!(MissingFreeSlots(2),   producer.try_batch_publish(4, |_iter| {} ).err().unwrap());
		assert_eq!(MissingFreeSlots(100), producer.try_batch_publish(102, |_iter| {} ).err().unwrap());

		barrier.store(false, Relaxed);
		drop(producer);

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1]);
	}

	#[test]
	fn spsc_disruptor() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};
		let mut producer = build_single_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();

		thread::scope(|s| {
			s.spawn(move || {
				for i in 0..10 {
					producer.publish(|e| e.num = i*i );
				}
			});
		});

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
	}

	#[test]
	fn spsc_disruptor_with_batch_publication() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};
		let mut producer = build_single_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();

		let mut i = 0;
		for _ in 0..3 {
			producer.batch_publish(3, |iter| {
				// We are guaranteed that the iterator will yield three elements:
				assert_eq!((3, Some(3)), iter.size_hint());

				// Publish.
				for e in iter {
					e.num = i*i;
					i    += 1;
				}
			});
		}
		drop(producer);

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64]);
	}

	#[test]
	fn spsc_disruptor_with_zero_batch_publication() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};
		let mut producer = build_single_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();

		producer.batch_publish(0, |iter| {
			for e in iter {
				e.num = 1;
			}
		});
		drop(producer);

		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, []);
	}

	#[test]
	fn spsc_disruptor_with_state() {
		let (s, r)           = mpsc::channel();
		let initialize_state = || { Rc::new(RefCell::new(0)) };
		let processor        = move |state: &mut Rc<RefCell<i64>>, e: &Event, _, _| {
			let mut ref_cell = state.borrow_mut();
			*ref_cell       += e.num;
			s.send(*ref_cell).expect("Should be able to send.");
		};
		let mut producer     = build_single_producer(8, factory(), BusySpin)
			.handle_events_and_state_with(processor, initialize_state)
			.build();

		for i in 0..10 {
			producer.publish(|e| e.num = i );
		}

		drop(producer);
		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]);
	}

	#[test]
	fn pipeline_of_two_spsc_disruptors() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		// Last Disruptor.
		let mut producer = build_single_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();
		let processor = move |e: &Event, _, _| {
			producer.publish(|e2| e2.num = e.num );
		};

		// First Disruptor.
		let mut producer = build_single_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();
		for i in 0..10 {
			producer.publish(|e| e.num = i*i );
		}

		drop(producer);
		let result: Vec<_> = r.iter().collect();
		assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
	}

	#[test]
	fn multi_publisher_disruptor() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		let mut producer1 = build_multi_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();
		let mut producer2 = producer1.clone();

		let num_items = 100;

		thread::scope(|s| {
			s.spawn(move || {
				for i in 0..num_items/2 {
					producer1.publish(|e| e.num = i);
				}
			});

			s.spawn(move || {
				for i in (num_items/2)..num_items {
					producer2.publish(|e| e.num = i);
				}
			});
		});

		let mut result: Vec<_> = r.iter().collect();
		result.sort();

		let expected: Vec<i64> = (0..num_items).collect();
		assert_eq!(result, expected);
	}

	#[test]
	fn multi_publisher_disruptor_with_batch_publication() {
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		let mut producer1 = build_multi_producer(8, factory(), BusySpin)
			.handle_events_with(processor)
			.build();
		let mut producer2 = producer1.clone();

		let num_items  = 100_i64;
		let batch_size = 5;

		thread::scope(|s| {
			s.spawn(move || {
				for b in 0..(num_items/2)/batch_size {
					producer1.batch_publish(batch_size as usize, |iter| {
						for (i, e) in iter.enumerate() {
							e.num = batch_size*b + i as i64;
						}
					});
				}
			});

			s.spawn(move || {
				for i in (num_items/2)..num_items {
					producer2.publish(|e| e.num = i as i64);
				}
			});
		});

		let mut result: Vec<_> = r.iter().collect();
		result.sort();

		let expected: Vec<i64> = (0..num_items).collect();
		assert_eq!(result, expected);
	}

	#[test]
	fn spmc_with_concurrent_consumers() {
		let (s, r) = mpsc::channel();

		let processor1 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 1).unwrap(); }
		};
		let processor2 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 2).unwrap(); }
		};
		let processor3 = {
			move |e: &Event, _, _| { s.send(e.num + 3).unwrap(); }
		};

		let builder      = build_single_producer(8, factory(), BusySpin);
		let mut producer = builder
			.handle_events_with(processor1)
			.handle_events_with(processor2)
			.handle_events_with(processor3)
			.build();

		producer.publish(|e| { e.num = 0; });
		drop(producer);

		let result: HashSet<i64> = r.iter().collect();
		let expected = HashSet::from([1, 2, 3]);
		assert_eq!(expected, result);
	}

	#[test]
	fn spmc_with_dependent_consumers_and_some_with_state() {
		let (s, r) = mpsc::channel();

		let processor1 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 0).unwrap(); }
		};
		let processor2 = {
			let s = s.clone();
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 10).unwrap();
			}
		};
		let processor3 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 2).unwrap(); }
		};
		let processor4 = {
			let s = s.clone();
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 20).unwrap();
			}
		};
		let processor5 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 4).unwrap(); }
		};
		let processor6 = {
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 30).unwrap();
			}
		};

		let builder      = build_single_producer(8, factory(), BusySpin);

		let mut producer = builder
			.handle_events_with(processor1)
			.handle_events_and_state_with(processor2, || { RefCell::new(0) })
			.and_then()
				.handle_events_with(processor3)
				.handle_events_and_state_with(processor4, || { RefCell::new(0) })
				.and_then()
					.handle_events_with(processor5)
					.handle_events_and_state_with(processor6, || { RefCell::new(0) })
			.build();

		producer.publish(|e| { e.num = 1; });
		drop(producer);

		let mut result: Vec<i64> = r.iter().collect();
		result.sort();
		assert_eq!(vec![1, 3, 5, 11, 21, 31], result);
	}

	#[test]
	fn mpmc_with_dependent_consumers_and_some_with_state() {
		let (s, r) = mpsc::channel();

		let processor1 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 0).unwrap(); }
		};
		let processor2 = {
			let s = s.clone();
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 10).unwrap();
			}
		};
		let processor3 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 2).unwrap(); }
		};
		let processor4 = {
			let s = s.clone();
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 20).unwrap();
			}
		};
		let processor5 = {
			let s = s.clone();
			move |e: &Event, _, _| { s.send(e.num + 4).unwrap(); }
		};
		let processor6 = {
			move |state: &mut RefCell<i64>, e: &Event, _, _| {
				*state.borrow_mut() += e.num;
				s.send(*state.borrow() + 30).unwrap();
			}
		};

		let builder      = build_multi_producer(8, factory(), BusySpin);
		let mut producer1 = builder
			.handle_events_with(processor1)
			.handle_events_and_state_with(processor2, || { RefCell::new(0) })
			.and_then()
				.handle_events_with(processor3)
				.handle_events_and_state_with(processor4, || { RefCell::new(0) })
				.and_then()
					.handle_events_with(processor5)
					.handle_events_and_state_with(processor6, || { RefCell::new(0) })
			.build();

		let mut producer2 = producer1.clone();

		thread::scope(|s| {
			s.spawn(move || {
				producer1.publish(|e| e.num = 1);
			});

			s.spawn(move || {
				producer2.publish(|e| e.num = 1);
			});
		});

		let mut result: Vec<i64> = r.iter().collect();
		result.sort();
		assert_eq!(vec![1, 1, 3, 3, 5, 5, 11, 12, 21, 22, 31, 32], result);
	}
}
