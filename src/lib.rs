//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # General Usage
//!
//! The Disruptor in this library can only be used once. I.e. it cannot be rewound and restarted.
//! It also owns and manages the processing thread(s) for the convenience of the library users.
//!
//! When the Disruptor is created, you choose whether publication to the Disruptor will happen from
//! one or multiple threads via **Producer** handles.
//! In any case, when the last Producer goes out of scope, all events published are processed and
//! then the processing thread(s) will be stopped and the entire Disruptor will be dropped.
//!
//! # Examples
//! ```
//! use disruptor::builder::build_single_producer;
//! use disruptor::producer::Producer;
//! use disruptor::BusySpin;
//! use disruptor::Sequence;
//! use disruptor::producer::RingBufferFull;
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
//! // Create a Disruptor with a ring buffer of size 8 and use the `BusySpin` wait strategy.
//! let mut builder = build_single_producer(8, factory, BusySpin);
//! let mut producer = builder.handle_events_with(processor).build();
//! // Publish into the Disruptor.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! // the Producer instance goes out of scope and the Disruptor (and the Producer) are dropped.
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

/// The type of Sequence numbers in the Ring Buffer.
pub type Sequence = i64;

pub use wait_strategies::BusySpin;
pub use wait_strategies::BusySpinWithSpinLoopHint;

mod affinity;
mod barrier;
mod consumer;
mod cursor;
mod ringbuffer;

pub mod producer;
pub mod single_producer;
pub mod multi_producer;
pub mod wait_strategies;
pub mod builder;

#[cfg(test)]
mod tests {
	use crate::builder::{build_multi_producer, build_single_producer};
	use crate::wait_strategies::BusySpin;
	use crate::producer::Producer;
	use std::sync::mpsc;
	use std::thread;

	#[derive(Debug)]
	struct Event {
		num: i64,
	}

	#[test]
	#[should_panic(expected = "Size must be power of 2.")]
	fn size_not_a_factor_of_2() {
		std::panic::set_hook(Box::new(|_| {})); // To avoid backtrace in console.
		build_single_producer(3, || { 0 }, BusySpin);
	}

	#[test]
	fn spsc_disruptor() {
		let factory   = || { Event { num: -1 }};
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		let mut producer = build_single_producer(8, factory, BusySpin)
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
	fn pipeline_of_two_spsc_disruptors() {
		let factory   = || { Event { num: -1 }};
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		// Last Disruptor.
		let mut producer = build_single_producer(8, factory, BusySpin)
			.handle_events_with(processor)
			.build();
		let processor = move |e: &Event, _, _| {
			producer.publish(|e2| e2.num = e.num );
		};

		// First Disruptor.
		let mut producer = build_single_producer(8, factory, BusySpin)
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
	fn multi_publisher_disruptor() {
		let factory   = || { Event { num: -1 }};
		let (s, r)    = mpsc::channel();
		let processor = move |e: &Event, _, _| {
			s.send(e.num).expect("Should be able to send.");
		};

		let mut producer1 = build_multi_producer(8, factory, BusySpin)
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
	fn spmc_with_dependent_consumers() {
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

		let factory      = || { Event { num: -1 }};
		let builder      = build_single_producer(8, factory, BusySpin);
		let mut producer = builder
			.handle_events_with(processor1)
			.and_then()
				.handle_events_with(processor2)
				.and_then()
					.handle_events_with(processor3)
			.build();

		producer.publish(|e| { e.num = 0; });

		drop(producer);

		let mut result: Vec<i64> = r.iter().collect();
		result.sort();
		assert_eq!(vec![1, 2, 3], result);
	}
}
