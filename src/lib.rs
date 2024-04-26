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
pub use crate::producer::{Producer, RingBufferFull};
pub use crate::wait_strategies::{BusySpin, BusySpinWithSpinLoopHint};
pub use crate::producer::multi::{MultiProducer};
pub use crate::producer::single::{SingleProducer};
pub use crate::consumer::{SingleConsumerBarrier, MultiConsumerBarrier};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::thread;
    use super::*;

    #[derive(Debug)]
    struct Event {
        num: i64,
    }

    #[test]
    #[should_panic(expected = "Size must be power of 2.")]
    fn size_not_a_factor_of_2() {
        build_single_producer(3, || { 0 }, BusySpin);
    }

    #[test]
    fn spsc_disruptor() {
        let factory = || { Event { num: -1 } };
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };

        let mut producer = build_single_producer(8, factory, BusySpin)
            .handle_events_with(processor)
            .build();
        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..10 {
                    producer.publish(|e| e.num = i * i);
                }
            });
        });

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    }

    #[test]
    fn pipeline_of_two_spsc_disruptors() {
        let factory = || { Event { num: -1 } };
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };

        // Last Disruptor.
        let mut producer = build_single_producer(8, factory, BusySpin)
            .handle_events_with(processor)
            .build();
        let processor = move |e: &Event, _, _| {
            producer.publish(|e2| e2.num = e.num);
        };

        // First Disruptor.
        let mut producer = build_single_producer(8, factory, BusySpin)
            .handle_events_with(processor)
            .build();
        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..10 {
                    producer.publish(|e| e.num = i * i);
                }
            });
        });

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    }

    #[test]
    fn multi_publisher_disruptor() {
        let factory = || { Event { num: -1 } };
        let (s, r) = mpsc::channel();
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
                for i in 0..num_items / 2 {
                    producer1.publish(|e| e.num = i);
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
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

        let factory = || { Event { num: -1 } };
        let builder = build_single_producer(8, factory, BusySpin);
        let mut producer = builder
            .pined_at_core(1).thread_named("my_thread").handle_events_with(processor1)
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

        let factory = || { Event { num: -1 } };
        let builder = build_single_producer(8, factory, BusySpin);
        let mut producer = builder
            .handle_events_with(processor1)
            .and_then()
            .handle_events_with(processor2)
            .and_then()
            .handle_events_with(processor3)
            .build();

        producer.publish(|e| { e.num = 0; });

        drop(producer);

        let result: Vec<i64> = r.iter().collect();
        assert_eq!(vec![1, 2, 3], result);
    }

    #[test]
    fn mpmc_with_dependent_consumers() {
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
            let s = s.clone();
            move |e: &Event, _, _| { s.send(e.num + 3).unwrap(); }
        };
        let processor4 = {
            let s = s.clone();
            move |e: &Event, _, _| { s.send(e.num + 4).unwrap(); }
        };
        let processor5 = {
            let s = s.clone();
            move |e: &Event, _, _| { s.send(e.num + 5).unwrap(); }
        };
        let processor6 = {
            move |e: &Event, _, _| { s.send(e.num + 6).unwrap(); }
        };

        let factory = || { Event { num: -1 } };
        let builder = build_multi_producer(8, factory, BusySpin);
        let mut producer1 = builder
            .handle_events_with(processor1)
            .handle_events_with(processor2)
            .and_then()
            .handle_events_with(processor3)
            .handle_events_with(processor4)
            .and_then()
            .handle_events_with(processor5)
            .handle_events_with(processor6)
            .build();

        let mut producer2 = producer1.clone();

        thread::scope(|s| {
            s.spawn(move || {
                producer1.publish(|e| e.num = 0);
            });

            s.spawn(move || {
                producer2.publish(|e| e.num = 10);
            });
        });

        let mut result: Vec<i64> = r.iter().collect();
        result.sort();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16], result);
    }
}
