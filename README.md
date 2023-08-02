# Disruptor

This library is a low latency, inter-thread communication library written in Rust.

It's heavily inspired by the brilliant
[Disruptor library from LMAX](https://github.com/LMAX-Exchange/disruptor).

# Getting Started

Add the following to your `Cargo.toml` file:

    disruptor = "0.1.0"

To read details of how to use the library, check out the documentation on [docs.rs/disruptor](https://docs.rs/disruptor).

Here's a minimal example:

```rust
use disruptor::Builder;
use disruptor::BusySpin;

// The data entity on the ring buffer.
struct Event {
    price: f64
}

fn main() {
    // Define a factory for populating the ring buffer with initialized events.
    let factory = || { Event { price: 0.0 }};

    // Define a closure for processing events. A thread, controlled by the disruptor, will run this
    // processor closure each time an event is published.
    let processor = |e: &Event, sequence: i64, end_of_batch: bool| {
        // Process the Event `e` published at `sequence`.
        // If `end_of_batch` is false, you can batch up events until it's invoked with
        // `end_of_batch` being true.
    };

    // Create a Disruptor by using a `disruptor::Builder`, In this example, the ring buffer has
    // size 8 and the `BusySpin` wait strategy.
    // Finally, the Disruptor is built by specifying that only a single thread will publish into
    // the Disruptor (via a `Producer` handle). There's also a `create_with_multi_producer()` for
    // publication from multiple threads.
    let mut producer = Builder::new(8, factory, processor, BusySpin).create_with_single_producer();
    // Publish 10 times into the Disruptor.
    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64;
        });
    }
    // At this point, the processor thread processes all published events and then stops as
    // the Producer instance goes out of scope and the Disruptor (and the Producer) are dropped.
}
```

# Features

- [x] Single Producer Single Consumer (SPSC). See roadmap for MPMC.
- [x] Multi Producer Single Consumer (MPSC).
- [x] Low-latency.
- [x] Busy-spin wait strategies.
- [x] Batch consumption of events.

# Design Choices

Everything in the library is about low-latency and this heavily influences all choices made in this library.
As an example, you cannot allocate an event and *move* that into the ringbuffer. Instead, events
are allocated on startup to ensure they are co-located in memory to increase cache coherency.
(However, you can still allocate a struct on the heap and move ownership to a field in the event on the Ringbuffer.
As long as you realize that this can add latency, because the struct is allocated by one thread and dropped by another.
Hence, there's synchronization happening in the allocator.)

# Related Work

There are multiple other Rust projects that mimic the LMAX Disruptor library:
1. [Turbine](https://github.com/polyfractal/Turbine)
2. [Disrustor](https://github.com/sklose/disrustor)

A key feature that this library supports is multiple producers from different threads
that neither of the above libraries support (at the time of writing).

# Roadmap

1. Support for setting thread affinity on event processor threads.
2. Support for batch publication.
3. Write benchmarks comparing this library to e.g. Crossbeam and the standard Rust channels.
4. Support for multiple consumers/event processor threads including interdependencies.