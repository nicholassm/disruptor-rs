![Crates.io](https://img.shields.io/crates/v/disruptor)
![Crates.io](https://img.shields.io/crates/d/disruptor)

# Disruptor

This library is a low latency, inter-thread communication library written in Rust.

It's heavily inspired by the brilliant
[Disruptor library from LMAX](https://github.com/LMAX-Exchange/disruptor).

# Getting Started

Add the following to your `Cargo.toml` file:

    disruptor = "0.4.0"

To read details of how to use the library, check out the documentation on [docs.rs/disruptor](https://docs.rs/disruptor).

Here's a minimal example:

```rust
use disruptor::Builder;
use disruptor::BusySpin;

// The event on the ring buffer.
struct Event {
    price: f64
}

fn main() {
    // Factory closure for initializing events.
    let factory = || { Event { price: 0.0 }};

    // Closure for processing events.
    let processor = |e: &Event, sequence: i64, end_of_batch: bool| {
        // Your processing logic here.
    };

    let size = 64;
    let mut producer = Builder::new(size, factory, processor, BusySpin)
        .create_with_single_producer();
    // Publish into the Disruptor.
    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64;
        });
    }
}// At this point, the Producer instance goes out of scope and when the
 // processor is done handling all events then the Disruptor is dropped
 // as well.
```

# Features

- [x] Single Producer Single Consumer (SPSC). See roadmap for MPMC.
- [x] Multi Producer Single Consumer (MPSC).
- [x] Low-latency.
- [x] Busy-spin wait strategies.
- [x] Batch consumption of events.
- [x] Thread affinity can be set for the event processor thread.

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

1. Add a Sleeping Wait Strategy.
2. Support for batch publication.
3. Verify correctness with Miri.
4. Write benchmarks comparing this library to e.g. Crossbeam and the standard Rust channels.
5. Support for multiple consumers/event processor threads including interdependencies.
