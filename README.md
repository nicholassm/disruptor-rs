# Disruptor

This library is a low latency, inter-thread communication library written in Rust.

It's heavily inspired by the brilliant
[Disruptor library from LMAX](https://github.com/LMAX-Exchange/disruptor).

# Getting Started

Add the following to your `Cargo.toml` file:

    disruptor = "0.1.0"

To see how to use the library, check out the documentation on [docs.rs/disruptor](https://docs.rs/disruptor).

# Features

1. Single Producer Single Consumer (SPSC). See roadmap for MPSC and MPMC.
2. Low-latency.
3. Busy-spin wait strategy.

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

A key feature that this library will support (soon!) is multiple producers that
neither of the above libraries support (at the time of writing).

# Roadmap

1. Support for multiple threads publishing to the Disruptor.
2. Support for batch publication.
3. Write benchmarks comparing this library to e.g. Crossbeam and the standard Rust channels.
4. Support for setting affinity event processor threads.
5. Support for multiple consumers/event processor threads including interdependencies.