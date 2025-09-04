![Crates.io](https://img.shields.io/crates/v/disruptor)
![Crates.io](https://img.shields.io/crates/d/disruptor)
![Build](https://github.com/nicholassm/disruptor-rs/actions/workflows/build_and_test.yml/badge.svg)
[![codecov](https://codecov.io/gh/nicholassm/disruptor-rs/graph/badge.svg?token=VW03K0AMI0)](https://codecov.io/gh/nicholassm/disruptor-rs)

# Disruptor

This library is a low latency, inter-thread communication library written in Rust.

It's heavily inspired by the brilliant
[Disruptor library from LMAX](https://github.com/LMAX-Exchange/disruptor).

# Contents

- [Getting Started](#getting-started)
- [Patterns](#patterns)
- [Features](#features)
- [Design Choices](#design-choices)
- [Correctness](#correctness)
- [Performance](#performance)
- [Related Work](#related-work)
- [Contributions](#contributions)
- [Roadmap](#roadmap)

# Getting Started

Add the following to your `Cargo.toml` file:

    disruptor = "3.6.0"

To read details of how to use the library, check out the documentation on [docs.rs/disruptor](https://docs.rs/disruptor).

## Processing Events

There are two ways to process events:

1. Supply a closure to the Disruptor and let it manage the processing thread(s).
2. Use the `EventPoller` API where you can poll for events (and manage your own threads).

Both have comparable performance so use what fits your use case best. (See also benchmarks.)

## Single and Batch Publication With Managed Threads

Here's a minimal example demonstrating both single and batch publication. Note, batch publication should be used whenever possible for best latency and throughput (see benchmarks below).

```rust
use disruptor::*;

// The event on the ring buffer.
struct Event {
    price: f64
}

fn main() {
    // Factory closure for initializing events in the Ring Buffer.
    let factory = || { Event { price: 0.0 }};

    // Closure for processing events.
    let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
        // Your processing logic here.
    };

    let size = 64;
    let mut producer = disruptor::build_single_producer(size, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    // Publish single events into the Disruptor via the `Producer` handle.
    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64;
        });
    }

    // Publish a batch of events into the Disruptor.
    producer.batch_publish(5, |iter| {
        for e in iter { // `iter` is guaranteed to yield 5 events.
            e.price = 42.0;
        }
    });
    // At this point, the Producer instance goes out of scope and when the
    // processor is done handling all events, the Disruptor is dropped
    // as well.
}
```

## Pinning Threads and Dependencies Between Processors

The library also supports pinning threads on cores to avoid latency induced by context switching.
A more advanced usage demonstrating this and with multiple producers and multiple interdependent consumers could look like this:

```rust
use disruptor::*;
use std::thread;

struct Event {
    price: f64
}

fn main() {
    let factory = || { Event { price: 0.0 }};

    // Closure for processing events.
    let h1 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
        // Processing logic here.
    };
    let h2 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
        // Some processing logic here.
    };
    let h3 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
        // More processing logic here.
    };

    let mut producer1 = disruptor::build_multi_producer(64, factory, BusySpin)
        // `h2` handles events concurrently with `h1`.
        .pin_at_core(1).handle_events_with(h1)
        .pin_at_core(2).handle_events_with(h2)
            .and_then()
            // `h3` handles events after `h1` and `h2`.
            .pin_at_core(3).handle_events_with(h3)
        .build();

    // Create another producer.
    let mut producer2 = producer1.clone();

    // Publish into the Disruptor.
    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..10 {
                producer1.publish(|e| {
                    e.price = i as f64;
                });
            }
        });
        s.spawn(move || {
            for i in 10..20 {
                producer2.publish(|e| {
                    e.price = i as f64;
                });
            }
        });
    });
    // At this point, the Producers instances go out of scope and when the
    // processors are done handling all events, the Disruptor is dropped
    // as well.
}
```

## Processors with State

If you need to store some state in the processor thread which is neither `Send` nor `Sync`, e.g. a `Rc<RefCell<i32>>`, then you can create a closure for initializing that state and pass it along with the processing closure when you build the Disruptor. The Disruptor will then pass a mutable reference to your state on each event. As an example:

```rust
use std::{cell::RefCell, rc::Rc};
use disruptor::*;

struct Event {
    price: f64
}

#[derive(Default)]
struct State {
    data: Rc<RefCell<i32>>
}

fn main() {
    let factory = || { Event { price: 0.0 }};
    let initial_state = || { State::default() };

    // Closure for processing events *with* state.
    let processor = |s: &mut State, e: &Event, _: Sequence, _: bool| {
        // Mutate your custom state:
        *s.data.borrow_mut() += 1;
    };

    let size = 64;
    let mut producer = disruptor::build_single_producer(size, factory, BusySpin)
        .handle_events_and_state_with(processor, initial_state)
        .build();

    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64;
        });
    }
}
```

## Event Polling

An alternative to storing state in the processor is to use the Event Poller API:

```rust
use disruptor::*;

// The event on the ring buffer.
struct Event {
    price: f64
}

fn main() {
    // Factory closure for initializing events in the Ring Buffer.
    let factory = || { Event { price: 0.0 }};

    let size = 64;
    let builder = disruptor::build_single_producer(size, factory, BusySpin);
    let (mut poller, builder) = builder.event_poller();
    let mut producer = builder.build();

    // Publish single events into the Disruptor via the `Producer` handle.
    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64;
        });
    }

    loop {
        match poller.poll() {
            Ok(mut events) => {
                // `&mut events` implements ExactSizeIterator so events can be
                // batch processed and handled with e.g. a for loop.
                for event in &mut events {
                    // Process events here.
                }
            },// When guard (here named `events`) goes out of scope,
              // it signals to the Disruptor that reading is done.
            Err(Polling::NoEvents) => {
                // Do other work or poll again.
            },
            Err(Polling::Shutdown) => {
                // Disruptor is shut down so no more events will be published.
                break;
            },
        }
    }
}
```

# Patterns

## A Disruptor with Different Event Types

Let's assume you have multiple different types of producers that each publish distinct events.
It could be an exchange where you receive e.g. client logins, logouts, orders, etc.

You can model this by using an enum as event type:

```rust
enum Event {
    Login{id: i32},
    Logout{id: i32},
    Order{user_id: i32, price: f64, quantity: i32},
    Heartbeat,
}
```

Then you can differentiate on different event types in your processor:

```rust
fn main() {
    let factory = || { Event::Heartbeat };

    let processor = |e: &Event, _sequence: Sequence, _end_of_batch: bool| {
        match e {
            Event::Login{id}                       => { /* Register client login.    */ }
            Event::Logout{id}                      => { /* Register client logout.   */ }
            Event::Order{user_id, price, quantity} => { /* Add an order to the CLOB. */ }
            Event::Heartbeat                       => { /* Heartbeat from somewhere. */ }
        }
    };

    let mut producer = disruptor::build_multi_producer(64, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    let mut login_producer  = producer.clone();
    let mut logout_producer = producer.clone();
    let mut order_producer  = producer.clone();

    login_producer.publish( |e| *e = Event::Login{id: 1});
    order_producer.publish( |e| *e = Event::Order{user_id: 1, price: 100.0, quantity: 10});
    logout_producer.publish(|e| *e = Event::Logout{id: 1});
    producer.publish(       |e| *e = Event::Heartbeat);
}
```

## Splitting Workload Across Processors

Let's assume you have a high ingress rate of events and you need to split the work across multiple processors
to cope with the load. You can do that by assigning an `id` to each processor and then only process events
with a sequence number that modulo the number of processors equal the `id`.
Here, for simplicity, we split it across two processors:

```rust
let processor0 = |e: &Event, sequence: Sequence, _end_of_batch: bool| {
    if sequence % 2 == 0 {
        // Process event.
    }
}
let processor1 = |e: &Event, sequence: Sequence, _end_of_batch: bool| {
    if sequence % 2 == 1 {
        // Process event.
    }
}
```

This scheme ensures each event is processed once.

# Features

- [x] Single Producer Single Consumer (SPSC).
- [x] Single Producer Multi Consumer (SPMC) with consumer interdependencies.
- [x] Multi Producer Single Consumer (MPSC).
- [x] Multi Producer Multi Consumer (MPMC) with consumer interdependencies.
- [x] Busy-spin wait strategies.
- [x] Batch publication of events.
- [x] Batch consumption of events.
- [x] Event Poller API.
- [x] Thread affinity can be set for the event processor thread(s).
- [x] Set thread name of each event processor thread.

# Design Choices

Everything in the library is about low-latency and this heavily influences all choices made in this library.
As an example, you cannot allocate an event and *move* that into the ringbuffer.
Instead, events are allocated on startup to ensure they are co-located in memory to increase cache coherency.
However, you can still allocate e.g. a struct and move ownership to a field in the event on the Ringbuffer.

There's also no use of dynamic dispatch - everything is monomorphed.

# Correctness

This library needs to use Unsafe to achieve low latency.
Although the absence of bugs cannot be guaranteed, these approaches have been used to eliminate bugs:

- Minimal usage of Unsafe blocks.
- High test coverage.
- All tests are run on Miri in CI/CD.
- Verification in TLA+ (see the `verification/` folder).

# Performance

The SPSC and MPSC Disruptor variants have been benchmarked and compared to Crossbeam. See the code in the `benches/spsc.rs` and `benches/mpsc.rs` files.

The results below of the SPSC benchmark are gathered from running the benchmarks on a 2016 Macbook Pro running a 2,6 GHz Quad-Core Intel Core i7. So on a modern Intel Xeon the numbers should be even better. Furthermore, it's not possible to isolate cores on Mac and pin threads which would produce even more stable results. This is future work.

If you have any suggestions to improving the benchmarks, please feel free to open an issue.

To provide a somewhat realistic benchmark not only burst of different sizes are considered but also variable pauses between bursts: 0 ms, 1 ms and 10 ms.

The latencies below are the mean latency per element with 95% confidence interval (standard `criterion` settings). Capturing all latencies and calculating misc. percentiles (and in particular the max latency) is future work. However, I expect the below measurements to be representative for the actual performance you can achieve in a real application.

## No Pause Between Bursts

*Latency:*

|  Burst Size | Crossbeam | Disruptor | Improvement |
|------------:|----------:|----------:|------------:|
|           1 |     65 ns |     32 ns |         51% |
|          10 |     68 ns |      9 ns |         87% |
|         100 |     29 ns |      8 ns |         72% |

*Throughput:*

|  Burst Size |  Crossbeam |   Disruptor | Improvement |
|------------:|-----------:|------------:|------------:|
|           1 |  15.2M / s |   31.7M / s |        109% |
|          10 |  14.5M / s |  117.3M / s |        709% |
|         100 |  34.3M / s |  119.7M / s |        249% |

## 1 ms Pause Between Bursts

*Latency:*

|  Burst Size | Crossbeam |  Disruptor | Improvement |
|------------:|----------:|-----------:|------------:|
|           1 |     63 ns |      33 ns |         48% |
|          10 |     67 ns |       8 ns |         88% |
|         100 |     30 ns |       9 ns |         70% |

*Throughput:*

|  Burst Size |  Crossbeam |  Disruptor | Improvement |
|------------:|-----------:|-----------:|------------:|
|           1 |  15.9M / s |  30.7M / s |         93% |
|          10 |  14.9M / s | 117.7M / s |        690% |
|         100 |  33.8M / s | 105.0M / s |        211% |

## 10 ms Pause Between Bursts

*Latency:*

|  Burst Size | Crossbeam | Disruptor | Improvement |
|------------:|----------:|----------:|------------:|
|           1 |     51 ns |     32 ns |         37% |
|          10 |     67 ns |      9 ns |         87% |
|         100 |     30 ns |     10 ns |         67% |

*Throughput:*

|  Burst Size | Crossbeam |  Disruptor | Improvement |
|------------:|----------:|-----------:|------------:|
|           1 | 19.5M / s |  31.6M / s |         62% |
|          10 | 14.9M / s | 114.5M / s |        668% |
|         100 | 33.6M / s | 105.0M / s |        213% |

## Conclusion

There's clearly a difference between the Disruptor and the Crossbeam libs. However, this is not because the Crossbeam library is not a great piece of software. It is. The Disruptor trades CPU and memory resources for lower latency and higher throughput and that is why it's able to achieve these results. The Disruptor also excels if you can publish batches of events as
demonstrated in the benchmarks with bursts of 10 and 100 events.

Both libraries greatly improves as the burst size goes up but the Disruptor's performance is more resilient to the pauses between bursts which is one of the design goals.

# Related Work

There are multiple other Rust projects that mimic the LMAX Disruptor library:
1. [Turbine](https://github.com/polyfractal/Turbine)
2. [Disrustor](https://github.com/sklose/disrustor)

A key feature that this library supports is multiple producers from different threads that neither of the above libraries support (at the time of writing).

# Contributions

You are welcome to create a Pull-Request or open an issue with suggestions for improvements.

Changes are accepted solely at my discretion and I will focus on whether the changes are a good fit for the purpose and design of this crate.

# Roadmap

Empty! All the items have been implemented.
