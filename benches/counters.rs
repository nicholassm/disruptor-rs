// Credit for this simple eval setup goes to this post: https://medium.com/@trunghuynh/rust-101-rust-crossbeam-vs-java-disruptor-a-wow-feeling-27eaffcda9cb
// However, this benchmark proves that, disruptor implementation in rust still beats crossbeam on Intel CPUs.  On M1 & AMD, crossbeam is faster in MPSC.
// For more details, see: https://github.com/nicholassm/disruptor-rs/issues/7#issuecomment-2094341906

use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam::channel::*;
use disruptor::{build_multi_producer, build_single_producer, BusySpin, Producer};
use std::hint::black_box;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

const BUF_SIZE: usize = 32_768;
const MAX_PRODUCER_EVENTS: usize = 10_000_000;
const BATCH_SIZE: usize = 2_000;

fn crossbeam_spsc() {
    let (s, r) = bounded(BUF_SIZE);

    // Producer
    let t1 = thread::spawn(move || {
        for _ in 0..MAX_PRODUCER_EVENTS {
            s.send(1).unwrap();
        }
    });

    let sink = Arc::new(AtomicI32::new(0)); //bcos we read and print value from main thread
    let sink_clone = Arc::clone(&sink);
    // Consumer
    let c1: JoinHandle<()> = thread::spawn(move || {
        for msg in r {
            sink_clone.fetch_add(msg, Ordering::Release);
        }
    });

    let _ = t1.join();
    let _ = c1.join();

    sink.load(Ordering::Acquire);
}

fn crossbeam_mpsc() {
    let (s, r) = bounded(BUF_SIZE);
    let s2 = s.clone();

    // Producer 1
    let t1 = thread::spawn(move || {
        for _ in 0..MAX_PRODUCER_EVENTS {
            s.send(black_box(1)).unwrap();
        }
    });

    // Producer 2
    let t2 = thread::spawn(move || {
        for _ in 0..MAX_PRODUCER_EVENTS {
            s2.send(black_box(1)).unwrap();
        }
    });

    let sink = Arc::new(AtomicI32::new(0)); //bcos we read and print value from main thread
    let sink_clone = Arc::clone(&sink);
    // Consumer
    let c1: JoinHandle<()> = thread::spawn(move || {
        for msg in r {
            sink_clone.fetch_add(msg, Ordering::Release);
        }
    });

    let _ = t1.join();
    let _ = t2.join();
    let _ = c1.join();

    sink.load(Ordering::Acquire);
}

fn crossbeam_spsc_benchmark(c: &mut Criterion) {
    c.bench_function("crossbeam_spsc", |b| {
        b.iter(|| {
            crossbeam_spsc();
        });
    });
}

fn crossbeam_mpsc_benchmark(c: &mut Criterion) {
    c.bench_function("crossbeam_mpsc", |b| {
        b.iter(|| {
            crossbeam_mpsc();
        });
    });
}

struct Event {
    val: i32,
}

fn disruptor_spsc() {
    let factory = || Event { val: 0 }; //to initialize disruptor

    let sink = Arc::new(AtomicI32::new(0)); //bcos we read and print value from main thread
                                            // Consumer
    let processor = {
        let sink = Arc::clone(&sink);
        move |event: &Event, _sequence: i64, _end_of_batch: bool| {
            sink.fetch_add(event.val, Ordering::Release);
        }
    };

    let mut producer = build_single_producer(BUF_SIZE, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    // Publish into the Disruptor.
    thread::scope(|s| {
        s.spawn(move || {
            for _ in 0..MAX_PRODUCER_EVENTS / BATCH_SIZE {
                producer.batch_publish(BATCH_SIZE, |iter| {
                    for e in iter {
                        e.val = black_box(1);
                    }
                });
            }
        });
    });

    sink.load(Ordering::Acquire);
}

fn disruptor_mpsc() {
    let factory = || Event { val: 0 }; //to initialize disruptor

    let sink = Arc::new(AtomicI32::new(0)); //bcos we read and print value from main thread
                                            // Consumer
    let processor = {
        let sink = Arc::clone(&sink);
        move |event: &Event, _sequence: i64, _end_of_batch: bool| {
            sink.fetch_add(event.val, Ordering::Release);
        }
    };

    let mut producer1 = build_multi_producer(BUF_SIZE, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    let mut producer2 = producer1.clone();

    // Publish into the Disruptor.
    thread::scope(|s| {
        s.spawn(move || {
            for _ in 0..MAX_PRODUCER_EVENTS / BATCH_SIZE {
                producer1.batch_publish(BATCH_SIZE, |iter| {
                    for e in iter {
                        e.val = black_box(1);
                    }
                });
            }
        });

        s.spawn(move || {
            for _ in 0..MAX_PRODUCER_EVENTS / BATCH_SIZE {
                producer2.batch_publish(BATCH_SIZE, |iter| {
                    for e in iter {
                        e.val = 1;
                    }
                });
            }
        });
    });

    sink.load(Ordering::Acquire);
}

fn disruptor_spsc_benchmark(c: &mut Criterion) {
    c.bench_function("disruptor_spsc", |b| {
        b.iter(|| {
            disruptor_spsc();
        });
    });
}

fn disruptor_mpsc_benchmark(c: &mut Criterion) {
    c.bench_function("disruptor_mpsc", |b| {
        b.iter(|| {
            disruptor_mpsc();
        });
    });
}

criterion_group!(
    counters,
    crossbeam_spsc_benchmark,
    disruptor_spsc_benchmark,
    crossbeam_mpsc_benchmark,
    disruptor_mpsc_benchmark
);
criterion_main!(counters);
