use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use disruptor::{BusySpin, Producer};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread::{self, spawn};
use std::time::{Duration, Instant};

const DATA_STRUCTURE_SIZE: usize = 128;
const BURST_SIZES: [u64; 3] = [1, 10, 100];
const PAUSES_MS: [u64; 3] = [0, 1, 10];

struct Event {
    data: i64,
}

fn pause(millis: u64) {
    if millis > 0 {
        thread::sleep(Duration::from_millis(millis));
    }
}

pub fn poller_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");
    for burst_size in BURST_SIZES.into_iter() {
        group.throughput(Throughput::Elements(burst_size));

        // Base: Benchmark overhead of measurement logic.
        base(&mut group, burst_size as i64);

        for pause_ms in PAUSES_MS.into_iter() {
            let inputs = (burst_size as i64, pause_ms);
            let param = format!("burst: {}, pause: {} ms", burst_size, pause_ms);

            polling(&mut group, inputs, &param);
            processing(&mut group, inputs, &param);
        }
    }
    group.finish();
}

// Synthetic benchmark to measure the overhead of the measurement itself.
fn base(group: &mut BenchmarkGroup<WallTime>, burst_size: i64) {
    let sink = Arc::new(AtomicI64::new(0));
    let benchmark_id = BenchmarkId::new("base", burst_size);
    group.bench_with_input(benchmark_id, &burst_size, move |b, size| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                for data in 1..=*size {
                    sink.store(black_box(data), Ordering::Release);
                }
                // Wait for the last data element to "be received".
                let last_data = black_box(*size);
                while sink.load(Ordering::Acquire) != last_data {}
            }
            start.elapsed()
        })
    });
}

fn polling(group: &mut BenchmarkGroup<WallTime>, inputs: (i64, u64), param: &str) {
    let factory = || Event { data: 0 };

    let builder = disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, BusySpin);
    let (mut poller, builder) = builder.event_poller();
    let mut producer = builder.build();

    // Use an AtomicI64 to "extract" the value from the processing thread with the Event Poller.
    let sink = Arc::new(AtomicI64::new(0));
    let join_handle = {
        let sink = Arc::clone(&sink);
        spawn(move || loop {
            match poller.poll() {
                Ok(mut events) => {
                    for event in &mut events {
                        sink.store(event.data, Ordering::Release);
                    }
                }
                Err(disruptor::Polling::NoEvents) => continue,
                Err(disruptor::Polling::Shutdown) => break,
            }
        })
    };

    let benchmark_id = BenchmarkId::new("polling", &param);
    group.bench_with_input(benchmark_id, &inputs, move |b, (size, pause_ms)| {
        b.iter_custom(|iters| {
            pause(*pause_ms);
            let start = Instant::now();
            for _ in 0..iters {
                producer.batch_publish(*size as usize, |iter| {
                    for (i, e) in iter.enumerate() {
                        e.data = black_box(i as i64 + 1);
                    }
                });

                // Wait for the last data element to be received inside processor.
                let last_data = black_box(*size);
                while sink.load(Ordering::Acquire) != last_data {}
            }
            start.elapsed()
        })
    });

    join_handle.join().expect("Polling thread panicked");
}

fn processing(group: &mut BenchmarkGroup<WallTime>, inputs: (i64, u64), param: &str) {
    let factory = || Event { data: 0 };
    // Use an AtomicI64 to "extract" the value from the processing thread.
    let sink = Arc::new(AtomicI64::new(0));
    let processor = {
        let sink = Arc::clone(&sink);
        move |event: &Event, _sequence: i64, _end_of_batch: bool| {
            sink.store(event.data, Ordering::Release);
        }
    };
    let mut producer = disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, BusySpin)
        .handle_events_with(processor)
        .build();
    let benchmark_id = BenchmarkId::new("processing", &param);
    group.bench_with_input(benchmark_id, &inputs, move |b, (size, pause_ms)| {
        b.iter_custom(|iters| {
            pause(*pause_ms);
            let start = Instant::now();
            for _ in 0..iters {
                producer.batch_publish(*size as usize, |iter| {
                    for (i, e) in iter.enumerate() {
                        e.data = black_box(i as i64 + 1);
                    }
                });
                // Wait for the last data element to be received inside processor.
                let last_data = black_box(*size);
                while sink.load(Ordering::Acquire) != last_data {}
            }
            start.elapsed()
        })
    });
}

criterion_group!(poller, poller_benchmark);
criterion_main!(poller);
