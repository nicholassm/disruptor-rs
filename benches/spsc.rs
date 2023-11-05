use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, BenchmarkGroup};
use criterion::measurement::WallTime;
use crossbeam::channel::bounded;
use disruptor::{Builder, BusySpin};

const DATA_STRUCTURE_SIZE: usize = 64;
const BURST_SIZES: [u64; 5]      = [1, 5, 10, 50, 100];
const GAPS_MS: [u64; 3]          = [0, 1, 10];

struct Event {
	data: i64
}

fn pause(millis: u64) {
	if millis > 0 {
		thread::sleep(Duration::from_millis(millis));
	}
}

pub fn spsc_benchmark(c: &mut Criterion) {
	let mut group = c.benchmark_group("spsc");
	for burst_size in BURST_SIZES.into_iter() {
		group.throughput(Throughput::Elements(burst_size));

		// Base: Benchmark overhead of measurement logic.
		base(&mut group, burst_size);

		for gap in GAPS_MS.into_iter() {
			let inputs = (burst_size, gap);
			let param  = format!("burst: {}, gap: {} ms", burst_size, gap);

			crossbeam(&mut group, inputs, &param);
			disruptor(&mut group, inputs, &param);
		}
	}
	group.finish();
}

fn base(group: &mut BenchmarkGroup<WallTime>, burst_size: u64) {
	let mut data     = black_box(0);
	let sink         = Arc::new(AtomicI64::new(0));
	let sink2        = Arc::clone(&sink);
	let benchmark_id = BenchmarkId::new("base", burst_size);
	group.bench_with_input(benchmark_id, &burst_size, move |b, size| b.iter_custom(|iters| {
		let start = Instant::now();
		for _ in 0..iters {
			for _ in 0..*size {
				data += 1;
				sink.store(black_box(data), Ordering::Relaxed);
			}
			// Wait for the last data element to be received.
			while sink2.load(Ordering::Relaxed) != data {}
		}
		start.elapsed()
	}));
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>, inputs: (u64, u64), param: &str) {
	// Use an AtomicI64 to "extract" the value from the receiving thread.
	let sink     = Arc::new(AtomicI64::new(0));
	let (s, r)   = bounded::<Event>(DATA_STRUCTURE_SIZE);
	let receiver = {
		let sink = Arc::clone(&sink);
		thread::spawn(move || {
			while let Ok(event) = r.recv() {
				sink.store(event.data, Ordering::Relaxed);
			}
		})
	};
	let mut data     = black_box(0);
	let benchmark_id = BenchmarkId::new("Crossbeam", &param);
	group.bench_with_input(benchmark_id, &inputs, move |b, (size, gap)| b.iter_custom(|iters| {
		pause(*gap);
		let start = Instant::now();
		for _ in 0..iters {
			for _ in 0..*size {
				data += 1;
				s.send(Event { data }).expect("Should successfully send.");
			}
			// Wait for the last data element to be received in the receiver thread.
			while sink.load(Ordering::Relaxed) != data {}
		}
		start.elapsed()
	}));
	receiver.join().expect("Should not have panicked.");
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>, inputs: (u64, u64), param: &str) {
	let factory = || { Event { data: 0 } };
	// Use an AtomicI64 to "extract" the value from the processing thread.
	let sink      = Arc::new(AtomicI64::new(0));
	let processor = {
		let sink = Arc::clone(&sink);
		move |event: &Event, sequence: i64, end_of_batch: bool| {
			black_box(sequence);     // To avoid dead code elimination.
			black_box(end_of_batch); // To avoid dead code elimination.
			sink.store(event.data, Ordering::Relaxed);
		}
	};
	let mut producer = Builder::new(DATA_STRUCTURE_SIZE, factory, processor, BusySpin)
		.create_with_single_producer();
	let mut data     = black_box(0);
	let benchmark_id = BenchmarkId::new("disruptor", &param);
	group.bench_with_input(benchmark_id, &inputs, move |b, (size, gap) | b.iter_custom(|iters| {
		pause(*gap);
		let start = Instant::now();
		for _ in 0..iters {
			for _ in 0..*size {
				data += 1;
				producer.publish(|e| {
					e.data = data;
				});
			}
			// Wait for the last data element to be received inside processor.
			while sink.load(Ordering::Relaxed) != data {}
		}
		start.elapsed()
	}));
}

criterion_group!(spsc, spsc_benchmark);
criterion_main!(spsc);
