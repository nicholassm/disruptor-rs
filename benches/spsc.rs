use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::Duration;
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use crossbeam::channel::bounded;
use disruptor::{Builder, BusySpin};

const DATA_STRUCTURE_SIZE: usize = 64;
const BATCH_SIZES: [usize; 4]    = [1, 10, 100, 1000];

struct Event {
	data: i64
}

/// Benchmark overhead of measurement logic.
pub fn base_benchmark(c: &mut Criterion) {
	let mut group = c.benchmark_group("base batch");
	for batch_size in BATCH_SIZES.iter() {
		let mut data     = black_box(0);
		let sink         = Arc::new(AtomicI64::new(0));
		let sink2        = Arc::clone(&sink);
		let benchmark_id = BenchmarkId::from_parameter(batch_size);
		group.throughput(Throughput::Elements(*batch_size as u64));
		group.bench_with_input(benchmark_id, &batch_size, move |b, &size| b.iter(|| {
			for _ in 0..*size {
				data += 1;
				sink.store(black_box(data), Ordering::Relaxed);
			}
			// Wait for the last data element to be received.
			while sink2.load(Ordering::Relaxed) != data {}
		}));
	}
	group.finish();
}

pub fn disruptor_benchmark(c: &mut Criterion) {
	let factory   = || { Event { data: 0 }};
	let mut group = c.benchmark_group("disruptor batch");
	for batch_size in BATCH_SIZES.iter() {
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
		let benchmark_id = BenchmarkId::from_parameter(batch_size);
		group.throughput(Throughput::Elements(*batch_size as u64));
		group.bench_with_input(benchmark_id, &batch_size, move |b, &size| b.iter(|| {
			for _ in 0..*size {
				data += 1;
				producer.publish(|e| {
					e.data = data;
				});
			}
			// Wait for the last data element to be received inside processor.
			while sink.load(Ordering::Relaxed) != data {}
		}));
	}
	group.finish();
}

pub fn crossbeam_benchmark(c: &mut Criterion) {
	let mut group = c.benchmark_group("crossbeam batch");
	for batch_size in BATCH_SIZES.iter() {
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
		let benchmark_id = BenchmarkId::from_parameter(batch_size);
		group.throughput(Throughput::Elements(*batch_size as u64));
		group.bench_with_input(benchmark_id, &batch_size,move |b, &size| b.iter(|| {
			for _ in 0..*size {
				data += 1;
				s.send(Event { data }).expect("Should successfully send.");
			}
			// Wait for the last data element to be received in the receiver thread.
			while sink.load(Ordering::Relaxed) != data {}
		}));
		receiver.join().expect("Should not have panicked.");
	}
	group.finish();
}

criterion_group!(base, base_benchmark);
criterion_group!(spsc, disruptor_benchmark, crossbeam_benchmark);
criterion_main!(base, spsc);
