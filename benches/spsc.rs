use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use crossbeam::channel::bounded;
use disruptor::{Builder, BusySpin};

struct Event {
	data: i64
}

pub fn base_benchmark(c: &mut Criterion) {
	let sink     = Arc::new(AtomicI64::new(0));
	let sink2    = Arc::clone(&sink);
	let mut data = 0;
	c.bench_function("base", move |b| b.iter(|| {
		data += 1;
		sink.store(black_box(data), Ordering::Relaxed);
		let expected = black_box(data);
		while sink2.load(Ordering::Relaxed) != expected {}
	}));
}

pub fn disruptor_benchmark(c: &mut Criterion) {
	let factory   = || { Event { data: 0 }};
	// Use an AtomicI64 to "extract" the value from the processing thread.
	let sink      = Arc::new(AtomicI64::new(0));
	let sink2     = Arc::clone(&sink);
	let processor = move |event: &Event, sequence: i64, end_of_batch: bool| {
		black_box(sequence);
		black_box(end_of_batch);
		sink.store(event.data, Ordering::Relaxed);
	};
	let mut producer = Builder::new(64, factory, processor, BusySpin).create_with_single_producer();
	let mut data     = 0;
	c.bench_function("spsc", move |b| b.iter(|| {
		data += 1;
		producer.publish(|e| {
			e.data = black_box(data);
		});
		let expected = black_box(data);
		while sink2.load(Ordering::Relaxed) != expected {}
	}));
}

pub fn crossbeam_benchmark(c: &mut Criterion) {
	// Use an AtomicI64 to "extract" the value from the processing thread.
	let sink      = Arc::new(AtomicI64::new(0));
	let sink2     = Arc::clone(&sink);

	let (s, r)    = bounded::<Event>(64);

	let receiver  = thread::spawn(move || {
		while let Ok(event) = r.recv() {
			sink.store(event.data, Ordering::Relaxed);
		}
	});

	let mut data  = 0;
	c.bench_function("crossbeam", move |b| b.iter(|| {
		data += 1;
		s.send(Event { data: black_box(data) }).expect("Should successfully send.");
		let expected = black_box(data);
		while sink2.load(Ordering::Relaxed) != expected {}
	}));

	receiver.join().expect("Should not have panicked.");
}

criterion_group!(base, base_benchmark);
criterion_group!(spsc, disruptor_benchmark, crossbeam_benchmark);
criterion_main!(base, spsc);
