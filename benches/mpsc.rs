use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::{Acquire, Release, Relaxed}};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, BenchmarkGroup};
use criterion::measurement::WallTime;
use crossbeam::channel::TrySendError::Full;
use crossbeam::channel::{bounded, TryRecvError::{Empty, Disconnected}};
use crossbeam_utils::CachePadded;
use disruptor::{BusySpin, Producer};

const PRODUCERS:           usize    = 2;
const DATA_STRUCTURE_SIZE: usize    = 256;
const BURST_SIZES:         [u64; 3] = [1, 10, 100];
const PAUSES_MS:           [u64; 3] = [0,  1,  10];

struct Event {
	data: i64
}

fn pause(millis: u64) {
	if millis > 0 {
		thread::sleep(Duration::from_millis(millis));
	}
}

pub fn mpsc_benchmark(c: &mut Criterion) {
	let mut group = c.benchmark_group("mpsc");
	for burst_size in BURST_SIZES.into_iter() {
		group.throughput(Throughput::Elements(burst_size));

		// Base: Benchmark overhead of measurement logic.
		base(&mut group, burst_size as i64);

		for pause_ms in PAUSES_MS.into_iter() {
			let params             = (burst_size as i64, pause_ms);
			let param_description  = format!("burst: {}, pause: {} ms", burst_size, pause_ms);

			crossbeam(&mut group, params, &param_description);
			disruptor(&mut group, params, &param_description);
		}
	}
	group.finish();
}

/// Structure for managing all producer threads so they can produce a burst again and again in
/// a benchmark after being released from a barrier. This is to avoid the overhead of creating
/// new threads for each sample.
struct BurstProducer {
	start_barrier: Arc<CachePadded<AtomicBool>>,
	stop:          Arc<CachePadded<AtomicBool>>,
	join_handle:   Option<JoinHandle<()>>,
}

impl BurstProducer {
	fn new<P>(mut produce_one_burst: P) -> Self
	where
		P: 'static + Send + FnMut()
	{
		let start_barrier = Arc::new(CachePadded::new(AtomicBool::new(false)));
		let stop          = Arc::new(CachePadded::new(AtomicBool::new(false)));

		let join_handle = {
			let stop          = Arc::clone(&stop);
			let start_barrier = Arc::clone(&start_barrier);
			thread::spawn(move || {
				while !stop.load(Acquire) {
					// Busy spin with a check if we're done.
					while start_barrier.compare_exchange(true, false, Acquire, Relaxed).is_err() {
						if stop.load(Acquire) { return; }
					}
					produce_one_burst();
				}
			})
		};

		Self {
			start_barrier,
			stop,
			join_handle: Some(join_handle)
		}
	}

	fn start(&self) {
		self.start_barrier.store(true, Release);
	}

	fn stop(&mut self) {
		self.stop.store(true, Release);
		self.join_handle.take().unwrap().join().expect("Should not panic.");
	}
}

fn run_benchmark(
	group:           &mut BenchmarkGroup<WallTime>,
	benchmark_id:    BenchmarkId,
	burst_size:      Arc<AtomicI64>,
	sink:            Arc<AtomicI64>,
	params:          (i64, u64),
	burst_producers: &[BurstProducer])
{
	group.bench_with_input(benchmark_id, &params, move |b, (size, pause_ms)| b.iter_custom(|iters| {
		burst_size.store(*size, Release);
		let count = black_box(*size * burst_producers.len() as i64);
		pause(*pause_ms);
		let start = Instant::now();
		for _ in 0..iters {
			sink.store(0, Release);
			burst_producers.iter().for_each(BurstProducer::start);
			// Wait for all producers to finish publication.
			while sink.load(Acquire) != count {/* Busy spin. */}
		}
		start.elapsed()
	}));
}

fn base(group: &mut BenchmarkGroup<WallTime>, size: i64) {
	let sink                = Arc::new(AtomicI64::new(0));
	let benchmark_id        = BenchmarkId::new("base", size);
	let burst_size          = Arc::new(AtomicI64::new(0));
	let mut burst_producers = (0..PRODUCERS)
		.into_iter()
		.map(|_| {
			let sink       = Arc::clone(&sink);
			let burst_size = Arc::clone(&burst_size);
			BurstProducer::new(move || {
				let burst_size = burst_size.load(Acquire);
				for _ in 0..burst_size {
					sink.fetch_add(1, Release);
				}
			})
		})
		.collect::<Vec<BurstProducer>>();

	run_benchmark(group, benchmark_id, burst_size, sink, (size, 0), &burst_producers);
	burst_producers.iter_mut().for_each(BurstProducer::stop);
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>, params: (i64, u64), param_description: &str) {
	// Use an AtomicI64 to count the number of events from the receiving thread.
	let sink     = Arc::new(AtomicI64::new(0));
	let (s, r)   = bounded::<Event>(DATA_STRUCTURE_SIZE);
	let receiver = {
		let sink = Arc::clone(&sink);
		thread::spawn(move || {
			loop {
				match r.try_recv() {
					Ok(event)         => {
						black_box(event.data);
						sink.fetch_add(1, Release);
					},
					Err(Empty)        => continue,
					Err(Disconnected) => break,
				}
			}
		})
	};
	let benchmark_id        = BenchmarkId::new("Crossbeam", &param_description);
	let burst_size          = Arc::new(AtomicI64::new(0));
	let mut burst_producers = (0..PRODUCERS)
		.into_iter()
		.map(|_| {
			let burst_size = Arc::clone(&burst_size);
			let s          = s.clone();
			BurstProducer::new(move || {
				let burst_size = burst_size.load(Acquire);
				for data in 0..burst_size {
					let mut event = Event { data: black_box(data) };
					loop {
						match s.try_send(event) {
							Err(Full(e)) => event = e,
							_            => break,
						}
					}
				}
			})
		})
		.collect::<Vec<BurstProducer>>();
	drop(s); // Original send channel not used.

	run_benchmark(group, benchmark_id, burst_size, sink, params, &burst_producers);

	burst_producers.iter_mut().for_each(BurstProducer::stop);
	receiver.join().expect("Should not have panicked.");
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>, params: (i64, u64), param_description: &str) {
	let factory   = || { Event { data: 0 } };
	// Use an AtomicI64 to count number of received events.
	let sink      = Arc::new(AtomicI64::new(0));
	let processor = {
		let sink = Arc::clone(&sink);
		move |event: &Event, _sequence: i64, _end_of_batch: bool| {
			// Black box event to avoid dead code elimination.
			black_box(event.data);
			sink.fetch_add(1, Release);
		}
	};
	let producer = disruptor::build_multi_producer(DATA_STRUCTURE_SIZE, factory, BusySpin)
		.handle_events_with(processor)
		.build();
	let benchmark_id        = BenchmarkId::new("disruptor", &param_description);
	let burst_size          = Arc::new(AtomicI64::new(0));
	let mut burst_producers = (0..PRODUCERS)
		.into_iter()
		.map(|_| {
			let burst_size   = Arc::clone(&burst_size);
			let mut producer = producer.clone();
			BurstProducer::new(move || {
				let burst_size = burst_size.load(Acquire);
				producer.batch_publish(burst_size as usize, |iter| {
					for (i, e) in iter.enumerate() {
						e.data = black_box(i as i64);
					}
				});
			})
		})
		.collect::<Vec<BurstProducer>>();
	drop(producer); // Original producer not used.

	run_benchmark(group, benchmark_id, burst_size, sink, params, &burst_producers);

	burst_producers.iter_mut().for_each(BurstProducer::stop);
}

criterion_group!(mpsc, mpsc_benchmark);
criterion_main!(mpsc);
