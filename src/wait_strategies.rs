//! Module with different strategies for waiting for an event to be published.
//!
//! The lowest latency possible is the [`BusySpin`] strategy.
//!
//! To "waste" less CPU time and power, use one of the other strategies which have higher latency.

use std::hint;
use std::thread;

use crate::Sequence;

/// Wait strategies are used by consumers when no new events are ready on the ring buffer.
pub trait WaitStrategy: Copy + Send {
	/// The wait strategy will wait for the sequence id being available.
	fn wait_for(&self, sequence: Sequence);
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for(&self, _sequence: Sequence) {
		// Do nothing, true busy spin.
	}
}

/// Busy spin wait strategy with spin loop hint which enables the processor to optimize its behavior
/// by e.g. saving power os switching hyper threads. Obviously, this can induce latency.
///
/// See also [`BusySpin`].
#[derive(Copy, Clone)]
pub struct BusySpinWithSpinLoopHint;

impl WaitStrategy for BusySpinWithSpinLoopHint {
	fn wait_for(&self, _sequence: Sequence) {
		hint::spin_loop();
	}
}

/// This strategy spins for a configurable number of times and then yields the thread.
#[derive(Copy, Clone)]
pub struct SpinThenYieldWaitStrategy {
	spin_cycles: u32
}

impl SpinThenYieldWaitStrategy {
	/// Creates a new `SpinThenYieldWaitStrategy`.
	///
	/// `spin_cycles` is the number of times the strategy will spin before yielding.
	pub const fn new(spin_cycles: u32) -> Self {
		Self { spin_cycles }
	}
}

impl Default for SpinThenYieldWaitStrategy {
	/// Creates a new `SpinThenYieldWaitStrategy` with 100 spin cycles.
	fn default() -> Self {
		Self { spin_cycles: 100 }
	}
}

impl WaitStrategy for SpinThenYieldWaitStrategy {
	fn wait_for(&self, _sequence: Sequence) {
		for _ in 0..self.spin_cycles {
			hint::spin_loop();
		}
		thread::yield_now();
	}
}
