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
	fn wait_for<F>(&self, sequence: Sequence, f: F) where F: FnMut() -> bool;
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for<F>(&self, _sequence: Sequence, mut f: F) where F: FnMut() -> bool {
		while !f() {
			// Do nothing, true busy spin.
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;

	#[test]
	fn test_spin_then_yield_wait_strategy_spins() {
		let strategy = SpinThenYieldWaitStrategy::new(10);
		let counter = Arc::new(AtomicUsize::new(0));

		strategy.wait_for(0, || {
			counter.fetch_add(1, Ordering::Relaxed);
			// Condition is met within the spin loop.
			counter.load(Ordering::Relaxed) > 5
		});

		assert_eq!(counter.load(Ordering::Relaxed), 6);
	}

	#[test]
	fn test_spin_then_yield_wait_strategy_yields() {
		let strategy = SpinThenYieldWaitStrategy::new(10);
		let counter = Arc::new(AtomicUsize::new(0));

		strategy.wait_for(0, || {
			counter.fetch_add(1, Ordering::Relaxed);
			// Condition is met after the spin loop, forcing a yield.
			counter.load(Ordering::Relaxed) > 15
		});

		assert_eq!(counter.load(Ordering::Relaxed), 16);
	}
}

/// Busy spin wait strategy with spin loop hint which enables the processor to optimize its behavior
/// by e.g. saving power os switching hyper threads. Obviously, this can induce latency.
///
/// See also [`BusySpin`].
#[derive(Copy, Clone)]
pub struct BusySpinWithSpinLoopHint;

impl WaitStrategy for BusySpinWithSpinLoopHint {
	fn wait_for<F>(&self, _sequence: Sequence, mut f: F) where F: FnMut() -> bool {
		while !f() {
			hint::spin_loop();
		}
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
	fn wait_for<F>(&self, _sequence: Sequence, mut f: F) where F: FnMut() -> bool {
		for _ in 0..self.spin_cycles {
			if f() {
				return;
			}
			hint::spin_loop();
		}

		while !f() {
			thread::yield_now();
		}
	}
}
