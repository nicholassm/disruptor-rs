//! Module with different strategies for waiting for an event to be published.
//!
//! The lowest latency possible is the [`BusySpin`] strategy.
//!
//! To "waste" less CPU time and power, use one of the other strategies which have higher latency.

use std::hint;
use std::thread;
use std::time::Duration;

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

/// Sleeps for a millisecond. Useful if you are developing, dont want to occupy a core 100% and listen
/// to the machine screaming.
#[derive(Copy, Clone)]
pub struct SlowSleepDebug;

impl WaitStrategy for SlowSleepDebug {
	fn wait_for(&self, _sequence: Sequence) {
		    thread::sleep(Duration::from_millis(1));
	}
}