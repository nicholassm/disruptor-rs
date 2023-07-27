//! Module with different strategies for waiting for an event to be published.
//!
//! The lowest latency possible is the [`BusySpin`] strategy.
//!
//! To "waist" less CPU time and power, use one of the other strategies which has higher latency.

/// Wait strategies are used by consumers when no new events are ready on the ring buffer.
pub trait WaitStrategy: Copy + Send {
	/// The wait strategy will wait for the sequence id being available.
	fn wait_for(&self, sequence: i64);
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for(&self, _sequence: i64) {
		// Do nothing, true busy spin.
	}
}
