//! Module with different strategies for waiting for an event to be published.
//!
//! The lowest latency possible is the [`BusySpin`] strategy.
//!
//! To "waste" less CPU time and power, use one of the other strategies which have higher latency.

use std::{
    hint,
    sync::atomic::{fence, AtomicI64, Ordering},
    thread,
};

use crate::{barrier::Barrier, Sequence};
use crossbeam_utils::CachePadded;

/// Error/alert conditions a waiter can encounter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    /// The disruptor is shutting down; consumer should stop.
    Shutdown,
    /// A wait timed out without progress.
    Timeout,
    /// The wait was interrupted (e.g. by a spurious wakeup or external signal).
    Interrupted,
}

/// Outcome of waiting for availability.
pub enum WaitOutcome {
    /// Highest contiguous sequence available (inclusive).
    Available {
        /// Highest available sequence (inclusive).
        upper: Sequence,
    },
    /// The disruptor is shutting down.
    Shutdown,
    /// The wait timed out without progress.
    Timeout,
}

/// Handle for nudging blocked waiters after publication.
pub trait WakeupNotifier: Send + Sync {
    /// Hint waiters that new data may be available.
    fn wake(&self);
}

/// No-op wakeup notifier for spin/yield strategies.
#[derive(Default, Clone)]
pub struct NoopWakeupNotifier;

impl WakeupNotifier for NoopWakeupNotifier {
    #[inline]
    fn wake(&self) {}
}

/// Public wait strategy; cloneable so producers can hold a copy for signaling.
pub trait WaitStrategy: Send + Sync + Clone + 'static {
    /// Per-consumer waiter type that holds any mutable state.
    type Waiter: Waiter;
    /// Handle type used by producers to wake blocked waiters.
    type Notifier: WakeupNotifier + Clone + Send + Sync + 'static;

    /// Create a new waiter instance for a consumer thread.
    fn new_waiter(&self) -> Self::Waiter;

    /// Called during build to hand producers a wakeup handle.
    fn notifier(&self) -> Self::Notifier;
}

/// Per-consumer stateful waiter; owns spin/backoff/park bookkeeping.
pub trait Waiter: Send {
    /// Return highest contiguous available ≥ requested, or a terminal/alert condition.
    ///
    /// Implementations must issue an `Acquire` fence before returning `Available`.
    fn wait_for(
        &mut self,
        requested: Sequence,
        barrier: &impl Barrier,
        shutdown: &CachePadded<AtomicI64>,
    ) -> WaitOutcome;
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Clone, Copy, Default)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
    type Waiter = BusySpinWaiter;
    type Notifier = NoopWakeupNotifier;

    #[inline]
    fn new_waiter(&self) -> Self::Waiter {
        BusySpinWaiter
    }

    #[inline]
    fn notifier(&self) -> Self::Notifier {
        NoopWakeupNotifier
    }
}

/// State holder for [`BusySpin`] wait strategy.
pub struct BusySpinWaiter;

impl Waiter for BusySpinWaiter {
    #[inline]
    fn wait_for(
        &mut self,
        requested: Sequence,
        barrier: &impl Barrier,
        shutdown: &CachePadded<AtomicI64>,
    ) -> WaitOutcome {
        loop {
            if shutdown.load(Ordering::Relaxed) == requested {
                return WaitOutcome::Shutdown;
            }

            let available = barrier.get_after(requested);
            if available >= requested {
                fence(Ordering::Acquire);
                return WaitOutcome::Available { upper: available };
            }
        }
    }
}

/// Busy spin wait strategy with spin loop hint which enables the processor to optimize its behavior
/// by e.g. saving power os switching hyper threads. Obviously, this can induce latency.
///
/// See also [`BusySpin`].
#[derive(Clone, Copy, Default)]
pub struct BusySpinWithSpinLoopHint;

impl WaitStrategy for BusySpinWithSpinLoopHint {
    type Waiter = BusySpinWithSpinLoopHintWaiter;
    type Notifier = NoopWakeupNotifier;

    fn new_waiter(&self) -> Self::Waiter {
        BusySpinWithSpinLoopHintWaiter
    }

    #[inline]
    fn notifier(&self) -> Self::Notifier {
        NoopWakeupNotifier
    }
}

/// State holder for [`BusySpinWithSpinLoopHint`] wait strategy.
pub struct BusySpinWithSpinLoopHintWaiter;

impl Waiter for BusySpinWithSpinLoopHintWaiter {
    fn wait_for(
        &mut self,
        requested: Sequence,
        barrier: &impl Barrier,
        shutdown: &CachePadded<AtomicI64>,
    ) -> WaitOutcome {
        loop {
            if shutdown.load(Ordering::Relaxed) == requested {
                return WaitOutcome::Shutdown;
            }

            let available = barrier.get_after(requested);
            if available >= requested {
                fence(Ordering::Acquire);
                return WaitOutcome::Available { upper: available };
            }

            hint::spin_loop();
        }
    }
}

const SPIN_TRIES: u32 = 100;

/// Yielding wait strategy: spin for a while, then yield the thread.
///
/// This mirrors the Java Disruptor's `YieldingWaitStrategy`, trading a small increase in latency
/// for reduced CPU usage under contention.
#[derive(Clone, Copy, Default)]
pub struct YieldingWaitStrategy;

impl WaitStrategy for YieldingWaitStrategy {
    type Waiter = YieldingWaiter;
    type Notifier = NoopWakeupNotifier;

    fn new_waiter(&self) -> Self::Waiter {
        YieldingWaiter {
            spins_remaining: SPIN_TRIES,
        }
    }

    #[inline]
    fn notifier(&self) -> Self::Notifier {
        NoopWakeupNotifier
    }
}

/// State holder for [`YieldingWaitStrategy`].
pub struct YieldingWaiter {
    spins_remaining: u32,
}

impl Waiter for YieldingWaiter {
    fn wait_for(
        &mut self,
        requested: Sequence,
        barrier: &impl Barrier,
        shutdown: &CachePadded<AtomicI64>,
    ) -> WaitOutcome {
        // Reset budget for this wait invocation.
        self.spins_remaining = SPIN_TRIES;
        loop {
            if shutdown.load(Ordering::Relaxed) == requested {
                return WaitOutcome::Shutdown;
            }

            let available = barrier.get_after(requested);
            if available >= requested {
                fence(Ordering::Acquire);
                return WaitOutcome::Available { upper: available };
            }

            if self.spins_remaining > 0 {
                self.spins_remaining -= 1;
                hint::spin_loop();
            } else {
                thread::yield_now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI64;

    struct TestBarrier {
        available: std::sync::Arc<AtomicI64>,
    }

    impl TestBarrier {
        fn new(v: i64) -> Self {
            Self {
                available: std::sync::Arc::new(AtomicI64::new(v)),
            }
        }
    }

    impl Barrier for TestBarrier {
        fn get_after(&self, _prev: Sequence) -> Sequence {
            self.available.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn yielding_waiter_resets_spin_budget_each_wait() {
        let mut waiter = YieldingWaiter {
            spins_remaining: 0, // force-reset to prove it is restored
        };
        let barrier = TestBarrier::new(5);
        let shutdown = CachePadded::new(AtomicI64::new(-1));

        // immediate availability: should reset and return without consuming spins
        let available = waiter.wait_for(5, &barrier, &shutdown);
        assert!(matches!(available, WaitOutcome::Available { upper: 5 }));
        assert_eq!(waiter.spins_remaining, SPIN_TRIES);

        // withhold availability briefly to consume budget, then release
        let barrier = TestBarrier::new(4); // less than requested
        let shutdown = CachePadded::new(AtomicI64::new(-1));
        let barrier_ref = barrier.available.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_micros(50));
            barrier_ref.store(6, Ordering::Relaxed);
        });
        let _ = waiter.wait_for(6, &barrier, &shutdown);
        handle.join().unwrap();

        // Next call should start with a fresh spin budget.
        let barrier = TestBarrier::new(7);
        let shutdown = CachePadded::new(AtomicI64::new(-1));
        let _ = waiter.wait_for(7, &barrier, &shutdown);
        assert_eq!(waiter.spins_remaining, SPIN_TRIES);
    }

    #[test]
    fn yielding_waiter_honors_shutdown() {
        let mut waiter = YieldingWaiter {
            spins_remaining: SPIN_TRIES,
        };
        let barrier = TestBarrier::new(-1);
        let shutdown = CachePadded::new(AtomicI64::new(10));

        let res = waiter.wait_for(10, &barrier, &shutdown);
        assert!(matches!(res, WaitOutcome::Shutdown));
    }

    #[test]
    fn busy_spin_waiter_observes_release_with_acquire_fence() {
        let mut waiter = BusySpinWaiter;
        let shutdown = std::sync::Arc::new(CachePadded::new(AtomicI64::new(-1)));
        let data = std::sync::Arc::new(AtomicI64::new(0));
        let barrier = TestBarrier::new(0);
        let barrier_for_thread = barrier.available.clone();
        let data_for_thread = data.clone();

        // Writer thread publishes data with Release before making sequence available.
        let handle = std::thread::spawn(move || {
            data_for_thread.store(1, Ordering::Release);
            barrier_for_thread.store(1, Ordering::Release);
        });

        // Wait for sequence 1; fence inside waiter should make the data visible.
        let available = waiter.wait_for(1, &barrier, shutdown.as_ref());
        assert!(matches!(available, WaitOutcome::Available { upper: 1 }));
        assert_eq!(data.load(Ordering::Acquire), 1);
        handle.join().unwrap();
    }

    #[test]
    fn busy_spin_waiter_exits_on_shutdown_race() {
        let mut waiter = BusySpinWaiter;
        let barrier = TestBarrier::new(-1);
        let shutdown = std::sync::Arc::new(CachePadded::new(AtomicI64::new(-1)));

        // Flip shutdown while waiter would otherwise spin.
        let shutdown_ref = shutdown.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_micros(50));
            shutdown_ref.store(5, Ordering::Relaxed);
        });

        let res = waiter.wait_for(5, &barrier, shutdown.as_ref());
        assert!(matches!(res, WaitOutcome::Shutdown));
        handle.join().unwrap();
    }
}
