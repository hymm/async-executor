//! adapted from https://zig.news/kprotty/resource-efficient-thread-pools-with-zig-3291
use std::{
    sync::atomic::{AtomicU32, Ordering},
    task::Waker,
};

use sharded_slab::Slab;

// Constants used for extracting counters out of packed value
/// Number of bits used to store thread counts
const THREAD_BITS: usize = 10;
const THREADS_MAX: u32 = (1 << THREAD_BITS) - 1;
const IDLE_SHIFT: usize = 0 * THREAD_BITS;
const SPAWNED_SHIFT: usize = 1 * THREAD_BITS;
const STEALING_SHIFT: usize = 2 * THREAD_BITS;
const SHUTDOWN_SHIFT: usize = 3 * THREAD_BITS + 1;

struct Sleepers {
    counters: AtomicCounters,
    /// List of wakers for different threads
    wakers: Slab<Waker>,
}

impl Sleepers {
    fn notify(&self) {
        let mut counters: Counters = self.counters.value.load(Ordering::Relaxed).into();
        loop {
            if counters.shutdown() || counters.stealing() != 0 {
                return;
            }

            let new_counters = counters;
            new_counters.set_stealing(1);

            if counters.idle() == 0 {
                // no idle threads to notify
                return;
            }

            // try to update counters for a new stealer.
            let Ok(read_counters): Result<Counters, _> = self
                .counters
                .value
                .compare_exchange(
                    counters.value,
                    new_counters.value,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .map(|value| value.into())
            else {
                // if we can't update the counters that means something else ...
                if counters.idle() > 0 {
                    let waker = self.wakers.remove_one();
                    waker.wake();
                    return;
                }

                return;
            };
            counters = read_counters;
        }
    }
}

struct AtomicCounters {
    /// idle: u10
    /// spawned: u10
    /// stealing: u10
    /// padding: bool
    /// shutdown: bool
    value: AtomicU32,
}

impl AtomicCounters {}

#[inline]
fn select_thread(word: u32, shift: usize) -> u32 {
    (word >> shift) & THREADS_MAX
}

#[derive(Clone, Copy)]
struct Counters {
    value: u32,
}

impl Counters {
    fn shutdown(&self) -> bool {
        self.value & 1 << SHUTDOWN_SHIFT != 0
    }

    fn idle(&self) -> u32 {
        select_thread(self.value, IDLE_SHIFT)
    }

    fn spawned(&self) -> u32 {
        select_thread(self.value, SPAWNED_SHIFT)
    }

    fn stealing(&self) -> u32 {
        select_thread(self.value, STEALING_SHIFT)
    }

    fn increment_spawned(&mut self) {
        // TODO: do we need to check for overflow?
        self.value += 1 << SPAWNED_SHIFT;
    }
}

impl From<u32> for Counters {
    fn from(value: u32) -> Self {
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_bit() {
        let counter = Counters {
            value: 1 << SHUTDOWN_SHIFT,
        };
        assert!(counter.shutdown());

        let counter = Counters {
            value: u32::MAX - 1 << SHUTDOWN_SHIFT,
        };
        assert!(!counter.shutdown());
    }
}
