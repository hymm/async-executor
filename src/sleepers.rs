//! adapted from https://zig.news/kprotty/resource-efficient-thread-pools-with-zig-3291
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
    task::Waker,
};

use concurrent_queue::ConcurrentQueue;
use sharded_slab::Slab;

// idle or stealing

// Constants used for extracting counters out of packed value
/// Number of bits used to store thread counts
const THREAD_BITS: usize = 10;
const THREADS_MAX: u32 = (1 << THREAD_BITS) - 1;
const IDLE_SHIFT: usize = 0 * THREAD_BITS;
const SPAWNED_SHIFT: usize = 1 * THREAD_BITS;
const STEALING_SHIFT: usize = 2 * THREAD_BITS;
const SHUTDOWN_SHIFT: usize = 3 * THREAD_BITS + 1;

pub struct Sleepers {
    counters: AtomicCounters,
    /// List of wakers for different threads
    wakers: Arc<Slab<Waker>>,
    // TODO: figure out a better data structure for storing the wakers
    wakers_ids: RwLock<ConcurrentQueue<usize>>,
}

impl Sleepers {
    pub const fn new() -> Self {
        Self {
            counters: AtomicCounters::new(),
            wakers: Arc::new(Slab::new()),
            wakers_ids: RwLock::new(ConcurrentQueue::unbounded()),
        }
    }

    pub fn notify(&self) {
        let mut counters: Counters = self.counters.value.load(Ordering::Relaxed).into();
        loop {
            if counters.shutdown() || counters.stealing() != 0 {
                return;
            }

            let mut new_counters = counters;
            new_counters.increment_stealing();

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
                    let waker = self.pop().unwrap();
                    waker.wake();
                    return;
                }

                return;
            };
            counters = read_counters;
        }
    }

    fn pop(&self) -> Option<Waker> {
        let id = self.wakers_ids.read().unwrap().pop().ok()?;
        self.wakers.take(id)
    }

    /// Re-inserts a sleeping ticker's waker if it was notified.
    ///
    /// Returns `true` if the ticker was notified.
    pub fn update(&self, id: usize, waker: &Waker) -> bool {
        let Some(entry) = self.wakers.get_owned(id) else {
            // this isn't quite right as the entry will be at a new id
            return self.insert(waker).is_some();
        };

        entry = waker.clone();
        false
    }

    /// Inserts a new sleeping ticker.
    pub fn insert(&self, waker: &Waker) -> Option<usize> {
        // TODO: update counters
        let id = self.wakers.insert(waker.clone());
        if let Some(id) = id {
            self.wakers_ids.write().unwrap().push(id);
        }
        id
    }

    /// Removes a previously inserted sleeping ticker.
    ///
    /// Returns `true` if the ticker was notified.
    pub fn remove(&self, id: usize) -> bool {
        // TODO: update counters
        self.wakers.remove(id)
    }

    /// Returns `true` if a sleeping ticker is notified or no tickers are sleeping.
    pub fn is_notified(&self) -> bool {
        self.wakers.is_empty() || self.count > self.wakers.len()
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

impl AtomicCounters {
    pub const fn new() -> Self {
        Self {
            value: AtomicU32::new(0),
        }
    }
}

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

    fn increment_stealing(&mut self) {
        // TODO: do we need to check for overflow?
        self.value += 1 << STEALING_SHIFT;
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
