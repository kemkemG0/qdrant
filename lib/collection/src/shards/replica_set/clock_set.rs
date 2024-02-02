use std::cmp;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct ClockSet {
    clocks: Vec<Arc<Clock>>,
}

impl ClockSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the first available clock from this set, or create a new one.
    pub fn get_clock(&mut self) -> ClockGuard {
        self.clocks
            .iter()
            .enumerate()
            .find_map(|(id, clock)| clock.try_lock(id))
            .unwrap_or_else(|| self.new_clock())
    }

    /// Create a new clock, lock it, and return a guard.
    fn new_clock(&mut self) -> ClockGuard {
        let id = self.clocks.len();
        let clock = Arc::new(Clock::new_unlocked());
        self.clocks.push(clock.clone());
        clock.try_lock(id).unwrap()
    }
}

#[derive(Debug)]
pub struct ClockGuard {
    id: usize,
    clock: Arc<Clock>,
}

impl ClockGuard {
    fn new(id: usize, clock: Arc<Clock>) -> Self {
        Self { id, clock }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    #[must_use = "new clock value must be used"]
    pub fn tick_once(&mut self) -> u64 {
        self.clock.tick_once()
    }

    pub fn advance_to(&mut self, new_tick: u64) -> u64 {
        self.clock.advance_to(new_tick)
    }
}

impl Drop for ClockGuard {
    fn drop(&mut self) {
        self.clock.release();
    }
}

#[derive(Debug)]
struct Clock {
    clock: AtomicU64,
    available: AtomicBool,
}

impl Clock {
    pub fn new_unlocked() -> Self {
        Self {
            clock: AtomicU64::new(0),
            available: AtomicBool::new(true),
        }
    }

    pub fn tick_once(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn advance_to(&self, new_tick: u64) -> u64 {
        let current_tick = self.clock.fetch_max(new_tick, Ordering::Relaxed);
        cmp::max(current_tick, new_tick)
    }

    /// Lock this clock, returning a guard.
    ///
    /// Returns `None` if this clock is unavailable.
    fn try_lock(self: &Arc<Self>, id: usize) -> Option<ClockGuard> {
        self.available
            .swap(false, Ordering::Relaxed)
            .then(|| ClockGuard::new(id, self.clone()))
    }

    /// Release this clock. Should never be invoked manually.
    fn release(&self) {
        self.available.store(true, Ordering::Relaxed);
    }
}
