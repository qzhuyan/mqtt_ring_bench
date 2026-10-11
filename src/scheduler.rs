// SPDX-License-Identifier: MPL-2.0

use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// One timer and at most one ready-queue entry per connection. Replacing a
/// deadline removes the old entry, so busy connections cannot accumulate stale
/// timers while moving their keepalive deadlines forward.
pub struct Scheduler {
    deadlines: Vec<Option<Instant>>,
    timers: BTreeSet<(Instant, usize)>,
    queued: Vec<bool>,
    ready: VecDeque<usize>,
}

impl Scheduler {
    pub fn new(capacity: usize) -> Self {
        Self {
            deadlines: vec![None; capacity],
            timers: BTreeSet::new(),
            queued: vec![false; capacity],
            ready: VecDeque::new(),
        }
    }

    pub fn mark_ready(&mut self, key: usize) {
        if !self.queued[key] {
            self.queued[key] = true;
            self.ready.push_back(key);
        }
    }

    pub fn pop_ready(&mut self) -> Option<usize> {
        let key = self.ready.pop_front()?;
        self.queued[key] = false;
        Some(key)
    }

    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub fn schedule(&mut self, key: usize, deadline: Option<Instant>) {
        if self.deadlines[key] == deadline {
            return;
        }
        if let Some(old) = self.deadlines[key].take() {
            self.timers.remove(&(old, key));
        }
        if let Some(at) = deadline {
            self.timers.insert((at, key));
        }
        self.deadlines[key] = deadline;
    }

    pub fn wake_due(&mut self, now: Instant) {
        while let Some(&(at, key)) = self.timers.first() {
            if at > now {
                break;
            }
            self.timers.pop_first();
            self.deadlines[key] = None;
            self.mark_ready(key);
        }
    }

    pub fn wait_duration(&self, now: Instant, ramp_deadline: Option<Instant>) -> Duration {
        if !self.ready.is_empty() {
            return Duration::ZERO;
        }
        self.timers
            .first()
            .map(|&(at, _)| at)
            .into_iter()
            .chain(ramp_deadline)
            .min()
            .map(|at| at.saturating_duration_since(now))
            .unwrap_or(Duration::from_secs(1))
            .min(Duration::from_secs(1))
    }
}

/// Accumulate fractional per-worker connection credit in 10 ms batches. A
/// bounded burst leaves time to service existing connections during ramp-up.
pub struct ConnectPacer {
    rate: f64,
    credit: f64,
    updated_at: Instant,
    next_at: Instant,
}

impl ConnectPacer {
    pub fn new(rate: usize, workers: usize, now: Instant) -> Self {
        Self {
            rate: rate as f64 / workers.max(1) as f64,
            credit: 0.0,
            updated_at: now,
            next_at: now + Duration::from_millis(10),
        }
    }

    pub fn take(&mut self, now: Instant, remaining: usize) -> usize {
        if now < self.next_at {
            return 0;
        }
        self.credit = (self.credit + self.rate * now.duration_since(self.updated_at).as_secs_f64())
            .min(remaining as f64);
        self.updated_at = now;
        let count = (self.credit as usize).min(256);
        self.credit -= count as f64;
        self.next_at = if self.credit >= 1.0 {
            now
        } else {
            now + Duration::from_millis(10)
        };
        count
    }

    pub fn next_at(&self) -> Instant {
        self.next_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_due_connections_wake_and_io_is_deduplicated() {
        let now = Instant::now();
        let mut scheduler = Scheduler::new(65_000);
        for key in 0..65_000 {
            scheduler.schedule(key, Some(now + Duration::from_secs(60)));
        }
        scheduler.schedule(42, Some(now));
        scheduler.mark_ready(42);
        scheduler.mark_ready(42);
        scheduler.wake_due(now);
        assert_eq!(scheduler.pop_ready(), Some(42));
        assert_eq!(scheduler.pop_ready(), None);
        assert_eq!(scheduler.timers.len(), 64_999);
    }

    #[test]
    fn moving_or_cancelling_deadlines_does_not_leave_stale_wakeups() {
        let now = Instant::now();
        let mut scheduler = Scheduler::new(2);
        for offset in 0..10_000 {
            scheduler.schedule(0, Some(now + Duration::from_millis(offset)));
        }
        assert_eq!(scheduler.timers.len(), 1);
        scheduler.wake_due(now + Duration::from_secs(1));
        assert_eq!(scheduler.pop_ready(), None);
        scheduler.schedule(0, None);
        scheduler.wake_due(now + Duration::from_secs(20));
        assert_eq!(scheduler.pop_ready(), None);
        assert!(scheduler.timers.is_empty());
    }

    #[test]
    fn ready_work_and_ramp_deadlines_bound_the_wait() {
        let now = Instant::now();
        let mut scheduler = Scheduler::new(1);
        scheduler.schedule(0, Some(now + Duration::from_secs(60)));
        assert_eq!(scheduler.wait_duration(now, None), Duration::from_secs(1));
        let ramp = now + Duration::from_millis(10);
        assert_eq!(
            scheduler.wait_duration(now, Some(ramp)),
            Duration::from_millis(10)
        );
        scheduler.mark_ready(0);
        assert_eq!(scheduler.wait_duration(now, None), Duration::ZERO);
    }

    #[test]
    fn low_connection_rates_work_with_multiple_workers_and_bursts_are_bounded() {
        let now = Instant::now();
        let mut pacer = ConnectPacer::new(1, 4, now);
        assert_eq!(pacer.take(now + Duration::from_secs(2), 10), 0);
        assert_eq!(pacer.take(now + Duration::from_secs(4), 10), 1);
        let mut pacer = ConnectPacer::new(100_000, 1, now);
        assert_eq!(pacer.take(now + Duration::from_secs(1), 1000), 256);
        assert_eq!(pacer.next_at(), now + Duration::from_secs(1));
        assert_eq!(pacer.take(now + Duration::from_secs(1), 10), 10);
    }
}
