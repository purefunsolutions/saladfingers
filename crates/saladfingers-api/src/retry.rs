// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Client-side rate limiting and retry policy.
//!
//! A token bucket keeps us comfortably under the key-wide 240 req/min ceiling
//! (default 180/min sustained, burst 20). [`RetryPolicy`] drives the request loop's
//! full-jitter exponential backoff.

use std::sync::Mutex;

use tokio::time::{Duration, Instant};

/// A simple refilling token bucket. Every request acquires one token first.
///
/// Uses `tokio::time` so it behaves correctly under paused-time tests.
pub struct TokenBucket {
    state: Mutex<State>,
    capacity: f64,
    refill_per_sec: f64,
}

struct State {
    tokens: f64,
    last: Option<Instant>,
}

impl TokenBucket {
    /// A bucket sustaining `rate_per_min` requests per minute with a burst of 20.
    #[must_use]
    pub fn per_minute(rate_per_min: u32) -> Self {
        let capacity = 20.0;
        Self {
            state: Mutex::new(State {
                tokens: capacity,
                last: None,
            }),
            capacity,
            refill_per_sec: f64::from(rate_per_min.max(1)) / 60.0,
        }
    }

    fn refill_locked(&self, st: &mut State, now: Instant) {
        let elapsed = match st.last {
            Some(last) => now.saturating_duration_since(last).as_secs_f64(),
            None => 0.0,
        };
        st.tokens = (st.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        st.last = Some(now);
    }

    /// Block until a token is available, then consume it.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut st = self.state.lock().expect("token bucket poisoned");
                self.refill_locked(&mut st, Instant::now());
                if st.tokens >= 1.0 {
                    st.tokens -= 1.0;
                    return;
                }
                let deficit = 1.0 - st.tokens;
                Duration::from_secs_f64(deficit / self.refill_per_sec)
            };
            tokio::time::sleep(wait).await;
        }
    }

    /// Empty the bucket (used after a 429 to force a cool-down).
    pub fn drain(&self) {
        let mut st = self.state.lock().expect("token bucket poisoned");
        st.tokens = 0.0;
        st.last = Some(Instant::now());
    }
}

/// Retry/backoff policy for the request loop.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first).
    pub max_attempts: u32,
    /// Base delay for the exponential backoff.
    pub base_delay: Duration,
    /// Ceiling on any single backoff delay.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(8),
        }
    }
}

impl RetryPolicy {
    /// Full-jitter exponential backoff for a zero-based `attempt`. `jitter01` is a
    /// value in `[0, 1)` supplying the jitter (a parameter so it stays testable).
    #[must_use]
    pub fn delay(&self, attempt: u32, jitter01: f64) -> Duration {
        let exp = self.base_delay.saturating_mul(1u32 << attempt.min(5));
        let capped = if exp > self.max_delay {
            self.max_delay
        } else {
            exp
        };
        capped.mul_f64(jitter01.clamp(0.05, 1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let p = RetryPolicy::default();
        assert_eq!(p.delay(0, 1.0), Duration::from_millis(500));
        assert_eq!(p.delay(1, 1.0), Duration::from_secs(1));
        assert_eq!(p.delay(2, 1.0), Duration::from_secs(2));
        assert_eq!(p.delay(10, 1.0), Duration::from_secs(8));
        assert!(p.delay(2, 0.0) >= Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn burst_is_instant_then_paces() {
        let bucket = TokenBucket::per_minute(60); // 1/s sustained, burst 20
        let start = Instant::now();
        for _ in 0..20 {
            bucket.acquire().await;
        }
        assert!(
            start.elapsed() < Duration::from_millis(10),
            "burst should be instant"
        );

        for _ in 0..5 {
            bucket.acquire().await;
        }
        // 5 more tokens at 1/s must take ~5 virtual seconds.
        assert!(
            start.elapsed() >= Duration::from_secs(4),
            "should pace after the burst"
        );
    }
}
