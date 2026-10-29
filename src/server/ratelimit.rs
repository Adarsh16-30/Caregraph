//! Per-principal token-bucket rate limiting.
//!
//! One bucket per `(principal, class)`: reads and writes are limited
//! independently so a client hammering `Traverse` cannot spend its own write
//! budget, and one principal cannot spend another's. A refusal reports how long
//! until a token is available so a well-behaved client can back off precisely.
//!
//! The clock is passed in, which makes the arithmetic testable without sleeping.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Which budget a request draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcClass {
    Read,
    Write,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: Mutex<HashMap<(String, RpcClass), Bucket>>,
}

/// Past this many tracked principals, buckets that have refilled completely
/// (indistinguishable from a fresh one) are dropped.
const MAX_TRACKED: usize = 10_000;

impl RateLimiter {
    /// `rps` tokens per second, holding at most `burst`. Returns `None` when
    /// `rps == 0` (limiting disabled).
    pub fn new(rps: u32, burst: u32) -> Option<Self> {
        (rps > 0).then(|| RateLimiter {
            rate: f64::from(rps),
            burst: f64::from(burst.max(1)),
            buckets: Mutex::new(HashMap::new()),
        })
    }

    /// Takes one token for `principal`, or says how long to wait.
    pub fn check(&self, principal: &str, class: RpcClass) -> Result<(), Duration> {
        self.check_at(principal, class, Instant::now())
    }

    pub fn check_at(&self, principal: &str, class: RpcClass, now: Instant) -> Result<(), Duration> {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= MAX_TRACKED {
            let (rate, burst) = (self.rate, self.burst);
            map.retain(|_, b| {
                let refilled =
                    b.tokens + now.saturating_duration_since(b.last).as_secs_f64() * rate;
                refilled < burst
            });
        }
        let bucket = map
            .entry((principal.to_string(), class))
            .or_insert_with(|| Bucket {
                tokens: self.burst,
                last: now,
            });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - bucket.tokens) / self.rate))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_rate_disables_limiting() {
        assert!(RateLimiter::new(0, 5).is_none());
    }

    #[test]
    fn burst_is_spent_then_refused_then_refills_with_time() {
        let rl = RateLimiter::new(10, 3).unwrap();
        let t0 = Instant::now();
        for _ in 0..3 {
            assert!(rl.check_at("a", RpcClass::Read, t0).is_ok());
        }
        let wait = rl.check_at("a", RpcClass::Read, t0).unwrap_err();
        assert!(wait <= Duration::from_millis(100) && wait > Duration::ZERO);
        // 100 ms at 10/s is one token.
        assert!(rl
            .check_at("a", RpcClass::Read, t0 + Duration::from_millis(101))
            .is_ok());
        assert!(rl
            .check_at("a", RpcClass::Read, t0 + Duration::from_millis(101))
            .is_err());
    }

    #[test]
    fn principals_and_classes_have_independent_budgets() {
        let rl = RateLimiter::new(1, 1).unwrap();
        let t0 = Instant::now();
        assert!(rl.check_at("a", RpcClass::Read, t0).is_ok());
        assert!(rl.check_at("a", RpcClass::Read, t0).is_err());
        assert!(rl.check_at("a", RpcClass::Write, t0).is_ok());
        assert!(rl.check_at("b", RpcClass::Read, t0).is_ok());
    }

    #[test]
    fn tokens_never_exceed_the_burst() {
        let rl = RateLimiter::new(1000, 2).unwrap();
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(60);
        assert!(rl.check_at("a", RpcClass::Read, t0).is_ok());
        assert!(rl.check_at("a", RpcClass::Read, later).is_ok());
        assert!(rl.check_at("a", RpcClass::Read, later).is_ok());
        assert!(rl.check_at("a", RpcClass::Read, later).is_err());
    }
}
