use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Consecutive failures that trip the breaker.
    pub failure_threshold: u32,
    /// How long to reject calls before letting one probe through.
    pub open_for: Duration,
    /// Label for the `circuit_breaker_state` gauge; use the dependency's node id.
    pub name: &'static str,
}

struct Inner {
    state: BreakerState,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    probe_in_flight: bool,
}

/// Classic three-state circuit breaker. The point of it in this project is
/// pedagogical: with the breaker open the order service fails fast instead
/// of piling timeouts onto an already sick payment service, and the panel
/// can show that decision as a state, not as a latency graph.
pub struct CircuitBreaker {
    config: Config,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    pub fn new(config: Config) -> Self {
        let breaker = Self {
            config,
            inner: Mutex::new(Inner {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                opened_at: None,
                probe_in_flight: false,
            }),
        };
        breaker.publish(BreakerState::Closed);
        breaker
    }

    /// Whether a call may proceed right now. In half-open only one probe is
    /// admitted until it reports back.
    pub fn allow(&self, now: Instant) -> bool {
        let mut inner = self.inner.lock().expect("breaker mutex");
        match inner.state {
            BreakerState::Closed => true,
            BreakerState::Open => {
                let elapsed = inner
                    .opened_at
                    .map(|t| now.duration_since(t))
                    .unwrap_or_default();
                if elapsed >= self.config.open_for {
                    inner.state = BreakerState::HalfOpen;
                    inner.probe_in_flight = true;
                    self.publish(BreakerState::HalfOpen);
                    true
                } else {
                    false
                }
            }
            BreakerState::HalfOpen => {
                if inner.probe_in_flight {
                    false
                } else {
                    inner.probe_in_flight = true;
                    true
                }
            }
        }
    }

    pub fn on_success(&self) {
        let mut inner = self.inner.lock().expect("breaker mutex");
        inner.consecutive_failures = 0;
        inner.probe_in_flight = false;
        if inner.state != BreakerState::Closed {
            inner.state = BreakerState::Closed;
            inner.opened_at = None;
            self.publish(BreakerState::Closed);
        }
    }

    pub fn on_failure(&self, now: Instant) {
        let mut inner = self.inner.lock().expect("breaker mutex");
        inner.probe_in_flight = false;
        inner.consecutive_failures += 1;
        let should_open = inner.state == BreakerState::HalfOpen
            || inner.consecutive_failures >= self.config.failure_threshold;
        if should_open && inner.state != BreakerState::Open {
            inner.state = BreakerState::Open;
            inner.opened_at = Some(now);
            self.publish(BreakerState::Open);
        } else if should_open {
            inner.opened_at = Some(now);
        }
    }

    pub fn state(&self) -> BreakerState {
        self.inner.lock().expect("breaker mutex").state
    }

    fn publish(&self, state: BreakerState) {
        let value = match state {
            BreakerState::Closed => 0.0,
            BreakerState::Open => 1.0,
            BreakerState::HalfOpen => 2.0,
        };
        metrics::gauge!("circuit_breaker_state", "name" => self.config.name).set(value);
        metrics::counter!("circuit_breaker_transitions_total", "name" => self.config.name)
            .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(Config {
            failure_threshold: 3,
            open_for: Duration::from_secs(10),
            name: "test",
        })
    }

    #[test]
    fn opens_after_threshold_and_rejects_until_timeout() {
        let b = breaker();
        let t0 = Instant::now();
        assert!(b.allow(t0));
        b.on_failure(t0);
        b.on_failure(t0);
        assert_eq!(b.state(), BreakerState::Closed);
        b.on_failure(t0);
        assert_eq!(b.state(), BreakerState::Open);
        assert!(!b.allow(t0 + Duration::from_secs(9)));
    }

    #[test]
    fn half_open_admits_one_probe_then_closes_on_success() {
        let b = breaker();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure(t0);
        }
        let t1 = t0 + Duration::from_secs(10);
        assert!(b.allow(t1), "first call after open_for is the probe");
        assert_eq!(b.state(), BreakerState::HalfOpen);
        assert!(
            !b.allow(t1),
            "second concurrent call is rejected while probing"
        );
        b.on_success();
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(b.allow(t1));
    }

    #[test]
    fn failed_probe_reopens_for_a_full_window() {
        let b = breaker();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure(t0);
        }
        let t1 = t0 + Duration::from_secs(10);
        assert!(b.allow(t1));
        b.on_failure(t1);
        assert_eq!(b.state(), BreakerState::Open);
        assert!(!b.allow(t1 + Duration::from_secs(9)));
        assert!(b.allow(t1 + Duration::from_secs(10)));
    }

    #[test]
    fn success_resets_failure_streak() {
        let b = breaker();
        let t0 = Instant::now();
        b.on_failure(t0);
        b.on_failure(t0);
        b.on_success();
        b.on_failure(t0);
        b.on_failure(t0);
        assert_eq!(b.state(), BreakerState::Closed);
    }
}
