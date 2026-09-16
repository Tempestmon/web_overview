use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::topology::FaultKind;

#[derive(Clone, Debug)]
pub struct Limits {
    pub default_ttl: Duration,
    pub max_ttl: Duration,
    pub cooldown: Duration,
    pub per_ip: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fault {
    pub id: u64,
    pub node: String,
    pub kind: FaultKind,
    pub owner: String,
    pub started: Instant,
    pub ends_at: Instant,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    Busy { remaining: Duration },
    Cooldown { remaining: Duration },
    RateLimited { retry_after: Duration },
}

/// Bookkeeping for the shared world: who broke what, until when, and who
/// has to wait. Pure logic with an explicit clock so it is testable.
pub struct FaultBook {
    limits: Limits,
    active: Vec<Fault>,
    cooldown_until: Option<Instant>,
    last_by_owner: HashMap<String, Instant>,
    next_id: u64,
}

impl FaultBook {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            active: Vec::new(),
            cooldown_until: None,
            last_by_owner: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn try_start(
        &mut self,
        node: &str,
        kind: FaultKind,
        owner: &str,
        ttl: Option<Duration>,
        now: Instant,
        admin: bool,
    ) -> Result<Fault, Refusal> {
        if !admin {
            self.check_limits(owner, now)?;
        }
        if self.active.iter().any(|f| f.node == node) {
            return Err(Refusal::Busy {
                remaining: self.busy_remaining(now),
            });
        }
        let ttl = ttl
            .unwrap_or(self.limits.default_ttl)
            .min(self.limits.max_ttl);
        let fault = Fault {
            id: self.next_id,
            node: node.to_string(),
            kind,
            owner: owner.to_string(),
            started: now,
            ends_at: now + ttl,
        };
        self.next_id += 1;
        self.active.push(fault.clone());
        self.last_by_owner.insert(owner.to_string(), now);
        Ok(fault)
    }

    fn check_limits(&mut self, owner: &str, now: Instant) -> Result<(), Refusal> {
        if !self.active.is_empty() {
            return Err(Refusal::Busy {
                remaining: self.busy_remaining(now),
            });
        }
        if let Some(remaining) = self.cooldown_remaining(now) {
            return Err(Refusal::Cooldown { remaining });
        }
        self.last_by_owner
            .retain(|_, t| now.duration_since(*t) < self.limits.per_ip);
        if let Some(last) = self.last_by_owner.get(owner) {
            return Err(Refusal::RateLimited {
                retry_after: self.limits.per_ip - now.duration_since(*last),
            });
        }
        Ok(())
    }

    fn busy_remaining(&self, now: Instant) -> Duration {
        self.active
            .iter()
            .map(|f| f.ends_at.saturating_duration_since(now))
            .max()
            .unwrap_or_default()
    }

    pub fn cooldown_remaining(&self, now: Instant) -> Option<Duration> {
        self.cooldown_until
            .map(|t| t.saturating_duration_since(now))
            .filter(|d| !d.is_zero())
    }

    pub fn active(&self) -> &[Fault] {
        &self.active
    }

    pub fn get(&self, id: u64) -> Option<&Fault> {
        self.active.iter().find(|f| f.id == id)
    }

    pub fn expired(&self, now: Instant) -> Vec<Fault> {
        self.active
            .iter()
            .filter(|f| f.ends_at <= now)
            .cloned()
            .collect()
    }

    /// Removes the fault and starts the cooldown. Returns `None` when it was
    /// already finished, so callers can heal exactly once.
    pub fn finish(&mut self, id: u64, now: Instant) -> Option<Fault> {
        let idx = self.active.iter().position(|f| f.id == id)?;
        let fault = self.active.remove(idx);
        self.cooldown_until = Some(now + self.limits.cooldown);
        Some(fault)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> FaultBook {
        FaultBook::new(Limits {
            default_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(300),
            cooldown: Duration::from_secs(15),
            per_ip: Duration::from_secs(120),
        })
    }

    #[test]
    fn second_fault_is_refused_while_one_is_active() {
        let mut b = book();
        let t0 = Instant::now();
        b.try_start("kitchen-svc", FaultKind::Stop, "1.1.1.1", None, t0, false)
            .unwrap();
        let refusal = b.try_start(
            "redis",
            FaultKind::Stop,
            "2.2.2.2",
            None,
            t0 + Duration::from_secs(10),
            false,
        );
        assert_eq!(
            refusal,
            Err(Refusal::Busy {
                remaining: Duration::from_secs(50)
            })
        );
    }

    #[test]
    fn cooldown_follows_finish_then_other_ips_may_go() {
        let mut b = book();
        let t0 = Instant::now();
        let f = b
            .try_start("kitchen-svc", FaultKind::Stop, "1.1.1.1", None, t0, false)
            .unwrap();
        assert!(b.finish(f.id, t0 + Duration::from_secs(30)).is_some());
        assert!(b.finish(f.id, t0 + Duration::from_secs(30)).is_none());
        let t1 = t0 + Duration::from_secs(35);
        assert!(matches!(
            b.try_start("redis", FaultKind::Stop, "2.2.2.2", None, t1, false),
            Err(Refusal::Cooldown { .. })
        ));
        let t2 = t0 + Duration::from_secs(46);
        assert!(
            b.try_start("redis", FaultKind::Stop, "2.2.2.2", None, t2, false)
                .is_ok()
        );
    }

    #[test]
    fn same_ip_is_rate_limited() {
        let mut b = book();
        let t0 = Instant::now();
        let f = b
            .try_start("kitchen-svc", FaultKind::Stop, "1.1.1.1", None, t0, false)
            .unwrap();
        b.finish(f.id, t0 + Duration::from_secs(60));
        let t1 = t0 + Duration::from_secs(90);
        assert!(matches!(
            b.try_start("redis", FaultKind::Stop, "1.1.1.1", None, t1, false),
            Err(Refusal::RateLimited { .. })
        ));
        let t2 = t0 + Duration::from_secs(121);
        assert!(
            b.try_start("redis", FaultKind::Stop, "1.1.1.1", None, t2, false)
                .is_ok()
        );
    }

    #[test]
    fn admin_bypasses_limits_but_not_double_break() {
        let mut b = book();
        let t0 = Instant::now();
        b.try_start("kitchen-svc", FaultKind::Stop, "1.1.1.1", None, t0, false)
            .unwrap();
        assert!(
            b.try_start("redis", FaultKind::Pause, "admin", None, t0, true)
                .is_ok()
        );
        assert!(matches!(
            b.try_start("redis", FaultKind::Stop, "admin", None, t0, true),
            Err(Refusal::Busy { .. })
        ));
        assert_eq!(b.active().len(), 2);
    }

    #[test]
    fn ttl_is_capped_and_expiry_is_reported() {
        let mut b = book();
        let t0 = Instant::now();
        let f = b
            .try_start(
                "kitchen-svc",
                FaultKind::Stop,
                "1.1.1.1",
                Some(Duration::from_secs(9999)),
                t0,
                false,
            )
            .unwrap();
        assert_eq!(f.ends_at, t0 + Duration::from_secs(300));
        assert!(b.expired(t0 + Duration::from_secs(299)).is_empty());
        assert_eq!(b.expired(t0 + Duration::from_secs(300)).len(), 1);
    }
}
