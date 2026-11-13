//! `/ops/load`: how many virtual customers there are right now. A boost
//! always carries a TTL and the generator itself reverts it, so a controller
//! that restarts mid-boost cannot leave the stand overloaded forever.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::crowd::Crowd;

#[derive(Clone, Copy)]
pub struct Limits {
    pub base_users: usize,
    pub max_users: usize,
    pub max_ttl: Duration,
}

#[derive(Deserialize)]
pub struct LoadRequest {
    users: usize,
    ttl_s: u64,
}

#[derive(Serialize)]
pub struct LoadStatus {
    users: usize,
    base_users: usize,
    remaining_s: f64,
}

struct Inner {
    crowd: Crowd,
    boost_until: Option<Instant>,
    /// Bumped on every change so that the revert timer of an older boost
    /// does not cancel a newer one.
    generation: u64,
}

#[derive(Clone)]
pub struct Load {
    limits: Limits,
    inner: Arc<Mutex<Inner>>,
}

impl Limits {
    fn check(&self, request: &LoadRequest) -> Result<(), String> {
        if request.users == 0 || request.users > self.max_users {
            return Err(format!("users must be within 1..={}", self.max_users));
        }
        if request.ttl_s == 0 || request.ttl_s > self.max_ttl.as_secs() {
            return Err(format!(
                "ttl_s must be within 1..={}",
                self.max_ttl.as_secs()
            ));
        }
        Ok(())
    }
}

impl Load {
    pub fn start(limits: Limits, mut crowd: Crowd) -> Self {
        crowd.resize(limits.base_users);
        metrics::gauge!("loadgen_target_users").set(limits.base_users as f64);
        Self {
            limits,
            inner: Arc::new(Mutex::new(Inner {
                crowd,
                boost_until: None,
                generation: 0,
            })),
        }
    }

    pub fn routes(self) -> Router {
        Router::new()
            .route("/ops/load", get(status).put(set))
            .with_state(self)
    }

    fn status(&self) -> LoadStatus {
        let inner = self.inner.lock().expect("load state poisoned");
        LoadStatus {
            users: inner.crowd.size(),
            base_users: self.limits.base_users,
            remaining_s: inner
                .boost_until
                .map(|until| {
                    until
                        .saturating_duration_since(Instant::now())
                        .as_secs_f64()
                })
                .unwrap_or(0.0),
        }
    }

    /// Returns the generation of the change, for the revert timer to check.
    fn apply(&self, users: usize, boost_until: Option<Instant>) -> u64 {
        let mut inner = self.inner.lock().expect("load state poisoned");
        inner.crowd.resize(users);
        inner.boost_until = boost_until;
        inner.generation += 1;
        metrics::gauge!("loadgen_target_users").set(users as f64);
        inner.generation
    }

    fn revert_if_current(&self, generation: u64) {
        let current = self.inner.lock().expect("load state poisoned").generation;
        if current == generation {
            self.apply(self.limits.base_users, None);
            tracing::info!(users = self.limits.base_users, "load boost expired");
        }
    }

    fn boost(&self, users: usize, ttl: Duration) {
        if users == self.limits.base_users {
            self.apply(users, None);
            return;
        }
        let generation = self.apply(users, Some(Instant::now() + ttl));
        let load = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            load.revert_if_current(generation);
        });
    }
}

async fn status(State(load): State<Load>) -> Json<LoadStatus> {
    Json(load.status())
}

async fn set(
    State(load): State<Load>,
    Json(request): Json<LoadRequest>,
) -> Result<Json<LoadStatus>, (StatusCode, String)> {
    load.limits
        .check(&request)
        .map_err(|reason| (StatusCode::BAD_REQUEST, reason))?;
    load.boost(request.users, Duration::from_secs(request.ttl_s));
    tracing::info!(users = request.users, ttl_s = request.ttl_s, "load changed");
    Ok(Json(load.status()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        base_users: 10,
        max_users: 200,
        max_ttl: Duration::from_secs(600),
    };

    fn idle_crowd() -> Crowd {
        Crowd::new(Box::new(
            |_delay| tokio::spawn(std::future::pending::<()>()),
        ))
    }

    fn request(users: usize, ttl_s: u64) -> LoadRequest {
        LoadRequest { users, ttl_s }
    }

    #[test]
    fn rejects_users_and_ttl_outside_the_limits() {
        assert!(LIMITS.check(&request(50, 120)).is_ok());
        assert!(LIMITS.check(&request(0, 120)).is_err());
        assert!(LIMITS.check(&request(201, 120)).is_err());
        assert!(LIMITS.check(&request(50, 0)).is_err());
        assert!(LIMITS.check(&request(50, 601)).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn boost_reverts_to_base_after_its_ttl() {
        let load = Load::start(LIMITS, idle_crowd());
        load.boost(50, Duration::from_secs(60));
        assert_eq!(load.status().users, 50);

        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(load.status().users, 10);
        assert_eq!(load.status().remaining_s, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn older_timer_does_not_cancel_a_newer_boost() {
        let load = Load::start(LIMITS, idle_crowd());
        load.boost(50, Duration::from_secs(60));
        tokio::time::sleep(Duration::from_secs(30)).await;
        load.boost(100, Duration::from_secs(60));

        tokio::time::sleep(Duration::from_secs(40)).await;
        assert_eq!(load.status().users, 100);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(load.status().users, 10);
    }

    #[tokio::test(start_paused = true)]
    async fn returning_to_base_ends_the_boost_at_once() {
        let load = Load::start(LIMITS, idle_crowd());
        load.boost(50, Duration::from_secs(60));
        load.boost(10, Duration::from_secs(60));
        assert_eq!(load.status().users, 10);
        assert_eq!(load.status().remaining_s, 0.0);
    }
}
