use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::put;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Application-level fault injection, set by the chaos controller through
/// `PUT /ops/fault`. Services consult it on their hot path via `apply`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Injected {
    #[serde(default)]
    pub error_rate: f64,
    #[serde(default)]
    pub latency_ms: u64,
}

#[derive(Clone, Default)]
pub struct FaultFlags {
    inner: Arc<RwLock<Injected>>,
}

#[derive(Debug, thiserror::Error)]
#[error("injected fault")]
pub struct InjectedFailure;

impl FaultFlags {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, injected: Injected) {
        *self.inner.write().await = injected;
    }

    pub async fn clear(&self) {
        *self.inner.write().await = Injected::default();
    }

    pub async fn current(&self) -> Injected {
        *self.inner.read().await
    }

    /// Delays and/or fails according to the current flags. Cheap when nothing
    /// is injected: one read lock, no allocation.
    pub async fn apply(&self) -> Result<(), InjectedFailure> {
        let injected = *self.inner.read().await;
        if injected.latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(injected.latency_ms)).await;
        }
        if injected.error_rate > 0.0 && rand::random::<f64>() < injected.error_rate {
            metrics::counter!("injected_failures_total").increment(1);
            return Err(InjectedFailure);
        }
        Ok(())
    }

    pub fn routes(&self) -> Router {
        Router::new()
            .route(
                "/ops/fault",
                put(set_fault).delete(clear_fault).get(get_fault),
            )
            .with_state(self.clone())
    }
}

async fn set_fault(State(flags): State<FaultFlags>, Json(injected): Json<Injected>) -> StatusCode {
    if !(0.0..=1.0).contains(&injected.error_rate) {
        return StatusCode::BAD_REQUEST;
    }
    tracing::warn!(?injected, "fault injected");
    flags.set(injected).await;
    StatusCode::NO_CONTENT
}

async fn clear_fault(State(flags): State<FaultFlags>) -> StatusCode {
    tracing::info!("injected fault cleared");
    flags.clear().await;
    StatusCode::NO_CONTENT
}

async fn get_fault(State(flags): State<FaultFlags>) -> Json<Injected> {
    Json(flags.current().await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn default_flags_pass() {
        let flags = FaultFlags::new();
        assert!(flags.apply().await.is_ok());
    }

    #[tokio::test]
    async fn full_error_rate_always_fails_and_clear_restores() {
        let flags = FaultFlags::new();
        flags
            .set(Injected {
                error_rate: 1.0,
                latency_ms: 0,
            })
            .await;
        for _ in 0..10 {
            assert!(flags.apply().await.is_err());
        }
        flags.clear().await;
        assert!(flags.apply().await.is_ok());
    }

    #[tokio::test]
    async fn latency_is_applied() {
        let flags = FaultFlags::new();
        flags
            .set(Injected {
                error_rate: 0.0,
                latency_ms: 50,
            })
            .await;
        let started = Instant::now();
        assert!(flags.apply().await.is_ok());
        assert!(started.elapsed() >= Duration::from_millis(50));
    }
}
