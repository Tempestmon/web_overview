use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::faultflags::FaultFlags;

pub type HealthFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

pub fn always_healthy() -> HealthFn {
    Arc::new(|| Box::pin(async { true }))
}

/// Builds `/healthz`, `/metrics` and `/ops/fault` and serves them with `extra`.
///
/// `/healthz` semantics are per-service: it should answer 200 when the
/// process can do useful work. Docker and the chaos controller both rely on it.
pub async fn serve_ops(
    addr: SocketAddr,
    handle: PrometheusHandle,
    health: HealthFn,
    flags: FaultFlags,
    extra: Router,
) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/healthz", get(move || healthz(health.clone())))
        .route("/metrics", get(move || std::future::ready(handle.render())))
        .merge(flags.routes())
        .merge(extra);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    crate::telemetry::shutdown();
    Ok(())
}

/// Resolves on SIGTERM or Ctrl-C. Docker sends SIGTERM on `stop`; without
/// this it waits out the grace period and SIGKILLs, which makes every
/// "break" in the panel take seconds instead of milliseconds.
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown signal received");
}

async fn healthz(health: HealthFn) -> impl IntoResponse {
    if health().await {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    }
}
