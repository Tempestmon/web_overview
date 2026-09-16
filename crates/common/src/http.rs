use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;

pub type HealthFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

pub fn always_healthy() -> HealthFn {
    Arc::new(|| Box::pin(async { true }))
}

/// Builds `/healthz` + `/metrics` and serves them together with `extra`.
///
/// `/healthz` semantics are per-service: it should answer 200 when the
/// process can do useful work. Docker and the chaos controller both rely on it.
pub async fn serve_ops(
    addr: SocketAddr,
    handle: PrometheusHandle,
    health: HealthFn,
    extra: Router,
) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/healthz", get(move || healthz(health.clone())))
        .route("/metrics", get(move || std::future::ready(handle.render())))
        .merge(extra);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");
    axum::serve(listener, router).await?;
    Ok(())
}

async fn healthz(health: HealthFn) -> impl IntoResponse {
    if health().await {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    }
}
