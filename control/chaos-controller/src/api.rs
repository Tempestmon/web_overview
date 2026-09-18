use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

use crate::apply;
use crate::docker::ContainerControl;
use crate::faults::{Fault, FaultBook, Refusal};
use crate::flow::{SharedFlow, failing_edges};
use crate::health::SharedObserved;
use crate::snapshot::{ActiveFault, EdgeSnapshot, NodeSnapshot, Snapshot};
use crate::state::derive_states;
use crate::topology::{FaultKind, Topology};

#[derive(Clone)]
pub struct AppState {
    pub topology: Arc<Topology>,
    pub book: Arc<Mutex<FaultBook>>,
    pub docker: Arc<dyn ContainerControl>,
    pub observed: SharedObserved,
    pub flow: SharedFlow,
    pub snapshot: watch::Receiver<Arc<Snapshot>>,
    pub admin_token: Option<String>,
    pub http: reqwest::Client,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/topology", get(topology))
        .route("/api/state", get(current_state))
        .route("/api/stream", get(stream))
        .route("/api/faults", post(start_fault))
        .route("/api/faults/{id}", delete(stop_fault))
        .fallback(static_asset)
        .with_state(state)
}

async fn topology(State(state): State<AppState>) -> Json<Topology> {
    Json((*state.topology).clone())
}

async fn current_state(State(state): State<AppState>) -> Json<Snapshot> {
    Json((**state.snapshot.borrow()).clone())
}

async fn stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.snapshot.clone();
    let stream = async_stream::stream! {
        loop {
            let snapshot = rx.borrow_and_update().clone();
            yield Ok(Event::default().event("snapshot").json_data(&*snapshot).expect("serializable"));
            if rx.changed().await.is_err() { break; }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
struct StartFaultBody {
    node: String,
    kind: FaultKind,
    ttl_s: Option<u64>,
}

#[derive(Serialize)]
struct FaultCreated {
    id: u64,
    node: String,
    kind: FaultKind,
    ttl_s: u64,
}

#[derive(Serialize)]
struct ApiErrorBody {
    error: String,
    retry_after_s: Option<u64>,
}

enum ApiError {
    UnknownNode,
    UnsupportedFault,
    Refused(Refusal),
    NotFound,
    Forbidden,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error, retry_after_s) = match self {
            ApiError::UnknownNode => (StatusCode::NOT_FOUND, "unknown node".to_string(), None),
            ApiError::UnsupportedFault => (
                StatusCode::BAD_REQUEST,
                "fault not supported on this node".to_string(),
                None,
            ),
            ApiError::Refused(Refusal::Busy { remaining }) => (
                StatusCode::CONFLICT,
                "another fault is active".to_string(),
                Some(remaining.as_secs()),
            ),
            ApiError::Refused(Refusal::Cooldown { remaining }) => (
                StatusCode::CONFLICT,
                "system is recovering".to_string(),
                Some(remaining.as_secs()),
            ),
            ApiError::Refused(Refusal::RateLimited { retry_after }) => (
                StatusCode::TOO_MANY_REQUESTS,
                "you broke something recently".to_string(),
                Some(retry_after.as_secs()),
            ),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "no such fault".to_string(), None),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "not your fault".to_string(), None),
        };
        (
            status,
            Json(ApiErrorBody {
                error,
                retry_after_s,
            }),
        )
            .into_response()
    }
}

async fn start_fault(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<StartFaultBody>,
) -> Result<(StatusCode, Json<FaultCreated>), ApiError> {
    let node = state
        .topology
        .node(&body.node)
        .ok_or(ApiError::UnknownNode)?;
    if !node.faults.contains(&body.kind) {
        return Err(ApiError::UnsupportedFault);
    }
    let owner = client_ip(&headers);
    let admin = is_admin(&state, &headers);
    let now = Instant::now();

    let fault = state
        .book
        .lock()
        .await
        .try_start(
            &node.id,
            body.kind,
            &owner,
            body.ttl_s.map(Duration::from_secs),
            now,
            admin,
        )
        .map_err(ApiError::Refused)?;

    tracing::info!(id = fault.id, node = %fault.node, kind = ?fault.kind, %owner, "fault started");
    metrics::counter!("chaos_faults_total", "node" => fault.node.clone()).increment(1);

    // Applied in the background: `docker stop` can take seconds for
    // components without a fast SIGTERM path, and the UI should not hang on
    // it. The health monitor reports the outcome; a failed apply ends the
    // fault so the book does not hold a phantom.
    let apply_state = state.clone();
    let apply_id = fault.id;
    let apply_kind = body.kind;
    let apply_node = node.clone();
    tokio::spawn(async move {
        if let Err(err) = apply::apply(&apply_state, &apply_node, apply_kind).await {
            tracing::error!(id = apply_id, %err, "fault apply failed");
            apply_state
                .book
                .lock()
                .await
                .finish(apply_id, Instant::now());
        }
    });

    let ttl = fault.ends_at.saturating_duration_since(now);
    Ok((
        StatusCode::CREATED,
        Json(FaultCreated {
            id: fault.id,
            node: fault.node,
            kind: fault.kind,
            ttl_s: ttl.as_secs(),
        }),
    ))
}

async fn stop_fault(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    let owner = client_ip(&headers);
    {
        let book = state.book.lock().await;
        let fault = book.get(id).ok_or(ApiError::NotFound)?;
        if fault.owner != owner && !is_admin(&state, &headers) {
            return Err(ApiError::Forbidden);
        }
    }
    heal(&state, id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// Idempotent: the book hands out each fault exactly once, so a timer and
/// an early DELETE racing each other cannot double-heal.
async fn heal(state: &AppState, id: u64) {
    let Some(fault) = state.book.lock().await.finish(id, Instant::now()) else {
        return;
    };
    let Some(node) = state.topology.node(&fault.node) else {
        tracing::error!(id, node = %fault.node, "fault refers to unknown node");
        return;
    };
    match apply::heal(state, node, fault.kind).await {
        Ok(()) => tracing::info!(id, node = %fault.node, "fault healed"),
        Err(err) => {
            tracing::error!(id, node = %fault.node, %err, "heal failed; reconcile loop will retry")
        }
    }
}

fn client_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn is_admin(state: &AppState, headers: &HeaderMap) -> bool {
    match (
        &state.admin_token,
        headers.get("x-admin-token").and_then(|v| v.to_str().ok()),
    ) {
        (Some(expected), Some(given)) => !expected.is_empty() && expected == given,
        _ => false,
    }
}

/// Once a second: heal whatever expired, then build and publish a snapshot.
/// Expiry is swept here rather than by per-fault timers so there is exactly
/// one code path that ends a fault.
pub async fn run_snapshotter(state: AppState, tx: watch::Sender<Arc<Snapshot>>) {
    loop {
        let expired = state.book.lock().await.expired(Instant::now());
        for fault in expired {
            heal(&state, fault.id).await;
        }
        let snapshot = build_snapshot(&state).await;
        let _ = tx.send(Arc::new(snapshot));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn build_snapshot(state: &AppState) -> Snapshot {
    let now = Instant::now();
    let observed = state.observed.read().await.clone();
    let flow = state.flow.read().await.clone();
    let (faults, cooldown_s) = {
        let book = state.book.lock().await;
        (
            book.active().to_vec(),
            book.cooldown_remaining(now)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
        )
    };
    let mut broken = observed.broken.clone();
    broken.extend(faults.iter().map(|f| f.node.clone()));
    let failing = failing_edges(&flow, &state.topology);
    let states = derive_states(&state.topology, &broken, &observed.recovering, &failing);

    Snapshot {
        ts_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        nodes: state
            .topology
            .nodes
            .iter()
            .map(|n| NodeSnapshot {
                id: n.id.clone(),
                state: states[&n.id],
                fault: faults.iter().find(|f| f.node == n.id).map(|f| f.kind),
            })
            .collect(),
        edges: state
            .topology
            .edges
            .iter()
            .map(|e| {
                let rate = flow
                    .edges
                    .get(&(e.from.clone(), e.to.clone()))
                    .cloned()
                    .unwrap_or_default();
                EdgeSnapshot {
                    from: e.from.clone(),
                    to: e.to.clone(),
                    rps: rate.rps,
                    error_rps: rate.error_rps,
                    p95_ms: rate.p95_s * 1000.0,
                    breaker: flow.breakers.get(&e.to).cloned(),
                }
            })
            .collect(),
        faults: faults.iter().map(|f| active_fault(f, now)).collect(),
        cooldown_s,
        people_online: flow.people_online,
        orders_per_min: flow.orders_per_min,
        kafka_lag: flow.kafka_lag,
        lead_time_s: flow.lead_time_s,
    }
}

fn active_fault(f: &Fault, now: Instant) -> ActiveFault {
    ActiveFault {
        id: f.id,
        node: f.node.clone(),
        kind: f.kind,
        remaining_s: f.ends_at.saturating_duration_since(now).as_secs_f64(),
    }
}

#[derive(rust_embed::Embed)]
#[folder = "../../web/dist"]
struct WebAssets;

/// Serves the SPA. Unknown paths fall back to index.html so client-side
/// routes like `/order` survive a refresh.
async fn static_asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let candidate = if path.is_empty() { "index.html" } else { path };
    let file = WebAssets::get(candidate).or_else(|| WebAssets::get("index.html"));
    match file {
        Some(content) => {
            let mime = mime_guess::from_path(if WebAssets::get(candidate).is_some() {
                candidate
            } else {
                "index.html"
            })
            .first_or_octet_stream();
            (
                [(header::CONTENT_TYPE, mime.as_ref().to_string())],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "frontend not built").into_response(),
    }
}
