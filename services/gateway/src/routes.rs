use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use common::Edge;
use futures::Stream;
use proto::order::{Order, OrderLine};
use serde::{Deserialize, Serialize};

use crate::client::{ApiError, OrderApi};

const TERMINAL_STATUS: &str = "cooked";
const SSE_MAX_AGE: Duration = Duration::from_secs(180);
const SSE_POLL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct AppState {
    pub orders: Arc<dyn OrderApi>,
    pub inbound: Edge,
}

#[derive(Deserialize)]
pub struct CreateOrderBody {
    pub lines: Vec<LineBody>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LineBody {
    pub item_id: i64,
    pub qty: i32,
}

#[derive(Serialize)]
pub struct OrderView {
    pub id: String,
    pub status: String,
    pub total_cents: i64,
    pub lines: Vec<LineBody>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct MenuView {
    pub items: Vec<MenuItemView>,
    pub from_cache: bool,
}

#[derive(Serialize)]
pub struct MenuItemView {
    pub id: i64,
    pub name: String,
    pub price_cents: i64,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub fn shop_router(state: AppState) -> Router {
    Router::new()
        .route("/api/shop/menu", get(get_menu))
        .route("/api/shop/orders", post(create_order))
        .route("/api/shop/orders/{id}", get(get_order))
        .route("/api/shop/orders/{id}/events", get(order_events))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            record_inbound,
        ))
        .with_state(state)
}

/// Counts what Traefik sends us so the scheme can draw the LB → replica edge.
async fn record_inbound(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let response = next.run(req).await;
    state
        .inbound
        .record(!response.status().is_server_error(), started.elapsed());
    response
}

async fn get_menu(State(state): State<AppState>) -> Result<Json<MenuView>, ApiError> {
    let menu = state.orders.menu().await?;
    Ok(Json(MenuView {
        items: menu
            .items
            .into_iter()
            .map(|i| MenuItemView {
                id: i.id,
                name: i.name,
                price_cents: i.price_cents,
            })
            .collect(),
        from_cache: menu.from_cache,
    }))
}

async fn create_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateOrderBody>,
) -> Result<(StatusCode, Json<OrderView>), ApiError> {
    let lines = body
        .lines
        .into_iter()
        .map(|l| OrderLine {
            item_id: l.item_id,
            qty: l.qty,
        })
        .collect();
    let client_id = headers
        .get("x-client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("web")
        .to_string();
    let order = state.orders.create(lines, client_id).await?;
    Ok((StatusCode::CREATED, Json(view(order))))
}

async fn get_order(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<OrderView>, ApiError> {
    Ok(Json(view(state.orders.get(id).await?)))
}

async fn order_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let _guard = SseGuard::open();
        let deadline = Instant::now() + SSE_MAX_AGE;
        let mut last_status: Option<String> = None;
        while Instant::now() < deadline {
            match state.orders.get(id.clone()).await {
                Ok(order) => {
                    let terminal = order.status == TERMINAL_STATUS;
                    if last_status.as_deref() != Some(order.status.as_str()) {
                        last_status = Some(order.status.clone());
                        yield Ok(Event::default().event("status").json_data(view(order)).expect("serializable"));
                    }
                    if terminal { break; }
                }
                Err(err) => {
                    yield Ok(Event::default().event("error").data(error_message(&err)));
                }
            }
            tokio::time::sleep(SSE_POLL).await;
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

static SSE_CONNECTIONS: AtomicI64 = AtomicI64::new(0);

struct SseGuard;

impl SseGuard {
    fn open() -> Self {
        let n = SSE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("gateway_sse_connections").set(n as f64);
        SseGuard
    }
}

impl Drop for SseGuard {
    fn drop(&mut self) {
        let n = SSE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed) - 1;
        metrics::gauge!("gateway_sse_connections").set(n as f64);
    }
}

fn view(order: Order) -> OrderView {
    OrderView {
        id: order.id,
        status: order.status,
        total_cents: order.total_cents,
        lines: order
            .lines
            .into_iter()
            .map(|l| LineBody {
                item_id: l.item_id,
                qty: l.qty,
            })
            .collect(),
        created_at: order.created_at,
        updated_at: order.updated_at,
    }
}

fn error_message(err: &ApiError) -> String {
    match err {
        ApiError::Unavailable => "order service unavailable".to_string(),
        ApiError::InvalidArgument(msg) => msg.clone(),
        ApiError::NotFound => "order not found".to_string(),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
        };
        (
            status,
            Json(ErrorBody {
                error: error_message(&self),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use proto::order::Menu;
    use tower::ServiceExt;

    struct FakeOrders {
        available: bool,
    }

    #[tonic::async_trait]
    impl OrderApi for FakeOrders {
        async fn menu(&self) -> Result<Menu, ApiError> {
            if self.available {
                Ok(Menu {
                    items: vec![],
                    from_cache: false,
                })
            } else {
                Err(ApiError::Unavailable)
            }
        }
        async fn create(
            &self,
            lines: Vec<OrderLine>,
            client_id: String,
        ) -> Result<Order, ApiError> {
            if !self.available {
                return Err(ApiError::Unavailable);
            }
            Ok(Order {
                id: "o1".into(),
                status: "accepted".into(),
                total_cents: 100,
                lines,
                created_at: client_id,
                updated_at: String::new(),
            })
        }
        async fn get(&self, _id: String) -> Result<Order, ApiError> {
            Err(ApiError::NotFound)
        }
    }

    fn app(available: bool) -> Router {
        shop_router(AppState {
            orders: Arc::new(FakeOrders { available }),
            inbound: Edge::new("traefik", "gateway-test", "http"),
        })
    }

    async fn post_order(router: Router) -> (StatusCode, serde_json::Value) {
        let req = Request::post("/api/shop/orders")
            .header("content-type", "application/json")
            .header("x-client-id", "tester")
            .body(Body::from(r#"{"lines":[{"item_id":1,"qty":2}]}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn create_order_returns_201_with_view() {
        let (status, body) = post_order(app(true)).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["id"], "o1");
        assert_eq!(body["lines"][0]["qty"], 2);
        assert_eq!(body["created_at"], "tester");
    }

    #[tokio::test]
    async fn unavailable_backend_maps_to_503() {
        let (status, body) = post_order(app(false)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "order service unavailable");
    }

    #[tokio::test]
    async fn unknown_order_is_404() {
        let resp = app(true)
            .oneshot(
                Request::get("/api/shop/orders/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
