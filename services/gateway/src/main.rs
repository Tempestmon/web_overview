mod client;
mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use common::{Edge, config};

use crate::client::GrpcOrderClient;
use crate::routes::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);
    let addr: SocketAddr = config::or_default("HTTP_ADDR", "0.0.0.0:8080").parse()?;
    let orders = GrpcOrderClient::connect_lazy(&config::required("ORDER_SVC_URL"), &service_id)?;

    let state = AppState {
        orders: Arc::new(orders),
        inbound: Edge::new("traefik", service_id, "http"),
    };
    // Liveness only: Traefik pulls a replica out of rotation when this fails,
    // which should happen when the replica is gone, not when a backend is.
    common::serve_ops(
        addr,
        metrics,
        common::always_healthy(),
        routes::shop_router(state),
    )
    .await
}
