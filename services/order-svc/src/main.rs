mod grpc;
mod kafka;
mod menu_cache;
mod repo;
mod status;

use std::net::SocketAddr;
use std::time::Duration;

use common::config;
use proto::order::order_service_server::OrderServiceServer;
use sqlx::postgres::PgPoolOptions;

use crate::grpc::OrderSvc;
use crate::kafka::Producer;
use crate::menu_cache::MenuCache;
use crate::repo::Repo;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);

    let pool = connect_with_retry(&config::required("DATABASE_URL")).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let repo = Repo::new(pool, &service_id);
    let cache = MenuCache::new(&config::required("REDIS_URL"), &service_id)?;
    let brokers = config::required("KAFKA_BROKERS");
    let producer = Producer::new(&brokers, &service_id)?;

    tokio::spawn(kafka::run_kitchen_events_consumer(
        brokers,
        service_id.clone(),
        repo.clone(),
    ));

    let grpc_addr: SocketAddr = config::or_default("GRPC_ADDR", "0.0.0.0:50051").parse()?;
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;

    // Liveness only: a Postgres outage must show up as "degraded" via the
    // dependency graph, not as a "broken" order service.
    let health = common::always_healthy();

    let grpc = tonic::transport::Server::builder()
        .add_service(OrderServiceServer::new(OrderSvc {
            repo,
            cache,
            producer,
        }))
        .serve(grpc_addr);
    tracing::info!(%grpc_addr, "grpc listening");

    tokio::select! {
        r = grpc => r?,
        r = common::serve_ops(ops_addr, metrics, health, axum::Router::new()) => r?,
    }
    Ok(())
}

/// Compose starts us after Postgres reports healthy, but a paused or
/// restarting database must not turn into a crash loop.
async fn connect_with_retry(url: &str) -> anyhow::Result<sqlx::PgPool> {
    let mut attempt = 0;
    loop {
        match PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(2))
            .connect(url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) if attempt < 30 => {
                attempt += 1;
                tracing::warn!(%err, attempt, "postgres not ready, retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
