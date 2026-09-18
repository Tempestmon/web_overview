//! Analytics: tails every order topic into ClickHouse and publishes a few
//! aggregates as Prometheus gauges. The point of it in the demo is a second,
//! columnar database and a consumer whose lag hurts nobody but dashboards.

mod clickhouse;
mod rows;

use std::net::SocketAddr;
use std::time::Duration;

use chrono::Utc;
use common::{Edge, config};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tracing::Instrument;

use crate::clickhouse::ClickHouse;
use crate::rows::{
    Row, TOPIC_CREATED, TOPIC_DELIVERY_EVENTS, TOPIC_KITCHEN_EVENTS, row_from_message,
};

const CONSUMER_GROUP: &str = "analytics";
const FLUSH_EVERY: Duration = Duration::from_secs(2);
const FLUSH_AT_ROWS: usize = 200;
const AGGREGATE_EVERY: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;

    let clickhouse = ClickHouse::new(
        config::required("CLICKHOUSE_URL"),
        config::or_default("CLICKHOUSE_USER", "default"),
        config::or_default("CLICKHOUSE_PASSWORD", ""),
        &service_id,
    )?;
    clickhouse.ensure_schema().await?;

    tokio::spawn(run_consumer(
        config::required("KAFKA_BROKERS"),
        service_id.clone(),
        clickhouse.clone(),
    ));
    tokio::spawn(run_aggregates(clickhouse));

    // Liveness only: ClickHouse or Kafka outages are "degraded" via the graph.
    common::serve_ops(
        ops_addr,
        metrics,
        common::always_healthy(),
        common::FaultFlags::new(),
        axum::Router::new(),
    )
    .await
}

/// Offsets are committed right after a message is buffered, not after the
/// batch lands in ClickHouse. That is at-most-once: a crash between the two
/// loses up to one batch of analytics rows. Acceptable here because the
/// rows are derived data for dashboards, and exactly-once would need a
/// per-partition offset store that adds nothing to the demonstration.
async fn run_consumer(brokers: String, service_id: String, clickhouse: ClickHouse) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", CONSUMER_GROUP)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .create()
        .expect("consumer config is valid");
    consumer
        .subscribe(&[TOPIC_CREATED, TOPIC_KITCHEN_EVENTS, TOPIC_DELIVERY_EVENTS])
        .expect("subscribe");
    let edge = Edge::new("kafka", service_id, "kafka");

    let mut buffer: Vec<Row> = Vec::with_capacity(FLUSH_AT_ROWS);
    let mut ticker = tokio::time::interval(FLUSH_EVERY);
    loop {
        tokio::select! {
            received = consumer.recv() => match received {
                Ok(msg) => {
                    let span = tracing::info_span!("kafka.consume", topic = msg.topic());
                    common::trace::adopt_parent(&span, common::trace::extract_kafka(msg.headers()));
                    let parsed = edge
                        .observe(async { row_from_message(msg.topic(), msg.payload().unwrap_or_default(), Utc::now()) })
                        .instrument(span)
                        .await;
                    match parsed {
                        Ok(row) => buffer.push(row),
                        Err(err) => tracing::warn!(%err, topic = msg.topic(), "message skipped"),
                    }
                    if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
                        tracing::warn!(%err, "offset commit failed");
                    }
                    if buffer.len() >= FLUSH_AT_ROWS {
                        flush(&clickhouse, &mut buffer).await;
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, "kafka consumer error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            },
            _ = ticker.tick() => flush(&clickhouse, &mut buffer).await,
        }
    }
}

async fn flush(clickhouse: &ClickHouse, buffer: &mut Vec<Row>) {
    if buffer.is_empty() {
        return;
    }
    match clickhouse.insert(buffer).await {
        Ok(()) => {
            metrics::counter!("analytics_rows_inserted_total").increment(buffer.len() as u64);
            buffer.clear();
        }
        // Keep the batch and retry on the next tick; the buffer is bounded
        // by memory only, which is fine for a demo-sized event rate.
        Err(err) => {
            tracing::warn!(%err, rows = buffer.len(), "clickhouse insert failed, will retry")
        }
    }
}

async fn run_aggregates(clickhouse: ClickHouse) {
    let mut ticker = tokio::time::interval(AGGREGATE_EVERY);
    loop {
        ticker.tick().await;
        match clickhouse.lead_time_seconds().await {
            Ok(v) => metrics::gauge!("analytics_lead_time_seconds").set(v),
            Err(err) => tracing::warn!(%err, "lead time query failed"),
        }
        match clickhouse.delivered_last_5m().await {
            Ok(v) => metrics::gauge!("analytics_delivered_5m").set(v),
            Err(err) => tracing::warn!(%err, "delivered count query failed"),
        }
    }
}
