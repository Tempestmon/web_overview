//! Courier: picks up cooked orders from `orders.kitchen-events`, drives for a
//! while, reports progress on `orders.delivery-events`. Second async stage
//! after the kitchen, so a courier outage stalls orders at `cooked` while
//! the kitchen keeps working. Same per-partition design as the kitchen: one
//! sequential task per partition, offsets committed in order.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{Edge, config};
use rand::Rng;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

const TOPIC_KITCHEN_EVENTS: &str = "orders.kitchen-events";
const TOPIC_DELIVERY_EVENTS: &str = "orders.delivery-events";
const CONSUMER_GROUP: &str = "courier";
const PICKUP_STATUS: &str = "cooked";

#[derive(Deserialize)]
struct KitchenEvent {
    order_id: Uuid,
    status: String,
}

#[derive(Serialize)]
struct DeliveryEvent<'a> {
    order_id: Uuid,
    status: &'a str,
}

struct Courier {
    producer: FutureProducer,
    consume_edge: Edge,
    produce_edge: Edge,
    trip_range: (u64, u64),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);
    let brokers = config::required("KAFKA_BROKERS");
    let partitions: i32 = config::parse_or("KAFKA_PARTITIONS", 12);
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;

    let courier = Arc::new(Courier {
        producer: ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000")
            .create()?,
        consume_edge: Edge::new("kafka", service_id.clone(), "kafka"),
        produce_edge: Edge::new(service_id.clone(), "kafka", "kafka"),
        trip_range: (
            config::parse_or("COURIER_MIN_MS", 4000),
            config::parse_or("COURIER_MAX_MS", 10000),
        ),
    });

    let consumer: Arc<StreamConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", CONSUMER_GROUP)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Generous on purpose: small hosts stall for seconds under load, and a
            // spurious rebalance looks exactly like the fault we are trying to show.
            .set("session.timeout.ms", "30000")
            .set("max.poll.interval.ms", "300000")
            .create()?,
    );
    for partition in 0..partitions {
        let queue = consumer
            .split_partition_queue(TOPIC_KITCHEN_EVENTS, partition)
            .expect("partition queue is created before consuming");
        let courier = courier.clone();
        let consumer = consumer.clone();
        tokio::spawn(async move {
            loop {
                match queue.recv().await {
                    Ok(msg) => {
                        let span =
                            tracing::info_span!("kafka.consume", topic = TOPIC_KITCHEN_EVENTS);
                        common::trace::adopt_parent(
                            &span,
                            common::trace::extract_kafka(msg.headers()),
                        );
                        let result = courier
                            .consume_edge
                            .observe(courier.handle(msg.payload()))
                            .instrument(span)
                            .await;
                        if let Err(err) = result {
                            tracing::warn!(%err, partition, "event skipped");
                        }
                        if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
                            tracing::warn!(%err, "offset commit failed");
                        }
                    }
                    Err(err) => {
                        tracing::warn!(%err, partition, "consumer error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
    consumer.subscribe(&[TOPIC_KITCHEN_EVENTS])?;

    // The main consumer must keep being polled for the partition queues to
    // receive anything; with every partition split off it only yields errors.
    let driver = consumer.clone();
    tokio::spawn(async move {
        loop {
            if let Err(err) = driver.recv().await {
                tracing::debug!(%err, "main consumer event");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    });

    // Liveness only: a Kafka outage must show as "degraded" via the graph.
    common::serve_ops(
        ops_addr,
        metrics,
        common::always_healthy(),
        common::FaultFlags::new(),
        axum::Router::new(),
    )
    .await
}

/// The kitchen topic carries every kitchen transition; the courier only
/// cares about the one that means "ready for pickup".
fn is_pickup(event: &KitchenEvent) -> bool {
    event.status == PICKUP_STATUS
}

impl Courier {
    async fn handle(&self, payload: Option<&[u8]>) -> anyhow::Result<()> {
        let event: KitchenEvent = serde_json::from_slice(payload.unwrap_or_default())?;
        if !is_pickup(&event) {
            return Ok(());
        }
        self.deliver(event.order_id).await
    }

    async fn deliver(&self, order_id: Uuid) -> anyhow::Result<()> {
        let in_transit = metrics::gauge!("courier_orders_in_transit");
        in_transit.increment(1.0);
        self.publish(order_id, "delivering").await?;
        tokio::time::sleep(self.trip_time()).await;
        let result = self.publish(order_id, "delivered").await;
        in_transit.decrement(1.0);
        metrics::counter!("courier_orders_delivered_total").increment(1);
        result
    }

    fn trip_time(&self) -> Duration {
        let (min, max) = self.trip_range;
        Duration::from_millis(rand::rng().random_range(min..=max.max(min)))
    }

    async fn publish(&self, order_id: Uuid, status: &str) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&DeliveryEvent { order_id, status })?;
        let key = order_id.to_string();
        self.produce_edge
            .observe(async {
                self.producer
                    .send(
                        FutureRecord::to(TOPIC_DELIVERY_EVENTS)
                            .key(&key)
                            .payload(&payload)
                            .headers(common::trace::kafka_headers()),
                        Duration::from_secs(5),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|(err, _)| anyhow::anyhow!("kafka produce failed: {err}"))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(status: &str) -> KitchenEvent {
        KitchenEvent {
            order_id: Uuid::nil(),
            status: status.to_string(),
        }
    }

    #[test]
    fn only_cooked_orders_are_picked_up() {
        assert!(is_pickup(&event("cooked")));
        assert!(!is_pickup(&event("cooking")));
        assert!(!is_pickup(&event("delivered")));
    }
}
