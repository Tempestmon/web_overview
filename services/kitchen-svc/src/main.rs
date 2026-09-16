//! Kitchen: consumes `orders.created`, "cooks" for a while, reports progress
//! on `orders.kitchen-events`. Each partition is handled sequentially in its
//! own task, so parallelism equals the partition count and offsets can be
//! committed in order without losing orders on restart.

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
use uuid::Uuid;

const TOPIC_CREATED: &str = "orders.created";
const TOPIC_KITCHEN_EVENTS: &str = "orders.kitchen-events";
const CONSUMER_GROUP: &str = "kitchen";

#[derive(Deserialize)]
struct OrderCreated {
    order_id: Uuid,
}

#[derive(Serialize)]
struct KitchenEvent<'a> {
    order_id: Uuid,
    status: &'a str,
}

struct Kitchen {
    producer: FutureProducer,
    consume_edge: Edge,
    produce_edge: Edge,
    cook_range: (u64, u64),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);
    let brokers = config::required("KAFKA_BROKERS");
    let partitions: i32 = config::parse_or("KAFKA_PARTITIONS", 12);
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;

    let kitchen = Arc::new(Kitchen {
        producer: ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000")
            .create()?,
        consume_edge: Edge::new("kafka", service_id.clone(), "kafka"),
        produce_edge: Edge::new(service_id.clone(), "kafka", "kafka"),
        cook_range: (
            config::parse_or("COOK_MIN_MS", 5000),
            config::parse_or("COOK_MAX_MS", 15000),
        ),
    });

    let consumer: Arc<StreamConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", CONSUMER_GROUP)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000")
            .create()?,
    );
    for partition in 0..partitions {
        let queue = consumer
            .split_partition_queue(TOPIC_CREATED, partition)
            .expect("partition queue is created before consuming");
        let kitchen = kitchen.clone();
        let consumer = consumer.clone();
        tokio::spawn(async move {
            loop {
                match queue.recv().await {
                    Ok(msg) => {
                        let result = kitchen
                            .consume_edge
                            .observe(kitchen.cook(msg.payload()))
                            .await;
                        if let Err(err) = result {
                            tracing::warn!(%err, partition, "order skipped");
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
    consumer.subscribe(&[TOPIC_CREATED])?;

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

    // Liveness only: dependency health is the chaos controller's job, and a
    // health check that fails when Kafka is down would paint the kitchen red
    // instead of yellow.
    common::serve_ops(
        ops_addr,
        metrics,
        common::always_healthy(),
        axum::Router::new(),
    )
    .await
}

impl Kitchen {
    async fn cook(&self, payload: Option<&[u8]>) -> anyhow::Result<()> {
        let created: OrderCreated = serde_json::from_slice(payload.unwrap_or_default())?;
        let order_id = created.order_id;
        let in_progress = metrics::gauge!("kitchen_orders_in_progress");
        in_progress.increment(1.0);
        self.publish(order_id, "cooking").await?;
        tokio::time::sleep(self.cooking_time()).await;
        let result = self.publish(order_id, "cooked").await;
        in_progress.decrement(1.0);
        metrics::counter!("kitchen_orders_cooked_total").increment(1);
        result
    }

    fn cooking_time(&self) -> Duration {
        let (min, max) = self.cook_range;
        Duration::from_millis(rand::rng().random_range(min..=max.max(min)))
    }

    async fn publish(&self, order_id: Uuid, status: &str) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&KitchenEvent { order_id, status })?;
        let key = order_id.to_string();
        self.produce_edge
            .observe(async {
                self.producer
                    .send(
                        FutureRecord::to(TOPIC_KITCHEN_EVENTS)
                            .key(&key)
                            .payload(&payload),
                        Duration::from_secs(5),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|(err, _)| anyhow::anyhow!("kafka produce failed: {err}"))
            })
            .await
    }
}
