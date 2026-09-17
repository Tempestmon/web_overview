use std::time::Duration;

use common::Edge;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::Repo;
use crate::status::OrderStatus;

pub const TOPIC_CREATED: &str = "orders.created";
pub const TOPIC_KITCHEN_EVENTS: &str = "orders.kitchen-events";
const CONSUMER_GROUP: &str = "order-svc";

#[derive(Serialize, Deserialize)]
pub struct OrderCreated {
    pub order_id: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct KitchenEvent {
    pub order_id: Uuid,
    pub status: String,
}

#[derive(Clone)]
pub struct Producer {
    inner: FutureProducer,
    edge: Edge,
}

impl Producer {
    pub fn new(brokers: &str, service_id: &str) -> anyhow::Result<Self> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "3000")
            .set("request.timeout.ms", "2000")
            .create()?;
        Ok(Self {
            inner,
            edge: Edge::new(service_id, "kafka", "kafka"),
        })
    }

    pub async fn publish_created(&self, order_id: Uuid) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&OrderCreated { order_id })?;
        let key = order_id.to_string();
        self.edge
            .observe(async {
                self.inner
                    .send(
                        FutureRecord::to(TOPIC_CREATED).key(&key).payload(&payload),
                        Duration::from_secs(3),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|(err, _)| anyhow::anyhow!("kafka produce failed: {err}"))
            })
            .await
    }
}

/// Applies kitchen progress to orders. Runs forever; broker outages surface
/// as logged errors and the loop keeps retrying, which is what we want to
/// demonstrate: an async consumer simply waits for the broker to come back.
pub async fn run_kitchen_events_consumer(brokers: String, service_id: String, repo: Repo) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", CONSUMER_GROUP)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Generous on purpose: small hosts stall for seconds under load, and a
        // spurious rebalance looks exactly like the fault we are trying to show.
        .set("session.timeout.ms", "30000")
        .create()
        .expect("consumer config is valid");
    consumer
        .subscribe(&[TOPIC_KITCHEN_EVENTS])
        .expect("subscribe");
    let edge = Edge::new("kafka", service_id, "kafka");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let handled = edge.observe(apply_event(&repo, msg.payload())).await;
                if let Err(err) = handled {
                    tracing::warn!(%err, "kitchen event not applied");
                }
                if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
                    tracing::warn!(%err, "offset commit failed");
                }
            }
            Err(err) => {
                tracing::warn!(%err, "kafka consumer error");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn apply_event(repo: &Repo, payload: Option<&[u8]>) -> anyhow::Result<()> {
    let event: KitchenEvent = serde_json::from_slice(payload.unwrap_or_default())?;
    let status: OrderStatus = event.status.parse()?;
    let changed = repo.advance_status(event.order_id, status).await?;
    tracing::debug!(order_id = %event.order_id, %status, changed, "kitchen event applied");
    Ok(())
}
