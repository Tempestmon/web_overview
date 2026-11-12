//! Virtual customers. Each one comes online for a while, places orders and
//! waits for them, then goes offline; the online count is what the panel
//! shows as "people". Failures are counted, never retried in a tight loop:
//! a broken stack should look slow and sad, not DoS itself.

mod crowd;
mod load;

use std::net::SocketAddr;
use std::time::Duration;

use common::config;
use rand::Rng;
use serde::Deserialize;

use crate::crowd::Crowd;
use crate::load::{Limits, Load};

#[derive(Clone)]
struct Shop {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Deserialize)]
struct MenuItem {
    id: i64,
}

#[derive(Deserialize)]
struct Menu {
    items: Vec<MenuItem>,
}

#[derive(Deserialize)]
struct Order {
    id: String,
    status: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::or_default("SERVICE_ID", "loadgen");
    let metrics = common::init_telemetry(&service_id);
    let limits = Limits {
        base_users: config::parse_or("USERS", 10),
        max_users: config::parse_or("MAX_USERS", 200),
        max_ttl: Duration::from_secs(config::parse_or("MAX_LOAD_TTL_S", 600)),
    };
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;
    let shop = Shop {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?,
        base_url: config::required("BASE_URL"),
    };

    let crowd = Crowd::new(Box::new(move |arrival_delay| {
        let shop = shop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(arrival_delay).await;
            user_lifecycle(shop).await;
        })
    }));
    let load = Load::start(limits, crowd);

    common::serve_ops(
        ops_addr,
        metrics,
        common::always_healthy(),
        common::FaultFlags::new(),
        load.routes(),
    )
    .await
}

/// Counts a customer as online for as long as it lives. A drop guard rather
/// than a decrement after the session: shrinking the crowd aborts the task
/// mid-session, and the gauge must not keep counting the departed.
struct Online(metrics::Gauge);

impl Online {
    fn enter() -> Self {
        let gauge = metrics::gauge!("loadgen_active_users");
        gauge.increment(1.0);
        Self(gauge)
    }
}

impl Drop for Online {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

async fn user_lifecycle(shop: Shop) {
    loop {
        let session = Duration::from_secs(rand_range(40, 120));
        {
            let _online = Online::enter();
            let _ = tokio::time::timeout(session, shopping_session(&shop)).await;
        }
        tokio::time::sleep(Duration::from_secs(rand_range(5, 30))).await;
    }
}

async fn shopping_session(shop: &Shop) {
    loop {
        let outcome = match shop.order_and_wait().await {
            Ok(status) if status == "delivered" => "delivered",
            Ok(_) => "timeout",
            Err(err) => {
                tracing::debug!(%err, "order failed");
                "error"
            }
        };
        metrics::counter!("loadgen_orders_total", "outcome" => outcome).increment(1);
        let pause = if outcome == "error" {
            5
        } else {
            rand_range(2, 6)
        };
        tokio::time::sleep(Duration::from_secs(pause)).await;
    }
}

impl Shop {
    async fn order_and_wait(&self) -> anyhow::Result<String> {
        let menu: Menu = self
            .http
            .get(format!("{}/api/shop/menu", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        anyhow::ensure!(!menu.items.is_empty(), "empty menu");
        let lines: Vec<serde_json::Value> = (0..rand_range(1, 3))
            .map(|_| {
                let item = &menu.items[rand::rng().random_range(0..menu.items.len())];
                serde_json::json!({"item_id": item.id, "qty": rand_range(1, 2)})
            })
            .collect();
        let order: Order = self
            .http
            .post(format!("{}/api/shop/orders", self.base_url))
            .header("x-client-id", "loadgen")
            .json(&serde_json::json!({"lines": lines}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        self.wait_for_terminal(&order.id).await
    }

    async fn wait_for_terminal(&self, id: &str) -> anyhow::Result<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut last = String::from("accepted");
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self
                .http
                .get(format!("{}/api/shop/orders/{id}", self.base_url))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let order: Order = resp.json().await?;
                    last = order.status;
                    if last == "delivered" {
                        break;
                    }
                }
                Ok(resp) => tracing::debug!(status = %resp.status(), "poll failed"),
                Err(err) => tracing::debug!(%err, "poll failed"),
            }
        }
        Ok(last)
    }
}

fn rand_range(min: u64, max: u64) -> u64 {
    rand::rng().random_range(min..=max)
}
