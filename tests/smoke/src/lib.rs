//! Helpers for end-to-end checks against a running stack.
//! Every test is `#[ignore]`; run them with `make smoke`.

use std::time::{Duration, Instant};

use serde_json::Value;

pub struct Stack {
    http: reqwest::Client,
    base: String,
    admin_token: String,
}

impl Stack {
    pub fn from_env() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
            base: std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into()),
            admin_token: std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "change-me".into()),
        }
    }

    pub async fn menu(&self) -> anyhow::Result<reqwest::Response> {
        Ok(self
            .http
            .get(format!("{}/api/shop/menu", self.base))
            .send()
            .await?)
    }

    pub async fn create_order(&self) -> anyhow::Result<String> {
        let resp = self
            .http
            .post(format!("{}/api/shop/orders", self.base))
            .header("x-client-id", "smoke")
            .json(&serde_json::json!({"lines": [{"item_id": 1, "qty": 1}]}))
            .send()
            .await?;
        anyhow::ensure!(resp.status() == 201, "create order: {}", resp.status());
        Ok(resp.json::<Value>().await?["id"]
            .as_str()
            .unwrap()
            .to_string())
    }

    pub async fn create_order_raw(&self) -> anyhow::Result<reqwest::Response> {
        Ok(self
            .http
            .post(format!("{}/api/shop/orders", self.base))
            .header("x-client-id", "smoke")
            .json(&serde_json::json!({"lines": [{"item_id": 1, "qty": 1}]}))
            .send()
            .await?)
    }

    pub async fn order_status(&self, id: &str) -> anyhow::Result<String> {
        let v: Value = self
            .http
            .get(format!("{}/api/shop/orders/{id}", self.base))
            .send()
            .await?
            .json()
            .await?;
        Ok(v["status"].as_str().unwrap_or("?").to_string())
    }

    pub async fn wait_for_status(
        &self,
        id: &str,
        wanted: &str,
        within: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.order_status(id).await.ok().as_deref() == Some(wanted) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("order {id} did not reach {wanted} within {within:?}")
    }

    pub async fn snapshot(&self) -> anyhow::Result<Value> {
        Ok(self
            .http
            .get(format!("{}/api/state", self.base))
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn node_state(&self, node: &str) -> anyhow::Result<String> {
        let snap = self.snapshot().await?;
        let found = snap["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == node);
        Ok(found
            .and_then(|n| n["state"].as_str())
            .unwrap_or("?")
            .to_string())
    }

    pub async fn wait_for_node_state(
        &self,
        node: &str,
        wanted: &str,
        within: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.node_state(node).await? == wanted {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("{node} did not become {wanted} within {within:?}")
    }

    fn edge<'a>(snap: &'a Value, from: &str, to: &str) -> Option<&'a Value> {
        snap["edges"]
            .as_array()?
            .iter()
            .find(|e| e["from"] == from && e["to"] == to)
    }

    pub async fn wait_for_breaker(
        &self,
        dependency: &str,
        wanted: &str,
        within: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            let snap = self.snapshot().await?;
            let state = snap["edges"]
                .as_array()
                .unwrap()
                .iter()
                .find(|e| e["to"] == dependency && e["breaker"].is_string())
                .and_then(|e| e["breaker"].as_str().map(str::to_string));
            if state.as_deref() == Some(wanted) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("breaker for {dependency} did not become {wanted} within {within:?}")
    }

    pub async fn wait_for_edge_p95_above(
        &self,
        from: &str,
        to: &str,
        p95_ms: f64,
        within: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            let snap = self.snapshot().await?;
            if Self::edge(&snap, from, to)
                .and_then(|e| e["p95_ms"].as_f64())
                .unwrap_or(0.0)
                > p95_ms
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        anyhow::bail!("edge {from}->{to} p95 did not exceed {p95_ms} ms within {within:?}")
    }

    pub async fn wait_all_healthy(&self, within: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            let snap = self.snapshot().await?;
            let all_healthy = snap["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["state"] == "healthy");
            if all_healthy && snap["faults"].as_array().unwrap().is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("stack did not become fully healthy within {within:?}")
    }

    pub async fn start_fault(&self, node: &str, kind: &str, ttl_s: u64) -> anyhow::Result<u64> {
        let resp = self
            .http
            .post(format!("{}/api/faults", self.base))
            .header("x-admin-token", &self.admin_token)
            .json(&serde_json::json!({"node": node, "kind": kind, "ttl_s": ttl_s}))
            .send()
            .await?;
        anyhow::ensure!(
            resp.status() == 201,
            "start fault: {} {}",
            resp.status(),
            resp.text().await?
        );
        Ok(resp.json::<Value>().await?["id"].as_u64().unwrap())
    }

    pub async fn stop_fault(&self, id: u64) -> anyhow::Result<()> {
        let resp = self
            .http
            .delete(format!("{}/api/faults/{id}", self.base))
            .header("x-admin-token", &self.admin_token)
            .send()
            .await?;
        anyhow::ensure!(
            resp.status().is_success() || resp.status() == 404,
            "stop fault: {}",
            resp.status()
        );
        Ok(())
    }
}
