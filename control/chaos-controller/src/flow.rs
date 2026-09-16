use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::RwLock;

use crate::topology::Topology;

pub const FLOW_POLL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub struct EdgeRate {
    pub rps: f64,
    pub error_rps: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Flow {
    pub edges: HashMap<(String, String), EdgeRate>,
    pub people_online: f64,
    pub orders_per_min: f64,
    pub kafka_lag: f64,
}

pub type SharedFlow = Arc<RwLock<Flow>>;

#[derive(Clone)]
pub struct PromClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Deserialize)]
struct PromResponse {
    data: PromData,
}

#[derive(Deserialize)]
struct PromData {
    result: Vec<PromSample>,
}

#[derive(Deserialize)]
struct PromSample {
    metric: HashMap<String, String>,
    value: (f64, String),
}

impl PromClient {
    pub fn new(base_url: String) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()?;
        Ok(Self { http, base_url })
    }

    async fn query(&self, expr: &str) -> anyhow::Result<Vec<PromSample>> {
        let resp: PromResponse = self
            .http
            .get(format!("{}/api/v1/query", self.base_url))
            .query(&[("query", expr)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.data.result)
    }

    async fn scalar(&self, expr: &str) -> f64 {
        match self.query(expr).await {
            Ok(samples) => samples
                .iter()
                .filter_map(|s| s.value.1.parse::<f64>().ok())
                .sum(),
            Err(err) => {
                tracing::debug!(%err, expr, "prometheus query failed");
                0.0
            }
        }
    }

    /// One aggregated query covers every edge that services report
    /// themselves; edges with a custom `query` (e.g. Traefik's own metrics)
    /// are fetched separately.
    pub async fn flow(&self, topology: &Topology) -> Flow {
        let mut edges: HashMap<(String, String), EdgeRate> = HashMap::new();
        if let Ok(samples) = self
            .query("sum by (src, dst, outcome) (rate(edge_requests_total[20s]))")
            .await
        {
            for s in samples {
                let (Some(src), Some(dst)) = (s.metric.get("src"), s.metric.get("dst")) else {
                    continue;
                };
                let value = s.value.1.parse::<f64>().unwrap_or(0.0);
                let entry = edges.entry((src.clone(), dst.clone())).or_default();
                entry.rps += value;
                if s.metric.get("outcome").map(String::as_str) == Some("error") {
                    entry.error_rps += value;
                }
            }
        }
        for edge in topology.edges.iter().filter(|e| e.query.is_some()) {
            let rps = self.scalar(edge.query.as_deref().unwrap_or_default()).await;
            edges.insert(
                (edge.from.clone(), edge.to.clone()),
                EdgeRate {
                    rps,
                    error_rps: 0.0,
                },
            );
        }
        Flow {
            edges,
            // Two queries on purpose: PromQL arithmetic with an absent series
            // yields nothing, and a gauge nobody touched yet is absent.
            people_online: self.scalar("sum(loadgen_active_users)").await
                + self.scalar("sum(gateway_sse_connections)").await,
            orders_per_min: self.scalar("sum(increase(orders_created_total[1m]))").await,
            kafka_lag: self.scalar("sum(kafka_consumergroup_lag)").await,
        }
    }
}

pub async fn run_poller(prom: PromClient, topology: Arc<Topology>, flow: SharedFlow) {
    loop {
        *flow.write().await = prom.flow(&topology).await;
        tokio::time::sleep(FLOW_POLL).await;
    }
}
