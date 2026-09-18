use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::RwLock;

use crate::topology::{Proto, Topology};

pub const FLOW_POLL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub struct EdgeRate {
    pub rps: f64,
    pub error_rps: f64,
    pub p95_s: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Flow {
    pub edges: HashMap<(String, String), EdgeRate>,
    /// Circuit breaker state per guarded dependency (keyed by its node id).
    pub breakers: HashMap<String, String>,
    pub people_online: f64,
    pub orders_per_min: f64,
    pub kafka_lag: f64,
    pub lead_time_s: f64,
}

/// Thresholds above which an edge counts as failing for the scheme even
/// though nobody stopped a container: this is how a slow or erroring
/// dependency paints its callers yellow.
const FAILING_MIN_RPS: f64 = 0.2;
const FAILING_ERROR_RATIO: f64 = 0.2;
const FAILING_P95_S: f64 = 1.0;

/// Latency only counts for request/response edges: on a Kafka consume edge
/// the "duration" is the handler's processing time (a kitchen cooking for
/// seconds is fine), so only the error ratio applies there.
pub fn failing_edges(flow: &Flow, topology: &Topology) -> Vec<(String, String)> {
    let synchronous = |from: &str, to: &str| {
        topology
            .edges
            .iter()
            .find(|e| e.from == from && e.to == to)
            .is_some_and(|e| e.proto != Proto::Kafka)
    };
    flow.edges
        .iter()
        .filter(|((from, to), r)| {
            let erroring = r.rps > FAILING_MIN_RPS && r.error_rps / r.rps > FAILING_ERROR_RATIO;
            erroring || (r.p95_s > FAILING_P95_S && synchronous(from, to))
        })
        .map(|(k, _)| k.clone())
        .collect()
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
        if let Ok(samples) = self
            .query("histogram_quantile(0.95, sum by (le, src, dst) (rate(edge_request_duration_seconds_bucket[1m])))")
            .await
        {
            for s in samples {
                let (Some(src), Some(dst)) = (s.metric.get("src"), s.metric.get("dst")) else {
                    continue;
                };
                if let Ok(p95) = s.value.1.parse::<f64>() {
                    edges.entry((src.clone(), dst.clone())).or_default().p95_s = p95;
                }
            }
        }
        let mut breakers = HashMap::new();
        if let Ok(samples) = self.query("max by (name) (circuit_breaker_state)").await {
            for s in samples {
                let Some(name) = s.metric.get("name") else {
                    continue;
                };
                let state = match s.value.1.parse::<f64>().unwrap_or(0.0) as u8 {
                    1 => "open",
                    2 => "half-open",
                    _ => "closed",
                };
                breakers.insert(name.clone(), state.to_string());
            }
        }
        for edge in topology.edges.iter().filter(|e| e.query.is_some()) {
            let rps = self.scalar(edge.query.as_deref().unwrap_or_default()).await;
            edges.insert(
                (edge.from.clone(), edge.to.clone()),
                EdgeRate {
                    rps,
                    error_rps: 0.0,
                    p95_s: 0.0,
                },
            );
        }
        Flow {
            edges,
            breakers,
            // Two queries on purpose: PromQL arithmetic with an absent series
            // yields nothing, and a gauge nobody touched yet is absent.
            people_online: self.scalar("sum(loadgen_active_users)").await
                + self.scalar("sum(gateway_sse_connections)").await,
            orders_per_min: self.scalar("sum(increase(orders_created_total[1m]))").await,
            // The exporter reports -1 for partitions without a committed offset.
            kafka_lag: self.scalar("sum(kafka_consumergroup_lag)").await.max(0.0),
            lead_time_s: self.scalar("analytics_lead_time_seconds").await,
        }
    }
}

pub async fn run_poller(prom: PromClient, topology: Arc<Topology>, flow: SharedFlow) {
    loop {
        *flow.write().await = prom.flow(&topology).await;
        tokio::time::sleep(FLOW_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flow_with(edges: Vec<((&str, &str), EdgeRate)>) -> Flow {
        Flow {
            edges: edges
                .into_iter()
                .map(|((a, b), r)| ((a.to_string(), b.to_string()), r))
                .collect(),
            ..Default::default()
        }
    }

    fn topology() -> Topology {
        Topology::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../topology.json"
        )))
        .unwrap()
    }

    #[test]
    fn failing_edges_need_real_traffic_or_high_latency_on_sync_edges() {
        let flow = flow_with(vec![
            (
                ("gateway-1", "order-svc"),
                EdgeRate {
                    rps: 10.0,
                    error_rps: 5.0,
                    p95_s: 0.1,
                },
            ),
            (
                ("gateway-2", "order-svc"),
                EdgeRate {
                    rps: 0.1,
                    error_rps: 0.1,
                    p95_s: 0.1,
                },
            ),
            (
                ("order-svc", "postgres"),
                EdgeRate {
                    rps: 3.0,
                    error_rps: 0.0,
                    p95_s: 2.5,
                },
            ),
            (
                ("order-svc", "redis"),
                EdgeRate {
                    rps: 3.0,
                    error_rps: 0.1,
                    p95_s: 0.2,
                },
            ),
            (
                ("kafka", "kitchen-svc"),
                EdgeRate {
                    rps: 1.0,
                    error_rps: 0.0,
                    p95_s: 6.0,
                },
            ),
            (
                ("kafka", "courier-svc"),
                EdgeRate {
                    rps: 1.0,
                    error_rps: 0.9,
                    p95_s: 6.0,
                },
            ),
        ]);
        let mut failing = failing_edges(&flow, &topology());
        failing.sort();
        assert_eq!(
            failing,
            vec![
                ("gateway-1".into(), "order-svc".into()),
                ("kafka".into(), "courier-svc".into()),
                ("order-svc".into(), "postgres".into()),
            ]
        );
    }
}
