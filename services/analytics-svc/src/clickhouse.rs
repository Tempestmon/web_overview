use std::time::Duration;

use common::Edge;

use crate::rows::{Row, json_each_row_line};

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS order_events \
    (order_id UUID, status String, ts DateTime64(3)) ENGINE = MergeTree ORDER BY ts";
const LEAD_TIME_QUERY: &str = "SELECT avg(lead) FROM (SELECT (maxIf(ts, status='delivered') - minIf(ts, status='accepted')) AS lead \
    FROM order_events WHERE ts > now() - INTERVAL 5 MINUTE GROUP BY order_id HAVING lead > 0) FORMAT TabSeparated";
const DELIVERED_QUERY: &str = "SELECT count() FROM order_events WHERE status='delivered' AND ts > now() - INTERVAL 5 MINUTE FORMAT TabSeparated";

/// Thin client over ClickHouse's HTTP interface; a driver crate would add
/// nothing the demo needs beyond two statements and one insert shape.
#[derive(Clone)]
pub struct ClickHouse {
    http: reqwest::Client,
    url: String,
    user: String,
    password: String,
    edge: Edge,
}

impl ClickHouse {
    pub fn new(
        url: String,
        user: String,
        password: String,
        service_id: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
            url,
            user,
            password,
            edge: Edge::new(service_id, "clickhouse", "sql"),
        })
    }

    pub async fn ensure_schema(&self) -> anyhow::Result<()> {
        for attempt in 1..=30 {
            match self.statement(CREATE_TABLE, String::new()).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    tracing::warn!(%err, attempt, "clickhouse not ready");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        anyhow::bail!("clickhouse never became ready")
    }

    pub async fn insert(&self, rows: &[Row]) -> anyhow::Result<()> {
        let body = rows
            .iter()
            .map(json_each_row_line)
            .collect::<Vec<_>>()
            .join("\n");
        self.statement("INSERT INTO order_events FORMAT JSONEachRow", body)
            .await
            .map(|_| ())
    }

    pub async fn lead_time_seconds(&self) -> anyhow::Result<f64> {
        self.scalar(LEAD_TIME_QUERY).await
    }

    pub async fn delivered_last_5m(&self) -> anyhow::Result<f64> {
        self.scalar(DELIVERED_QUERY).await
    }

    async fn scalar(&self, query: &str) -> anyhow::Result<f64> {
        let text = self.statement(query, String::new()).await?;
        let value = text.trim();
        // ClickHouse prints `\N` for NULL, e.g. avg() over an empty set.
        if value == r"\N" || value.is_empty() {
            return Ok(0.0);
        }
        Ok(value.parse()?)
    }

    async fn statement(&self, query: &str, body: String) -> anyhow::Result<String> {
        self.edge
            .observe(async {
                let response = self
                    .http
                    .post(&self.url)
                    .basic_auth(&self.user, Some(&self.password))
                    .query(&[("query", query)])
                    .body(body)
                    .send()
                    .await?;
                let status = response.status();
                let text = response.text().await?;
                anyhow::ensure!(status.is_success(), "clickhouse {status}: {text}");
                Ok(text)
            })
            .await
    }
}
