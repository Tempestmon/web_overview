use std::sync::Arc;
use std::time::Duration;

use common::Edge;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

use crate::repo::MenuRow;

const KEY: &str = "menu:v1";
const TTL_SECONDS: u64 = 30;
const CALL_TIMEOUT: Duration = Duration::from_millis(300);

/// Cache-aside for the menu. Redis is a soft dependency: any failure here
/// is logged, counted as an error on the edge, and treated as a miss, so
/// the service keeps working (slower) when Redis is down.
#[derive(Clone)]
pub struct MenuCache {
    client: redis::Client,
    conn: Arc<Mutex<Option<MultiplexedConnection>>>,
    edge: Edge,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CachedItem {
    id: i64,
    name: String,
    price_cents: i64,
}

impl MenuCache {
    pub fn new(redis_url: &str, service_id: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: redis::Client::open(redis_url)?,
            conn: Arc::new(Mutex::new(None)),
            edge: Edge::new(service_id, "redis", "redis"),
        })
    }

    pub async fn get(&self) -> Option<Vec<MenuRow>> {
        let raw: Option<String> = self.call(|mut c| async move { c.get(KEY).await }).await?;
        let items: Vec<CachedItem> = serde_json::from_str(&raw?).ok()?;
        Some(
            items
                .into_iter()
                .map(|i| MenuRow {
                    id: i.id,
                    name: i.name,
                    price_cents: i.price_cents,
                })
                .collect(),
        )
    }

    pub async fn put(&self, items: &[MenuRow]) {
        let payload = serde_json::to_string(
            &items
                .iter()
                .map(|i| CachedItem {
                    id: i.id,
                    name: i.name.clone(),
                    price_cents: i.price_cents,
                })
                .collect::<Vec<_>>(),
        )
        .expect("menu items serialize");
        let _: Option<()> = self
            .call(|mut c| async move { c.set_ex(KEY, payload, TTL_SECONDS).await })
            .await;
    }

    /// Lazily opens the connection so a dead Redis at startup does not
    /// prevent the service from booting; a failed call drops the connection
    /// so the next one reconnects.
    async fn call<T, F, Fut>(&self, op: F) -> Option<T>
    where
        F: FnOnce(MultiplexedConnection) -> Fut,
        Fut: std::future::Future<Output = redis::RedisResult<T>>,
    {
        let result = self
            .edge
            .observe(async {
                let conn = self.connection().await?;
                tokio::time::timeout(CALL_TIMEOUT, op(conn))
                    .await
                    .map_err(|_| {
                        redis::RedisError::from((redis::ErrorKind::Io, "redis call timed out"))
                    })?
            })
            .await;
        match result {
            Ok(v) => Some(v),
            Err(err) => {
                tracing::warn!(%err, "redis unavailable, treating as cache miss");
                *self.conn.lock().await = None;
                None
            }
        }
    }

    async fn connection(&self) -> redis::RedisResult<MultiplexedConnection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = tokio::time::timeout(CALL_TIMEOUT, self.client.get_multiplexed_async_connection())
            .await
            .map_err(|_| {
                redis::RedisError::from((redis::ErrorKind::Io, "redis connect timed out"))
            })??;
        *guard = Some(c.clone());
        Ok(c)
    }
}
