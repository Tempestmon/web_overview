use std::sync::Arc;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

use crate::Edge;

const CALL_TIMEOUT: Duration = Duration::from_millis(300);

/// A Redis handle that connects on first use and forgets a broken
/// connection so the next call reconnects. A dead Redis at startup must not
/// prevent a service from booting: on the panel that would look like a
/// crash loop instead of a dependency outage.
#[derive(Clone)]
pub struct LazyRedis {
    client: redis::Client,
    conn: Arc<Mutex<Option<MultiplexedConnection>>>,
    edge: Edge,
}

impl LazyRedis {
    pub fn new(url: &str, service_id: &str) -> redis::RedisResult<Self> {
        Ok(Self {
            client: redis::Client::open(url)?,
            conn: Arc::new(Mutex::new(None)),
            edge: Edge::new(service_id, "redis", "redis"),
        })
    }

    /// Runs one operation with a short timeout, recording it on the
    /// `service → redis` edge. Errors drop the cached connection.
    pub async fn call<T, F, Fut>(&self, op: F) -> redis::RedisResult<T>
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
                    .map_err(|_| timeout("redis call timed out"))?
            })
            .await;
        if result.is_err() {
            *self.conn.lock().await = None;
        }
        result
    }

    async fn connection(&self) -> redis::RedisResult<MultiplexedConnection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = tokio::time::timeout(CALL_TIMEOUT, self.client.get_multiplexed_async_connection())
            .await
            .map_err(|_| timeout("redis connect timed out"))??;
        *guard = Some(c.clone());
        Ok(c)
    }
}

fn timeout(msg: &'static str) -> redis::RedisError {
    redis::RedisError::from((redis::ErrorKind::Io, msg))
}
