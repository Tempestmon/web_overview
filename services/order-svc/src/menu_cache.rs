use common::LazyRedis;
use redis::AsyncCommands;

use crate::repo::MenuRow;

const KEY: &str = "menu:v1";
const TTL_SECONDS: u64 = 30;

/// Cache-aside for the menu. Redis is a soft dependency: any failure is
/// logged, counted as an error on the edge, and treated as a miss, so the
/// service keeps working (slower) when Redis is down.
#[derive(Clone)]
pub struct MenuCache {
    redis: LazyRedis,
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
            redis: LazyRedis::new(redis_url, service_id)?,
        })
    }

    pub async fn get(&self) -> Option<Vec<MenuRow>> {
        let raw: Option<String> = self.soft(
            self.redis
                .call(|mut c| async move { c.get(KEY).await })
                .await,
        )?;
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
        let _: Option<()> = self.soft(
            self.redis
                .call(|mut c| async move { c.set_ex(KEY, payload, TTL_SECONDS).await })
                .await,
        );
    }

    fn soft<T>(&self, result: redis::RedisResult<T>) -> Option<T> {
        match result {
            Ok(v) => Some(v),
            Err(err) => {
                tracing::warn!(%err, "redis unavailable, treating as cache miss");
                None
            }
        }
    }
}
