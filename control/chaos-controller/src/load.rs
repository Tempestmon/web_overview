//! Load boosts: the panel asks for "N times more customers" and the
//! controller forwards it to the load generator, which owns the TTL.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

const POLL: Duration = Duration::from_secs(1);

/// What the load generator reports on `GET /ops/load`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoadStatus {
    pub users: usize,
    pub base_users: usize,
    pub remaining_s: f64,
}

pub type SharedLoad = Arc<RwLock<Option<LoadStatus>>>;

/// Shared-world rule for boosts: while one is running, only its owner or an
/// admin may change it. Anyone may wait it out; it ends by TTL.
#[derive(Default)]
pub struct BoostBook {
    active: Option<(String, Instant)>,
}

impl BoostBook {
    pub fn claim(
        &mut self,
        owner: &str,
        admin: bool,
        now: Instant,
        ttl: Duration,
    ) -> Result<(), Duration> {
        if let Some((holder, until)) = &self.active {
            let remaining = until.saturating_duration_since(now);
            if !remaining.is_zero() && holder != owner && !admin {
                return Err(remaining);
            }
        }
        self.active = Some((owner.to_string(), now + ttl));
        Ok(())
    }

    pub fn release(&mut self) {
        self.active = None;
    }
}

#[derive(Clone)]
pub struct LoadControl {
    http: reqwest::Client,
    url: String,
    pub max_factor: usize,
    pub ttl: Duration,
    pub book: Arc<Mutex<BoostBook>>,
    pub status: SharedLoad,
}

impl LoadControl {
    pub fn new(http: reqwest::Client, loadgen_url: &str, max_factor: usize, ttl: Duration) -> Self {
        Self {
            http,
            url: format!("{}/ops/load", loadgen_url.trim_end_matches('/')),
            max_factor,
            ttl,
            book: Default::default(),
            status: Default::default(),
        }
    }

    pub async fn fetch(&self) -> anyhow::Result<LoadStatus> {
        let response = self.http.get(&self.url).send().await?;
        Ok(response.error_for_status()?.json().await?)
    }

    pub async fn set_factor(&self, factor: usize) -> anyhow::Result<LoadStatus> {
        let base_users = self.fetch().await?.base_users;
        let body = serde_json::json!({"users": base_users * factor, "ttl_s": self.ttl.as_secs()});
        let response = self.http.put(&self.url).json(&body).send().await?;
        let status: LoadStatus = response.error_for_status()?.json().await?;
        *self.status.write().await = Some(status.clone());
        Ok(status)
    }

    /// Keeps the snapshot's view of the load fresh. An unreachable generator
    /// is reported as "unknown" rather than as the last value seen.
    pub async fn run_poller(self) {
        loop {
            let status = self.fetch().await;
            if let Err(err) = &status {
                tracing::debug!(%err, "load generator status unavailable");
            }
            *self.status.write().await = status.ok();
            tokio::time::sleep(POLL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(120);

    #[test]
    fn second_visitor_waits_for_the_running_boost() {
        let mut book = BoostBook::default();
        let now = Instant::now();
        assert!(book.claim("alice", false, now, TTL).is_ok());

        let refused = book.claim("bob", false, now + Duration::from_secs(20), TTL);
        assert_eq!(refused, Err(Duration::from_secs(100)));
    }

    #[test]
    fn owner_and_admin_may_change_a_running_boost() {
        let mut book = BoostBook::default();
        let now = Instant::now();
        book.claim("alice", false, now, TTL).unwrap();

        assert!(book.claim("alice", false, now, TTL).is_ok());
        assert!(book.claim("bob", true, now, TTL).is_ok());
    }

    #[test]
    fn boost_is_free_again_after_its_ttl_or_a_release() {
        let mut book = BoostBook::default();
        let now = Instant::now();
        book.claim("alice", false, now, TTL).unwrap();
        assert!(book.claim("bob", false, now + TTL, TTL).is_ok());

        book.release();
        assert!(book.claim("carol", false, now + TTL, TTL).is_ok());
    }
}
