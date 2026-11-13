use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

use crate::docker::{ContainerControl, ContainerStatus, STATUS_POLL};
use crate::faults::FaultBook;
use crate::topology::Topology;

const FORCED_RESTARTS: &str = "stand_forced_restarts_total";

/// What Docker says about the containers, refreshed every few seconds.
#[derive(Clone, Debug, Default)]
pub struct Observed {
    pub broken: HashSet<String>,
    pub recovering: HashSet<String>,
}

pub type SharedObserved = Arc<RwLock<Observed>>;

impl Observed {
    fn down(&self) -> HashSet<String> {
        self.broken.union(&self.recovering).cloned().collect()
    }
}

/// Remembers since when each node has been down for no declared reason.
#[derive(Default)]
pub struct Watchdog {
    down_since: HashMap<String, Instant>,
}

impl Watchdog {
    /// Nodes that stayed down for `limit` with no fault on them. A reported
    /// node gets a fresh window, so a restart that did not help is retried
    /// once per `limit` rather than on every poll.
    pub fn overdue(
        &mut self,
        down: &HashSet<String>,
        faulted: &HashSet<String>,
        now: Instant,
        limit: Duration,
    ) -> Vec<String> {
        self.down_since
            .retain(|node, _| down.contains(node) && !faulted.contains(node));
        let mut overdue = Vec::new();
        for node in down.difference(faulted) {
            let since = self.down_since.entry(node.clone()).or_insert(now);
            if now.duration_since(*since) >= limit {
                *since = now;
                overdue.push(node.clone());
            }
        }
        overdue
    }
}

pub async fn run_monitor(
    topology: Arc<Topology>,
    docker: Arc<dyn ContainerControl>,
    observed: SharedObserved,
    book: Arc<Mutex<FaultBook>>,
    force_start_after: Duration,
) {
    // A counter that first appears at 1 gives `increase()` nothing to compare
    // with, and the alert on it would miss the first restart.
    for (node, _) in topology.container_nodes() {
        metrics::counter!(FORCED_RESTARTS, "node" => node.id.clone()).absolute(0);
    }
    let mut watchdog = Watchdog::default();
    loop {
        let next = observe(&topology, docker.as_ref()).await;
        let down = next.down();
        *observed.write().await = next;

        let faulted = faulted_nodes(&book).await;
        for node in watchdog.overdue(&down, &faulted, Instant::now(), force_start_after) {
            force_start(&topology, docker.as_ref(), &node).await;
        }
        tokio::time::sleep(STATUS_POLL).await;
    }
}

async fn faulted_nodes(book: &Mutex<FaultBook>) -> HashSet<String> {
    let book = book.lock().await;
    book.active().iter().map(|f| f.node.clone()).collect()
}

async fn force_start(topology: &Topology, docker: &dyn ContainerControl, node: &str) {
    let Some(container) = topology
        .container_nodes()
        .find_map(|(n, container)| (n.id == node).then_some(container))
    else {
        return;
    };
    metrics::counter!(FORCED_RESTARTS, "node" => node.to_string()).increment(1);
    match revive(docker, container).await {
        Ok(()) => tracing::error!(node, "node stayed down with no fault on it, forced a start"),
        Err(err) => tracing::error!(%err, node, "forced start failed"),
    }
}

async fn revive(docker: &dyn ContainerControl, container: &str) -> anyhow::Result<()> {
    match docker.status(container).await? {
        ContainerStatus::Serving => Ok(()),
        ContainerStatus::Paused => docker.unpause(container).await,
        ContainerStatus::Exited | ContainerStatus::Missing => docker.start(container).await,
        // Running but never healthy: only a fresh process can help.
        ContainerStatus::Starting | ContainerStatus::Unhealthy => {
            docker.stop(container).await?;
            docker.start(container).await
        }
    }
}

async fn observe(topology: &Topology, docker: &dyn ContainerControl) -> Observed {
    let mut observed = Observed::default();
    for (node, container) in topology.container_nodes() {
        match docker.status(container).await {
            Ok(ContainerStatus::Serving) => {}
            Ok(ContainerStatus::Starting) => {
                observed.recovering.insert(node.id.clone());
            }
            Ok(_) => {
                observed.broken.insert(node.id.clone());
            }
            Err(err) => {
                tracing::warn!(%err, container, "docker inspect failed");
            }
        }
    }
    observed
}

/// A controller restart must not leave yesterday's chaos in place: anything
/// stopped or paused from the topology is brought back before we serve.
pub async fn reconcile(topology: &Topology, docker: &dyn ContainerControl) {
    for (node, container) in topology.container_nodes() {
        let result = match docker.status(container).await {
            Ok(ContainerStatus::Exited) => docker.start(container).await,
            Ok(ContainerStatus::Paused) => docker.unpause(container).await,
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        };
        if let Err(err) = result {
            tracing::warn!(%err, node = node.id, "reconcile failed");
        } else {
            tracing::debug!(node = node.id, "reconciled");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(120);

    fn set(nodes: &[&str]) -> HashSet<String> {
        nodes.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn node_is_overdue_only_after_the_limit() {
        let mut watchdog = Watchdog::default();
        let start = Instant::now();
        let down = set(&["kafka"]);

        assert!(watchdog.overdue(&down, &set(&[]), start, LIMIT).is_empty());
        let just_before = start + LIMIT - Duration::from_secs(1);
        assert!(
            watchdog
                .overdue(&down, &set(&[]), just_before, LIMIT)
                .is_empty()
        );
        assert_eq!(
            watchdog.overdue(&down, &set(&[]), start + LIMIT, LIMIT),
            ["kafka"]
        );
    }

    #[test]
    fn faulted_node_is_never_overdue_and_gets_a_fresh_window_after_the_fault() {
        let mut watchdog = Watchdog::default();
        let start = Instant::now();
        let down = set(&["kafka"]);
        let faulted = set(&["kafka"]);

        assert!(watchdog.overdue(&down, &faulted, start, LIMIT).is_empty());
        let fault_end = start + LIMIT * 2;
        assert!(
            watchdog
                .overdue(&down, &faulted, fault_end, LIMIT)
                .is_empty()
        );
        assert!(
            watchdog
                .overdue(&down, &set(&[]), fault_end, LIMIT)
                .is_empty()
        );
        assert_eq!(
            watchdog.overdue(&down, &set(&[]), fault_end + LIMIT, LIMIT),
            ["kafka"]
        );
    }

    #[test]
    fn recovery_clears_the_window() {
        let mut watchdog = Watchdog::default();
        let start = Instant::now();

        watchdog.overdue(&set(&["kafka"]), &set(&[]), start, LIMIT);
        watchdog.overdue(&set(&[]), &set(&[]), start + LIMIT / 2, LIMIT);
        assert!(
            watchdog
                .overdue(&set(&["kafka"]), &set(&[]), start + LIMIT, LIMIT)
                .is_empty()
        );
    }

    #[test]
    fn failed_restart_is_retried_once_per_limit() {
        let mut watchdog = Watchdog::default();
        let start = Instant::now();
        let down = set(&["kafka"]);

        watchdog.overdue(&down, &set(&[]), start, LIMIT);
        assert_eq!(
            watchdog
                .overdue(&down, &set(&[]), start + LIMIT, LIMIT)
                .len(),
            1
        );
        let next_poll = start + LIMIT + Duration::from_secs(2);
        assert!(
            watchdog
                .overdue(&down, &set(&[]), next_poll, LIMIT)
                .is_empty()
        );
        assert_eq!(
            watchdog
                .overdue(&down, &set(&[]), start + LIMIT * 2, LIMIT)
                .len(),
            1
        );
    }
}
