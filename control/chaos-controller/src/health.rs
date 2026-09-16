use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::docker::{ContainerControl, ContainerStatus, STATUS_POLL};
use crate::topology::Topology;

/// What Docker says about the containers, refreshed every few seconds.
#[derive(Clone, Debug, Default)]
pub struct Observed {
    pub broken: HashSet<String>,
    pub recovering: HashSet<String>,
}

pub type SharedObserved = Arc<RwLock<Observed>>;

pub async fn run_monitor(
    topology: Arc<Topology>,
    docker: Arc<dyn ContainerControl>,
    observed: SharedObserved,
) {
    loop {
        let next = observe(&topology, docker.as_ref()).await;
        *observed.write().await = next;
        tokio::time::sleep(STATUS_POLL).await;
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
