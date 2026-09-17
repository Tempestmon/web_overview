//! How each fault kind is put in place and taken away. Kept apart from the
//! HTTP layer so the same code serves the initial apply, the TTL sweep and an
//! early "fix it" request.

use crate::api::AppState;
use crate::topology::{FaultKind, Node};

const NETEM_DELAY: &[&str] = &[
    "tc",
    "qdisc",
    "replace",
    "dev",
    "eth0",
    "root",
    "netem",
    "delay",
    "800ms",
    "200ms",
    "distribution",
    "normal",
];
const NETEM_LOSS: &[&str] = &[
    "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", "loss", "30%",
];
const NETEM_CLEAR: &[&str] = &["tc", "qdisc", "del", "dev", "eth0", "root"];

pub async fn apply(state: &AppState, node: &Node, kind: FaultKind) -> anyhow::Result<()> {
    let container = container_of(node)?;
    match kind {
        FaultKind::Stop => state.docker.stop(container).await,
        FaultKind::Pause => state.docker.pause(container).await,
        FaultKind::Latency => state.docker.exec(container, NETEM_DELAY).await,
        FaultKind::Loss => state.docker.exec(container, NETEM_LOSS).await,
        FaultKind::AppError => {
            state
                .http
                .put(ops_fault_url(node)?)
                .json(&serde_json::json!({ "error_rate": 0.5, "latency_ms": 0 }))
                .send()
                .await?
                .error_for_status()?;
            Ok(())
        }
    }
}

pub async fn heal(state: &AppState, node: &Node, kind: FaultKind) -> anyhow::Result<()> {
    let container = container_of(node)?;
    match kind {
        FaultKind::Stop => state.docker.start(container).await,
        FaultKind::Pause => state.docker.unpause(container).await,
        FaultKind::Latency | FaultKind::Loss => {
            // A missing qdisc means the network is already clean (e.g. the
            // container restarted in between); that is success, not failure.
            if let Err(err) = state.docker.exec(container, NETEM_CLEAR).await {
                tracing::warn!(node = node.id, %err, "netem clear reported an error; assuming clean");
            }
            Ok(())
        }
        FaultKind::AppError => {
            state
                .http
                .delete(ops_fault_url(node)?)
                .send()
                .await?
                .error_for_status()?;
            Ok(())
        }
    }
}

fn container_of(node: &Node) -> anyhow::Result<&str> {
    node.container
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("node {} has no container", node.id))
}

fn ops_fault_url(node: &Node) -> anyhow::Result<String> {
    let port = node
        .ops_port
        .ok_or_else(|| anyhow::anyhow!("node {} has no ops_port", node.id))?;
    Ok(format!("http://{}:{port}/ops/fault", container_of(node)?))
}
