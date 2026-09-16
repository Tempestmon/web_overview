use serde::Serialize;

use crate::state::NodeState;
use crate::topology::FaultKind;

/// What the frontend renders. Sent whole every second: a diff protocol
/// would save bytes we do not need to save.
#[derive(Clone, Debug, Serialize, Default)]
pub struct Snapshot {
    pub ts_ms: u64,
    pub nodes: Vec<NodeSnapshot>,
    pub edges: Vec<EdgeSnapshot>,
    pub faults: Vec<ActiveFault>,
    pub cooldown_s: f64,
    pub people_online: f64,
    pub orders_per_min: f64,
    pub kafka_lag: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeSnapshot {
    pub id: String,
    pub state: NodeState,
    pub fault: Option<FaultKind>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EdgeSnapshot {
    pub from: String,
    pub to: String,
    pub rps: f64,
    pub error_rps: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ActiveFault {
    pub id: u64,
    pub node: String,
    pub kind: FaultKind,
    pub remaining_s: f64,
}
