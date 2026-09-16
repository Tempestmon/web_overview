use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum FaultKind {
    Stop,
    Pause,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Actor,
    Lb,
    Service,
    Db,
    Cache,
    Broker,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Proto {
    Http,
    Grpc,
    Sql,
    Redis,
    Kafka,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Dep {
    Hard,
    Soft,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pos {
    pub x: f32,
    pub y: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    #[serde(default)]
    pub container: Option<String>,
    pub pos: Pos,
    #[serde(default)]
    pub grafana: Option<String>,
    #[serde(default)]
    pub faults: Vec<FaultKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub proto: Proto,
    pub dep: Dep,
    #[serde(default)]
    pub query: Option<String>,
}

/// The single description of the demo system shared with the frontend.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Topology {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("cannot read topology: {0}")]
    Io(#[from] std::io::Error),
    #[error("topology is not valid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("duplicate node id {0:?}")]
    DuplicateNode(String),
    #[error("edge {from:?} -> {to:?} references unknown node {unknown:?}")]
    UnknownNode {
        from: String,
        to: String,
        unknown: String,
    },
    #[error("node {0:?} has faults but no container")]
    FaultWithoutContainer(String),
}

impl Topology {
    pub fn load(path: &Path) -> Result<Self, TopologyError> {
        let topology: Topology = serde_json::from_str(&std::fs::read_to_string(path)?)?;
        topology.validate()?;
        Ok(topology)
    }

    pub fn validate(&self) -> Result<(), TopologyError> {
        let mut ids = HashSet::new();
        for node in &self.nodes {
            if !ids.insert(node.id.as_str()) {
                return Err(TopologyError::DuplicateNode(node.id.clone()));
            }
            if !node.faults.is_empty() && node.container.is_none() {
                return Err(TopologyError::FaultWithoutContainer(node.id.clone()));
            }
        }
        for edge in &self.edges {
            for end in [&edge.from, &edge.to] {
                if !ids.contains(end.as_str()) {
                    return Err(TopologyError::UnknownNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        unknown: end.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn container_nodes(&self) -> impl Iterator<Item = (&Node, &str)> {
        self.nodes
            .iter()
            .filter_map(|n| n.container.as_deref().map(|c| (n, c)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_topology() -> Topology {
        Topology::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../topology.json"
        )))
        .unwrap()
    }

    #[test]
    fn repository_topology_is_valid() {
        let t = repo_topology();
        assert!(t.node("kitchen-svc").is_some());
        assert!(t.edges.len() >= 10);
    }

    #[test]
    fn unknown_edge_endpoint_is_rejected() {
        let mut t = repo_topology();
        t.edges.push(Edge {
            from: "users".into(),
            to: "ghost".into(),
            proto: Proto::Http,
            dep: Dep::Hard,
            query: None,
        });
        assert!(matches!(
            t.validate(),
            Err(TopologyError::UnknownNode { .. })
        ));
    }

    #[test]
    fn faults_require_a_container() {
        let mut t = repo_topology();
        t.nodes
            .iter_mut()
            .find(|n| n.id == "users")
            .unwrap()
            .faults
            .push(FaultKind::Stop);
        assert!(matches!(
            t.validate(),
            Err(TopologyError::FaultWithoutContainer(_))
        ));
    }
}
