use std::collections::{HashMap, HashSet, VecDeque};

use serde::Serialize;

use crate::topology::{Dep, NodeKind, Proto, Topology};

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    Healthy,
    Broken,
    Degraded,
    Recovering,
}

/// Who depends on whom, derived from data-flow edges:
/// request/response protocols make the caller depend on the callee, while
/// Kafka makes both producer and consumer depend on the broker only.
pub fn dependencies(t: &Topology) -> HashMap<&str, Vec<(&str, Dep)>> {
    let mut deps: HashMap<&str, Vec<(&str, Dep)>> = HashMap::new();
    for edge in &t.edges {
        if let Some((dependent, provider)) = dependent_of(t, edge) {
            deps.entry(dependent)
                .or_default()
                .push((provider, edge.dep));
        }
    }
    deps
}

/// `(dependent, provider)` for an edge, or `None` when the edge carries no
/// dependency (actors depend on nothing; a kafka edge without a broker end).
fn dependent_of<'a>(
    t: &'a Topology,
    edge: &'a crate::topology::Edge,
) -> Option<(&'a str, &'a str)> {
    let pair = match edge.proto {
        Proto::Http | Proto::Grpc | Proto::Sql | Proto::Redis => {
            (edge.from.as_str(), edge.to.as_str())
        }
        Proto::Kafka => match (is_broker(t, &edge.from), is_broker(t, &edge.to)) {
            (true, false) => (edge.to.as_str(), edge.from.as_str()),
            (false, true) => (edge.from.as_str(), edge.to.as_str()),
            _ => return None,
        },
    };
    if t.node(pair.0).is_some_and(|n| n.kind == NodeKind::Actor) {
        return None;
    }
    Some(pair)
}

fn is_broker(t: &Topology, id: &str) -> bool {
    t.node(id).is_some_and(|n| n.kind == NodeKind::Broker)
}

/// Paints the scheme. `down` nodes are those whose own container is not
/// serving (broken or still recovering); everything that transitively needs
/// them through hard edges is degraded, soft edges degrade one hop only.
/// `failing` edges come from live metrics: their dependent node is
/// degraded as if its provider were down, so a slow dependency shows up
/// on the scheme without anyone stopping a container.
pub fn derive_states(
    t: &Topology,
    broken: &HashSet<String>,
    recovering: &HashSet<String>,
    failing: &[(String, String)],
) -> HashMap<String, NodeState> {
    let mut states: HashMap<String, NodeState> = t
        .nodes
        .iter()
        .map(|n| {
            let s = if broken.contains(&n.id) {
                NodeState::Broken
            } else if recovering.contains(&n.id) {
                NodeState::Recovering
            } else {
                NodeState::Healthy
            };
            (n.id.clone(), s)
        })
        .collect();

    let mut dependents: HashMap<&str, Vec<(&str, Dep)>> = HashMap::new();
    for (dependent, providers) in dependencies(t) {
        for (provider, dep) in providers {
            dependents
                .entry(provider)
                .or_default()
                .push((dependent, dep));
        }
    }

    let mut queue: VecDeque<&str> = broken
        .iter()
        .chain(recovering)
        .map(String::as_str)
        .collect();
    for edge in t
        .edges
        .iter()
        .filter(|e| failing.iter().any(|(f, to)| *f == e.from && *to == e.to))
    {
        let Some((dependent, _)) = dependent_of(t, edge) else {
            continue;
        };
        if states.get(dependent) != Some(&NodeState::Healthy) {
            continue;
        }
        states.insert(dependent.to_string(), NodeState::Degraded);
        if edge.dep == Dep::Hard {
            queue.push_back(dependent);
        }
    }
    while let Some(provider) = queue.pop_front() {
        for (dependent, dep) in dependents.get(provider).into_iter().flatten() {
            if states.get(*dependent) != Some(&NodeState::Healthy) {
                continue;
            }
            states.insert(dependent.to_string(), NodeState::Degraded);
            if *dep == Dep::Hard {
                queue.push_back(dependent);
            }
        }
    }
    states
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn topology() -> Topology {
        Topology::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../topology.json"
        )))
        .unwrap()
    }

    fn states(broken: &[&str]) -> HashMap<String, NodeState> {
        let broken: HashSet<String> = broken.iter().map(|s| s.to_string()).collect();
        derive_states(&topology(), &broken, &HashSet::new(), &[])
    }

    fn edge(a: &str, b: &str) -> (String, String) {
        (a.to_string(), b.to_string())
    }

    #[test]
    fn failing_hard_edge_degrades_caller_and_propagates() {
        let s = derive_states(
            &topology(),
            &HashSet::new(),
            &HashSet::new(),
            &[edge("gateway-1", "order-svc")],
        );
        assert_eq!(s["gateway-1"], NodeState::Degraded);
        assert_eq!(s["traefik"], NodeState::Degraded);
        assert_eq!(s["order-svc"], NodeState::Healthy);
        assert_eq!(s["gateway-2"], NodeState::Healthy);
    }

    #[test]
    fn failing_soft_edge_degrades_one_hop_only() {
        let s = derive_states(
            &topology(),
            &HashSet::new(),
            &HashSet::new(),
            &[edge("order-svc", "redis")],
        );
        assert_eq!(s["order-svc"], NodeState::Degraded);
        assert_eq!(s["gateway-1"], NodeState::Healthy);
        assert_eq!(s["redis"], NodeState::Healthy);
    }

    #[test]
    fn failing_kafka_edge_degrades_the_service_end() {
        let s = derive_states(
            &topology(),
            &HashSet::new(),
            &HashSet::new(),
            &[edge("kafka", "kitchen-svc")],
        );
        assert_eq!(s["kitchen-svc"], NodeState::Degraded);
        assert_eq!(s["kafka"], NodeState::Healthy);
    }

    #[test]
    fn broken_consumer_does_not_hurt_producers() {
        let s = states(&["kitchen-svc"]);
        assert_eq!(s["kitchen-svc"], NodeState::Broken);
        assert_eq!(s["kafka"], NodeState::Healthy);
        assert_eq!(s["order-svc"], NodeState::Healthy);
        assert_eq!(s["gateway-1"], NodeState::Healthy);
    }

    #[test]
    fn broken_broker_degrades_both_sides() {
        let s = states(&["kafka"]);
        assert_eq!(s["kitchen-svc"], NodeState::Degraded);
        assert_eq!(s["order-svc"], NodeState::Degraded);
        assert_eq!(s["gateway-1"], NodeState::Degraded);
        assert_eq!(s["gateway-2"], NodeState::Degraded);
        assert_eq!(s["traefik"], NodeState::Degraded);
    }

    #[test]
    fn soft_dependency_degrades_one_hop_only() {
        let s = states(&["redis"]);
        assert_eq!(s["order-svc"], NodeState::Degraded);
        assert_eq!(s["gateway-1"], NodeState::Healthy);
        assert_eq!(s["gateway-2"], NodeState::Healthy);
    }

    #[test]
    fn broken_db_cascades_to_the_edge() {
        let s = states(&["postgres"]);
        assert_eq!(s["order-svc"], NodeState::Degraded);
        assert_eq!(s["gateway-1"], NodeState::Degraded);
        assert_eq!(s["traefik"], NodeState::Degraded);
        assert_eq!(s["users"], NodeState::Healthy);
        assert_eq!(s["kitchen-svc"], NodeState::Healthy);
    }

    #[test]
    fn one_dead_replica_keeps_lb_degraded_not_broken() {
        let s = states(&["gateway-1"]);
        assert_eq!(s["gateway-1"], NodeState::Broken);
        assert_eq!(s["traefik"], NodeState::Degraded);
        assert_eq!(s["gateway-2"], NodeState::Healthy);
    }

    #[test]
    fn recovering_node_propagates_like_broken() {
        let recovering: HashSet<String> = ["postgres".to_string()].into();
        let s = derive_states(&topology(), &HashSet::new(), &recovering, &[]);
        assert_eq!(s["postgres"], NodeState::Recovering);
        assert_eq!(s["order-svc"], NodeState::Degraded);
    }
}
