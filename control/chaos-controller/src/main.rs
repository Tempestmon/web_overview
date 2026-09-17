mod api;
mod apply;
mod docker;
mod faults;
mod flow;
mod health;
mod snapshot;
mod state;
mod topology;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::config;
use tokio::sync::{Mutex, RwLock, watch};

use crate::api::AppState;
use crate::docker::{BollardControl, ContainerControl};
use crate::faults::{FaultBook, Limits};
use crate::flow::PromClient;
use crate::snapshot::Snapshot;
use crate::topology::Topology;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::or_default("SERVICE_ID", "chaos-controller");
    let metrics = common::init_telemetry(&service_id);

    let topology = Arc::new(Topology::load(&PathBuf::from(config::or_default(
        "TOPOLOGY_PATH",
        "topology.json",
    )))?);
    let docker: Arc<dyn ContainerControl> = Arc::new(BollardControl::connect(
        &config::or_default("DOCKER_URL", "http://docker-socket-proxy:2375"),
    )?);
    let prom = PromClient::new(config::or_default(
        "PROMETHEUS_URL",
        "http://prometheus:9090",
    ))?;

    health::reconcile(&topology, docker.as_ref()).await;

    let limits = Limits {
        default_ttl: Duration::from_secs(config::parse_or("FAULT_TTL_S", 60)),
        max_ttl: Duration::from_secs(config::parse_or("FAULT_MAX_TTL_S", 300)),
        cooldown: Duration::from_secs(config::parse_or("COOLDOWN_S", 15)),
        per_ip: Duration::from_secs(config::parse_or("IP_LIMIT_S", 120)),
    };
    let observed = Arc::new(RwLock::new(Default::default()));
    let flow = Arc::new(RwLock::new(Default::default()));
    let (tx, rx) = watch::channel(Arc::new(Snapshot::default()));

    let state = AppState {
        topology: topology.clone(),
        book: Arc::new(Mutex::new(FaultBook::new(limits))),
        docker: docker.clone(),
        observed: observed.clone(),
        flow: flow.clone(),
        snapshot: rx,
        admin_token: std::env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty()),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()?,
    };

    tokio::spawn(health::run_monitor(topology.clone(), docker, observed));
    tokio::spawn(flow::run_poller(prom, topology, flow));
    tokio::spawn(api::run_snapshotter(state.clone(), tx));

    let addr: SocketAddr = config::or_default("HTTP_ADDR", "0.0.0.0:8080").parse()?;
    common::serve_ops(
        addr,
        metrics,
        common::always_healthy(),
        common::FaultFlags::new(),
        api::router(state),
    )
    .await
}
