use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Installs tracing and the Prometheus recorder. Every metric gets a
/// `service` label so dashboards can filter by instance without each
/// service remembering to add it.
pub fn init(service_id: &str) -> PrometheusHandle {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();

    PrometheusBuilder::new()
        .add_global_label("service", service_id)
        .set_buckets_for_metric(
            Matcher::Full("edge_request_duration_seconds".into()),
            LATENCY_BUCKETS,
        )
        .expect("buckets are non-empty")
        .install_recorder()
        .expect("prometheus recorder installs once per process")
}
