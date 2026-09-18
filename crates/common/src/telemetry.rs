use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// The provider must outlive every span: dropping the last handle shuts
/// the batch exporter down, so it lives here for the whole process.
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Installs tracing (stdout, plus OTLP spans when `OTLP_ENDPOINT` is set)
/// and the Prometheus recorder. Every metric gets a `service` label so
/// dashboards can filter by instance without each service remembering to
/// add it.
pub fn init(service_id: &str) -> PrometheusHandle {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let fmt = tracing_subscriber::fmt::layer().with_target(false);
    let otel = std::env::var("OTLP_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())
        .and_then(|endpoint| match tracer_provider(service_id, &endpoint) {
            Ok(provider) => {
                let tracer = provider.tracer(service_id.to_string());
                let _ = TRACER_PROVIDER.set(provider);
                Some(tracing_opentelemetry::layer().with_tracer(tracer))
            }
            Err(err) => {
                eprintln!("otlp exporter disabled: {err}");
                None
            }
        });
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
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

/// OTLP over HTTP on purpose: the gRPC exporter would drag in a second
/// tonic version next to the one the services use. The blocking reqwest
/// client is deliberate too: the batch processor exports from its own
/// thread, where an async client has no runtime to run on.
fn tracer_provider(service_id: &str, endpoint: &str) -> anyhow::Result<SdkTracerProvider> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_id.to_string())
                .build(),
        )
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(provider)
}

/// Flushes buffered spans; call once the server has stopped accepting work.
pub fn shutdown() {
    if let Some(provider) = TRACER_PROVIDER.get()
        && let Err(err) = provider.shutdown()
    {
        eprintln!("tracer shutdown: {err}");
    }
}
