//! W3C trace-context propagation across the three transports the demo
//! uses. Each carrier gets a tiny adapter; the propagator itself is the
//! global one installed by `telemetry::init`.

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use rdkafka::message::{BorrowedHeaders, Headers, OwnedHeaders};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn current_context() -> Context {
    tracing::Span::current().context()
}

/// Hex trace id of the current span, empty when tracing is off.
pub fn current_trace_id() -> String {
    let cx = current_context();
    let id = cx.span().span_context().trace_id();
    if id == opentelemetry::trace::TraceId::INVALID {
        String::new()
    } else {
        id.to_string()
    }
}

/// Makes `span` a child of whatever the carrier describes. Call it right
/// after creating the server-side span for a request or message.
pub fn adopt_parent(span: &tracing::Span, parent: Context) {
    if let Err(err) = span.set_parent(parent) {
        tracing::debug!(%err, "span has no otel context to adopt");
    }
}

// ---------------------------------------------------------------- HTTP

pub fn inject_http(headers: &mut http::HeaderMap) {
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(
            &current_context(),
            &mut opentelemetry_http::HeaderInjector(headers),
        )
    });
}

pub fn extract_http(headers: &http::HeaderMap) -> Context {
    opentelemetry::global::get_text_map_propagator(|p| {
        p.extract(&opentelemetry_http::HeaderExtractor(headers))
    })
}

// ---------------------------------------------------------------- gRPC

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            MetadataKey::from_bytes(key.as_bytes()),
            MetadataValue::try_from(value),
        ) {
            self.0.insert(k, v);
        }
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => k.as_str(),
                tonic::metadata::KeyRef::Binary(k) => k.as_str(),
            })
            .collect()
    }
}

pub fn inject_grpc(metadata: &mut MetadataMap) {
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&current_context(), &mut MetadataInjector(metadata))
    });
}

pub fn extract_grpc(metadata: &MetadataMap) -> Context {
    opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(metadata)))
}

// ---------------------------------------------------------------- Kafka

struct KafkaInjector(Vec<(String, String)>);

impl Injector for KafkaInjector {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

struct KafkaExtractor<'a>(&'a BorrowedHeaders);

impl Extractor for KafkaExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|h| h.key == key)
            .and_then(|h| h.value)
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|h| h.key).collect()
    }
}

/// Headers carrying the current span for a produced message.
pub fn kafka_headers() -> OwnedHeaders {
    let mut injector = KafkaInjector(Vec::new());
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&current_context(), &mut injector)
    });
    injector
        .0
        .into_iter()
        .fold(OwnedHeaders::new(), |headers, (key, value)| {
            headers.insert(rdkafka::message::Header {
                key: &key,
                value: Some(&value),
            })
        })
}

pub fn extract_kafka(headers: Option<&BorrowedHeaders>) -> Context {
    match headers {
        Some(h) => {
            opentelemetry::global::get_text_map_propagator(|p| p.extract(&KafkaExtractor(h)))
        }
        None => Context::new(),
    }
}

// ---------------------------------------------------------------- servers

/// `MakeSpan` for tower-http: one span per inbound request, parented to
/// the caller's trace when a `traceparent` header is present. gRPC metadata
/// travels as HTTP headers, so the same type serves axum and tonic.
#[derive(Clone, Copy, Debug, Default)]
pub struct PropagatingSpan;

impl<B> tower_http::trace::MakeSpan<B> for PropagatingSpan {
    fn make_span(&mut self, request: &http::Request<B>) -> tracing::Span {
        let span = tracing::info_span!(
            "request",
            method = %request.method(),
            path = %request.uri().path(),
        );
        adopt_parent(&span, extract_http(request.headers()));
        span
    }
}

pub type HttpTraceLayer = tower_http::trace::TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    PropagatingSpan,
>;

pub type GrpcTraceLayer = tower_http::trace::TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::GrpcErrorsAsFailures>,
    PropagatingSpan,
>;

pub fn http_trace_layer() -> HttpTraceLayer {
    tower_http::trace::TraceLayer::new_for_http().make_span_with(PropagatingSpan)
}

pub fn grpc_trace_layer() -> GrpcTraceLayer {
    tower_http::trace::TraceLayer::new_for_grpc().make_span_with(PropagatingSpan)
}
