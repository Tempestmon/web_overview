use std::future::Future;

use std::time::{Duration, Instant};
use tracing::Instrument;

/// One directed call path between two components of the demo system.
///
/// The chaos panel draws traffic per edge, so every remote call a service
/// makes (and every inbound request it serves) should go through `observe`
/// or `record`. Labels match node ids in `topology.json`.
#[derive(Clone, Debug)]
pub struct Edge {
    pub src: String,
    pub dst: String,
    pub proto: &'static str,
}

impl Edge {
    pub fn new(src: impl Into<String>, dst: impl Into<String>, proto: &'static str) -> Self {
        Self {
            src: src.into(),
            dst: dst.into(),
            proto,
        }
    }

    /// Times the call, counts it, and wraps it in a span so every hop of a
    /// request shows up in the trace without per-call-site instrumentation.
    pub async fn observe<T, E, F>(&self, call: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let span =
            tracing::info_span!("edge", src = %self.src, dst = %self.dst, proto = self.proto);
        let started = Instant::now();
        let result = call.instrument(span).await;
        self.record(result.is_ok(), started.elapsed());
        result
    }

    pub fn record(&self, ok: bool, elapsed: Duration) {
        let outcome = if ok { "ok" } else { "error" };
        metrics::counter!(
            "edge_requests_total",
            "src" => self.src.clone(),
            "dst" => self.dst.clone(),
            "proto" => self.proto,
            "outcome" => outcome,
        )
        .increment(1);
        metrics::histogram!(
            "edge_request_duration_seconds",
            "src" => self.src.clone(),
            "dst" => self.dst.clone(),
            "proto" => self.proto,
        )
        .record(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    type SnapshotEntry = (
        CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    fn counter_value(entries: &[SnapshotEntry], outcome: &str) -> Option<u64> {
        entries
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "edge_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "outcome" && l.value() == outcome)
            })
            .and_then(|(_, _, _, value)| match value {
                DebugValue::Counter(v) => Some(*v),
                _ => None,
            })
    }

    #[test]
    fn observe_counts_ok_and_error_separately() {
        let recorder = DebuggingRecorder::new();
        let edge = Edge::new("a", "b", "grpc");
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let _ = edge.observe(async { Ok::<_, ()>(1) }).await;
                let _ = edge.observe(async { Err::<(), _>(()) }).await;
                let _ = edge.observe(async { Err::<(), _>(()) }).await;
            });
        });
        // A snapshot drains the recorder, so take exactly one.
        let entries = recorder.snapshotter().snapshot().into_vec();
        assert_eq!(counter_value(&entries, "ok"), Some(1));
        assert_eq!(counter_value(&entries, "error"), Some(2));
    }
}
