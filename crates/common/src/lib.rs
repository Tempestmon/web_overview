//! Shared plumbing for every service in the stack: configuration, telemetry,
//! edge metrics (the data the chaos panel animates) and the ops HTTP server.

pub mod config;
pub mod edge;
pub mod http;
pub mod telemetry;

pub use edge::Edge;
pub use http::{HealthFn, always_healthy, serve_ops, shutdown_signal};
pub use telemetry::init as init_telemetry;
