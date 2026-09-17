//! Shared plumbing for every service in the stack: configuration, telemetry,
//! edge metrics (the data the chaos panel animates) and the ops HTTP server.

pub mod breaker;
pub mod config;
pub mod edge;
pub mod faultflags;
pub mod http;
pub mod telemetry;

pub use breaker::{BreakerState, CircuitBreaker};
pub use edge::Edge;
pub use faultflags::{FaultFlags, Injected, InjectedFailure};
pub use http::{HealthFn, always_healthy, serve_ops, shutdown_signal};
pub use telemetry::init as init_telemetry;
