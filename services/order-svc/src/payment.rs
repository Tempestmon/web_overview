use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{CircuitBreaker, Edge, breaker};
use proto::payment::ChargeRequest;
use proto::payment::payment_service_client::PaymentServiceClient;
use tonic::Code;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum PaymentError {
    #[error("payment circuit open")]
    CircuitOpen,
    #[error("payment unavailable")]
    Unavailable,
    #[error("payment rejected: {0}")]
    Rejected(String),
}

/// gRPC client with the three defensive layers a synchronous dependency
/// needs: a per-call timeout, one retry on transient failure, and a circuit
/// breaker so a sick payment service gets fast failures instead of a queue
/// of waiting orders.
#[derive(Clone)]
pub struct PaymentClient {
    channel: Channel,
    breaker: Arc<CircuitBreaker>,
    edge: Edge,
}

impl PaymentClient {
    pub fn connect_lazy(url: &str, service_id: &str) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(url.to_string())?
            .timeout(Duration::from_millis(1500))
            .connect_timeout(Duration::from_secs(1))
            .connect_lazy();
        let breaker = CircuitBreaker::new(breaker::Config {
            failure_threshold: 5,
            open_for: Duration::from_secs(20),
            name: "payment-svc",
        });
        Ok(Self {
            channel,
            breaker: Arc::new(breaker),
            edge: Edge::new(service_id, "payment-svc", "grpc"),
        })
    }

    pub async fn charge(&self, order_id: Uuid, amount_cents: i64) -> Result<(), PaymentError> {
        let mut last = PaymentError::Unavailable;
        for attempt in 0..2 {
            if !self.breaker.allow(Instant::now()) {
                metrics::counter!("payment_calls_total", "outcome" => "circuit_open").increment(1);
                return Err(PaymentError::CircuitOpen);
            }
            match self.call(order_id, amount_cents).await {
                Ok(()) => {
                    self.breaker.on_success();
                    return Ok(());
                }
                Err(err) => {
                    self.breaker.on_failure(Instant::now());
                    tracing::warn!(%err, attempt, %order_id, "payment attempt failed");
                    let retryable = matches!(err, PaymentError::Unavailable);
                    last = err;
                    if !retryable {
                        break;
                    }
                }
            }
        }
        Err(last)
    }

    async fn call(&self, order_id: Uuid, amount_cents: i64) -> Result<(), PaymentError> {
        let mut client = PaymentServiceClient::new(self.channel.clone());
        let request = ChargeRequest {
            order_id: order_id.to_string(),
            amount_cents,
        };
        let response = self
            .edge
            .observe(client.charge(request))
            .await
            .map_err(|status| match status.code() {
                Code::InvalidArgument | Code::FailedPrecondition => {
                    PaymentError::Rejected(status.message().to_string())
                }
                _ => PaymentError::Unavailable,
            })?;
        tracing::debug!(%order_id, status = %response.into_inner().status, "charged");
        Ok(())
    }
}
