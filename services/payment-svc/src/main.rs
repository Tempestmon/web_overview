//! Payment: the synchronous, latency-sensitive dependency of the order
//! service. It keeps idempotency keys in Redis (a hard dependency here, in
//! contrast to the menu cache) and pretends to talk to a slow PSP. It is the
//! only service the panel can poison with injected errors, which is what
//! trips the circuit breaker on the order-service side.

use std::net::SocketAddr;
use std::time::Duration;

use common::{FaultFlags, LazyRedis, config};
use proto::payment::payment_service_server::{PaymentService, PaymentServiceServer};
use proto::payment::{ChargeRequest, ChargeResponse};
use rand::Rng;
use tonic::{Request, Response, Status};

const KEY_TTL_SECONDS: u64 = 3600;

struct PaymentSvc {
    redis: LazyRedis,
    flags: FaultFlags,
    psp_range: (u64, u64),
}

#[tonic::async_trait]
impl PaymentService for PaymentSvc {
    async fn charge(
        &self,
        req: Request<ChargeRequest>,
    ) -> Result<Response<ChargeResponse>, Status> {
        let req = req.into_inner();
        if req.amount_cents <= 0 {
            return Err(Status::invalid_argument("amount must be positive"));
        }
        self.flags
            .apply()
            .await
            .map_err(|_| Status::unavailable("payment provider error"))?;

        let first_charge = self.claim_idempotency_key(&req.order_id).await?;
        if !first_charge {
            metrics::counter!("payments_total", "outcome" => "duplicate").increment(1);
            return Ok(Response::new(ChargeResponse {
                status: "duplicate".into(),
            }));
        }
        tokio::time::sleep(self.psp_latency()).await;
        metrics::counter!("payments_total", "outcome" => "paid").increment(1);
        Ok(Response::new(ChargeResponse {
            status: "paid".into(),
        }))
    }
}

impl PaymentSvc {
    /// `SET NX` is the whole idempotency story: a retry from the order
    /// service after a timeout must not charge twice.
    async fn claim_idempotency_key(&self, order_id: &str) -> Result<bool, Status> {
        let key = format!("pay:{order_id}");
        let claimed: Option<String> = self
            .redis
            .call(|mut c| async move {
                redis::cmd("SET")
                    .arg(&key)
                    .arg("1")
                    .arg("NX")
                    .arg("EX")
                    .arg(KEY_TTL_SECONDS)
                    .query_async(&mut c)
                    .await
            })
            .await
            .map_err(|err| {
                tracing::warn!(%err, "redis unavailable");
                Status::unavailable("idempotency store unavailable")
            })?;
        Ok(claimed.is_some())
    }

    fn psp_latency(&self) -> Duration {
        let (min, max) = self.psp_range;
        Duration::from_millis(rand::rng().random_range(min..=max.max(min)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service_id = config::service_id();
    let metrics = common::init_telemetry(&service_id);
    let grpc_addr: SocketAddr = config::or_default("GRPC_ADDR", "0.0.0.0:50052").parse()?;
    let ops_addr: SocketAddr = config::or_default("OPS_ADDR", "0.0.0.0:8081").parse()?;
    let flags = FaultFlags::new();

    let svc = PaymentSvc {
        redis: LazyRedis::new(&config::required("REDIS_URL"), &service_id)?,
        flags: flags.clone(),
        psp_range: (
            config::parse_or("PSP_MIN_MS", 100),
            config::parse_or("PSP_MAX_MS", 400),
        ),
    };
    let grpc = tonic::transport::Server::builder()
        .layer(common::trace::grpc_trace_layer())
        .add_service(PaymentServiceServer::new(svc))
        .serve_with_shutdown(grpc_addr, common::shutdown_signal());
    tracing::info!(%grpc_addr, "grpc listening");

    tokio::select! {
        r = grpc => r?,
        r = common::serve_ops(ops_addr, metrics, common::always_healthy(), flags, axum::Router::new()) => r?,
    }
    Ok(())
}
