use std::time::Duration;

use common::Edge;
use proto::order::order_service_client::OrderServiceClient;
use proto::order::{CreateOrderRequest, GetMenuRequest, GetOrderRequest, Menu, Order, OrderLine};
use tonic::Code;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, PartialEq, Eq)]
pub enum ApiError {
    /// Upstream said why (e.g. "payment circuit open"); the customer sees it.
    Unavailable(String),
    InvalidArgument(String),
    NotFound,
}

impl From<tonic::Status> for ApiError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            Code::InvalidArgument => ApiError::InvalidArgument(status.message().to_string()),
            Code::NotFound => ApiError::NotFound,
            _ => ApiError::Unavailable(status.message().to_string()),
        }
    }
}

/// The slice of OrderService the HTTP layer needs. A trait so handlers can
/// be tested without a gRPC server.
#[tonic::async_trait]
pub trait OrderApi: Send + Sync {
    async fn menu(&self) -> Result<Menu, ApiError>;
    async fn create(&self, lines: Vec<OrderLine>, client_id: String) -> Result<Order, ApiError>;
    async fn get(&self, id: String) -> Result<Order, ApiError>;
}

#[derive(Clone)]
pub struct GrpcOrderClient {
    channel: Channel,
    edge: Edge,
}

impl GrpcOrderClient {
    pub fn connect_lazy(url: &str, service_id: &str) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(url.to_string())?
            .timeout(Duration::from_secs(4))
            .connect_timeout(Duration::from_secs(1))
            .connect_lazy();
        Ok(Self {
            channel,
            edge: Edge::new(service_id, "order-svc", "grpc"),
        })
    }

    fn client(&self) -> OrderServiceClient<Channel> {
        OrderServiceClient::new(self.channel.clone())
    }
}

#[tonic::async_trait]
impl OrderApi for GrpcOrderClient {
    async fn menu(&self) -> Result<Menu, ApiError> {
        let resp = self
            .edge
            .observe(async {
                let mut request = tonic::Request::new(GetMenuRequest {});
                common::trace::inject_grpc(request.metadata_mut());
                self.client().get_menu(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    async fn create(&self, lines: Vec<OrderLine>, client_id: String) -> Result<Order, ApiError> {
        let resp = self
            .edge
            .observe(async {
                let mut request = tonic::Request::new(CreateOrderRequest { lines, client_id });
                common::trace::inject_grpc(request.metadata_mut());
                self.client().create_order(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    async fn get(&self, id: String) -> Result<Order, ApiError> {
        let resp = self
            .edge
            .observe(async {
                let mut request = tonic::Request::new(GetOrderRequest { id });
                common::trace::inject_grpc(request.metadata_mut());
                self.client().get_order(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }
}
