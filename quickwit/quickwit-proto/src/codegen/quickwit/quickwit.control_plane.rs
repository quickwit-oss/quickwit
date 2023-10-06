#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsSubrequest>,
    #[prost(string, repeated, tag = "2")]
    pub unavailable_ingesters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub closed_shards: ::prost::alloc::vec::Vec<u64>,
}
/// TODO: Handle partial failures.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<GetOpenShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOpenShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub open_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait ControlPlaneService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// Creates a new index.
    async fn create_index(
        &mut self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::CreateIndexResponse>;
    /// Deletes an index.
    async fn delete_index(
        &mut self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Adds a source to an index.
    async fn add_source(
        &mut self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Enables or disables a source.
    async fn toggle_source(
        &mut self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Removes a source from an index.
    async fn delete_source(
        &mut self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Returns the list of open shards for one or several sources. If the control plane is not able to find any
    /// for a source, it will pick a pair of leader-follower ingesters and will open a new shard.
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse>;
    /// Closes a list of shards. This RPC is a metastore callback.
    async fn close_shards(
        &mut self,
        request: super::metastore::CloseShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Deletes a list of shards. This RPC is a metastore callback.
    async fn delete_shards(
        &mut self,
        request: super::metastore::DeleteShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
}
dyn_clone::clone_trait_object!(ControlPlaneService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockControlPlaneService {
    fn clone(&self) -> Self {
        MockControlPlaneService::new()
    }
}
#[derive(Debug, Clone)]
pub struct ControlPlaneServiceClient {
    inner: Box<dyn ControlPlaneService>,
}
impl ControlPlaneServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: ControlPlaneService,
    {
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
    ) -> control_plane_service_grpc_server::ControlPlaneServiceGrpcServer<
        ControlPlaneServiceGrpcServerAdapter,
    > {
        let adapter = ControlPlaneServiceGrpcServerAdapter::new(self.clone());
        control_plane_service_grpc_server::ControlPlaneServiceGrpcServer::new(adapter)
    }
    pub fn from_channel<C>(channel: C) -> Self
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        ControlPlaneServiceClient::new(
            ControlPlaneServiceGrpcClientAdapter::new(
                control_plane_service_grpc_client::ControlPlaneServiceGrpcClient::new(
                    channel,
                ),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ControlPlaneServiceMailbox<A>: ControlPlaneService,
    {
        ControlPlaneServiceClient::new(ControlPlaneServiceMailbox::new(mailbox))
    }
    pub fn tower() -> ControlPlaneServiceTowerBlockBuilder {
        ControlPlaneServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockControlPlaneService {
        MockControlPlaneService::new()
    }
}
#[async_trait::async_trait]
impl ControlPlaneService for ControlPlaneServiceClient {
    async fn create_index(
        &mut self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.inner.create_index(request).await
    }
    async fn delete_index(
        &mut self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.delete_index(request).await
    }
    async fn add_source(
        &mut self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.add_source(request).await
    }
    async fn toggle_source(
        &mut self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.toggle_source(request).await
    }
    async fn delete_source(
        &mut self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.delete_source(request).await
    }
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.inner.get_or_create_open_shards(request).await
    }
    async fn close_shards(
        &mut self,
        request: super::metastore::CloseShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.close_shards(request).await
    }
    async fn delete_shards(
        &mut self,
        request: super::metastore::DeleteShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.delete_shards(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod control_plane_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockControlPlaneServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockControlPlaneService>>,
    }
    #[async_trait::async_trait]
    impl ControlPlaneService for MockControlPlaneServiceWrapper {
        async fn create_index(
            &mut self,
            request: super::super::metastore::CreateIndexRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::CreateIndexResponse,
        > {
            self.inner.lock().await.create_index(request).await
        }
        async fn delete_index(
            &mut self,
            request: super::super::metastore::DeleteIndexRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.delete_index(request).await
        }
        async fn add_source(
            &mut self,
            request: super::super::metastore::AddSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.add_source(request).await
        }
        async fn toggle_source(
            &mut self,
            request: super::super::metastore::ToggleSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.toggle_source(request).await
        }
        async fn delete_source(
            &mut self,
            request: super::super::metastore::DeleteSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.delete_source(request).await
        }
        async fn get_or_create_open_shards(
            &mut self,
            request: super::GetOrCreateOpenShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::GetOrCreateOpenShardsResponse,
        > {
            self.inner.lock().await.get_or_create_open_shards(request).await
        }
        async fn close_shards(
            &mut self,
            request: super::super::metastore::CloseShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.close_shards(request).await
        }
        async fn delete_shards(
            &mut self,
            request: super::super::metastore::DeleteShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.delete_shards(request).await
        }
    }
    impl From<MockControlPlaneService> for ControlPlaneServiceClient {
        fn from(mock: MockControlPlaneService) -> Self {
            let mock_wrapper = MockControlPlaneServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            ControlPlaneServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<super::metastore::CreateIndexRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::CreateIndexResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::CreateIndexRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.create_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::DeleteIndexRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::DeleteIndexRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::AddSourceRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::AddSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.add_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::ToggleSourceRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::ToggleSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.toggle_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::DeleteSourceRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::DeleteSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<GetOrCreateOpenShardsRequest> for Box<dyn ControlPlaneService> {
    type Response = GetOrCreateOpenShardsResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GetOrCreateOpenShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.get_or_create_open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::CloseShardsRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::CloseShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.close_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::DeleteShardsRequest>
for Box<dyn ControlPlaneService> {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::DeleteShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_shards(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct ControlPlaneServiceTowerBlock {
    create_index_svc: quickwit_common::tower::BoxService<
        super::metastore::CreateIndexRequest,
        super::metastore::CreateIndexResponse,
        crate::control_plane::ControlPlaneError,
    >,
    delete_index_svc: quickwit_common::tower::BoxService<
        super::metastore::DeleteIndexRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    add_source_svc: quickwit_common::tower::BoxService<
        super::metastore::AddSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    toggle_source_svc: quickwit_common::tower::BoxService<
        super::metastore::ToggleSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    delete_source_svc: quickwit_common::tower::BoxService<
        super::metastore::DeleteSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    get_or_create_open_shards_svc: quickwit_common::tower::BoxService<
        GetOrCreateOpenShardsRequest,
        GetOrCreateOpenShardsResponse,
        crate::control_plane::ControlPlaneError,
    >,
    close_shards_svc: quickwit_common::tower::BoxService<
        super::metastore::CloseShardsRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    delete_shards_svc: quickwit_common::tower::BoxService<
        super::metastore::DeleteShardsRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
}
impl Clone for ControlPlaneServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            create_index_svc: self.create_index_svc.clone(),
            delete_index_svc: self.delete_index_svc.clone(),
            add_source_svc: self.add_source_svc.clone(),
            toggle_source_svc: self.toggle_source_svc.clone(),
            delete_source_svc: self.delete_source_svc.clone(),
            get_or_create_open_shards_svc: self.get_or_create_open_shards_svc.clone(),
            close_shards_svc: self.close_shards_svc.clone(),
            delete_shards_svc: self.delete_shards_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl ControlPlaneService for ControlPlaneServiceTowerBlock {
    async fn create_index(
        &mut self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.create_index_svc.ready().await?.call(request).await
    }
    async fn delete_index(
        &mut self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.delete_index_svc.ready().await?.call(request).await
    }
    async fn add_source(
        &mut self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.add_source_svc.ready().await?.call(request).await
    }
    async fn toggle_source(
        &mut self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.toggle_source_svc.ready().await?.call(request).await
    }
    async fn delete_source(
        &mut self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.delete_source_svc.ready().await?.call(request).await
    }
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.get_or_create_open_shards_svc.ready().await?.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: super::metastore::CloseShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.close_shards_svc.ready().await?.call(request).await
    }
    async fn delete_shards(
        &mut self,
        request: super::metastore::DeleteShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.delete_shards_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct ControlPlaneServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    create_index_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::CreateIndexRequest,
            super::metastore::CreateIndexResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_index_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::DeleteIndexRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    add_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::AddSourceRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    toggle_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::ToggleSourceRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::DeleteSourceRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    get_or_create_open_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            GetOrCreateOpenShardsRequest,
            GetOrCreateOpenShardsResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    close_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::CloseShardsRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            super::metastore::DeleteShardsRequest,
            super::metastore::EmptyResponse,
            crate::control_plane::ControlPlaneError,
        >,
    >,
}
impl ControlPlaneServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::CreateIndexRequest,
                Response = super::metastore::CreateIndexResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::CreateIndexRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteIndexRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteIndexRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::AddSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::AddSourceRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::ToggleSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::ToggleSourceRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteSourceRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::CloseShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::CloseShardsRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteShardsRequest,
        >>::Future: Send + 'static,
    {
        self
            .create_index_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_index_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .add_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .toggle_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .get_or_create_open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .close_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self.delete_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn create_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::CreateIndexRequest,
                Response = super::metastore::CreateIndexResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::CreateIndexRequest,
        >>::Future: Send + 'static,
    {
        self.create_index_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn delete_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteIndexRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteIndexRequest,
        >>::Future: Send + 'static,
    {
        self.delete_index_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn add_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::AddSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::AddSourceRequest,
        >>::Future: Send + 'static,
    {
        self.add_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn toggle_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::ToggleSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::ToggleSourceRequest,
        >>::Future: Send + 'static,
    {
        self.toggle_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn delete_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteSourceRequest,
        >>::Future: Send + 'static,
    {
        self.delete_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn get_or_create_open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
    {
        self
            .get_or_create_open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn close_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::CloseShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::CloseShardsRequest,
        >>::Future: Send + 'static,
    {
        self.close_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn delete_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn ControlPlaneService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteShardsRequest,
        >>::Future: Send + 'static,
    {
        self.delete_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> ControlPlaneServiceClient
    where
        T: ControlPlaneService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T, C>(self, channel: C) -> ControlPlaneServiceClient
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        self.build_from_boxed(Box::new(ControlPlaneServiceClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> ControlPlaneServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ControlPlaneServiceMailbox<A>: ControlPlaneService,
    {
        self.build_from_boxed(Box::new(ControlPlaneServiceClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn ControlPlaneService>,
    ) -> ControlPlaneServiceClient {
        let create_index_svc = if let Some(layer) = self.create_index_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_index_svc = if let Some(layer) = self.delete_index_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let add_source_svc = if let Some(layer) = self.add_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let toggle_source_svc = if let Some(layer) = self.toggle_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_source_svc = if let Some(layer) = self.delete_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let get_or_create_open_shards_svc = if let Some(layer)
            = self.get_or_create_open_shards_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let close_shards_svc = if let Some(layer) = self.close_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_shards_svc = if let Some(layer) = self.delete_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = ControlPlaneServiceTowerBlock {
            create_index_svc,
            delete_index_svc,
            add_source_svc,
            toggle_source_svc,
            delete_source_svc,
            get_or_create_open_shards_svc,
            close_shards_svc,
            delete_shards_svc,
        };
        ControlPlaneServiceClient::new(tower_block)
    }
}
#[derive(Debug, Clone)]
struct MailboxAdapter<A: quickwit_actors::Actor, E> {
    inner: quickwit_actors::Mailbox<A>,
    phantom: std::marker::PhantomData<E>,
}
impl<A, E> std::ops::Deref for MailboxAdapter<A, E>
where
    A: quickwit_actors::Actor,
{
    type Target = quickwit_actors::Mailbox<A>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug)]
pub struct ControlPlaneServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::control_plane::ControlPlaneError>,
}
impl<A: quickwit_actors::Actor> ControlPlaneServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for ControlPlaneServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for ControlPlaneServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::control_plane::ControlPlaneError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
        //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
        //! mailbox in `poll_ready` and consume it in `call`.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, message: M) -> Self::Future {
        let mailbox = self.inner.clone();
        let fut = async move {
            mailbox.ask_for_res(message).await.map_err(|error| error.into())
        };
        Box::pin(fut)
    }
}
#[async_trait::async_trait]
impl<A> ControlPlaneService for ControlPlaneServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    ControlPlaneServiceMailbox<
        A,
    >: tower::Service<
            super::metastore::CreateIndexRequest,
            Response = super::metastore::CreateIndexResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::CreateIndexResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::DeleteIndexRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::AddSourceRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::ToggleSourceRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::DeleteSourceRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            GetOrCreateOpenShardsRequest,
            Response = GetOrCreateOpenShardsResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                GetOrCreateOpenShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::CloseShardsRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::DeleteShardsRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >,
{
    async fn create_index(
        &mut self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.call(request).await
    }
    async fn delete_index(
        &mut self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
    async fn add_source(
        &mut self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
    async fn toggle_source(
        &mut self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
    async fn delete_source(
        &mut self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: super::metastore::CloseShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
    async fn delete_shards(
        &mut self,
        request: super::metastore::DeleteShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct ControlPlaneServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> ControlPlaneServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> ControlPlaneService
for ControlPlaneServiceGrpcClientAdapter<
    control_plane_service_grpc_client::ControlPlaneServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn create_index(
        &mut self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.inner
            .create_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_index(
        &mut self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .delete_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn add_source(
        &mut self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .add_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn toggle_source(
        &mut self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .toggle_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_source(
        &mut self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .delete_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn get_or_create_open_shards(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.inner
            .get_or_create_open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &mut self,
        request: super::metastore::CloseShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .close_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_shards(
        &mut self,
        request: super::metastore::DeleteShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .delete_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct ControlPlaneServiceGrpcServerAdapter {
    inner: Box<dyn ControlPlaneService>,
}
impl ControlPlaneServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: ControlPlaneService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl control_plane_service_grpc_server::ControlPlaneServiceGrpc
for ControlPlaneServiceGrpcServerAdapter {
    async fn create_index(
        &self,
        request: tonic::Request<super::metastore::CreateIndexRequest>,
    ) -> Result<tonic::Response<super::metastore::CreateIndexResponse>, tonic::Status> {
        self.inner
            .clone()
            .create_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_index(
        &self,
        request: tonic::Request<super::metastore::DeleteIndexRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn add_source(
        &self,
        request: tonic::Request<super::metastore::AddSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .add_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn toggle_source(
        &self,
        request: tonic::Request<super::metastore::ToggleSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .toggle_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_source(
        &self,
        request: tonic::Request<super::metastore::DeleteSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn get_or_create_open_shards(
        &self,
        request: tonic::Request<GetOrCreateOpenShardsRequest>,
    ) -> Result<tonic::Response<GetOrCreateOpenShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .get_or_create_open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &self,
        request: tonic::Request<super::metastore::CloseShardsRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .close_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_shards(
        &self,
        request: tonic::Request<super::metastore::DeleteShardsRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod control_plane_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ControlPlaneServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ControlPlaneServiceGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ControlPlaneServiceGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ControlPlaneServiceGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ControlPlaneServiceGrpcClient::new(
                InterceptedService::new(inner, interceptor),
            )
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Creates a new index.
        pub async fn create_index(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::CreateIndexResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/CreateIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "CreateIndex",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes an index.
        pub async fn delete_index(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::DeleteIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/DeleteIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "DeleteIndex",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Adds a source to an index.
        pub async fn add_source(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::AddSourceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/AddSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "AddSource",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Enables or disables a source.
        pub async fn toggle_source(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::metastore::ToggleSourceRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/ToggleSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "ToggleSource",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Removes a source from an index.
        pub async fn delete_source(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::metastore::DeleteSourceRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/DeleteSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "DeleteSource",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Returns the list of open shards for one or several sources. If the control plane is not able to find any
        /// for a source, it will pick a pair of leader-follower ingesters and will open a new shard.
        pub async fn get_or_create_open_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOrCreateOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOrCreateOpenShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/GetOrCreateOpenShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "GetOrCreateOpenShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Closes a list of shards. This RPC is a metastore callback.
        pub async fn close_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/CloseShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "CloseShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes a list of shards. This RPC is a metastore callback.
        pub async fn delete_shards(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::metastore::DeleteShardsRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.control_plane.ControlPlaneService/DeleteShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "DeleteShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod control_plane_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ControlPlaneServiceGrpcServer.
    #[async_trait]
    pub trait ControlPlaneServiceGrpc: Send + Sync + 'static {
        /// Creates a new index.
        async fn create_index(
            &self,
            request: tonic::Request<super::super::metastore::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::CreateIndexResponse>,
            tonic::Status,
        >;
        /// Deletes an index.
        async fn delete_index(
            &self,
            request: tonic::Request<super::super::metastore::DeleteIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
        /// Adds a source to an index.
        async fn add_source(
            &self,
            request: tonic::Request<super::super::metastore::AddSourceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
        /// Enables or disables a source.
        async fn toggle_source(
            &self,
            request: tonic::Request<super::super::metastore::ToggleSourceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
        /// Removes a source from an index.
        async fn delete_source(
            &self,
            request: tonic::Request<super::super::metastore::DeleteSourceRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
        /// Returns the list of open shards for one or several sources. If the control plane is not able to find any
        /// for a source, it will pick a pair of leader-follower ingesters and will open a new shard.
        async fn get_or_create_open_shards(
            &self,
            request: tonic::Request<super::GetOrCreateOpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOrCreateOpenShardsResponse>,
            tonic::Status,
        >;
        /// Closes a list of shards. This RPC is a metastore callback.
        async fn close_shards(
            &self,
            request: tonic::Request<super::super::metastore::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
        /// Deletes a list of shards. This RPC is a metastore callback.
        async fn delete_shards(
            &self,
            request: tonic::Request<super::super::metastore::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::EmptyResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ControlPlaneServiceGrpcServer<T: ControlPlaneServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ControlPlaneServiceGrpc> ControlPlaneServiceGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for ControlPlaneServiceGrpcServer<T>
    where
        T: ControlPlaneServiceGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit.control_plane.ControlPlaneService/CreateIndex" => {
                    #[allow(non_camel_case_types)]
                    struct CreateIndexSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::CreateIndexRequest,
                    > for CreateIndexSvc<T> {
                        type Response = super::super::metastore::CreateIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::CreateIndexRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).create_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/DeleteIndex" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteIndexSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::DeleteIndexRequest,
                    > for DeleteIndexSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::DeleteIndexRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/AddSource" => {
                    #[allow(non_camel_case_types)]
                    struct AddSourceSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::AddSourceRequest,
                    > for AddSourceSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::AddSourceRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).add_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/ToggleSource" => {
                    #[allow(non_camel_case_types)]
                    struct ToggleSourceSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::ToggleSourceRequest,
                    > for ToggleSourceSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::ToggleSourceRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).toggle_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ToggleSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/DeleteSource" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSourceSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::DeleteSourceRequest,
                    > for DeleteSourceSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::DeleteSourceRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/GetOrCreateOpenShards" => {
                    #[allow(non_camel_case_types)]
                    struct GetOrCreateOpenShardsSvc<T: ControlPlaneServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<super::GetOrCreateOpenShardsRequest>
                    for GetOrCreateOpenShardsSvc<T> {
                        type Response = super::GetOrCreateOpenShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetOrCreateOpenShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_or_create_open_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetOrCreateOpenShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/CloseShards" => {
                    #[allow(non_camel_case_types)]
                    struct CloseShardsSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::CloseShardsRequest,
                    > for CloseShardsSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::CloseShardsRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).close_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CloseShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.control_plane.ControlPlaneService/DeleteShards" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardsSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::DeleteShardsRequest,
                    > for DeleteShardsSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::DeleteShardsRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ControlPlaneServiceGrpc> Clone for ControlPlaneServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ControlPlaneServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ControlPlaneServiceGrpc> tonic::server::NamedService
    for ControlPlaneServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.control_plane.ControlPlaneService";
    }
}
