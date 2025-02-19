#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsSubrequest>,
    #[prost(message, repeated, tag = "2")]
    pub closed_shards: ::prost::alloc::vec::Vec<super::ingest::ShardIds>,
    /// The control plane should return shards that are not present on the supplied leaders.
    ///
    /// The control plane does not change the status of those leaders just from this signal.
    /// It will check the status of its own ingester pool.
    #[prost(string, repeated, tag = "3")]
    pub unavailable_leaders: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsSubrequest {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub successes: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsSuccess>,
    #[prost(message, repeated, tag = "2")]
    pub failures: ::prost::alloc::vec::Vec<GetOrCreateOpenShardsFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsSuccess {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub open_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrCreateOpenShardsFailure {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "GetOrCreateOpenShardsFailureReason", tag = "4")]
    pub reason: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdviseResetShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub shard_ids: ::prost::alloc::vec::Vec<super::ingest::ShardIds>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdviseResetShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub shards_to_delete: ::prost::alloc::vec::Vec<super::ingest::ShardIds>,
    #[prost(message, repeated, tag = "2")]
    pub shards_to_truncate: ::prost::alloc::vec::Vec<super::ingest::ShardIdPositions>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum GetOrCreateOpenShardsFailureReason {
    Unspecified = 0,
    IndexNotFound = 1,
    SourceNotFound = 2,
    NoIngestersAvailable = 3,
}
impl GetOrCreateOpenShardsFailureReason {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            GetOrCreateOpenShardsFailureReason::Unspecified => {
                "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_UNSPECIFIED"
            }
            GetOrCreateOpenShardsFailureReason::IndexNotFound => {
                "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_INDEX_NOT_FOUND"
            }
            GetOrCreateOpenShardsFailureReason::SourceNotFound => {
                "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_SOURCE_NOT_FOUND"
            }
            GetOrCreateOpenShardsFailureReason::NoIngestersAvailable => {
                "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_NO_INGESTERS_AVAILABLE"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_UNSPECIFIED" => {
                Some(Self::Unspecified)
            }
            "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_INDEX_NOT_FOUND" => {
                Some(Self::IndexNotFound)
            }
            "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_SOURCE_NOT_FOUND" => {
                Some(Self::SourceNotFound)
            }
            "GET_OR_CREATE_OPEN_SHARDS_FAILURE_REASON_NO_INGESTERS_AVAILABLE" => {
                Some(Self::NoIngestersAvailable)
            }
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait ControlPlaneService: std::fmt::Debug + Send + Sync + 'static {
    /// Creates a new index.
    async fn create_index(
        &self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::CreateIndexResponse>;
    /// Updates an index.
    async fn update_index(
        &self,
        request: super::metastore::UpdateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::IndexMetadataResponse,
    >;
    /// Deletes an index.
    async fn delete_index(
        &self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Adds a source to an index.
    async fn add_source(
        &self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Update a source.
    async fn update_source(
        &self,
        request: super::metastore::UpdateSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Enables or disables a source.
    async fn toggle_source(
        &self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Removes a source from an index.
    async fn delete_source(
        &self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
    /// Returns the list of open shards for one or several sources. If the control plane is not able to find any
    /// for a source, it will pick a pair of leader-follower ingesters and will open a new shard.
    async fn get_or_create_open_shards(
        &self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse>;
    /// Asks the control plane whether the shards listed in the request should be deleted or truncated.
    async fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<AdviseResetShardsResponse>;
    /// Performs a debounced shard pruning request to the metastore.
    async fn prune_shards(
        &self,
        request: super::metastore::PruneShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse>;
}
#[derive(Debug, Clone)]
pub struct ControlPlaneServiceClient {
    inner: InnerControlPlaneServiceClient,
}
#[derive(Debug, Clone)]
struct InnerControlPlaneServiceClient(std::sync::Arc<dyn ControlPlaneService>);
impl ControlPlaneServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: ControlPlaneService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockControlPlaneService > (),
            "`MockControlPlaneService` must be wrapped in a `MockControlPlaneServiceWrapper`: use `ControlPlaneServiceClient::from_mock(mock)` to instantiate the client"
        );
        Self {
            inner: InnerControlPlaneServiceClient(std::sync::Arc::new(instance)),
        }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> control_plane_service_grpc_server::ControlPlaneServiceGrpcServer<
        ControlPlaneServiceGrpcServerAdapter,
    > {
        let adapter = ControlPlaneServiceGrpcServerAdapter::new(self.clone());
        control_plane_service_grpc_server::ControlPlaneServiceGrpcServer::new(adapter)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }
    pub fn from_channel(
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> Self {
        let (_, connection_keys_watcher) = tokio::sync::watch::channel(
            std::collections::HashSet::from_iter([addr]),
        );
        let client = control_plane_service_grpc_client::ControlPlaneServiceGrpcClient::new(
                channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = ControlPlaneServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> ControlPlaneServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = control_plane_service_grpc_client::ControlPlaneServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = ControlPlaneServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ControlPlaneServiceMailbox<A>: ControlPlaneService,
    {
        ControlPlaneServiceClient::new(ControlPlaneServiceMailbox::new(mailbox))
    }
    pub fn tower() -> ControlPlaneServiceTowerLayerStack {
        ControlPlaneServiceTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockControlPlaneService) -> Self {
        let mock_wrapper = mock_control_plane_service::MockControlPlaneServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        };
        Self::new(mock_wrapper)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockControlPlaneService::new())
    }
}
#[async_trait::async_trait]
impl ControlPlaneService for ControlPlaneServiceClient {
    async fn create_index(
        &self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.inner.0.create_index(request).await
    }
    async fn update_index(
        &self,
        request: super::metastore::UpdateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::IndexMetadataResponse,
    > {
        self.inner.0.update_index(request).await
    }
    async fn delete_index(
        &self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.delete_index(request).await
    }
    async fn add_source(
        &self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.add_source(request).await
    }
    async fn update_source(
        &self,
        request: super::metastore::UpdateSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.update_source(request).await
    }
    async fn toggle_source(
        &self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.toggle_source(request).await
    }
    async fn delete_source(
        &self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.delete_source(request).await
    }
    async fn get_or_create_open_shards(
        &self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.inner.0.get_or_create_open_shards(request).await
    }
    async fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<AdviseResetShardsResponse> {
        self.inner.0.advise_reset_shards(request).await
    }
    async fn prune_shards(
        &self,
        request: super::metastore::PruneShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner.0.prune_shards(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock_control_plane_service {
    use super::*;
    #[derive(Debug)]
    pub struct MockControlPlaneServiceWrapper {
        pub(super) inner: tokio::sync::Mutex<MockControlPlaneService>,
    }
    #[async_trait::async_trait]
    impl ControlPlaneService for MockControlPlaneServiceWrapper {
        async fn create_index(
            &self,
            request: super::super::metastore::CreateIndexRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::CreateIndexResponse,
        > {
            self.inner.lock().await.create_index(request).await
        }
        async fn update_index(
            &self,
            request: super::super::metastore::UpdateIndexRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::IndexMetadataResponse,
        > {
            self.inner.lock().await.update_index(request).await
        }
        async fn delete_index(
            &self,
            request: super::super::metastore::DeleteIndexRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.delete_index(request).await
        }
        async fn add_source(
            &self,
            request: super::super::metastore::AddSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.add_source(request).await
        }
        async fn update_source(
            &self,
            request: super::super::metastore::UpdateSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.update_source(request).await
        }
        async fn toggle_source(
            &self,
            request: super::super::metastore::ToggleSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.toggle_source(request).await
        }
        async fn delete_source(
            &self,
            request: super::super::metastore::DeleteSourceRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.delete_source(request).await
        }
        async fn get_or_create_open_shards(
            &self,
            request: super::GetOrCreateOpenShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::GetOrCreateOpenShardsResponse,
        > {
            self.inner.lock().await.get_or_create_open_shards(request).await
        }
        async fn advise_reset_shards(
            &self,
            request: super::AdviseResetShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<super::AdviseResetShardsResponse> {
            self.inner.lock().await.advise_reset_shards(request).await
        }
        async fn prune_shards(
            &self,
            request: super::super::metastore::PruneShardsRequest,
        ) -> crate::control_plane::ControlPlaneResult<
            super::super::metastore::EmptyResponse,
        > {
            self.inner.lock().await.prune_shards(request).await
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<super::metastore::CreateIndexRequest>
for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.create_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::UpdateIndexRequest>
for InnerControlPlaneServiceClient {
    type Response = super::metastore::IndexMetadataResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::UpdateIndexRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.update_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::DeleteIndexRequest>
for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.delete_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::AddSourceRequest>
for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.add_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::UpdateSourceRequest>
for InnerControlPlaneServiceClient {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::UpdateSourceRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.update_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::ToggleSourceRequest>
for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.toggle_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::DeleteSourceRequest>
for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.delete_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<GetOrCreateOpenShardsRequest> for InnerControlPlaneServiceClient {
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
        let svc = self.clone();
        let fut = async move { svc.0.get_or_create_open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AdviseResetShardsRequest> for InnerControlPlaneServiceClient {
    type Response = AdviseResetShardsResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AdviseResetShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.advise_reset_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<super::metastore::PruneShardsRequest>
for InnerControlPlaneServiceClient {
    type Response = super::metastore::EmptyResponse;
    type Error = crate::control_plane::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: super::metastore::PruneShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.prune_shards(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct ControlPlaneServiceTowerServiceStack {
    #[allow(dead_code)]
    inner: InnerControlPlaneServiceClient,
    create_index_svc: quickwit_common::tower::BoxService<
        super::metastore::CreateIndexRequest,
        super::metastore::CreateIndexResponse,
        crate::control_plane::ControlPlaneError,
    >,
    update_index_svc: quickwit_common::tower::BoxService<
        super::metastore::UpdateIndexRequest,
        super::metastore::IndexMetadataResponse,
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
    update_source_svc: quickwit_common::tower::BoxService<
        super::metastore::UpdateSourceRequest,
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
    advise_reset_shards_svc: quickwit_common::tower::BoxService<
        AdviseResetShardsRequest,
        AdviseResetShardsResponse,
        crate::control_plane::ControlPlaneError,
    >,
    prune_shards_svc: quickwit_common::tower::BoxService<
        super::metastore::PruneShardsRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
}
#[async_trait::async_trait]
impl ControlPlaneService for ControlPlaneServiceTowerServiceStack {
    async fn create_index(
        &self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.create_index_svc.clone().ready().await?.call(request).await
    }
    async fn update_index(
        &self,
        request: super::metastore::UpdateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::IndexMetadataResponse,
    > {
        self.update_index_svc.clone().ready().await?.call(request).await
    }
    async fn delete_index(
        &self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.delete_index_svc.clone().ready().await?.call(request).await
    }
    async fn add_source(
        &self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.add_source_svc.clone().ready().await?.call(request).await
    }
    async fn update_source(
        &self,
        request: super::metastore::UpdateSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.update_source_svc.clone().ready().await?.call(request).await
    }
    async fn toggle_source(
        &self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.toggle_source_svc.clone().ready().await?.call(request).await
    }
    async fn delete_source(
        &self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.delete_source_svc.clone().ready().await?.call(request).await
    }
    async fn get_or_create_open_shards(
        &self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.get_or_create_open_shards_svc.clone().ready().await?.call(request).await
    }
    async fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<AdviseResetShardsResponse> {
        self.advise_reset_shards_svc.clone().ready().await?.call(request).await
    }
    async fn prune_shards(
        &self,
        request: super::metastore::PruneShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.prune_shards_svc.clone().ready().await?.call(request).await
    }
}
type CreateIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::CreateIndexRequest,
        super::metastore::CreateIndexResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::CreateIndexRequest,
    super::metastore::CreateIndexResponse,
    crate::control_plane::ControlPlaneError,
>;
type UpdateIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::UpdateIndexRequest,
        super::metastore::IndexMetadataResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::UpdateIndexRequest,
    super::metastore::IndexMetadataResponse,
    crate::control_plane::ControlPlaneError,
>;
type DeleteIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::DeleteIndexRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::DeleteIndexRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
type AddSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::AddSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::AddSourceRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
type UpdateSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::UpdateSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::UpdateSourceRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
type ToggleSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::ToggleSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::ToggleSourceRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
type DeleteSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::DeleteSourceRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::DeleteSourceRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
type GetOrCreateOpenShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        GetOrCreateOpenShardsRequest,
        GetOrCreateOpenShardsResponse,
        crate::control_plane::ControlPlaneError,
    >,
    GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsResponse,
    crate::control_plane::ControlPlaneError,
>;
type AdviseResetShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        AdviseResetShardsRequest,
        AdviseResetShardsResponse,
        crate::control_plane::ControlPlaneError,
    >,
    AdviseResetShardsRequest,
    AdviseResetShardsResponse,
    crate::control_plane::ControlPlaneError,
>;
type PruneShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        super::metastore::PruneShardsRequest,
        super::metastore::EmptyResponse,
        crate::control_plane::ControlPlaneError,
    >,
    super::metastore::PruneShardsRequest,
    super::metastore::EmptyResponse,
    crate::control_plane::ControlPlaneError,
>;
#[derive(Debug, Default)]
pub struct ControlPlaneServiceTowerLayerStack {
    create_index_layers: Vec<CreateIndexLayer>,
    update_index_layers: Vec<UpdateIndexLayer>,
    delete_index_layers: Vec<DeleteIndexLayer>,
    add_source_layers: Vec<AddSourceLayer>,
    update_source_layers: Vec<UpdateSourceLayer>,
    toggle_source_layers: Vec<ToggleSourceLayer>,
    delete_source_layers: Vec<DeleteSourceLayer>,
    get_or_create_open_shards_layers: Vec<GetOrCreateOpenShardsLayer>,
    advise_reset_shards_layers: Vec<AdviseResetShardsLayer>,
    prune_shards_layers: Vec<PruneShardsLayer>,
}
impl ControlPlaneServiceTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::CreateIndexRequest,
                    super::metastore::CreateIndexResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::CreateIndexRequest,
                super::metastore::CreateIndexResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::CreateIndexRequest,
                Response = super::metastore::CreateIndexResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::CreateIndexRequest,
                super::metastore::CreateIndexResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::CreateIndexRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::UpdateIndexRequest,
                    super::metastore::IndexMetadataResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::UpdateIndexRequest,
                super::metastore::IndexMetadataResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::UpdateIndexRequest,
                Response = super::metastore::IndexMetadataResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::UpdateIndexRequest,
                super::metastore::IndexMetadataResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::UpdateIndexRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::DeleteIndexRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::DeleteIndexRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::DeleteIndexRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::DeleteIndexRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::DeleteIndexRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::AddSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::AddSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::AddSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::AddSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::AddSourceRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::UpdateSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::UpdateSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::UpdateSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::UpdateSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::UpdateSourceRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::ToggleSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::ToggleSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::ToggleSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::ToggleSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::ToggleSourceRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::DeleteSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::DeleteSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::DeleteSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::DeleteSourceRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::DeleteSourceRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetOrCreateOpenShardsRequest,
                    GetOrCreateOpenShardsResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetOrCreateOpenShardsRequest,
                GetOrCreateOpenShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetOrCreateOpenShardsRequest,
                GetOrCreateOpenShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AdviseResetShardsRequest,
                    AdviseResetShardsResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                AdviseResetShardsRequest,
                AdviseResetShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                AdviseResetShardsRequest,
                Response = AdviseResetShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                AdviseResetShardsRequest,
                AdviseResetShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<AdviseResetShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::PruneShardsRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::PruneShardsRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service: tower::Service<
                super::metastore::PruneShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                super::metastore::PruneShardsRequest,
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >>::Service as tower::Service<
            super::metastore::PruneShardsRequest,
        >>::Future: Send + 'static,
    {
        self.create_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.update_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.add_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.update_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.toggle_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.get_or_create_open_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.advise_reset_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.prune_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_create_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::CreateIndexRequest,
                    super::metastore::CreateIndexResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::CreateIndexRequest,
                Response = super::metastore::CreateIndexResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::CreateIndexRequest,
        >>::Future: Send + 'static,
    {
        self.create_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_update_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::UpdateIndexRequest,
                    super::metastore::IndexMetadataResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::UpdateIndexRequest,
                Response = super::metastore::IndexMetadataResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::UpdateIndexRequest,
        >>::Future: Send + 'static,
    {
        self.update_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::DeleteIndexRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteIndexRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteIndexRequest,
        >>::Future: Send + 'static,
    {
        self.delete_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_add_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::AddSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::AddSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::AddSourceRequest,
        >>::Future: Send + 'static,
    {
        self.add_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_update_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::UpdateSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::UpdateSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::UpdateSourceRequest,
        >>::Future: Send + 'static,
    {
        self.update_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_toggle_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::ToggleSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::ToggleSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::ToggleSourceRequest,
        >>::Future: Send + 'static,
    {
        self.toggle_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::DeleteSourceRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::DeleteSourceRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::DeleteSourceRequest,
        >>::Future: Send + 'static,
    {
        self.delete_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_get_or_create_open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetOrCreateOpenShardsRequest,
                    GetOrCreateOpenShardsResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                GetOrCreateOpenShardsRequest,
                Response = GetOrCreateOpenShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            GetOrCreateOpenShardsRequest,
        >>::Future: Send + 'static,
    {
        self.get_or_create_open_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_advise_reset_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AdviseResetShardsRequest,
                    AdviseResetShardsResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                AdviseResetShardsRequest,
                Response = AdviseResetShardsResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AdviseResetShardsRequest>>::Future: Send + 'static,
    {
        self.advise_reset_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_prune_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    super::metastore::PruneShardsRequest,
                    super::metastore::EmptyResponse,
                    crate::control_plane::ControlPlaneError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                super::metastore::PruneShardsRequest,
                Response = super::metastore::EmptyResponse,
                Error = crate::control_plane::ControlPlaneError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            super::metastore::PruneShardsRequest,
        >>::Future: Send + 'static,
    {
        self.prune_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> ControlPlaneServiceClient
    where
        T: ControlPlaneService,
    {
        let inner_client = InnerControlPlaneServiceClient(std::sync::Arc::new(instance));
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> ControlPlaneServiceClient {
        let client = ControlPlaneServiceClient::from_channel(
            addr,
            channel,
            max_message_size,
        );
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> ControlPlaneServiceClient {
        let client = ControlPlaneServiceClient::from_balance_channel(
            balance_channel,
            max_message_size,
        );
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> ControlPlaneServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ControlPlaneServiceMailbox<A>: ControlPlaneService,
    {
        let inner_client = InnerControlPlaneServiceClient(
            std::sync::Arc::new(ControlPlaneServiceMailbox::new(mailbox)),
        );
        self.build_from_inner_client(inner_client)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn build_from_mock(
        self,
        mock: MockControlPlaneService,
    ) -> ControlPlaneServiceClient {
        let client = ControlPlaneServiceClient::from_mock(mock);
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    fn build_from_inner_client(
        self,
        inner_client: InnerControlPlaneServiceClient,
    ) -> ControlPlaneServiceClient {
        let create_index_svc = self
            .create_index_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let update_index_svc = self
            .update_index_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let delete_index_svc = self
            .delete_index_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let add_source_svc = self
            .add_source_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let update_source_svc = self
            .update_source_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let toggle_source_svc = self
            .toggle_source_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let delete_source_svc = self
            .delete_source_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let get_or_create_open_shards_svc = self
            .get_or_create_open_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let advise_reset_shards_svc = self
            .advise_reset_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let prune_shards_svc = self
            .prune_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = ControlPlaneServiceTowerServiceStack {
            inner: inner_client,
            create_index_svc,
            update_index_svc,
            delete_index_svc,
            add_source_svc,
            update_source_svc,
            toggle_source_svc,
            delete_source_svc,
            get_or_create_open_shards_svc,
            advise_reset_shards_svc,
            prune_shards_svc,
        };
        ControlPlaneServiceClient::new(tower_svc_stack)
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
            super::metastore::UpdateIndexRequest,
            Response = super::metastore::IndexMetadataResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::IndexMetadataResponse,
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
            super::metastore::UpdateSourceRequest,
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
            AdviseResetShardsRequest,
            Response = AdviseResetShardsResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                AdviseResetShardsResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >
        + tower::Service<
            super::metastore::PruneShardsRequest,
            Response = super::metastore::EmptyResponse,
            Error = crate::control_plane::ControlPlaneError,
            Future = BoxFuture<
                super::metastore::EmptyResponse,
                crate::control_plane::ControlPlaneError,
            >,
        >,
{
    async fn create_index(
        &self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.clone().call(request).await
    }
    async fn update_index(
        &self,
        request: super::metastore::UpdateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::IndexMetadataResponse,
    > {
        self.clone().call(request).await
    }
    async fn delete_index(
        &self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
    async fn add_source(
        &self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
    async fn update_source(
        &self,
        request: super::metastore::UpdateSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
    async fn toggle_source(
        &self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
    async fn delete_source(
        &self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
    async fn get_or_create_open_shards(
        &self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.clone().call(request).await
    }
    async fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<AdviseResetShardsResponse> {
        self.clone().call(request).await
    }
    async fn prune_shards(
        &self,
        request: super::metastore::PruneShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.clone().call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct ControlPlaneServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> ControlPlaneServiceGrpcClientAdapter<T> {
    pub fn new(
        instance: T,
        connection_addrs_rx: tokio::sync::watch::Receiver<
            std::collections::HashSet<std::net::SocketAddr>,
        >,
    ) -> Self {
        Self {
            inner: instance,
            connection_addrs_rx,
        }
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
        &self,
        request: super::metastore::CreateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::CreateIndexResponse,
    > {
        self.inner
            .clone()
            .create_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::CreateIndexRequest::rpc_name(),
            ))
    }
    async fn update_index(
        &self,
        request: super::metastore::UpdateIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<
        super::metastore::IndexMetadataResponse,
    > {
        self.inner
            .clone()
            .update_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::UpdateIndexRequest::rpc_name(),
            ))
    }
    async fn delete_index(
        &self,
        request: super::metastore::DeleteIndexRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .delete_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::DeleteIndexRequest::rpc_name(),
            ))
    }
    async fn add_source(
        &self,
        request: super::metastore::AddSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .add_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::AddSourceRequest::rpc_name(),
            ))
    }
    async fn update_source(
        &self,
        request: super::metastore::UpdateSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .update_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::UpdateSourceRequest::rpc_name(),
            ))
    }
    async fn toggle_source(
        &self,
        request: super::metastore::ToggleSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .toggle_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::ToggleSourceRequest::rpc_name(),
            ))
    }
    async fn delete_source(
        &self,
        request: super::metastore::DeleteSourceRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .delete_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::DeleteSourceRequest::rpc_name(),
            ))
    }
    async fn get_or_create_open_shards(
        &self,
        request: GetOrCreateOpenShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.inner
            .clone()
            .get_or_create_open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                GetOrCreateOpenShardsRequest::rpc_name(),
            ))
    }
    async fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<AdviseResetShardsResponse> {
        self.inner
            .clone()
            .advise_reset_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                AdviseResetShardsRequest::rpc_name(),
            ))
    }
    async fn prune_shards(
        &self,
        request: super::metastore::PruneShardsRequest,
    ) -> crate::control_plane::ControlPlaneResult<super::metastore::EmptyResponse> {
        self.inner
            .clone()
            .prune_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                super::metastore::PruneShardsRequest::rpc_name(),
            ))
    }
}
#[derive(Debug)]
pub struct ControlPlaneServiceGrpcServerAdapter {
    inner: InnerControlPlaneServiceClient,
}
impl ControlPlaneServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: ControlPlaneService,
    {
        Self {
            inner: InnerControlPlaneServiceClient(std::sync::Arc::new(instance)),
        }
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
            .0
            .create_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn update_index(
        &self,
        request: tonic::Request<super::metastore::UpdateIndexRequest>,
    ) -> Result<
        tonic::Response<super::metastore::IndexMetadataResponse>,
        tonic::Status,
    > {
        self.inner
            .0
            .update_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_index(
        &self,
        request: tonic::Request<super::metastore::DeleteIndexRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn add_source(
        &self,
        request: tonic::Request<super::metastore::AddSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .add_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn update_source(
        &self,
        request: tonic::Request<super::metastore::UpdateSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .update_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn toggle_source(
        &self,
        request: tonic::Request<super::metastore::ToggleSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .toggle_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_source(
        &self,
        request: tonic::Request<super::metastore::DeleteSourceRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn get_or_create_open_shards(
        &self,
        request: tonic::Request<GetOrCreateOpenShardsRequest>,
    ) -> Result<tonic::Response<GetOrCreateOpenShardsResponse>, tonic::Status> {
        self.inner
            .0
            .get_or_create_open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn advise_reset_shards(
        &self,
        request: tonic::Request<AdviseResetShardsRequest>,
    ) -> Result<tonic::Response<AdviseResetShardsResponse>, tonic::Status> {
        self.inner
            .0
            .advise_reset_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn prune_shards(
        &self,
        request: tonic::Request<super::metastore::PruneShardsRequest>,
    ) -> Result<tonic::Response<super::metastore::EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .prune_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
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
        /// Updates an index.
        pub async fn update_index(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::UpdateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::IndexMetadataResponse>,
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
                "/quickwit.control_plane.ControlPlaneService/UpdateIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "UpdateIndex",
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
        /// Update a source.
        pub async fn update_source(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::metastore::UpdateSourceRequest,
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
                "/quickwit.control_plane.ControlPlaneService/UpdateSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "UpdateSource",
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
        /// Asks the control plane whether the shards listed in the request should be deleted or truncated.
        pub async fn advise_reset_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::AdviseResetShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AdviseResetShardsResponse>,
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
                "/quickwit.control_plane.ControlPlaneService/AdviseResetShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "AdviseResetShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Performs a debounced shard pruning request to the metastore.
        pub async fn prune_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::super::metastore::PruneShardsRequest>,
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
                "/quickwit.control_plane.ControlPlaneService/PruneShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.control_plane.ControlPlaneService",
                        "PruneShards",
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
        /// Updates an index.
        async fn update_index(
            &self,
            request: tonic::Request<super::super::metastore::UpdateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::metastore::IndexMetadataResponse>,
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
        /// Update a source.
        async fn update_source(
            &self,
            request: tonic::Request<super::super::metastore::UpdateSourceRequest>,
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
        /// Asks the control plane whether the shards listed in the request should be deleted or truncated.
        async fn advise_reset_shards(
            &self,
            request: tonic::Request<super::AdviseResetShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AdviseResetShardsResponse>,
            tonic::Status,
        >;
        /// Performs a debounced shard pruning request to the metastore.
        async fn prune_shards(
            &self,
            request: tonic::Request<super::super::metastore::PruneShardsRequest>,
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
                "/quickwit.control_plane.ControlPlaneService/UpdateIndex" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateIndexSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::UpdateIndexRequest,
                    > for UpdateIndexSvc<T> {
                        type Response = super::super::metastore::IndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::UpdateIndexRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).update_index(request).await
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
                        let method = UpdateIndexSvc(inner);
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
                "/quickwit.control_plane.ControlPlaneService/UpdateSource" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSourceSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::UpdateSourceRequest,
                    > for UpdateSourceSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::UpdateSourceRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).update_source(request).await
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
                        let method = UpdateSourceSvc(inner);
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
                "/quickwit.control_plane.ControlPlaneService/AdviseResetShards" => {
                    #[allow(non_camel_case_types)]
                    struct AdviseResetShardsSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<super::AdviseResetShardsRequest>
                    for AdviseResetShardsSvc<T> {
                        type Response = super::AdviseResetShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AdviseResetShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).advise_reset_shards(request).await
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
                        let method = AdviseResetShardsSvc(inner);
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
                "/quickwit.control_plane.ControlPlaneService/PruneShards" => {
                    #[allow(non_camel_case_types)]
                    struct PruneShardsSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<
                        super::super::metastore::PruneShardsRequest,
                    > for PruneShardsSvc<T> {
                        type Response = super::super::metastore::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::metastore::PruneShardsRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).prune_shards(request).await
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
                        let method = PruneShardsSvc(inner);
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
