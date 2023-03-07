#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyIndexChangeRequest {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyIndexChangeResponse {}
/// BEGIN quickwit-codegen
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait ControlPlaneService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn notify_index_change(
        &mut self,
        request: NotifyIndexChangeRequest,
    ) -> crate::Result<NotifyIndexChangeResponse>;
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
    pub fn from_channel(
        channel: tower::timeout::Timeout<tonic::transport::Channel>,
    ) -> Self {
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
        A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
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
    async fn notify_index_change(
        &mut self,
        request: NotifyIndexChangeRequest,
    ) -> crate::Result<NotifyIndexChangeResponse> {
        self.inner.notify_index_change(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
impl From<MockControlPlaneService> for ControlPlaneServiceClient {
    fn from(mock: MockControlPlaneService) -> Self {
        ControlPlaneServiceClient::new(mock)
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<NotifyIndexChangeRequest> for Box<dyn ControlPlaneService> {
    type Response = NotifyIndexChangeResponse;
    type Error = crate::ControlPlaneError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: NotifyIndexChangeRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.notify_index_change(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct ControlPlaneServiceTowerBlock {
    notify_index_change_svc: quickwit_common::tower::BoxService<
        NotifyIndexChangeRequest,
        NotifyIndexChangeResponse,
        crate::ControlPlaneError,
    >,
}
impl Clone for ControlPlaneServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            notify_index_change_svc: self.notify_index_change_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl ControlPlaneService for ControlPlaneServiceTowerBlock {
    async fn notify_index_change(
        &mut self,
        request: NotifyIndexChangeRequest,
    ) -> crate::Result<NotifyIndexChangeResponse> {
        self.notify_index_change_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct ControlPlaneServiceTowerBlockBuilder {
    notify_index_change_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            NotifyIndexChangeRequest,
            NotifyIndexChangeResponse,
            crate::ControlPlaneError,
        >,
    >,
}
impl ControlPlaneServiceTowerBlockBuilder {
    pub fn notify_index_change_layer(
        mut self,
        layer: quickwit_common::tower::BoxLayer<
            Box<dyn ControlPlaneService>,
            NotifyIndexChangeRequest,
            NotifyIndexChangeResponse,
            crate::ControlPlaneError,
        >,
    ) -> Self {
        self.notify_index_change_layer = Some(layer);
        self
    }
    pub fn service<T>(self, instance: T) -> ControlPlaneServiceClient
    where
        T: ControlPlaneService + Clone,
    {
        let boxed_instance: Box<dyn ControlPlaneService> = Box::new(instance);
        let notify_index_change_svc = if let Some(layer) = self.notify_index_change_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = ControlPlaneServiceTowerBlock {
            notify_index_change_svc,
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
    inner: MailboxAdapter<A, crate::ControlPlaneError>,
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
use tower::{Layer, Service, ServiceExt};
impl<A, M, T, E> tower::Service<M> for ControlPlaneServiceMailbox<A>
where
    A: quickwit_actors::Actor + quickwit_actors::Handler<M, Reply = Result<T, E>> + Send
        + Sync + 'static,
    M: std::fmt::Debug + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    crate::ControlPlaneError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::ControlPlaneError;
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
    A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
    ControlPlaneServiceMailbox<
        A,
    >: tower::Service<
        NotifyIndexChangeRequest,
        Response = NotifyIndexChangeResponse,
        Error = crate::ControlPlaneError,
        Future = BoxFuture<NotifyIndexChangeResponse, crate::ControlPlaneError>,
    >,
{
    async fn notify_index_change(
        &mut self,
        request: NotifyIndexChangeRequest,
    ) -> crate::Result<NotifyIndexChangeResponse> {
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
    async fn notify_index_change(
        &mut self,
        request: NotifyIndexChangeRequest,
    ) -> crate::Result<NotifyIndexChangeResponse> {
        self.inner
            .notify_index_change(request)
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
    async fn notify_index_change(
        &self,
        request: tonic::Request<NotifyIndexChangeRequest>,
    ) -> Result<tonic::Response<NotifyIndexChangeResponse>, tonic::Status> {
        self.inner
            .clone()
            .notify_index_change(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
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
            D: std::convert::TryInto<tonic::transport::Endpoint>,
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
        /// / Notify the Control Plane that a change on an index occurred. The change
        /// / can be an index creation, deletion, or update that includes a source creation/deletion/num pipeline update.
        /// Note(fmassot): it's not very clear for a user to know which change triggers a control plane notification.
        /// This can be explicited in the attributes of `NotifyIndexChangeRequest` with an enum that describes the
        /// type of change. The index ID and/or source ID could also be added.
        /// However, these attributes will not be used by the Control Plane, at least at short term.
        pub async fn notify_index_change(
            &mut self,
            request: impl tonic::IntoRequest<super::NotifyIndexChangeRequest>,
        ) -> Result<tonic::Response<super::NotifyIndexChangeResponse>, tonic::Status> {
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
                "/control_plane_service.ControlPlaneService/notifyIndexChange",
            );
            self.inner.unary(request.into_request(), path, codec).await
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
        /// / Notify the Control Plane that a change on an index occurred. The change
        /// / can be an index creation, deletion, or update that includes a source creation/deletion/num pipeline update.
        /// Note(fmassot): it's not very clear for a user to know which change triggers a control plane notification.
        /// This can be explicited in the attributes of `NotifyIndexChangeRequest` with an enum that describes the
        /// type of change. The index ID and/or source ID could also be added.
        /// However, these attributes will not be used by the Control Plane, at least at short term.
        async fn notify_index_change(
            &self,
            request: tonic::Request<super::NotifyIndexChangeRequest>,
        ) -> Result<tonic::Response<super::NotifyIndexChangeResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ControlPlaneServiceGrpcServer<T: ControlPlaneServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
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
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/control_plane_service.ControlPlaneService/notifyIndexChange" => {
                    #[allow(non_camel_case_types)]
                    struct notifyIndexChangeSvc<T: ControlPlaneServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ControlPlaneServiceGrpc,
                    > tonic::server::UnaryService<super::NotifyIndexChangeRequest>
                    for notifyIndexChangeSvc<T> {
                        type Response = super::NotifyIndexChangeResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::NotifyIndexChangeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).notify_index_change(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = notifyIndexChangeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
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
            }
        }
    }
    impl<T: ControlPlaneServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ControlPlaneServiceGrpc> tonic::server::NamedService
    for ControlPlaneServiceGrpcServer<T> {
        const NAME: &'static str = "control_plane_service.ControlPlaneService";
    }
}
