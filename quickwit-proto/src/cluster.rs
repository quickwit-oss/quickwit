//// The member information.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Member {
    //// Member ID.　A string of the UUID.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    //// Cluster listen address. string of IP and port number.
    //// E.g. 127.0.0.1:5000
    #[prost(string, tag = "2")]
    pub listen_address: ::prost::alloc::string::String,
    //// If true, it means self.
    #[prost(bool, tag = "3")]
    pub is_self: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MembersRequest {}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MembersResult {
    #[prost(message, repeated, tag = "1")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaveRequest {}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaveResult {}
#[doc = r" Generated client implementations."]
pub mod cluster_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ClusterServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClusterServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClusterServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = "/ Retrieves members in the cluster."]
        pub async fn members(
            &mut self,
            request: impl tonic::IntoRequest<super::MembersRequest>,
        ) -> Result<tonic::Response<super::MembersResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cluster.ClusterService/Members");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = "/ Removes itself from the cluster."]
        #[doc = "/ Removed node will be isolated from the cluster."]
        pub async fn leave(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaveRequest>,
        ) -> Result<tonic::Response<super::LeaveResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cluster.ClusterService/Leave");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for ClusterServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ClusterServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ClusterServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod cluster_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ClusterServiceServer."]
    #[async_trait]
    pub trait ClusterService: Send + Sync + 'static {
        #[doc = "/ Retrieves members in the cluster."]
        async fn members(
            &self,
            request: tonic::Request<super::MembersRequest>,
        ) -> Result<tonic::Response<super::MembersResult>, tonic::Status>;
        #[doc = "/ Removes itself from the cluster."]
        #[doc = "/ Removed node will be isolated from the cluster."]
        async fn leave(
            &self,
            request: tonic::Request<super::LeaveRequest>,
        ) -> Result<tonic::Response<super::LeaveResult>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ClusterServiceServer<T: ClusterService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: ClusterService> ClusterServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for ClusterServiceServer<T>
    where
        T: ClusterService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/cluster.ClusterService/Members" => {
                    #[allow(non_camel_case_types)]
                    struct MembersSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService> tonic::server::UnaryService<super::MembersRequest> for MembersSvc<T> {
                        type Response = super::MembersResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MembersRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).members(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MembersSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/cluster.ClusterService/Leave" => {
                    #[allow(non_camel_case_types)]
                    struct LeaveSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService> tonic::server::UnaryService<super::LeaveRequest> for LeaveSvc<T> {
                        type Response = super::LeaveResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaveRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).leave(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeaveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ClusterService> Clone for ClusterServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: ClusterService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ClusterService> tonic::transport::NamedService for ClusterServiceServer<T> {
        const NAME: &'static str = "cluster.ClusterService";
    }
}
