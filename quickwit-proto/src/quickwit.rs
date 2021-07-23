// -- Search -------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    /// Index ID
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    /// Query
    #[prost(string, tag = "2")]
    pub query: ::prost::alloc::string::String,
    /// Fields to search on
    #[prost(string, repeated, tag = "3")]
    pub search_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Time filter
    #[prost(int64, optional, tag = "4")]
    pub start_timestamp: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "5")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Maximum number of hits to return.
    #[prost(uint64, tag = "6")]
    pub max_hits: u64,
    /// First hit to return. Together with max_hits, this parameter
    /// can be used for pagination.
    ///
    /// E.g.
    /// The results with rank [start_offset..start_offset + max_hits) are returned.
    #[prost(uint64, tag = "7")]
    pub start_offset: u64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResult {
    /// Number of hits matching the query.
    #[prost(uint64, tag = "1")]
    pub num_hits: u64,
    /// Matched hits
    #[prost(message, repeated, tag = "2")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
    /// Elapsed time to perform the request. This time is measured
    /// server-side and expressed in microseconds.
    #[prost(uint64, tag = "3")]
    pub elapsed_time_micros: u64,
    /// The searcherrors that occured formatted as string.
    #[prost(string, repeated, tag = "4")]
    pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitSearchError {
    /// The searcherror that occured formatted as string.
    #[prost(string, tag = "1")]
    pub error: ::prost::alloc::string::String,
    /// Split id that failed.
    #[prost(string, tag = "2")]
    pub split_id: ::prost::alloc::string::String,
    /// Flag to indicate if the error can be considered a retryable error
    #[prost(bool, tag = "3")]
    pub retryable_error: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchRequest {
    /// Search request. This is a perfect copy of the original search request,
    /// that was sent to root apart from the start_offset & max_hits params.
    #[prost(message, optional, tag = "1")]
    pub search_request: ::core::option::Option<SearchRequest>,
    /// Index split ids to apply the query on.
    /// This ids are resolved from the index_uri defined in the search_request.
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Hit {
    /// The actual content of the hit/
    #[prost(string, tag = "1")]
    pub json: ::prost::alloc::string::String,
    /// The partial hit (ie: the sorting field + the document address)
    #[prost(message, optional, tag = "2")]
    pub partial_hit: ::core::option::Option<PartialHit>,
}
/// A partial hit, is a hit for which we have not fetch the content yet.
/// Instead, it holds a record_uri which is enough information to
/// go and fetch the actual document data, by performing a `get_doc(...)`
/// request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartialHit {
    /// Sorting field value. (e.g. timestamp)
    ///
    /// Quickwit only computes top-K of this sorting field.
    /// If the user requested for a bottom-K of a given fast field, then quickwit simply
    /// emits an decreasing mapping of this fast field.
    ///
    /// In case of a tie, quickwit uses the increasing order of
    /// - the split_id,
    /// - the segment_ord,
    /// - the doc id.
    #[prost(uint64, tag = "1")]
    pub sorting_field_value: u64,
    #[prost(string, tag = "2")]
    pub split_id: ::prost::alloc::string::String,
    /// (segment_ord, doc) form a tantivy DocAddress, which is sufficient to identify a document
    /// within a split
    #[prost(uint32, tag = "3")]
    pub segment_ord: u32,
    /// The DocId identifies a unique document at the scale of a tantivy segment.
    #[prost(uint32, tag = "4")]
    pub doc_id: u32,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchResult {
    /// Total number of documents matched by the query.
    #[prost(uint64, tag = "1")]
    pub num_hits: u64,
    /// List of the best top-K candidates for the given leaf query.
    #[prost(message, repeated, tag = "2")]
    pub partial_hits: ::prost::alloc::vec::Vec<PartialHit>,
    /// The list of requests that failed. LeafSearchResult can be an aggregation of results, so there may be multiple.
    #[prost(message, repeated, tag = "3")]
    pub failed_requests: ::prost::alloc::vec::Vec<SplitSearchError>,
    /// Total number of splits the leaf(s) were in charge of.
    /// num_attempted_splits = num_successful_splits + num_failed_splits.
    #[prost(uint64, tag = "4")]
    pub num_attempted_splits: u64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchDocsRequest {
    /// Request fetching the content of a given list of partial_hits.
    #[prost(message, repeated, tag = "1")]
    pub partial_hits: ::prost::alloc::vec::Vec<PartialHit>,
    /// Index ID
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchDocsResult {
    /// List of complete hits.
    #[prost(message, repeated, tag = "1")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
}
#[doc = r" Generated client implementations."]
pub mod search_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct SearchServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SearchServiceClient<tonic::transport::Channel> {
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
    impl<T> SearchServiceClient<T>
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
        #[doc = " Root search API."]
        #[doc = " This RPC identifies the set of splits on which the query should run on,"]
        #[doc = " and dispatch the several calls to `LeafSearch`."]
        #[doc = ""]
        #[doc = " It is also in charge of merging back the results."]
        pub async fn root_search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/quickwit.SearchService/RootSearch");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Perform a leaf search on a given set of splits."]
        #[doc = ""]
        #[doc = " It is like a regular search except that:"]
        #[doc = " - the node should perform the search locally instead of dispatching"]
        #[doc = " it to other nodes."]
        #[doc = " - it should be applied on the given subset of splits"]
        #[doc = " - Hit content is not fetched, and we instead return so called `PartialHit`."]
        pub async fn leaf_search(
            &mut self,
            request: impl tonic::IntoRequest<super::LeafSearchRequest>,
        ) -> Result<tonic::Response<super::LeafSearchResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/quickwit.SearchService/LeafSearch");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = "/ Fetches the documents contents from the document store."]
        #[doc = "/ This methods takes `PartialHit`s and returns `Hit`s."]
        pub async fn fetch_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchDocsRequest>,
        ) -> Result<tonic::Response<super::FetchDocsResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/quickwit.SearchService/FetchDocs");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for SearchServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for SearchServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SearchServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod search_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with SearchServiceServer."]
    #[async_trait]
    pub trait SearchService: Send + Sync + 'static {
        #[doc = " Root search API."]
        #[doc = " This RPC identifies the set of splits on which the query should run on,"]
        #[doc = " and dispatch the several calls to `LeafSearch`."]
        #[doc = ""]
        #[doc = " It is also in charge of merging back the results."]
        async fn root_search(
            &self,
            request: tonic::Request<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResult>, tonic::Status>;
        #[doc = " Perform a leaf search on a given set of splits."]
        #[doc = ""]
        #[doc = " It is like a regular search except that:"]
        #[doc = " - the node should perform the search locally instead of dispatching"]
        #[doc = " it to other nodes."]
        #[doc = " - it should be applied on the given subset of splits"]
        #[doc = " - Hit content is not fetched, and we instead return so called `PartialHit`."]
        async fn leaf_search(
            &self,
            request: tonic::Request<super::LeafSearchRequest>,
        ) -> Result<tonic::Response<super::LeafSearchResult>, tonic::Status>;
        #[doc = "/ Fetches the documents contents from the document store."]
        #[doc = "/ This methods takes `PartialHit`s and returns `Hit`s."]
        async fn fetch_docs(
            &self,
            request: tonic::Request<super::FetchDocsRequest>,
        ) -> Result<tonic::Response<super::FetchDocsResult>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct SearchServiceServer<T: SearchService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: SearchService> SearchServiceServer<T> {
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
    impl<T, B> Service<http::Request<B>> for SearchServiceServer<T>
    where
        T: SearchService,
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
                "/quickwit.SearchService/RootSearch" => {
                    #[allow(non_camel_case_types)]
                    struct RootSearchSvc<T: SearchService>(pub Arc<T>);
                    impl<T: SearchService> tonic::server::UnaryService<super::SearchRequest> for RootSearchSvc<T> {
                        type Response = super::SearchResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).root_search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RootSearchSvc(inner);
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
                "/quickwit.SearchService/LeafSearch" => {
                    #[allow(non_camel_case_types)]
                    struct LeafSearchSvc<T: SearchService>(pub Arc<T>);
                    impl<T: SearchService> tonic::server::UnaryService<super::LeafSearchRequest> for LeafSearchSvc<T> {
                        type Response = super::LeafSearchResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeafSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).leaf_search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeafSearchSvc(inner);
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
                "/quickwit.SearchService/FetchDocs" => {
                    #[allow(non_camel_case_types)]
                    struct FetchDocsSvc<T: SearchService>(pub Arc<T>);
                    impl<T: SearchService> tonic::server::UnaryService<super::FetchDocsRequest> for FetchDocsSvc<T> {
                        type Response = super::FetchDocsResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchDocsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fetch_docs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = FetchDocsSvc(inner);
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
    impl<T: SearchService> Clone for SearchServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: SearchService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: SearchService> tonic::transport::NamedService for SearchServiceServer<T> {
        const NAME: &'static str = "quickwit.SearchService";
    }
}
