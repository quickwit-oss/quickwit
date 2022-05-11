// -- Search -------------------

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    /// Index ID
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    /// Query
    #[prost(string, tag="2")]
    pub query: ::prost::alloc::string::String,
    /// Fields to search on
    #[prost(string, repeated, tag="3")]
    pub search_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Time filter
    #[prost(int64, optional, tag="4")]
    pub start_timestamp: ::core::option::Option<i64>,
    #[prost(int64, optional, tag="5")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Maximum number of hits to return.
    #[prost(uint64, tag="6")]
    pub max_hits: u64,
    /// First hit to return. Together with max_hits, this parameter
    /// can be used for pagination.
    ///
    /// E.g.
    /// The results with rank [start_offset..start_offset + max_hits) are returned.
    #[prost(uint64, tag="7")]
    pub start_offset: u64,
    /// Sort order
    #[prost(enumeration="SortOrder", optional, tag="9")]
    pub sort_order: ::core::option::Option<i32>,
    /// Sort by fast field. If unset sort by docid
    #[prost(string, optional, tag="10")]
    pub sort_by_field: ::core::option::Option<::prost::alloc::string::String>,
    /// json serialized aggregation_request
    #[prost(string, optional, tag="11")]
    pub aggregation_request: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    /// Number of hits matching the query.
    #[prost(uint64, tag="1")]
    pub num_hits: u64,
    /// Matched hits
    #[prost(message, repeated, tag="2")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
    /// Elapsed time to perform the request. This time is measured
    /// server-side and expressed in microseconds.
    #[prost(uint64, tag="3")]
    pub elapsed_time_micros: u64,
    /// The searcherrors that occured formatted as string.
    #[prost(string, repeated, tag="4")]
    pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Serialized aggregation response
    #[prost(string, optional, tag="5")]
    pub aggregation: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitSearchError {
    /// The searcherror that occured formatted as string.
    #[prost(string, tag="1")]
    pub error: ::prost::alloc::string::String,
    /// Split id that failed.
    #[prost(string, tag="2")]
    pub split_id: ::prost::alloc::string::String,
    /// Flag to indicate if the error can be considered a retryable error
    #[prost(bool, tag="3")]
    pub retryable_error: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchRequest {
    /// Search request. This is a perfect copy of the original search request,
    /// that was sent to root apart from the start_offset & max_hits params.
    #[prost(message, optional, tag="1")]
    pub search_request: ::core::option::Option<SearchRequest>,
    /// Index split ids to apply the query on.
    /// This ids are resolved from the index_uri defined in the search_request.
    #[prost(message, repeated, tag="4")]
    pub split_offsets: ::prost::alloc::vec::Vec<SplitIdAndFooterOffsets>,
    /// `DocMapper` as json serialized trait.
    #[prost(string, tag="5")]
    pub doc_mapper: ::prost::alloc::string::String,
    /// Index URI. The index URI defines the location of the storage that contains the
    /// split files.
    #[prost(string, tag="6")]
    pub index_uri: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitIdAndFooterOffsets {
    /// Index split id to apply the query on.
    /// This id is resolved from the index_uri defined in the search_request.
    #[prost(string, tag="1")]
    pub split_id: ::prost::alloc::string::String,
    /// The offset of the start of footer in the split bundle. The footer contains the file bundle metadata and the hotcache.
    #[prost(uint64, tag="2")]
    pub split_footer_start: u64,
    /// The offset of the end of the footer in split bundle. The footer contains the file bundle metada and the hotcache.
    #[prost(uint64, tag="3")]
    pub split_footer_end: u64,
}
//// Hits returned by a FetchDocRequest.
////
//// The json that is joined is the raw tantivy json doc.
//// It is very different from a quickwit json doc.
////
//// For instance:
//// - it may contain a _source and a _dynamic field.
//// - since tantivy has no notion of cardinality,
//// all fields is  are arrays.
//// - since tantivy has no notion of object, the object is
//// flattened by concatenating the path to the root.
////
//// See  `quickwit_search::convert_leaf_hit`
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafHit {
    /// The actual content of the hit/
    #[prost(string, tag="1")]
    pub leaf_json: ::prost::alloc::string::String,
    /// The partial hit (ie: the sorting field + the document address)
    #[prost(message, optional, tag="2")]
    pub partial_hit: ::core::option::Option<PartialHit>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Hit {
    /// The actual content of the hit/
    #[prost(string, tag="1")]
    pub json: ::prost::alloc::string::String,
    /// The partial hit (ie: the sorting field + the document address)
    #[prost(message, optional, tag="2")]
    pub partial_hit: ::core::option::Option<PartialHit>,
}
/// A partial hit, is a hit for which we have not fetch the content yet.
/// Instead, it holds a document_uri which is enough information to
/// go and fetch the actual document data, by performing a `get_doc(...)`
/// request.
#[derive(Serialize, Deserialize)]
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
    #[prost(uint64, tag="1")]
    pub sorting_field_value: u64,
    #[prost(string, tag="2")]
    pub split_id: ::prost::alloc::string::String,
    /// (segment_ord, doc) form a tantivy DocAddress, which is sufficient to identify a document
    /// within a split
    #[prost(uint32, tag="3")]
    pub segment_ord: u32,
    /// The DocId identifies a unique document at the scale of a tantivy segment.
    #[prost(uint32, tag="4")]
    pub doc_id: u32,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchResponse {
    /// Total number of documents matched by the query.
    #[prost(uint64, tag="1")]
    pub num_hits: u64,
    /// List of the best top-K candidates for the given leaf query.
    #[prost(message, repeated, tag="2")]
    pub partial_hits: ::prost::alloc::vec::Vec<PartialHit>,
    /// The list of splits that failed. LeafSearchResponse can be an aggregation of results, so there may be multiple.
    #[prost(message, repeated, tag="3")]
    pub failed_splits: ::prost::alloc::vec::Vec<SplitSearchError>,
    /// Total number of splits the leaf(s) were in charge of.
    /// num_attempted_splits = num_successful_splits + num_failed_splits.
    #[prost(uint64, tag="4")]
    pub num_attempted_splits: u64,
    /// json serialized intermediate aggregation_result.
    #[prost(string, optional, tag="5")]
    pub intermediate_aggregation_result: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchDocsRequest {
    /// Request fetching the content of a given list of partial_hits.
    #[prost(message, repeated, tag="1")]
    pub partial_hits: ::prost::alloc::vec::Vec<PartialHit>,
    /// Index ID
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    /// Split footer offsets. They are required for fetch docs to
    /// fetch the document content in two reads, when the footer is not
    /// cached.
    #[prost(message, repeated, tag="3")]
    pub split_offsets: ::prost::alloc::vec::Vec<SplitIdAndFooterOffsets>,
    /// Index URI. The index URI defines the location of the storage that contains the
    /// split files.
    #[prost(string, tag="4")]
    pub index_uri: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchDocsResponse {
    /// List of complete hits.
    #[prost(message, repeated, tag="1")]
    pub hits: ::prost::alloc::vec::Vec<LeafHit>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchStreamRequest {
    /// Index ID
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    /// Query
    #[prost(string, tag="2")]
    pub query: ::prost::alloc::string::String,
    /// Fields to search on
    #[prost(string, repeated, tag="3")]
    pub search_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// The time filter is interpreted as a semi-open interval. [start, end)
    #[prost(int64, optional, tag="4")]
    pub start_timestamp: ::core::option::Option<i64>,
    #[prost(int64, optional, tag="5")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Name of the fast field to extract
    #[prost(string, tag="6")]
    pub fast_field: ::prost::alloc::string::String,
    /// The output format
    #[prost(enumeration="OutputFormat", tag="7")]
    pub output_format: i32,
    /// The field by which we want to partition
    #[prost(string, optional, tag="9")]
    pub partition_by_field: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchStreamRequest {
    /// Stream request. This is a perfect copy of the original stream request,
    /// that was sent to root.
    #[prost(message, optional, tag="1")]
    pub request: ::core::option::Option<SearchStreamRequest>,
    /// Index split ids to apply the query on.
    /// This ids are resolved from the index_uri defined in the stream request.
    #[prost(message, repeated, tag="2")]
    pub split_offsets: ::prost::alloc::vec::Vec<SplitIdAndFooterOffsets>,
    /// `DocMapper` as json serialized trait.
    #[prost(string, tag="5")]
    pub doc_mapper: ::prost::alloc::string::String,
    /// Index URI. The index URI defines the location of the storage that contains the
    /// split files.
    #[prost(string, tag="6")]
    pub index_uri: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafSearchStreamResponse {
    /// Row of data serialized in bytes.
    #[prost(bytes="vec", tag="1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    /// Split id.
    #[prost(string, tag="2")]
    pub split_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SortOrder {
    //// Ascending order.
    Asc = 0,
    //// Descending order.
    ///
    ///< This will be the default value;
    Desc = 1,
}
// -- Stream -------------------

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OutputFormat {
    //// Comma Separated Values format (<https://datatracker.ietf.org/doc/html/rfc4180>).
    //// The delimiter is `,`.
    ///
    ///< This will be the default value
    Csv = 0,
    //// Format data by row in ClickHouse binary format.
    //// <https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary>
    ClickHouseRowBinary = 1,
}
/// Generated client implementations.
pub mod search_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct SearchServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SearchServiceClient<tonic::transport::Channel> {
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
    impl<T> SearchServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> SearchServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
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
            SearchServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Root search API.
        /// This RPC identifies the set of splits on which the query should run on,
        /// and dispatch the several calls to `LeafSearch`.
        ///
        /// It is also in charge of merging back the results.
        pub async fn root_search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status> {
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
                "/quickwit.SearchService/RootSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Perform a leaf search on a given set of splits.
        ///
        /// It is like a regular search except that:
        /// - the node should perform the search locally instead of dispatching
        /// it to other nodes.
        /// - it should be applied on the given subset of splits
        /// - Hit content is not fetched, and we instead return so called `PartialHit`.
        pub async fn leaf_search(
            &mut self,
            request: impl tonic::IntoRequest<super::LeafSearchRequest>,
        ) -> Result<tonic::Response<super::LeafSearchResponse>, tonic::Status> {
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
                "/quickwit.SearchService/LeafSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        //// Fetches the documents contents from the document store.
        //// This methods takes `PartialHit`s and returns `Hit`s.
        pub async fn fetch_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchDocsRequest>,
        ) -> Result<tonic::Response<super::FetchDocsResponse>, tonic::Status> {
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
                "/quickwit.SearchService/FetchDocs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Perform a leaf stream on a given set of splits.
        pub async fn leaf_search_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::LeafSearchStreamRequest>,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<super::LeafSearchStreamResponse>,
                >,
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
                "/quickwit.SearchService/LeafSearchStream",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod search_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with SearchServiceServer.
    #[async_trait]
    pub trait SearchService: Send + Sync + 'static {
        /// Root search API.
        /// This RPC identifies the set of splits on which the query should run on,
        /// and dispatch the several calls to `LeafSearch`.
        ///
        /// It is also in charge of merging back the results.
        async fn root_search(
            &self,
            request: tonic::Request<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        /// Perform a leaf search on a given set of splits.
        ///
        /// It is like a regular search except that:
        /// - the node should perform the search locally instead of dispatching
        /// it to other nodes.
        /// - it should be applied on the given subset of splits
        /// - Hit content is not fetched, and we instead return so called `PartialHit`.
        async fn leaf_search(
            &self,
            request: tonic::Request<super::LeafSearchRequest>,
        ) -> Result<tonic::Response<super::LeafSearchResponse>, tonic::Status>;
        //// Fetches the documents contents from the document store.
        //// This methods takes `PartialHit`s and returns `Hit`s.
        async fn fetch_docs(
            &self,
            request: tonic::Request<super::FetchDocsRequest>,
        ) -> Result<tonic::Response<super::FetchDocsResponse>, tonic::Status>;
        ///Server streaming response type for the LeafSearchStream method.
        type LeafSearchStreamStream: futures_core::Stream<
                Item = Result<super::LeafSearchStreamResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Perform a leaf stream on a given set of splits.
        async fn leaf_search_stream(
            &self,
            request: tonic::Request<super::LeafSearchStreamRequest>,
        ) -> Result<tonic::Response<Self::LeafSearchStreamStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct SearchServiceServer<T: SearchService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: SearchService> SearchServiceServer<T> {
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
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for SearchServiceServer<T>
    where
        T: SearchService,
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
                "/quickwit.SearchService/RootSearch" => {
                    #[allow(non_camel_case_types)]
                    struct RootSearchSvc<T: SearchService>(pub Arc<T>);
                    impl<
                        T: SearchService,
                    > tonic::server::UnaryService<super::SearchRequest>
                    for RootSearchSvc<T> {
                        type Response = super::SearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).root_search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RootSearchSvc(inner);
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
                "/quickwit.SearchService/LeafSearch" => {
                    #[allow(non_camel_case_types)]
                    struct LeafSearchSvc<T: SearchService>(pub Arc<T>);
                    impl<
                        T: SearchService,
                    > tonic::server::UnaryService<super::LeafSearchRequest>
                    for LeafSearchSvc<T> {
                        type Response = super::LeafSearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeafSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).leaf_search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LeafSearchSvc(inner);
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
                "/quickwit.SearchService/FetchDocs" => {
                    #[allow(non_camel_case_types)]
                    struct FetchDocsSvc<T: SearchService>(pub Arc<T>);
                    impl<
                        T: SearchService,
                    > tonic::server::UnaryService<super::FetchDocsRequest>
                    for FetchDocsSvc<T> {
                        type Response = super::FetchDocsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchDocsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fetch_docs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchDocsSvc(inner);
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
                "/quickwit.SearchService/LeafSearchStream" => {
                    #[allow(non_camel_case_types)]
                    struct LeafSearchStreamSvc<T: SearchService>(pub Arc<T>);
                    impl<
                        T: SearchService,
                    > tonic::server::ServerStreamingService<
                        super::LeafSearchStreamRequest,
                    > for LeafSearchStreamSvc<T> {
                        type Response = super::LeafSearchStreamResponse;
                        type ResponseStream = T::LeafSearchStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeafSearchStreamRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).leaf_search_stream(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LeafSearchStreamSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
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
    impl<T: SearchService> Clone for SearchServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: SearchService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
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
