#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {
    #[prost(int64, tag = "1")]
    pub org_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListRequest {
    /// this is always a com.dd.queryparser.proto.QueryNode, but we can't import logs-backend protobuf from here
    #[prost(message, optional, tag = "1")]
    pub query: ::core::option::Option<::prost_types::Any>,
    #[prost(uint32, tag = "2")]
    pub num_events_to_fetch: u32,
    #[prost(bool, tag = "3")]
    pub should_compute_count: bool,
    #[prost(string, repeated, tag = "4")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "5")]
    pub sort: ::prost::alloc::vec::Vec<SortKv>,
    #[prost(int64, tag = "6")]
    pub org_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortKv {
    #[prost(bool, tag = "1")]
    pub ascending: bool,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResponse {
    #[prost(uint64, tag = "1")]
    pub count: u64,
    #[prost(message, repeated, tag = "2")]
    pub streams: ::prost::alloc::vec::Vec<Stream>,
    #[prost(message, optional, tag = "3")]
    pub statistics: ::core::option::Option<Statistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Statistics {
    #[prost(uint64, tag = "1")]
    pub hit_count: u64,
    #[prost(uint64, tag = "2")]
    pub scanned_count: u64,
    #[prost(uint64, tag = "3")]
    pub result_memory_size: u64,
    #[prost(uint64, tag = "4")]
    pub max_result_memory_size: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stream {
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<Event>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Event {
    #[prost(message, optional, tag = "1")]
    pub tracker: ::core::option::Option<EventTracker>,
    #[prost(string, tag = "2")]
    pub content_json: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventTracker {
    /// A unique id tied to the event.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// The epoch in milliseconds marking when the event was created.
    #[prost(uint64, tag = "2")]
    pub epoch_ms: u64,
    /// An extra int to break ties.
    #[prost(uint32, tag = "3")]
    pub tiebreaker: u32,
    #[prost(string, optional, tag = "4")]
    pub fragment_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "5")]
    pub row_number: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchOneRequest {
    #[prost(message, optional, tag = "1")]
    pub event_tracker: ::core::option::Option<EventTracker>,
    #[prost(int64, tag = "2")]
    pub org_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchOneResponse {
    #[prost(message, optional, tag = "1")]
    pub event: ::core::option::Option<Event>,
    #[prost(message, optional, tag = "2")]
    pub statistics: ::core::option::Option<Statistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregationRequest {
    /// this is always a com.dd.queryparser.proto.QueryNode, but we can't import logs-backend protobuf from here
    #[prost(message, optional, tag = "1")]
    pub query: ::core::option::Option<::prost_types::Any>,
    #[prost(message, optional, tag = "2")]
    pub aggregation: ::core::option::Option<Aggregation>,
    #[prost(int64, tag = "3")]
    pub org_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Aggregation {
    #[prost(oneof = "aggregation::Aggregation", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    pub aggregation: ::core::option::Option<aggregation::Aggregation>,
}
/// Nested message and enum types in `Aggregation`.
pub mod aggregation {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Aggregation {
        #[prost(message, tag = "1")]
        AttributeGroupBy(::prost::alloc::boxed::Box<super::AttributeGroupBy>),
        #[prost(message, tag = "2")]
        TimeGroupBy(::prost::alloc::boxed::Box<super::TimeGrouping>),
        #[prost(message, tag = "3")]
        HistogramGroupBy(super::HistogramGroupBy),
        #[prost(message, tag = "4")]
        FlatFieldsGroupBy(::prost::alloc::boxed::Box<super::FlatFieldsGroupBy>),
        #[prost(message, tag = "5")]
        Computes(super::Computes),
        #[prost(message, tag = "6")]
        ListCompute(super::ListCompute),
        #[prost(message, tag = "7")]
        AnyCompute(super::AnyCompute),
        #[prost(message, tag = "8")]
        MetricCompute(super::MetricCompute),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AttributeGroupBy {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<ExpressionNode>,
    #[prost(uint32, tag = "2")]
    pub limit: u32,
    #[prost(message, optional, tag = "3")]
    pub sort: ::core::option::Option<SortByExprAndAgg>,
    #[prost(string, optional, tag = "4")]
    pub missing: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "5")]
    pub total: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, boxed, tag = "6")]
    pub child: ::core::option::Option<::prost::alloc::boxed::Box<Aggregation>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeGrouping {
    #[prost(string, tag = "1")]
    pub output: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub time_zone: ::prost::alloc::string::String,
    #[prost(uint64, optional, tag = "4")]
    pub interval_ns: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "5")]
    pub rollup: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, boxed, tag = "6")]
    pub child: ::core::option::Option<::prost::alloc::boxed::Box<Aggregation>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HistogramGroupBy {
    #[prost(string, tag = "1")]
    pub output: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub attribute: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub bucket: ::core::option::Option<Bucket>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Bucket {
    #[prost(double, tag = "1")]
    pub interval: f64,
    #[prost(double, tag = "2")]
    pub min: f64,
    #[prost(double, tag = "3")]
    pub max: f64,
    #[prost(bool, tag = "4")]
    pub with_out_of_bounds_bucket: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlatFieldsGroupBy {
    #[prost(message, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
    #[prost(string, repeated, tag = "2")]
    pub outputs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
    #[prost(message, optional, tag = "4")]
    pub sort: ::core::option::Option<SortByExprAndAgg>,
    #[prost(string, optional, tag = "5")]
    pub total: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, boxed, tag = "6")]
    pub child: ::core::option::Option<::prost::alloc::boxed::Box<Aggregation>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<ExpressionNode>,
    #[prost(string, optional, tag = "2")]
    pub missing: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Computes {
    #[prost(message, repeated, tag = "1")]
    pub aggregation: ::prost::alloc::vec::Vec<Aggregation>,
    #[prost(message, repeated, tag = "2")]
    pub time_grouping: ::prost::alloc::vec::Vec<TimeGrouping>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCompute {
    #[prost(message, optional, tag = "1")]
    pub sort: ::core::option::Option<ExpressionNode>,
    #[prost(message, repeated, tag = "2")]
    pub to_list: ::prost::alloc::vec::Vec<ExpressionNode>,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
    #[prost(bool, tag = "4")]
    pub ascending: bool,
    #[prost(string, tag = "5")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyCompute {
    #[prost(message, repeated, tag = "1")]
    pub to_list: ::prost::alloc::vec::Vec<ExpressionNode>,
    #[prost(uint32, tag = "2")]
    pub limit: u32,
    #[prost(string, tag = "3")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricCompute {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<ExpressionNode>,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub r#type: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregationResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<AggregationResult>,
    #[prost(message, optional, tag = "2")]
    pub statistics: ::core::option::Option<Statistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregationResult {
    #[prost(string, repeated, tag = "1")]
    pub key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub value: ::prost::alloc::vec::Vec<AggValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortByExprAndAgg {
    #[prost(bool, tag = "1")]
    pub ascending: bool,
    #[prost(message, optional, tag = "2")]
    pub expr_and_agg: ::core::option::Option<ExprAndAgg>,
    #[prost(enumeration = "SortType", tag = "3")]
    pub r#type: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExprAndAgg {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<ExpressionNode>,
    #[prost(string, tag = "2")]
    pub agg_function: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExpressionNode {
    /// this is always a com.dd.calc_fields.proto.CalcNode
    #[prost(message, optional, tag = "1")]
    pub calc_node: ::core::option::Option<::prost_types::Any>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggValue {
    #[prost(oneof = "agg_value::Value", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub value: ::core::option::Option<agg_value::Value>,
}
/// Nested message and enum types in `AggValue`.
pub mod agg_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(::prost::alloc::string::String),
        #[prost(int64, tag = "2")]
        Int64Value(i64),
        #[prost(uint64, tag = "3")]
        Uint64Value(u64),
        #[prost(double, tag = "4")]
        Float64Value(f64),
        #[prost(bytes, tag = "5")]
        SketchValue(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "6")]
        HllValue(::prost::alloc::vec::Vec<u8>),
        #[prost(message, tag = "7")]
        AvgValue(super::Avg),
        #[prost(message, tag = "8")]
        FirstLastValue(super::FirstLast),
        #[prost(message, tag = "9")]
        HighlightValue(super::Highlight),
        #[prost(bytes, tag = "10")]
        HllDataSketchValue(::prost::alloc::vec::Vec<u8>),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Highlight {
    /// We support multiple FTS columns, so in order to distinguish their highlighted version,
    /// we'll use a map from column name to highlighted text.
    #[prost(map = "string, string", tag = "1")]
    pub highlight: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Avg {
    #[prost(double, tag = "1")]
    pub sum: f64,
    #[prost(uint64, tag = "2")]
    pub count: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FirstLast {
    /// This is repeated in case the FIRST/LAST function was called with
    /// limit > 1. For now though, we only support limit == 1.
    #[prost(message, repeated, tag = "1")]
    pub entries: ::prost::alloc::vec::Vec<FirstLastEntry>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FirstLastEntry {
    #[prost(bytes = "vec", tag = "1")]
    pub sort_by: ::prost::alloc::vec::Vec<u8>,
    /// This field contains an encoded FirstLastValues message.
    #[prost(bytes = "vec", tag = "6")]
    pub encoded_values: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SortType {
    Invalid = 0,
    Time = 1,
    Field = 2,
    Metric = 3,
}
impl SortType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SortType::Invalid => "INVALID",
            SortType::Time => "TIME",
            SortType::Field => "FIELD",
            SortType::Metric => "METRIC",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INVALID" => Some(Self::Invalid),
            "TIME" => Some(Self::Time),
            "FIELD" => Some(Self::Field),
            "METRIC" => Some(Self::Metric),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
use quickwit_common::tower::RpcName;
impl RpcName for PingRequest {
    fn rpc_name() -> &'static str {
        "ping"
    }
}
impl RpcName for ListRequest {
    fn rpc_name() -> &'static str {
        "list"
    }
}
impl RpcName for FetchOneRequest {
    fn rpc_name() -> &'static str {
        "fetch_one"
    }
}
impl RpcName for AggregationRequest {
    fn rpc_name() -> &'static str {
        "aggregate"
    }
}
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait CloudPremService: std::fmt::Debug + Send + Sync + 'static {
    async fn ping(
        &self,
        request: PingRequest,
    ) -> crate::cloudprem::CloudPremResult<PingResponse>;
    async fn list(
        &self,
        request: ListRequest,
    ) -> crate::cloudprem::CloudPremResult<ListResponse>;
    async fn fetch_one(
        &self,
        request: FetchOneRequest,
    ) -> crate::cloudprem::CloudPremResult<FetchOneResponse>;
    async fn aggregate(
        &self,
        request: AggregationRequest,
    ) -> crate::cloudprem::CloudPremResult<AggregationResponse>;
}
#[derive(Debug, Clone)]
pub struct CloudPremServiceClient {
    inner: InnerCloudPremServiceClient,
}
#[derive(Debug, Clone)]
struct InnerCloudPremServiceClient(std::sync::Arc<dyn CloudPremService>);
impl CloudPremServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: CloudPremService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockCloudPremService > (),
            "`MockCloudPremService` must be wrapped in a `MockCloudPremServiceWrapper`: use `CloudPremServiceClient::from_mock(mock)` to instantiate the client"
        );
        Self {
            inner: InnerCloudPremServiceClient(std::sync::Arc::new(instance)),
        }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> cloud_prem_service_grpc_server::CloudPremServiceGrpcServer<
        CloudPremServiceGrpcServerAdapter,
    > {
        let adapter = CloudPremServiceGrpcServerAdapter::new(self.clone());
        cloud_prem_service_grpc_server::CloudPremServiceGrpcServer::new(adapter)
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
        let client = cloud_prem_service_grpc_client::CloudPremServiceGrpcClient::new(
                channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = CloudPremServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> CloudPremServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = cloud_prem_service_grpc_client::CloudPremServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = CloudPremServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        CloudPremServiceMailbox<A>: CloudPremService,
    {
        CloudPremServiceClient::new(CloudPremServiceMailbox::new(mailbox))
    }
    pub fn tower() -> CloudPremServiceTowerLayerStack {
        CloudPremServiceTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockCloudPremService) -> Self {
        let mock_wrapper = mock_cloud_prem_service::MockCloudPremServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        };
        Self::new(mock_wrapper)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockCloudPremService::new())
    }
}
#[async_trait::async_trait]
impl CloudPremService for CloudPremServiceClient {
    async fn ping(
        &self,
        request: PingRequest,
    ) -> crate::cloudprem::CloudPremResult<PingResponse> {
        self.inner.0.ping(request).await
    }
    async fn list(
        &self,
        request: ListRequest,
    ) -> crate::cloudprem::CloudPremResult<ListResponse> {
        self.inner.0.list(request).await
    }
    async fn fetch_one(
        &self,
        request: FetchOneRequest,
    ) -> crate::cloudprem::CloudPremResult<FetchOneResponse> {
        self.inner.0.fetch_one(request).await
    }
    async fn aggregate(
        &self,
        request: AggregationRequest,
    ) -> crate::cloudprem::CloudPremResult<AggregationResponse> {
        self.inner.0.aggregate(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock_cloud_prem_service {
    use super::*;
    #[derive(Debug)]
    pub struct MockCloudPremServiceWrapper {
        pub(super) inner: tokio::sync::Mutex<MockCloudPremService>,
    }
    #[async_trait::async_trait]
    impl CloudPremService for MockCloudPremServiceWrapper {
        async fn ping(
            &self,
            request: super::PingRequest,
        ) -> crate::cloudprem::CloudPremResult<super::PingResponse> {
            self.inner.lock().await.ping(request).await
        }
        async fn list(
            &self,
            request: super::ListRequest,
        ) -> crate::cloudprem::CloudPremResult<super::ListResponse> {
            self.inner.lock().await.list(request).await
        }
        async fn fetch_one(
            &self,
            request: super::FetchOneRequest,
        ) -> crate::cloudprem::CloudPremResult<super::FetchOneResponse> {
            self.inner.lock().await.fetch_one(request).await
        }
        async fn aggregate(
            &self,
            request: super::AggregationRequest,
        ) -> crate::cloudprem::CloudPremResult<super::AggregationResponse> {
            self.inner.lock().await.aggregate(request).await
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<PingRequest> for InnerCloudPremServiceClient {
    type Response = PingResponse;
    type Error = crate::cloudprem::CloudPremError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PingRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.ping(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListRequest> for InnerCloudPremServiceClient {
    type Response = ListResponse;
    type Error = crate::cloudprem::CloudPremError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<FetchOneRequest> for InnerCloudPremServiceClient {
    type Response = FetchOneResponse;
    type Error = crate::cloudprem::CloudPremError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: FetchOneRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.fetch_one(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AggregationRequest> for InnerCloudPremServiceClient {
    type Response = AggregationResponse;
    type Error = crate::cloudprem::CloudPremError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AggregationRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.aggregate(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct CloudPremServiceTowerServiceStack {
    #[allow(dead_code)]
    inner: InnerCloudPremServiceClient,
    ping_svc: quickwit_common::tower::BoxService<
        PingRequest,
        PingResponse,
        crate::cloudprem::CloudPremError,
    >,
    list_svc: quickwit_common::tower::BoxService<
        ListRequest,
        ListResponse,
        crate::cloudprem::CloudPremError,
    >,
    fetch_one_svc: quickwit_common::tower::BoxService<
        FetchOneRequest,
        FetchOneResponse,
        crate::cloudprem::CloudPremError,
    >,
    aggregate_svc: quickwit_common::tower::BoxService<
        AggregationRequest,
        AggregationResponse,
        crate::cloudprem::CloudPremError,
    >,
}
#[async_trait::async_trait]
impl CloudPremService for CloudPremServiceTowerServiceStack {
    async fn ping(
        &self,
        request: PingRequest,
    ) -> crate::cloudprem::CloudPremResult<PingResponse> {
        self.ping_svc.clone().ready().await?.call(request).await
    }
    async fn list(
        &self,
        request: ListRequest,
    ) -> crate::cloudprem::CloudPremResult<ListResponse> {
        self.list_svc.clone().ready().await?.call(request).await
    }
    async fn fetch_one(
        &self,
        request: FetchOneRequest,
    ) -> crate::cloudprem::CloudPremResult<FetchOneResponse> {
        self.fetch_one_svc.clone().ready().await?.call(request).await
    }
    async fn aggregate(
        &self,
        request: AggregationRequest,
    ) -> crate::cloudprem::CloudPremResult<AggregationResponse> {
        self.aggregate_svc.clone().ready().await?.call(request).await
    }
}
type PingLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        PingRequest,
        PingResponse,
        crate::cloudprem::CloudPremError,
    >,
    PingRequest,
    PingResponse,
    crate::cloudprem::CloudPremError,
>;
type ListLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListRequest,
        ListResponse,
        crate::cloudprem::CloudPremError,
    >,
    ListRequest,
    ListResponse,
    crate::cloudprem::CloudPremError,
>;
type FetchOneLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        FetchOneRequest,
        FetchOneResponse,
        crate::cloudprem::CloudPremError,
    >,
    FetchOneRequest,
    FetchOneResponse,
    crate::cloudprem::CloudPremError,
>;
type AggregateLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        AggregationRequest,
        AggregationResponse,
        crate::cloudprem::CloudPremError,
    >,
    AggregationRequest,
    AggregationResponse,
    crate::cloudprem::CloudPremError,
>;
#[derive(Debug, Default)]
pub struct CloudPremServiceTowerLayerStack {
    ping_layers: Vec<PingLayer>,
    list_layers: Vec<ListLayer>,
    fetch_one_layers: Vec<FetchOneLayer>,
    aggregate_layers: Vec<AggregateLayer>,
}
impl CloudPremServiceTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PingRequest,
                    PingResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                PingRequest,
                PingResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service: tower::Service<
                PingRequest,
                Response = PingResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                PingRequest,
                PingResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service as tower::Service<PingRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListRequest,
                    ListResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListRequest,
                ListResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service: tower::Service<
                ListRequest,
                Response = ListResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListRequest,
                ListResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service as tower::Service<ListRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FetchOneRequest,
                    FetchOneResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                FetchOneRequest,
                FetchOneResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service: tower::Service<
                FetchOneRequest,
                Response = FetchOneResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                FetchOneRequest,
                FetchOneResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service as tower::Service<FetchOneRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AggregationRequest,
                    AggregationResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                AggregationRequest,
                AggregationResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service: tower::Service<
                AggregationRequest,
                Response = AggregationResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                AggregationRequest,
                AggregationResponse,
                crate::cloudprem::CloudPremError,
            >,
        >>::Service as tower::Service<AggregationRequest>>::Future: Send + 'static,
    {
        self.ping_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.fetch_one_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.aggregate_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_ping_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PingRequest,
                    PingResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                PingRequest,
                Response = PingResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PingRequest>>::Future: Send + 'static,
    {
        self.ping_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListRequest,
                    ListResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListRequest,
                Response = ListResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListRequest>>::Future: Send + 'static,
    {
        self.list_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_fetch_one_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FetchOneRequest,
                    FetchOneResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                FetchOneRequest,
                Response = FetchOneResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<FetchOneRequest>>::Future: Send + 'static,
    {
        self.fetch_one_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_aggregate_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AggregationRequest,
                    AggregationResponse,
                    crate::cloudprem::CloudPremError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                AggregationRequest,
                Response = AggregationResponse,
                Error = crate::cloudprem::CloudPremError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AggregationRequest>>::Future: Send + 'static,
    {
        self.aggregate_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> CloudPremServiceClient
    where
        T: CloudPremService,
    {
        let inner_client = InnerCloudPremServiceClient(std::sync::Arc::new(instance));
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> CloudPremServiceClient {
        let client = CloudPremServiceClient::from_channel(
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
    ) -> CloudPremServiceClient {
        let client = CloudPremServiceClient::from_balance_channel(
            balance_channel,
            max_message_size,
        );
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> CloudPremServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        CloudPremServiceMailbox<A>: CloudPremService,
    {
        let inner_client = InnerCloudPremServiceClient(
            std::sync::Arc::new(CloudPremServiceMailbox::new(mailbox)),
        );
        self.build_from_inner_client(inner_client)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn build_from_mock(self, mock: MockCloudPremService) -> CloudPremServiceClient {
        let client = CloudPremServiceClient::from_mock(mock);
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    fn build_from_inner_client(
        self,
        inner_client: InnerCloudPremServiceClient,
    ) -> CloudPremServiceClient {
        let ping_svc = self
            .ping_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let list_svc = self
            .list_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let fetch_one_svc = self
            .fetch_one_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let aggregate_svc = self
            .aggregate_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = CloudPremServiceTowerServiceStack {
            inner: inner_client,
            ping_svc,
            list_svc,
            fetch_one_svc,
            aggregate_svc,
        };
        CloudPremServiceClient::new(tower_svc_stack)
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
pub struct CloudPremServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::cloudprem::CloudPremError>,
}
impl<A: quickwit_actors::Actor> CloudPremServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for CloudPremServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for CloudPremServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::cloudprem::CloudPremError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::cloudprem::CloudPremError;
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
impl<A> CloudPremService for CloudPremServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    CloudPremServiceMailbox<
        A,
    >: tower::Service<
            PingRequest,
            Response = PingResponse,
            Error = crate::cloudprem::CloudPremError,
            Future = BoxFuture<PingResponse, crate::cloudprem::CloudPremError>,
        >
        + tower::Service<
            ListRequest,
            Response = ListResponse,
            Error = crate::cloudprem::CloudPremError,
            Future = BoxFuture<ListResponse, crate::cloudprem::CloudPremError>,
        >
        + tower::Service<
            FetchOneRequest,
            Response = FetchOneResponse,
            Error = crate::cloudprem::CloudPremError,
            Future = BoxFuture<FetchOneResponse, crate::cloudprem::CloudPremError>,
        >
        + tower::Service<
            AggregationRequest,
            Response = AggregationResponse,
            Error = crate::cloudprem::CloudPremError,
            Future = BoxFuture<AggregationResponse, crate::cloudprem::CloudPremError>,
        >,
{
    async fn ping(
        &self,
        request: PingRequest,
    ) -> crate::cloudprem::CloudPremResult<PingResponse> {
        self.clone().call(request).await
    }
    async fn list(
        &self,
        request: ListRequest,
    ) -> crate::cloudprem::CloudPremResult<ListResponse> {
        self.clone().call(request).await
    }
    async fn fetch_one(
        &self,
        request: FetchOneRequest,
    ) -> crate::cloudprem::CloudPremResult<FetchOneResponse> {
        self.clone().call(request).await
    }
    async fn aggregate(
        &self,
        request: AggregationRequest,
    ) -> crate::cloudprem::CloudPremResult<AggregationResponse> {
        self.clone().call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct CloudPremServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> CloudPremServiceGrpcClientAdapter<T> {
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
impl<T> CloudPremService
for CloudPremServiceGrpcClientAdapter<
    cloud_prem_service_grpc_client::CloudPremServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn ping(
        &self,
        request: PingRequest,
    ) -> crate::cloudprem::CloudPremResult<PingResponse> {
        self.inner
            .clone()
            .ping(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                PingRequest::rpc_name(),
            ))
    }
    async fn list(
        &self,
        request: ListRequest,
    ) -> crate::cloudprem::CloudPremResult<ListResponse> {
        self.inner
            .clone()
            .list(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListRequest::rpc_name(),
            ))
    }
    async fn fetch_one(
        &self,
        request: FetchOneRequest,
    ) -> crate::cloudprem::CloudPremResult<FetchOneResponse> {
        self.inner
            .clone()
            .fetch_one(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                FetchOneRequest::rpc_name(),
            ))
    }
    async fn aggregate(
        &self,
        request: AggregationRequest,
    ) -> crate::cloudprem::CloudPremResult<AggregationResponse> {
        self.inner
            .clone()
            .aggregate(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                AggregationRequest::rpc_name(),
            ))
    }
}
#[derive(Debug)]
pub struct CloudPremServiceGrpcServerAdapter {
    inner: InnerCloudPremServiceClient,
}
impl CloudPremServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: CloudPremService,
    {
        Self {
            inner: InnerCloudPremServiceClient(std::sync::Arc::new(instance)),
        }
    }
}
#[async_trait::async_trait]
impl cloud_prem_service_grpc_server::CloudPremServiceGrpc
for CloudPremServiceGrpcServerAdapter {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        self.inner
            .0
            .ping(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list(
        &self,
        request: tonic::Request<ListRequest>,
    ) -> Result<tonic::Response<ListResponse>, tonic::Status> {
        self.inner
            .0
            .list(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn fetch_one(
        &self,
        request: tonic::Request<FetchOneRequest>,
    ) -> Result<tonic::Response<FetchOneResponse>, tonic::Status> {
        self.inner
            .0
            .fetch_one(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn aggregate(
        &self,
        request: tonic::Request<AggregationRequest>,
    ) -> Result<tonic::Response<AggregationResponse>, tonic::Status> {
        self.inner
            .0
            .aggregate(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
}
/// Generated client implementations.
pub mod cloud_prem_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CloudPremServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CloudPremServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> CloudPremServiceGrpcClient<T>
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
        ) -> CloudPremServiceGrpcClient<InterceptedService<T, F>>
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
            CloudPremServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn ping(
            &mut self,
            request: impl tonic::IntoRequest<super::PingRequest>,
        ) -> std::result::Result<tonic::Response<super::PingResponse>, tonic::Status> {
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
                "/cloudprem.CloudPremService/Ping",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cloudprem.CloudPremService", "Ping"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<super::ListRequest>,
        ) -> std::result::Result<tonic::Response<super::ListResponse>, tonic::Status> {
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
                "/cloudprem.CloudPremService/List",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cloudprem.CloudPremService", "List"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn fetch_one(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchOneRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchOneResponse>,
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
                "/cloudprem.CloudPremService/FetchOne",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cloudprem.CloudPremService", "FetchOne"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn aggregate(
            &mut self,
            request: impl tonic::IntoRequest<super::AggregationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AggregationResponse>,
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
                "/cloudprem.CloudPremService/Aggregate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cloudprem.CloudPremService", "Aggregate"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod cloud_prem_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CloudPremServiceGrpcServer.
    #[async_trait]
    pub trait CloudPremServiceGrpc: Send + Sync + 'static {
        async fn ping(
            &self,
            request: tonic::Request<super::PingRequest>,
        ) -> std::result::Result<tonic::Response<super::PingResponse>, tonic::Status>;
        async fn list(
            &self,
            request: tonic::Request<super::ListRequest>,
        ) -> std::result::Result<tonic::Response<super::ListResponse>, tonic::Status>;
        async fn fetch_one(
            &self,
            request: tonic::Request<super::FetchOneRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchOneResponse>,
            tonic::Status,
        >;
        async fn aggregate(
            &self,
            request: tonic::Request<super::AggregationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AggregationResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CloudPremServiceGrpcServer<T: CloudPremServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CloudPremServiceGrpc> CloudPremServiceGrpcServer<T> {
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
    for CloudPremServiceGrpcServer<T>
    where
        T: CloudPremServiceGrpc,
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
                "/cloudprem.CloudPremService/Ping" => {
                    #[allow(non_camel_case_types)]
                    struct PingSvc<T: CloudPremServiceGrpc>(pub Arc<T>);
                    impl<
                        T: CloudPremServiceGrpc,
                    > tonic::server::UnaryService<super::PingRequest> for PingSvc<T> {
                        type Response = super::PingResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PingRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).ping(request).await };
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
                        let method = PingSvc(inner);
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
                "/cloudprem.CloudPremService/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: CloudPremServiceGrpc>(pub Arc<T>);
                    impl<
                        T: CloudPremServiceGrpc,
                    > tonic::server::UnaryService<super::ListRequest> for ListSvc<T> {
                        type Response = super::ListResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list(request).await };
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
                        let method = ListSvc(inner);
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
                "/cloudprem.CloudPremService/FetchOne" => {
                    #[allow(non_camel_case_types)]
                    struct FetchOneSvc<T: CloudPremServiceGrpc>(pub Arc<T>);
                    impl<
                        T: CloudPremServiceGrpc,
                    > tonic::server::UnaryService<super::FetchOneRequest>
                    for FetchOneSvc<T> {
                        type Response = super::FetchOneResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchOneRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).fetch_one(request).await };
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
                        let method = FetchOneSvc(inner);
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
                "/cloudprem.CloudPremService/Aggregate" => {
                    #[allow(non_camel_case_types)]
                    struct AggregateSvc<T: CloudPremServiceGrpc>(pub Arc<T>);
                    impl<
                        T: CloudPremServiceGrpc,
                    > tonic::server::UnaryService<super::AggregationRequest>
                    for AggregateSvc<T> {
                        type Response = super::AggregationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AggregationRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).aggregate(request).await };
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
                        let method = AggregateSvc(inner);
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
    impl<T: CloudPremServiceGrpc> Clone for CloudPremServiceGrpcServer<T> {
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
    impl<T: CloudPremServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CloudPremServiceGrpc> tonic::server::NamedService
    for CloudPremServiceGrpcServer<T> {
        const NAME: &'static str = "cloudprem.CloudPremService";
    }
}
