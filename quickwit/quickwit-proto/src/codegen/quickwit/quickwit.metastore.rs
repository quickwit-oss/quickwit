#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyResponse {}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(string, tag = "2")]
    pub index_config_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadatasRequest {
    #[prost(string, tag = "1")]
    pub query_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadatasResponse {
    #[prost(string, tag = "1")]
    pub indexes_metadatas_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataResponse {
    #[prost(string, tag = "1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListAllSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsRequest {
    #[prost(string, tag = "1")]
    pub query_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsResponse {
    #[prost(string, tag = "1")]
    pub splits_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub split_metadata_list_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub staged_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub replaced_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "4")]
    pub index_checkpoint_delta_json_opt: ::core::option::Option<
        ::prost::alloc::string::String,
    >,
    #[prost(string, optional, tag = "5")]
    pub publish_token_opt: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MarkSplitsForDeletionRequest {
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSplitsRequest {
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_config_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ToggleSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub enable: bool,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetSourceCheckpointRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteTask {
    #[prost(int64, tag = "1")]
    pub create_timestamp: i64,
    #[prost(uint64, tag = "2")]
    pub opstamp: u64,
    #[prost(message, optional, tag = "3")]
    pub delete_query: ::core::option::Option<DeleteQuery>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteQuery {
    /// Index ID.
    #[prost(string, tag = "1")]
    #[serde(alias = "index_id")]
    pub index_uid: ::prost::alloc::string::String,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    #[prost(int64, optional, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: ::core::option::Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    #[prost(int64, optional, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Query text. The query language is that of tantivy.
    /// Query AST serialized in JSON
    #[prost(string, tag = "6")]
    #[serde(alias = "query")]
    pub query_ast: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint64, tag = "3")]
    pub delete_opstamp: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampResponse {}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampResponse {
    #[prost(uint64, tag = "1")]
    pub last_delete_opstamp: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListStaleSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub delete_opstamp: u64,
    #[prost(uint64, tag = "3")]
    pub num_splits: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub opstamp_start: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksResponse {
    #[prost(message, repeated, tag = "1")]
    pub delete_tasks: ::prost::alloc::vec::Vec<DeleteTask>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<OpenShardsSubrequest>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "4")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, tag = "5")]
    pub next_shard_id: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<OpenShardsSubresponse>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub open_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
    #[prost(uint64, tag = "4")]
    pub next_shard_id: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<AcquireShardsSubrequest>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(string, tag = "4")]
    pub publish_token: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<AcquireShardsSubresponse>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub acquired_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<CloseShardsSubrequest>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(enumeration = "super::ingest::ShardState", tag = "4")]
    pub shard_state: i32,
    #[prost(uint64, optional, tag = "5")]
    pub replication_position_inclusive: ::core::option::Option<u64>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub successes: ::prost::alloc::vec::Vec<CloseShardsSuccess>,
    #[prost(message, repeated, tag = "2")]
    pub failures: ::prost::alloc::vec::Vec<CloseShardsFailure>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsSuccess {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(string, tag = "4")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "5")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, tag = "6")]
    pub publish_position_inclusive: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsFailure {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(enumeration = "CloseShardsFailureKind", tag = "4")]
    pub failure_kind: i32,
    #[prost(string, tag = "5")]
    pub failure_message: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<DeleteShardsSubrequest>,
    #[prost(bool, tag = "2")]
    pub force: bool,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsResponse {}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<ListShardsSubrequest>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::ingest::ShardState", optional, tag = "3")]
    pub shard_state: ::core::option::Option<i32>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<ListShardsSubresponse>,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
    #[prost(uint64, tag = "4")]
    pub next_shard_id: u64,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    Cli = 0,
    File = 1,
    GcpPubsub = 2,
    IngestV1 = 3,
    IngestV2 = 4,
    Kafka = 5,
    Kinesis = 6,
    Nats = 7,
    Pulsar = 8,
    Vec = 9,
    Void = 10,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SourceType::Cli => "CLI",
            SourceType::File => "FILE",
            SourceType::GcpPubsub => "GCP_PUBSUB",
            SourceType::IngestV1 => "INGEST_V1",
            SourceType::IngestV2 => "INGEST_V2",
            SourceType::Kafka => "KAFKA",
            SourceType::Kinesis => "KINESIS",
            SourceType::Nats => "NATS",
            SourceType::Pulsar => "PULSAR",
            SourceType::Vec => "VEC",
            SourceType::Void => "VOID",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CLI" => Some(Self::Cli),
            "FILE" => Some(Self::File),
            "GCP_PUBSUB" => Some(Self::GcpPubsub),
            "INGEST_V1" => Some(Self::IngestV1),
            "INGEST_V2" => Some(Self::IngestV2),
            "KAFKA" => Some(Self::Kafka),
            "KINESIS" => Some(Self::Kinesis),
            "NATS" => Some(Self::Nats),
            "PULSAR" => Some(Self::Pulsar),
            "VEC" => Some(Self::Vec),
            "VOID" => Some(Self::Void),
            _ => None,
        }
    }
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CloseShardsFailureKind {
    InvalidArgument = 0,
    NotFound = 1,
}
impl CloseShardsFailureKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CloseShardsFailureKind::InvalidArgument => "INVALID_ARGUMENT",
            CloseShardsFailureKind::NotFound => "NOT_FOUND",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INVALID_ARGUMENT" => Some(Self::InvalidArgument),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod metastore_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct MetastoreServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetastoreServiceClient<tonic::transport::Channel> {
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
    impl<T> MetastoreServiceClient<T>
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
        ) -> MetastoreServiceClient<InterceptedService<T, F>>
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
            MetastoreServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Creates an index.
        pub async fn create_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
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
                "/quickwit.metastore.MetastoreService/create_index",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "create_index",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets an index metadata.
        pub async fn index_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
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
                "/quickwit.metastore.MetastoreService/index_metadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "index_metadata",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets an indexes metadatas.
        pub async fn list_indexes_metadatas(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexesMetadatasRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadatasResponse>,
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
                "/quickwit.metastore.MetastoreService/list_indexes_metadatas",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_indexes_metadatas",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes an index
        pub async fn delete_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteIndexRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/delete_index",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_index",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets all splits from index.
        pub async fn list_all_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListAllSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
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
                "/quickwit.metastore.MetastoreService/list_all_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_all_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets splits from index.
        pub async fn list_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
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
                "/quickwit.metastore.MetastoreService/list_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "list_splits"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Stages several splits.
        pub async fn stage_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::StageSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/stage_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "stage_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Publishes split.
        pub async fn publish_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::PublishSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/publish_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "publish_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Marks splits for deletion.
        pub async fn mark_splits_for_deletion(
            &mut self,
            request: impl tonic::IntoRequest<super::MarkSplitsForDeletionRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/mark_splits_for_deletion",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "mark_splits_for_deletion",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes splits.
        pub async fn delete_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/delete_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Adds source.
        pub async fn add_source(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/add_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "add_source"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Toggles source.
        pub async fn toggle_source(
            &mut self,
            request: impl tonic::IntoRequest<super::ToggleSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/toggle_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "toggle_source",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Removes source.
        pub async fn delete_source(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/delete_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_source",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Resets source checkpoint.
        pub async fn reset_source_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::ResetSourceCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/reset_source_checkpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "reset_source_checkpoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets last opstamp for a given `index_id`.
        pub async fn last_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::LastDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LastDeleteOpstampResponse>,
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
                "/quickwit.metastore.MetastoreService/last_delete_opstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "last_delete_opstamp",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Creates a delete task.
        pub async fn create_delete_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteQuery>,
        ) -> std::result::Result<tonic::Response<super::DeleteTask>, tonic::Status> {
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
                "/quickwit.metastore.MetastoreService/create_delete_task",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "create_delete_task",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Updates splits `delete_opstamp`.
        pub async fn update_splits_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
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
                "/quickwit.metastore.MetastoreService/update_splits_delete_opstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "update_splits_delete_opstamp",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        pub async fn list_delete_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::ListDeleteTasksRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDeleteTasksResponse>,
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
                "/quickwit.metastore.MetastoreService/list_delete_tasks",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_delete_tasks",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// / Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        pub async fn list_stale_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListStaleSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
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
                "/quickwit.metastore.MetastoreService/list_stale_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_stale_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn open_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::OpenShardsResponse>,
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
                "/quickwit.metastore.MetastoreService/OpenShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "OpenShards"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
        /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
        /// list of acquired shards along with the positions to index from.
        pub async fn acquire_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::AcquireShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AcquireShardsResponse>,
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
                "/quickwit.metastore.MetastoreService/AcquireShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "AcquireShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn close_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CloseShardsResponse>,
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
                "/quickwit.metastore.MetastoreService/CloseShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "CloseShards"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardsResponse>,
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
                "/quickwit.metastore.MetastoreService/DeleteShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "DeleteShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
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
                "/quickwit.metastore.MetastoreService/ListShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "ListShards"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod metastore_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MetastoreServiceServer.
    #[async_trait]
    pub trait MetastoreService: Send + Sync + 'static {
        /// Creates an index.
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        >;
        /// Gets an index metadata.
        async fn index_metadata(
            &self,
            request: tonic::Request<super::IndexMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
            tonic::Status,
        >;
        /// Gets an indexes metadatas.
        async fn list_indexes_metadatas(
            &self,
            request: tonic::Request<super::ListIndexesMetadatasRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadatasResponse>,
            tonic::Status,
        >;
        /// Deletes an index
        async fn delete_index(
            &self,
            request: tonic::Request<super::DeleteIndexRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Gets all splits from index.
        async fn list_all_splits(
            &self,
            request: tonic::Request<super::ListAllSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        /// Gets splits from index.
        async fn list_splits(
            &self,
            request: tonic::Request<super::ListSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        /// Stages several splits.
        async fn stage_splits(
            &self,
            request: tonic::Request<super::StageSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Publishes split.
        async fn publish_splits(
            &self,
            request: tonic::Request<super::PublishSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Marks splits for deletion.
        async fn mark_splits_for_deletion(
            &self,
            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Deletes splits.
        async fn delete_splits(
            &self,
            request: tonic::Request<super::DeleteSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Adds source.
        async fn add_source(
            &self,
            request: tonic::Request<super::AddSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Toggles source.
        async fn toggle_source(
            &self,
            request: tonic::Request<super::ToggleSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Removes source.
        async fn delete_source(
            &self,
            request: tonic::Request<super::DeleteSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Resets source checkpoint.
        async fn reset_source_checkpoint(
            &self,
            request: tonic::Request<super::ResetSourceCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Gets last opstamp for a given `index_id`.
        async fn last_delete_opstamp(
            &self,
            request: tonic::Request<super::LastDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LastDeleteOpstampResponse>,
            tonic::Status,
        >;
        /// Creates a delete task.
        async fn create_delete_task(
            &self,
            request: tonic::Request<super::DeleteQuery>,
        ) -> std::result::Result<tonic::Response<super::DeleteTask>, tonic::Status>;
        /// Updates splits `delete_opstamp`.
        async fn update_splits_delete_opstamp(
            &self,
            request: tonic::Request<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
            tonic::Status,
        >;
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        async fn list_delete_tasks(
            &self,
            request: tonic::Request<super::ListDeleteTasksRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDeleteTasksResponse>,
            tonic::Status,
        >;
        /// / Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        async fn list_stale_splits(
            &self,
            request: tonic::Request<super::ListStaleSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        async fn open_shards(
            &self,
            request: tonic::Request<super::OpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::OpenShardsResponse>,
            tonic::Status,
        >;
        /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
        /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
        /// list of acquired shards along with the positions to index from.
        async fn acquire_shards(
            &self,
            request: tonic::Request<super::AcquireShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AcquireShardsResponse>,
            tonic::Status,
        >;
        async fn close_shards(
            &self,
            request: tonic::Request<super::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CloseShardsResponse>,
            tonic::Status,
        >;
        async fn delete_shards(
            &self,
            request: tonic::Request<super::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardsResponse>,
            tonic::Status,
        >;
        async fn list_shards(
            &self,
            request: tonic::Request<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct MetastoreServiceServer<T: MetastoreService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetastoreService> MetastoreServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MetastoreServiceServer<T>
    where
        T: MetastoreService,
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
                "/quickwit.metastore.MetastoreService/create_index" => {
                    #[allow(non_camel_case_types)]
                    struct create_indexSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::CreateIndexRequest>
                    for create_indexSvc<T> {
                        type Response = super::CreateIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateIndexRequest>,
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
                        let method = create_indexSvc(inner);
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
                "/quickwit.metastore.MetastoreService/index_metadata" => {
                    #[allow(non_camel_case_types)]
                    struct index_metadataSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::IndexMetadataRequest>
                    for index_metadataSvc<T> {
                        type Response = super::IndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).index_metadata(request).await
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
                        let method = index_metadataSvc(inner);
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
                "/quickwit.metastore.MetastoreService/list_indexes_metadatas" => {
                    #[allow(non_camel_case_types)]
                    struct list_indexes_metadatasSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListIndexesMetadatasRequest>
                    for list_indexes_metadatasSvc<T> {
                        type Response = super::ListIndexesMetadatasResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexesMetadatasRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_indexes_metadatas(request).await
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
                        let method = list_indexes_metadatasSvc(inner);
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
                "/quickwit.metastore.MetastoreService/delete_index" => {
                    #[allow(non_camel_case_types)]
                    struct delete_indexSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DeleteIndexRequest>
                    for delete_indexSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteIndexRequest>,
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
                        let method = delete_indexSvc(inner);
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
                "/quickwit.metastore.MetastoreService/list_all_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_all_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListAllSplitsRequest>
                    for list_all_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListAllSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_all_splits(request).await
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
                        let method = list_all_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/list_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListSplitsRequest>
                    for list_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list_splits(request).await };
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
                        let method = list_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/stage_splits" => {
                    #[allow(non_camel_case_types)]
                    struct stage_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::StageSplitsRequest>
                    for stage_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StageSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).stage_splits(request).await
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
                        let method = stage_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/publish_splits" => {
                    #[allow(non_camel_case_types)]
                    struct publish_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::PublishSplitsRequest>
                    for publish_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PublishSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).publish_splits(request).await
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
                        let method = publish_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/mark_splits_for_deletion" => {
                    #[allow(non_camel_case_types)]
                    struct mark_splits_for_deletionSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::MarkSplitsForDeletionRequest>
                    for mark_splits_for_deletionSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).mark_splits_for_deletion(request).await
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
                        let method = mark_splits_for_deletionSvc(inner);
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
                "/quickwit.metastore.MetastoreService/delete_splits" => {
                    #[allow(non_camel_case_types)]
                    struct delete_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DeleteSplitsRequest>
                    for delete_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_splits(request).await
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
                        let method = delete_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/add_source" => {
                    #[allow(non_camel_case_types)]
                    struct add_sourceSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::AddSourceRequest>
                    for add_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSourceRequest>,
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
                        let method = add_sourceSvc(inner);
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
                "/quickwit.metastore.MetastoreService/toggle_source" => {
                    #[allow(non_camel_case_types)]
                    struct toggle_sourceSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ToggleSourceRequest>
                    for toggle_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ToggleSourceRequest>,
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
                        let method = toggle_sourceSvc(inner);
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
                "/quickwit.metastore.MetastoreService/delete_source" => {
                    #[allow(non_camel_case_types)]
                    struct delete_sourceSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DeleteSourceRequest>
                    for delete_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSourceRequest>,
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
                        let method = delete_sourceSvc(inner);
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
                "/quickwit.metastore.MetastoreService/reset_source_checkpoint" => {
                    #[allow(non_camel_case_types)]
                    struct reset_source_checkpointSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ResetSourceCheckpointRequest>
                    for reset_source_checkpointSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResetSourceCheckpointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).reset_source_checkpoint(request).await
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
                        let method = reset_source_checkpointSvc(inner);
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
                "/quickwit.metastore.MetastoreService/last_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct last_delete_opstampSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::LastDeleteOpstampRequest>
                    for last_delete_opstampSvc<T> {
                        type Response = super::LastDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LastDeleteOpstampRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).last_delete_opstamp(request).await
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
                        let method = last_delete_opstampSvc(inner);
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
                "/quickwit.metastore.MetastoreService/create_delete_task" => {
                    #[allow(non_camel_case_types)]
                    struct create_delete_taskSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DeleteQuery>
                    for create_delete_taskSvc<T> {
                        type Response = super::DeleteTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteQuery>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).create_delete_task(request).await
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
                        let method = create_delete_taskSvc(inner);
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
                "/quickwit.metastore.MetastoreService/update_splits_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct update_splits_delete_opstampSvc<T: MetastoreService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<
                        super::UpdateSplitsDeleteOpstampRequest,
                    > for update_splits_delete_opstampSvc<T> {
                        type Response = super::UpdateSplitsDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UpdateSplitsDeleteOpstampRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).update_splits_delete_opstamp(request).await
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
                        let method = update_splits_delete_opstampSvc(inner);
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
                "/quickwit.metastore.MetastoreService/list_delete_tasks" => {
                    #[allow(non_camel_case_types)]
                    struct list_delete_tasksSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListDeleteTasksRequest>
                    for list_delete_tasksSvc<T> {
                        type Response = super::ListDeleteTasksResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListDeleteTasksRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_delete_tasks(request).await
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
                        let method = list_delete_tasksSvc(inner);
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
                "/quickwit.metastore.MetastoreService/list_stale_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_stale_splitsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListStaleSplitsRequest>
                    for list_stale_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListStaleSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_stale_splits(request).await
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
                        let method = list_stale_splitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/OpenShards" => {
                    #[allow(non_camel_case_types)]
                    struct OpenShardsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::OpenShardsRequest>
                    for OpenShardsSvc<T> {
                        type Response = super::OpenShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).open_shards(request).await };
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
                        let method = OpenShardsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/AcquireShards" => {
                    #[allow(non_camel_case_types)]
                    struct AcquireShardsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::AcquireShardsRequest>
                    for AcquireShardsSvc<T> {
                        type Response = super::AcquireShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AcquireShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).acquire_shards(request).await
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
                        let method = AcquireShardsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/CloseShards" => {
                    #[allow(non_camel_case_types)]
                    struct CloseShardsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::CloseShardsRequest>
                    for CloseShardsSvc<T> {
                        type Response = super::CloseShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CloseShardsRequest>,
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
                "/quickwit.metastore.MetastoreService/DeleteShards" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DeleteShardsRequest>
                    for DeleteShardsSvc<T> {
                        type Response = super::DeleteShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteShardsRequest>,
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
                "/quickwit.metastore.MetastoreService/ListShards" => {
                    #[allow(non_camel_case_types)]
                    struct ListShardsSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::ListShardsRequest>
                    for ListShardsSvc<T> {
                        type Response = super::ListShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list_shards(request).await };
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
                        let method = ListShardsSvc(inner);
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
    impl<T: MetastoreService> Clone for MetastoreServiceServer<T> {
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
    impl<T: MetastoreService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetastoreService> tonic::server::NamedService for MetastoreServiceServer<T> {
        const NAME: &'static str = "quickwit.metastore.MetastoreService";
    }
}
