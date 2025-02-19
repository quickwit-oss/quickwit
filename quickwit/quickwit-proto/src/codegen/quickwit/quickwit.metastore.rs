#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(string, tag = "2")]
    pub index_config_json: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub source_configs_json: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub index_metadata_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateIndexRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub search_settings_json: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "3")]
    pub retention_policy_json: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, tag = "4")]
    pub indexing_settings_json: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub doc_mapping_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadataRequest {
    /// List of patterns an index should match or not match to get considered
    /// An index must match at least one positive pattern (a pattern not starting
    /// with a '-'), and no negative pattern (a pattern starting with a '-').
    #[prost(string, repeated, tag = "2")]
    pub index_id_patterns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadataResponse {
    /// Deprecated (v0.9.0), use `indexes_metadata_json_zstd` instead.
    #[prost(string, optional, tag = "1")]
    pub indexes_metadata_json_opt: ::core::option::Option<
        ::prost::alloc::string::String,
    >,
    /// A JSON serialized then ZSTD compressed list of `IndexMetadata`: `Vec<IndexMetadata> | JSON | ZSTD`.
    /// We don't use `repeated` here to increase the compression rate and ratio.
    #[prost(bytes = "bytes", tag = "2")]
    pub indexes_metadata_json_zstd: ::prost::bytes::Bytes,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
}
/// Request the metadata of an index.
/// Either `index_uid` or `index_id` must be specified.
///
/// If both are supplied, `index_uid` is used.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataRequest {
    #[prost(string, optional, tag = "1")]
    pub index_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataResponse {
    #[prost(string, tag = "1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexesMetadataRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<IndexMetadataSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataSubrequest {
    #[prost(string, optional, tag = "1")]
    pub index_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexesMetadataResponse {
    /// A JSON serialized then ZSTD compressed list of `IndexMetadata`: `Vec<IndexMetadata> | JSON | ZSTD`.
    /// We don't use `repeated` here to increase the compression rate and ratio.
    #[prost(bytes = "bytes", tag = "1")]
    pub indexes_metadata_json_zstd: ::prost::bytes::Bytes,
    #[prost(message, repeated, tag = "2")]
    pub failures: ::prost::alloc::vec::Vec<IndexMetadataFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataFailure {
    #[prost(string, optional, tag = "1")]
    pub index_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(enumeration = "IndexMetadataFailureReason", tag = "3")]
    pub reason: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsRequest {
    /// Predicate used to filter splits.
    /// The predicate is expressed as a JSON serialized
    /// `ListSplitsQuery`.
    #[prost(string, tag = "1")]
    pub query_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsResponse {
    /// TODO use repeated and encode splits json individually.
    #[prost(string, tag = "1")]
    pub splits_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageSplitsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub split_metadata_list_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishSplitsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
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
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MarkSplitsForDeletionRequest {
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSplitsRequest {
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_config_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_config_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ToggleSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub enable: bool,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetSourceCheckpointRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
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
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteQuery {
    /// Index UID.
    #[prost(message, optional, tag = "1")]
    #[schema(value_type = String)]
    #[serde(alias = "index_id")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    #[prost(int64, optional, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: ::core::option::Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    #[prost(int64, optional, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Query AST serialized in JSON
    #[prost(string, tag = "6")]
    #[serde(alias = "query")]
    pub query_ast: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, repeated, tag = "2")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint64, tag = "3")]
    pub delete_opstamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampResponse {
    #[prost(uint64, tag = "1")]
    pub last_delete_opstamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListStaleSplitsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(uint64, tag = "2")]
    pub delete_opstamp: u64,
    #[prost(uint64, tag = "3")]
    pub num_splits: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(uint64, tag = "2")]
    pub opstamp_start: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksResponse {
    #[prost(message, repeated, tag = "1")]
    pub delete_tasks: ::prost::alloc::vec::Vec<DeleteTask>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<OpenShardSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardSubrequest {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(message, optional, tag = "2")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub shard_id: ::core::option::Option<crate::types::ShardId>,
    #[prost(string, tag = "5")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "6")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "7")]
    pub doc_mapping_uid: ::core::option::Option<crate::types::DocMappingUid>,
    #[prost(string, optional, tag = "8")]
    pub publish_token: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<OpenShardSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardSubresponse {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(message, optional, tag = "4")]
    pub open_shard: ::core::option::Option<super::ingest::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<crate::types::ShardId>,
    #[prost(string, tag = "4")]
    pub publish_token: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsResponse {
    /// List of acquired shards, in no specific order.
    #[prost(message, repeated, tag = "3")]
    pub acquired_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<crate::types::ShardId>,
    /// If false, only shards at EOF positions will be deleted.
    #[prost(bool, tag = "4")]
    pub force: bool,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsResponse {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    /// List of shard IDs that were successfully deleted.
    #[prost(message, repeated, tag = "3")]
    pub successes: ::prost::alloc::vec::Vec<crate::types::ShardId>,
    /// List of shard IDs that could not be deleted because `force` was set to `false` in the request,
    /// and the shards are not at EOF, i.e., not fully indexed.
    #[prost(message, repeated, tag = "4")]
    pub failures: ::prost::alloc::vec::Vec<crate::types::ShardId>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PruneShardsRequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    /// The maximum age of the shards to keep, in seconds.
    #[prost(uint32, optional, tag = "5")]
    pub max_age_secs: ::core::option::Option<u32>,
    /// The maximum number of the shards to keep. Delete older shards first.
    #[prost(uint32, optional, tag = "6")]
    pub max_count: ::core::option::Option<u32>,
    /// The interval between two pruning operations, in seconds.
    #[prost(uint32, optional, tag = "7")]
    pub interval_secs: ::core::option::Option<u32>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<ListShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubrequest {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::ingest::ShardState", optional, tag = "3")]
    pub shard_state: ::core::option::Option<i32>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<ListShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubresponse {
    #[prost(message, optional, tag = "1")]
    pub index_uid: ::core::option::Option<crate::types::IndexUid>,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexTemplateRequest {
    #[prost(string, tag = "1")]
    pub index_template_json: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub overwrite: bool,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexTemplateRequest {
    #[prost(string, tag = "1")]
    pub template_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexTemplateResponse {
    #[prost(string, tag = "1")]
    pub index_template_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindIndexTemplateMatchesRequest {
    #[prost(string, repeated, tag = "1")]
    pub index_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindIndexTemplateMatchesResponse {
    #[prost(message, repeated, tag = "1")]
    pub matches: ::prost::alloc::vec::Vec<IndexTemplateMatch>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexTemplateMatch {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub template_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub index_template_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexTemplatesRequest {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexTemplatesResponse {
    #[prost(string, repeated, tag = "1")]
    pub index_templates_json: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexTemplatesRequest {
    #[prost(string, repeated, tag = "1")]
    pub template_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    Unspecified = 0,
    Cli = 1,
    File = 2,
    IngestV1 = 4,
    IngestV2 = 5,
    /// Apache Kafka
    Kafka = 6,
    /// Amazon Kinesis
    Kinesis = 7,
    Nats = 8,
    /// Google Cloud Pub/Sub
    PubSub = 3,
    /// Apache Pulsar
    Pulsar = 9,
    Vec = 10,
    Void = 11,
    Stdin = 13,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SourceType::Unspecified => "SOURCE_TYPE_UNSPECIFIED",
            SourceType::Cli => "SOURCE_TYPE_CLI",
            SourceType::File => "SOURCE_TYPE_FILE",
            SourceType::IngestV1 => "SOURCE_TYPE_INGEST_V1",
            SourceType::IngestV2 => "SOURCE_TYPE_INGEST_V2",
            SourceType::Kafka => "SOURCE_TYPE_KAFKA",
            SourceType::Kinesis => "SOURCE_TYPE_KINESIS",
            SourceType::Nats => "SOURCE_TYPE_NATS",
            SourceType::PubSub => "SOURCE_TYPE_PUB_SUB",
            SourceType::Pulsar => "SOURCE_TYPE_PULSAR",
            SourceType::Vec => "SOURCE_TYPE_VEC",
            SourceType::Void => "SOURCE_TYPE_VOID",
            SourceType::Stdin => "SOURCE_TYPE_STDIN",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SOURCE_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
            "SOURCE_TYPE_CLI" => Some(Self::Cli),
            "SOURCE_TYPE_FILE" => Some(Self::File),
            "SOURCE_TYPE_INGEST_V1" => Some(Self::IngestV1),
            "SOURCE_TYPE_INGEST_V2" => Some(Self::IngestV2),
            "SOURCE_TYPE_KAFKA" => Some(Self::Kafka),
            "SOURCE_TYPE_KINESIS" => Some(Self::Kinesis),
            "SOURCE_TYPE_NATS" => Some(Self::Nats),
            "SOURCE_TYPE_PUB_SUB" => Some(Self::PubSub),
            "SOURCE_TYPE_PULSAR" => Some(Self::Pulsar),
            "SOURCE_TYPE_VEC" => Some(Self::Vec),
            "SOURCE_TYPE_VOID" => Some(Self::Void),
            "SOURCE_TYPE_STDIN" => Some(Self::Stdin),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IndexMetadataFailureReason {
    Unspecified = 0,
    NotFound = 1,
    Internal = 2,
}
impl IndexMetadataFailureReason {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IndexMetadataFailureReason::Unspecified => {
                "INDEX_METADATA_FAILURE_REASON_UNSPECIFIED"
            }
            IndexMetadataFailureReason::NotFound => {
                "INDEX_METADATA_FAILURE_REASON_NOT_FOUND"
            }
            IndexMetadataFailureReason::Internal => {
                "INDEX_METADATA_FAILURE_REASON_INTERNAL"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INDEX_METADATA_FAILURE_REASON_UNSPECIFIED" => Some(Self::Unspecified),
            "INDEX_METADATA_FAILURE_REASON_NOT_FOUND" => Some(Self::NotFound),
            "INDEX_METADATA_FAILURE_REASON_INTERNAL" => Some(Self::Internal),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
use quickwit_common::tower::RpcName;
impl RpcName for CreateIndexRequest {
    fn rpc_name() -> &'static str {
        "create_index"
    }
}
impl RpcName for UpdateIndexRequest {
    fn rpc_name() -> &'static str {
        "update_index"
    }
}
impl RpcName for IndexMetadataRequest {
    fn rpc_name() -> &'static str {
        "index_metadata"
    }
}
impl RpcName for IndexesMetadataRequest {
    fn rpc_name() -> &'static str {
        "indexes_metadata"
    }
}
impl RpcName for ListIndexesMetadataRequest {
    fn rpc_name() -> &'static str {
        "list_indexes_metadata"
    }
}
impl RpcName for DeleteIndexRequest {
    fn rpc_name() -> &'static str {
        "delete_index"
    }
}
impl RpcName for ListSplitsRequest {
    fn rpc_name() -> &'static str {
        "list_splits"
    }
}
impl RpcName for StageSplitsRequest {
    fn rpc_name() -> &'static str {
        "stage_splits"
    }
}
impl RpcName for PublishSplitsRequest {
    fn rpc_name() -> &'static str {
        "publish_splits"
    }
}
impl RpcName for MarkSplitsForDeletionRequest {
    fn rpc_name() -> &'static str {
        "mark_splits_for_deletion"
    }
}
impl RpcName for DeleteSplitsRequest {
    fn rpc_name() -> &'static str {
        "delete_splits"
    }
}
impl RpcName for AddSourceRequest {
    fn rpc_name() -> &'static str {
        "add_source"
    }
}
impl RpcName for UpdateSourceRequest {
    fn rpc_name() -> &'static str {
        "update_source"
    }
}
impl RpcName for ToggleSourceRequest {
    fn rpc_name() -> &'static str {
        "toggle_source"
    }
}
impl RpcName for DeleteSourceRequest {
    fn rpc_name() -> &'static str {
        "delete_source"
    }
}
impl RpcName for ResetSourceCheckpointRequest {
    fn rpc_name() -> &'static str {
        "reset_source_checkpoint"
    }
}
impl RpcName for LastDeleteOpstampRequest {
    fn rpc_name() -> &'static str {
        "last_delete_opstamp"
    }
}
impl RpcName for DeleteQuery {
    fn rpc_name() -> &'static str {
        "create_delete_task"
    }
}
impl RpcName for UpdateSplitsDeleteOpstampRequest {
    fn rpc_name() -> &'static str {
        "update_splits_delete_opstamp"
    }
}
impl RpcName for ListDeleteTasksRequest {
    fn rpc_name() -> &'static str {
        "list_delete_tasks"
    }
}
impl RpcName for ListStaleSplitsRequest {
    fn rpc_name() -> &'static str {
        "list_stale_splits"
    }
}
impl RpcName for OpenShardsRequest {
    fn rpc_name() -> &'static str {
        "open_shards"
    }
}
impl RpcName for AcquireShardsRequest {
    fn rpc_name() -> &'static str {
        "acquire_shards"
    }
}
impl RpcName for DeleteShardsRequest {
    fn rpc_name() -> &'static str {
        "delete_shards"
    }
}
impl RpcName for PruneShardsRequest {
    fn rpc_name() -> &'static str {
        "prune_shards"
    }
}
impl RpcName for ListShardsRequest {
    fn rpc_name() -> &'static str {
        "list_shards"
    }
}
impl RpcName for CreateIndexTemplateRequest {
    fn rpc_name() -> &'static str {
        "create_index_template"
    }
}
impl RpcName for GetIndexTemplateRequest {
    fn rpc_name() -> &'static str {
        "get_index_template"
    }
}
impl RpcName for FindIndexTemplateMatchesRequest {
    fn rpc_name() -> &'static str {
        "find_index_template_matches"
    }
}
impl RpcName for ListIndexTemplatesRequest {
    fn rpc_name() -> &'static str {
        "list_index_templates"
    }
}
impl RpcName for DeleteIndexTemplatesRequest {
    fn rpc_name() -> &'static str {
        "delete_index_templates"
    }
}
pub type MetastoreServiceStream<T> = quickwit_common::ServiceStream<
    crate::metastore::MetastoreResult<T>,
>;
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait MetastoreService: std::fmt::Debug + Send + Sync + 'static {
    /// Creates an index.
    ///
    /// This API creates a new index in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse>;
    /// Update an index.
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse>;
    /// Returns the `IndexMetadata` of an index identified by its IndexID or its IndexUID.
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse>;
    /// Fetches the metadata of a list of indexes identified by their Index IDs or UIDs.
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexesMetadataResponse>;
    /// Gets an indexes metadatas.
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse>;
    /// Deletes an index
    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Streams splits from index.
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<MetastoreServiceStream<ListSplitsResponse>>;
    /// Stages several splits.
    async fn stage_splits(
        &self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Publishes split.
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Marks splits for deletion.
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Deletes splits.
    async fn delete_splits(
        &self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Adds a source.
    async fn add_source(
        &self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Updates a source.
    async fn update_source(
        &self,
        request: UpdateSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Toggles (turns on or off) source.
    async fn toggle_source(
        &self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Removes source.
    async fn delete_source(
        &self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Resets source checkpoint.
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Gets last opstamp for a given `index_id`.
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse>;
    /// Creates a delete task.
    async fn create_delete_task(
        &self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask>;
    /// Updates splits `delete_opstamp`.
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse>;
    /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse>;
    /// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse>;
    /// Shard API
    ///
    /// Note that for the file-backed metastore implementation, the requests are not processed atomically.
    /// Indeed, each request comprises one or more subrequests that target different indexes and sources processed
    /// independently. Responses list the requests that succeeded or failed in the fields `successes` and
    /// `failures`.
    async fn open_shards(
        &self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse>;
    /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
    /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
    /// list of acquired shards along with the positions to index from.
    ///
    /// If a requested shard is missing, this method does not return an error. It should simply return the list of
    /// shards that were actually acquired.
    ///
    /// For this reason, AcquireShards.acquire_shards may return less subresponse than there was in the request.
    /// Also they may be returned in any order.
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse>;
    /// Deletes a set of shards. This RPC deletes the shards from the metastore.
    /// If the shard did not exist to begin with, the operation is successful and does not return any error.
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse>;
    /// Deletes outdated shards. This RPC deletes the shards from the metastore.
    async fn prune_shards(
        &self,
        request: PruneShardsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    async fn list_shards(
        &self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse>;
    /// Creates an index template.
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Fetches an index template.
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<GetIndexTemplateResponse>;
    /// Finds matching index templates.
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> crate::metastore::MetastoreResult<FindIndexTemplateMatchesResponse>;
    /// Returns all the index templates.
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexTemplatesResponse>;
    /// Deletes index templates.
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    async fn check_connectivity(&self) -> anyhow::Result<()>;
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri>;
}
#[derive(Debug, Clone)]
pub struct MetastoreServiceClient {
    inner: InnerMetastoreServiceClient,
}
#[derive(Debug, Clone)]
struct InnerMetastoreServiceClient(std::sync::Arc<dyn MetastoreService>);
impl MetastoreServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: MetastoreService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockMetastoreService > (),
            "`MockMetastoreService` must be wrapped in a `MockMetastoreServiceWrapper`: use `MetastoreServiceClient::from_mock(mock)` to instantiate the client"
        );
        Self {
            inner: InnerMetastoreServiceClient(std::sync::Arc::new(instance)),
        }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> metastore_service_grpc_server::MetastoreServiceGrpcServer<
        MetastoreServiceGrpcServerAdapter,
    > {
        let adapter = MetastoreServiceGrpcServerAdapter::new(self.clone());
        metastore_service_grpc_server::MetastoreServiceGrpcServer::new(adapter)
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
        let client = metastore_service_grpc_client::MetastoreServiceGrpcClient::new(
                channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = MetastoreServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> MetastoreServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = metastore_service_grpc_client::MetastoreServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = MetastoreServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        MetastoreServiceMailbox<A>: MetastoreService,
    {
        MetastoreServiceClient::new(MetastoreServiceMailbox::new(mailbox))
    }
    pub fn tower() -> MetastoreServiceTowerLayerStack {
        MetastoreServiceTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockMetastoreService) -> Self {
        let mock_wrapper = mock_metastore_service::MockMetastoreServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        };
        Self::new(mock_wrapper)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockMetastoreService::new())
    }
}
#[async_trait::async_trait]
impl MetastoreService for MetastoreServiceClient {
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.inner.0.create_index(request).await
    }
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner.0.update_index(request).await
    }
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner.0.index_metadata(request).await
    }
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexesMetadataResponse> {
        self.inner.0.indexes_metadata(request).await
    }
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.inner.0.list_indexes_metadata(request).await
    }
    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.delete_index(request).await
    }
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.inner.0.list_splits(request).await
    }
    async fn stage_splits(
        &self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.stage_splits(request).await
    }
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.publish_splits(request).await
    }
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.mark_splits_for_deletion(request).await
    }
    async fn delete_splits(
        &self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.delete_splits(request).await
    }
    async fn add_source(
        &self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.add_source(request).await
    }
    async fn update_source(
        &self,
        request: UpdateSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.update_source(request).await
    }
    async fn toggle_source(
        &self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.toggle_source(request).await
    }
    async fn delete_source(
        &self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.delete_source(request).await
    }
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.reset_source_checkpoint(request).await
    }
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.inner.0.last_delete_opstamp(request).await
    }
    async fn create_delete_task(
        &self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.inner.0.create_delete_task(request).await
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.inner.0.update_splits_delete_opstamp(request).await
    }
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.inner.0.list_delete_tasks(request).await
    }
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner.0.list_stale_splits(request).await
    }
    async fn open_shards(
        &self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.inner.0.open_shards(request).await
    }
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.inner.0.acquire_shards(request).await
    }
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.inner.0.delete_shards(request).await
    }
    async fn prune_shards(
        &self,
        request: PruneShardsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.prune_shards(request).await
    }
    async fn list_shards(
        &self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.inner.0.list_shards(request).await
    }
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.create_index_template(request).await
    }
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<GetIndexTemplateResponse> {
        self.inner.0.get_index_template(request).await
    }
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> crate::metastore::MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.inner.0.find_index_template_matches(request).await
    }
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexTemplatesResponse> {
        self.inner.0.list_index_templates(request).await
    }
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.0.delete_index_templates(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.0.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.0.endpoints()
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock_metastore_service {
    use super::*;
    #[derive(Debug)]
    pub struct MockMetastoreServiceWrapper {
        pub(super) inner: tokio::sync::Mutex<MockMetastoreService>,
    }
    #[async_trait::async_trait]
    impl MetastoreService for MockMetastoreServiceWrapper {
        async fn create_index(
            &self,
            request: super::CreateIndexRequest,
        ) -> crate::metastore::MetastoreResult<super::CreateIndexResponse> {
            self.inner.lock().await.create_index(request).await
        }
        async fn update_index(
            &self,
            request: super::UpdateIndexRequest,
        ) -> crate::metastore::MetastoreResult<super::IndexMetadataResponse> {
            self.inner.lock().await.update_index(request).await
        }
        async fn index_metadata(
            &self,
            request: super::IndexMetadataRequest,
        ) -> crate::metastore::MetastoreResult<super::IndexMetadataResponse> {
            self.inner.lock().await.index_metadata(request).await
        }
        async fn indexes_metadata(
            &self,
            request: super::IndexesMetadataRequest,
        ) -> crate::metastore::MetastoreResult<super::IndexesMetadataResponse> {
            self.inner.lock().await.indexes_metadata(request).await
        }
        async fn list_indexes_metadata(
            &self,
            request: super::ListIndexesMetadataRequest,
        ) -> crate::metastore::MetastoreResult<super::ListIndexesMetadataResponse> {
            self.inner.lock().await.list_indexes_metadata(request).await
        }
        async fn delete_index(
            &self,
            request: super::DeleteIndexRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_index(request).await
        }
        async fn list_splits(
            &self,
            request: super::ListSplitsRequest,
        ) -> crate::metastore::MetastoreResult<
            MetastoreServiceStream<super::ListSplitsResponse>,
        > {
            self.inner.lock().await.list_splits(request).await
        }
        async fn stage_splits(
            &self,
            request: super::StageSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.stage_splits(request).await
        }
        async fn publish_splits(
            &self,
            request: super::PublishSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.publish_splits(request).await
        }
        async fn mark_splits_for_deletion(
            &self,
            request: super::MarkSplitsForDeletionRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.mark_splits_for_deletion(request).await
        }
        async fn delete_splits(
            &self,
            request: super::DeleteSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_splits(request).await
        }
        async fn add_source(
            &self,
            request: super::AddSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.add_source(request).await
        }
        async fn update_source(
            &self,
            request: super::UpdateSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.update_source(request).await
        }
        async fn toggle_source(
            &self,
            request: super::ToggleSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.toggle_source(request).await
        }
        async fn delete_source(
            &self,
            request: super::DeleteSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_source(request).await
        }
        async fn reset_source_checkpoint(
            &self,
            request: super::ResetSourceCheckpointRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.reset_source_checkpoint(request).await
        }
        async fn last_delete_opstamp(
            &self,
            request: super::LastDeleteOpstampRequest,
        ) -> crate::metastore::MetastoreResult<super::LastDeleteOpstampResponse> {
            self.inner.lock().await.last_delete_opstamp(request).await
        }
        async fn create_delete_task(
            &self,
            request: super::DeleteQuery,
        ) -> crate::metastore::MetastoreResult<super::DeleteTask> {
            self.inner.lock().await.create_delete_task(request).await
        }
        async fn update_splits_delete_opstamp(
            &self,
            request: super::UpdateSplitsDeleteOpstampRequest,
        ) -> crate::metastore::MetastoreResult<
            super::UpdateSplitsDeleteOpstampResponse,
        > {
            self.inner.lock().await.update_splits_delete_opstamp(request).await
        }
        async fn list_delete_tasks(
            &self,
            request: super::ListDeleteTasksRequest,
        ) -> crate::metastore::MetastoreResult<super::ListDeleteTasksResponse> {
            self.inner.lock().await.list_delete_tasks(request).await
        }
        async fn list_stale_splits(
            &self,
            request: super::ListStaleSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListSplitsResponse> {
            self.inner.lock().await.list_stale_splits(request).await
        }
        async fn open_shards(
            &self,
            request: super::OpenShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::OpenShardsResponse> {
            self.inner.lock().await.open_shards(request).await
        }
        async fn acquire_shards(
            &self,
            request: super::AcquireShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::AcquireShardsResponse> {
            self.inner.lock().await.acquire_shards(request).await
        }
        async fn delete_shards(
            &self,
            request: super::DeleteShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::DeleteShardsResponse> {
            self.inner.lock().await.delete_shards(request).await
        }
        async fn prune_shards(
            &self,
            request: super::PruneShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.prune_shards(request).await
        }
        async fn list_shards(
            &self,
            request: super::ListShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListShardsResponse> {
            self.inner.lock().await.list_shards(request).await
        }
        async fn create_index_template(
            &self,
            request: super::CreateIndexTemplateRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.create_index_template(request).await
        }
        async fn get_index_template(
            &self,
            request: super::GetIndexTemplateRequest,
        ) -> crate::metastore::MetastoreResult<super::GetIndexTemplateResponse> {
            self.inner.lock().await.get_index_template(request).await
        }
        async fn find_index_template_matches(
            &self,
            request: super::FindIndexTemplateMatchesRequest,
        ) -> crate::metastore::MetastoreResult<super::FindIndexTemplateMatchesResponse> {
            self.inner.lock().await.find_index_template_matches(request).await
        }
        async fn list_index_templates(
            &self,
            request: super::ListIndexTemplatesRequest,
        ) -> crate::metastore::MetastoreResult<super::ListIndexTemplatesResponse> {
            self.inner.lock().await.list_index_templates(request).await
        }
        async fn delete_index_templates(
            &self,
            request: super::DeleteIndexTemplatesRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_index_templates(request).await
        }
        async fn check_connectivity(&self) -> anyhow::Result<()> {
            self.inner.lock().await.check_connectivity().await
        }
        fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
            futures::executor::block_on(self.inner.lock()).endpoints()
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<CreateIndexRequest> for InnerMetastoreServiceClient {
    type Response = CreateIndexResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: CreateIndexRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.create_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<UpdateIndexRequest> for InnerMetastoreServiceClient {
    type Response = IndexMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: UpdateIndexRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.update_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<IndexMetadataRequest> for InnerMetastoreServiceClient {
    type Response = IndexMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: IndexMetadataRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.index_metadata(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<IndexesMetadataRequest> for InnerMetastoreServiceClient {
    type Response = IndexesMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: IndexesMetadataRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.indexes_metadata(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListIndexesMetadataRequest> for InnerMetastoreServiceClient {
    type Response = ListIndexesMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListIndexesMetadataRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_indexes_metadata(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteIndexRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteIndexRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.delete_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListSplitsRequest> for InnerMetastoreServiceClient {
    type Response = MetastoreServiceStream<ListSplitsResponse>;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListSplitsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<StageSplitsRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: StageSplitsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.stage_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<PublishSplitsRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PublishSplitsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.publish_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<MarkSplitsForDeletionRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: MarkSplitsForDeletionRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.mark_splits_for_deletion(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteSplitsRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteSplitsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.delete_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AddSourceRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AddSourceRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.add_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<UpdateSourceRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: UpdateSourceRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.update_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ToggleSourceRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ToggleSourceRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.toggle_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteSourceRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteSourceRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.delete_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ResetSourceCheckpointRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ResetSourceCheckpointRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.reset_source_checkpoint(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<LastDeleteOpstampRequest> for InnerMetastoreServiceClient {
    type Response = LastDeleteOpstampResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: LastDeleteOpstampRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.last_delete_opstamp(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteQuery> for InnerMetastoreServiceClient {
    type Response = DeleteTask;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteQuery) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.create_delete_task(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<UpdateSplitsDeleteOpstampRequest> for InnerMetastoreServiceClient {
    type Response = UpdateSplitsDeleteOpstampResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: UpdateSplitsDeleteOpstampRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.update_splits_delete_opstamp(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListDeleteTasksRequest> for InnerMetastoreServiceClient {
    type Response = ListDeleteTasksResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListDeleteTasksRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_delete_tasks(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListStaleSplitsRequest> for InnerMetastoreServiceClient {
    type Response = ListSplitsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListStaleSplitsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_stale_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<OpenShardsRequest> for InnerMetastoreServiceClient {
    type Response = OpenShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: OpenShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AcquireShardsRequest> for InnerMetastoreServiceClient {
    type Response = AcquireShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AcquireShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.acquire_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteShardsRequest> for InnerMetastoreServiceClient {
    type Response = DeleteShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.delete_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<PruneShardsRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PruneShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.prune_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListShardsRequest> for InnerMetastoreServiceClient {
    type Response = ListShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListShardsRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<CreateIndexTemplateRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: CreateIndexTemplateRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.create_index_template(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<GetIndexTemplateRequest> for InnerMetastoreServiceClient {
    type Response = GetIndexTemplateResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GetIndexTemplateRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.get_index_template(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<FindIndexTemplateMatchesRequest> for InnerMetastoreServiceClient {
    type Response = FindIndexTemplateMatchesResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: FindIndexTemplateMatchesRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.find_index_template_matches(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListIndexTemplatesRequest> for InnerMetastoreServiceClient {
    type Response = ListIndexTemplatesResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListIndexTemplatesRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.list_index_templates(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteIndexTemplatesRequest> for InnerMetastoreServiceClient {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteIndexTemplatesRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.delete_index_templates(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct MetastoreServiceTowerServiceStack {
    #[allow(dead_code)]
    inner: InnerMetastoreServiceClient,
    create_index_svc: quickwit_common::tower::BoxService<
        CreateIndexRequest,
        CreateIndexResponse,
        crate::metastore::MetastoreError,
    >,
    update_index_svc: quickwit_common::tower::BoxService<
        UpdateIndexRequest,
        IndexMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    index_metadata_svc: quickwit_common::tower::BoxService<
        IndexMetadataRequest,
        IndexMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    indexes_metadata_svc: quickwit_common::tower::BoxService<
        IndexesMetadataRequest,
        IndexesMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    list_indexes_metadata_svc: quickwit_common::tower::BoxService<
        ListIndexesMetadataRequest,
        ListIndexesMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    delete_index_svc: quickwit_common::tower::BoxService<
        DeleteIndexRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    list_splits_svc: quickwit_common::tower::BoxService<
        ListSplitsRequest,
        MetastoreServiceStream<ListSplitsResponse>,
        crate::metastore::MetastoreError,
    >,
    stage_splits_svc: quickwit_common::tower::BoxService<
        StageSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    publish_splits_svc: quickwit_common::tower::BoxService<
        PublishSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    mark_splits_for_deletion_svc: quickwit_common::tower::BoxService<
        MarkSplitsForDeletionRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    delete_splits_svc: quickwit_common::tower::BoxService<
        DeleteSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    add_source_svc: quickwit_common::tower::BoxService<
        AddSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    update_source_svc: quickwit_common::tower::BoxService<
        UpdateSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    toggle_source_svc: quickwit_common::tower::BoxService<
        ToggleSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    delete_source_svc: quickwit_common::tower::BoxService<
        DeleteSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    reset_source_checkpoint_svc: quickwit_common::tower::BoxService<
        ResetSourceCheckpointRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    last_delete_opstamp_svc: quickwit_common::tower::BoxService<
        LastDeleteOpstampRequest,
        LastDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    create_delete_task_svc: quickwit_common::tower::BoxService<
        DeleteQuery,
        DeleteTask,
        crate::metastore::MetastoreError,
    >,
    update_splits_delete_opstamp_svc: quickwit_common::tower::BoxService<
        UpdateSplitsDeleteOpstampRequest,
        UpdateSplitsDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    list_delete_tasks_svc: quickwit_common::tower::BoxService<
        ListDeleteTasksRequest,
        ListDeleteTasksResponse,
        crate::metastore::MetastoreError,
    >,
    list_stale_splits_svc: quickwit_common::tower::BoxService<
        ListStaleSplitsRequest,
        ListSplitsResponse,
        crate::metastore::MetastoreError,
    >,
    open_shards_svc: quickwit_common::tower::BoxService<
        OpenShardsRequest,
        OpenShardsResponse,
        crate::metastore::MetastoreError,
    >,
    acquire_shards_svc: quickwit_common::tower::BoxService<
        AcquireShardsRequest,
        AcquireShardsResponse,
        crate::metastore::MetastoreError,
    >,
    delete_shards_svc: quickwit_common::tower::BoxService<
        DeleteShardsRequest,
        DeleteShardsResponse,
        crate::metastore::MetastoreError,
    >,
    prune_shards_svc: quickwit_common::tower::BoxService<
        PruneShardsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    list_shards_svc: quickwit_common::tower::BoxService<
        ListShardsRequest,
        ListShardsResponse,
        crate::metastore::MetastoreError,
    >,
    create_index_template_svc: quickwit_common::tower::BoxService<
        CreateIndexTemplateRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    get_index_template_svc: quickwit_common::tower::BoxService<
        GetIndexTemplateRequest,
        GetIndexTemplateResponse,
        crate::metastore::MetastoreError,
    >,
    find_index_template_matches_svc: quickwit_common::tower::BoxService<
        FindIndexTemplateMatchesRequest,
        FindIndexTemplateMatchesResponse,
        crate::metastore::MetastoreError,
    >,
    list_index_templates_svc: quickwit_common::tower::BoxService<
        ListIndexTemplatesRequest,
        ListIndexTemplatesResponse,
        crate::metastore::MetastoreError,
    >,
    delete_index_templates_svc: quickwit_common::tower::BoxService<
        DeleteIndexTemplatesRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
}
#[async_trait::async_trait]
impl MetastoreService for MetastoreServiceTowerServiceStack {
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.create_index_svc.clone().ready().await?.call(request).await
    }
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.update_index_svc.clone().ready().await?.call(request).await
    }
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.index_metadata_svc.clone().ready().await?.call(request).await
    }
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexesMetadataResponse> {
        self.indexes_metadata_svc.clone().ready().await?.call(request).await
    }
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.list_indexes_metadata_svc.clone().ready().await?.call(request).await
    }
    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_index_svc.clone().ready().await?.call(request).await
    }
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.list_splits_svc.clone().ready().await?.call(request).await
    }
    async fn stage_splits(
        &self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.stage_splits_svc.clone().ready().await?.call(request).await
    }
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.publish_splits_svc.clone().ready().await?.call(request).await
    }
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.mark_splits_for_deletion_svc.clone().ready().await?.call(request).await
    }
    async fn delete_splits(
        &self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_splits_svc.clone().ready().await?.call(request).await
    }
    async fn add_source(
        &self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.add_source_svc.clone().ready().await?.call(request).await
    }
    async fn update_source(
        &self,
        request: UpdateSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.update_source_svc.clone().ready().await?.call(request).await
    }
    async fn toggle_source(
        &self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.toggle_source_svc.clone().ready().await?.call(request).await
    }
    async fn delete_source(
        &self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_source_svc.clone().ready().await?.call(request).await
    }
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.reset_source_checkpoint_svc.clone().ready().await?.call(request).await
    }
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.last_delete_opstamp_svc.clone().ready().await?.call(request).await
    }
    async fn create_delete_task(
        &self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.create_delete_task_svc.clone().ready().await?.call(request).await
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.update_splits_delete_opstamp_svc.clone().ready().await?.call(request).await
    }
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.list_delete_tasks_svc.clone().ready().await?.call(request).await
    }
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.list_stale_splits_svc.clone().ready().await?.call(request).await
    }
    async fn open_shards(
        &self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.open_shards_svc.clone().ready().await?.call(request).await
    }
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.acquire_shards_svc.clone().ready().await?.call(request).await
    }
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.delete_shards_svc.clone().ready().await?.call(request).await
    }
    async fn prune_shards(
        &self,
        request: PruneShardsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.prune_shards_svc.clone().ready().await?.call(request).await
    }
    async fn list_shards(
        &self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.list_shards_svc.clone().ready().await?.call(request).await
    }
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.create_index_template_svc.clone().ready().await?.call(request).await
    }
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<GetIndexTemplateResponse> {
        self.get_index_template_svc.clone().ready().await?.call(request).await
    }
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> crate::metastore::MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.find_index_template_matches_svc.clone().ready().await?.call(request).await
    }
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexTemplatesResponse> {
        self.list_index_templates_svc.clone().ready().await?.call(request).await
    }
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_index_templates_svc.clone().ready().await?.call(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.0.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.0.endpoints()
    }
}
type CreateIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        CreateIndexRequest,
        CreateIndexResponse,
        crate::metastore::MetastoreError,
    >,
    CreateIndexRequest,
    CreateIndexResponse,
    crate::metastore::MetastoreError,
>;
type UpdateIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        UpdateIndexRequest,
        IndexMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    UpdateIndexRequest,
    IndexMetadataResponse,
    crate::metastore::MetastoreError,
>;
type IndexMetadataLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        IndexMetadataRequest,
        IndexMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    IndexMetadataRequest,
    IndexMetadataResponse,
    crate::metastore::MetastoreError,
>;
type IndexesMetadataLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        IndexesMetadataRequest,
        IndexesMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    IndexesMetadataRequest,
    IndexesMetadataResponse,
    crate::metastore::MetastoreError,
>;
type ListIndexesMetadataLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListIndexesMetadataRequest,
        ListIndexesMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    ListIndexesMetadataRequest,
    ListIndexesMetadataResponse,
    crate::metastore::MetastoreError,
>;
type DeleteIndexLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteIndexRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    DeleteIndexRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type ListSplitsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListSplitsRequest,
        MetastoreServiceStream<ListSplitsResponse>,
        crate::metastore::MetastoreError,
    >,
    ListSplitsRequest,
    MetastoreServiceStream<ListSplitsResponse>,
    crate::metastore::MetastoreError,
>;
type StageSplitsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        StageSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    StageSplitsRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type PublishSplitsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        PublishSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    PublishSplitsRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type MarkSplitsForDeletionLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        MarkSplitsForDeletionRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    MarkSplitsForDeletionRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type DeleteSplitsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    DeleteSplitsRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type AddSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        AddSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    AddSourceRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type UpdateSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        UpdateSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    UpdateSourceRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type ToggleSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ToggleSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    ToggleSourceRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type DeleteSourceLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    DeleteSourceRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type ResetSourceCheckpointLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ResetSourceCheckpointRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    ResetSourceCheckpointRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type LastDeleteOpstampLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        LastDeleteOpstampRequest,
        LastDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    LastDeleteOpstampRequest,
    LastDeleteOpstampResponse,
    crate::metastore::MetastoreError,
>;
type CreateDeleteTaskLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteQuery,
        DeleteTask,
        crate::metastore::MetastoreError,
    >,
    DeleteQuery,
    DeleteTask,
    crate::metastore::MetastoreError,
>;
type UpdateSplitsDeleteOpstampLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        UpdateSplitsDeleteOpstampRequest,
        UpdateSplitsDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse,
    crate::metastore::MetastoreError,
>;
type ListDeleteTasksLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListDeleteTasksRequest,
        ListDeleteTasksResponse,
        crate::metastore::MetastoreError,
    >,
    ListDeleteTasksRequest,
    ListDeleteTasksResponse,
    crate::metastore::MetastoreError,
>;
type ListStaleSplitsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListStaleSplitsRequest,
        ListSplitsResponse,
        crate::metastore::MetastoreError,
    >,
    ListStaleSplitsRequest,
    ListSplitsResponse,
    crate::metastore::MetastoreError,
>;
type OpenShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        OpenShardsRequest,
        OpenShardsResponse,
        crate::metastore::MetastoreError,
    >,
    OpenShardsRequest,
    OpenShardsResponse,
    crate::metastore::MetastoreError,
>;
type AcquireShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        AcquireShardsRequest,
        AcquireShardsResponse,
        crate::metastore::MetastoreError,
    >,
    AcquireShardsRequest,
    AcquireShardsResponse,
    crate::metastore::MetastoreError,
>;
type DeleteShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteShardsRequest,
        DeleteShardsResponse,
        crate::metastore::MetastoreError,
    >,
    DeleteShardsRequest,
    DeleteShardsResponse,
    crate::metastore::MetastoreError,
>;
type PruneShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        PruneShardsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    PruneShardsRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type ListShardsLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListShardsRequest,
        ListShardsResponse,
        crate::metastore::MetastoreError,
    >,
    ListShardsRequest,
    ListShardsResponse,
    crate::metastore::MetastoreError,
>;
type CreateIndexTemplateLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        CreateIndexTemplateRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    CreateIndexTemplateRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
type GetIndexTemplateLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        GetIndexTemplateRequest,
        GetIndexTemplateResponse,
        crate::metastore::MetastoreError,
    >,
    GetIndexTemplateRequest,
    GetIndexTemplateResponse,
    crate::metastore::MetastoreError,
>;
type FindIndexTemplateMatchesLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        FindIndexTemplateMatchesRequest,
        FindIndexTemplateMatchesResponse,
        crate::metastore::MetastoreError,
    >,
    FindIndexTemplateMatchesRequest,
    FindIndexTemplateMatchesResponse,
    crate::metastore::MetastoreError,
>;
type ListIndexTemplatesLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        ListIndexTemplatesRequest,
        ListIndexTemplatesResponse,
        crate::metastore::MetastoreError,
    >,
    ListIndexTemplatesRequest,
    ListIndexTemplatesResponse,
    crate::metastore::MetastoreError,
>;
type DeleteIndexTemplatesLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        DeleteIndexTemplatesRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    DeleteIndexTemplatesRequest,
    EmptyResponse,
    crate::metastore::MetastoreError,
>;
#[derive(Debug, Default)]
pub struct MetastoreServiceTowerLayerStack {
    create_index_layers: Vec<CreateIndexLayer>,
    update_index_layers: Vec<UpdateIndexLayer>,
    index_metadata_layers: Vec<IndexMetadataLayer>,
    indexes_metadata_layers: Vec<IndexesMetadataLayer>,
    list_indexes_metadata_layers: Vec<ListIndexesMetadataLayer>,
    delete_index_layers: Vec<DeleteIndexLayer>,
    list_splits_layers: Vec<ListSplitsLayer>,
    stage_splits_layers: Vec<StageSplitsLayer>,
    publish_splits_layers: Vec<PublishSplitsLayer>,
    mark_splits_for_deletion_layers: Vec<MarkSplitsForDeletionLayer>,
    delete_splits_layers: Vec<DeleteSplitsLayer>,
    add_source_layers: Vec<AddSourceLayer>,
    update_source_layers: Vec<UpdateSourceLayer>,
    toggle_source_layers: Vec<ToggleSourceLayer>,
    delete_source_layers: Vec<DeleteSourceLayer>,
    reset_source_checkpoint_layers: Vec<ResetSourceCheckpointLayer>,
    last_delete_opstamp_layers: Vec<LastDeleteOpstampLayer>,
    create_delete_task_layers: Vec<CreateDeleteTaskLayer>,
    update_splits_delete_opstamp_layers: Vec<UpdateSplitsDeleteOpstampLayer>,
    list_delete_tasks_layers: Vec<ListDeleteTasksLayer>,
    list_stale_splits_layers: Vec<ListStaleSplitsLayer>,
    open_shards_layers: Vec<OpenShardsLayer>,
    acquire_shards_layers: Vec<AcquireShardsLayer>,
    delete_shards_layers: Vec<DeleteShardsLayer>,
    prune_shards_layers: Vec<PruneShardsLayer>,
    list_shards_layers: Vec<ListShardsLayer>,
    create_index_template_layers: Vec<CreateIndexTemplateLayer>,
    get_index_template_layers: Vec<GetIndexTemplateLayer>,
    find_index_template_matches_layers: Vec<FindIndexTemplateMatchesLayer>,
    list_index_templates_layers: Vec<ListIndexTemplatesLayer>,
    delete_index_templates_layers: Vec<DeleteIndexTemplatesLayer>,
}
impl MetastoreServiceTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    CreateIndexRequest,
                    CreateIndexResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                CreateIndexRequest,
                CreateIndexResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                CreateIndexRequest,
                Response = CreateIndexResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                CreateIndexRequest,
                CreateIndexResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<CreateIndexRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateIndexRequest,
                    IndexMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateIndexRequest,
                IndexMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                UpdateIndexRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateIndexRequest,
                IndexMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<UpdateIndexRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    IndexMetadataRequest,
                    IndexMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                IndexMetadataRequest,
                IndexMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                IndexMetadataRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                IndexMetadataRequest,
                IndexMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<IndexMetadataRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    IndexesMetadataRequest,
                    IndexesMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                IndexesMetadataRequest,
                IndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                IndexesMetadataRequest,
                Response = IndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                IndexesMetadataRequest,
                IndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<IndexesMetadataRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListIndexesMetadataRequest,
                    ListIndexesMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListIndexesMetadataRequest,
                ListIndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListIndexesMetadataRequest,
                Response = ListIndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListIndexesMetadataRequest,
                ListIndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            ListIndexesMetadataRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteIndexRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteIndexRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteIndexRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteIndexRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<DeleteIndexRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListSplitsRequest,
                    MetastoreServiceStream<ListSplitsResponse>,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListSplitsRequest,
                MetastoreServiceStream<ListSplitsResponse>,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListSplitsRequest,
                Response = MetastoreServiceStream<ListSplitsResponse>,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListSplitsRequest,
                MetastoreServiceStream<ListSplitsResponse>,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<ListSplitsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    StageSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                StageSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                StageSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                StageSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<StageSplitsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PublishSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                PublishSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                PublishSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                PublishSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<PublishSplitsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    MarkSplitsForDeletionRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                MarkSplitsForDeletionRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                MarkSplitsForDeletionRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                MarkSplitsForDeletionRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            MarkSplitsForDeletionRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteSplitsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<DeleteSplitsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AddSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                AddSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                AddSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                AddSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<AddSourceRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                UpdateSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<UpdateSourceRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ToggleSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ToggleSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ToggleSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ToggleSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<ToggleSourceRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteSourceRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<DeleteSourceRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ResetSourceCheckpointRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ResetSourceCheckpointRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ResetSourceCheckpointRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ResetSourceCheckpointRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            ResetSourceCheckpointRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    LastDeleteOpstampRequest,
                    LastDeleteOpstampResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                LastDeleteOpstampRequest,
                LastDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                LastDeleteOpstampRequest,
                Response = LastDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                LastDeleteOpstampRequest,
                LastDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<LastDeleteOpstampRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteQuery,
                    DeleteTask,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteQuery,
                DeleteTask,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteQuery,
                Response = DeleteTask,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteQuery,
                DeleteTask,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<DeleteQuery>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateSplitsDeleteOpstampRequest,
                    UpdateSplitsDeleteOpstampResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateSplitsDeleteOpstampRequest,
                UpdateSplitsDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                UpdateSplitsDeleteOpstampRequest,
                Response = UpdateSplitsDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                UpdateSplitsDeleteOpstampRequest,
                UpdateSplitsDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            UpdateSplitsDeleteOpstampRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListDeleteTasksRequest,
                    ListDeleteTasksResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListDeleteTasksRequest,
                ListDeleteTasksResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListDeleteTasksRequest,
                Response = ListDeleteTasksResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListDeleteTasksRequest,
                ListDeleteTasksResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<ListDeleteTasksRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListStaleSplitsRequest,
                    ListSplitsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListStaleSplitsRequest,
                ListSplitsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListStaleSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListStaleSplitsRequest,
                ListSplitsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<ListStaleSplitsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    OpenShardsRequest,
                    OpenShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                OpenShardsRequest,
                OpenShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                OpenShardsRequest,
                Response = OpenShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                OpenShardsRequest,
                OpenShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<OpenShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AcquireShardsRequest,
                    AcquireShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                AcquireShardsRequest,
                AcquireShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                AcquireShardsRequest,
                Response = AcquireShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                AcquireShardsRequest,
                AcquireShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<AcquireShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteShardsRequest,
                    DeleteShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteShardsRequest,
                DeleteShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteShardsRequest,
                Response = DeleteShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteShardsRequest,
                DeleteShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<DeleteShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PruneShardsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                PruneShardsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                PruneShardsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                PruneShardsRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<PruneShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListShardsRequest,
                    ListShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListShardsRequest,
                ListShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListShardsRequest,
                ListShardsResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    CreateIndexTemplateRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                CreateIndexTemplateRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                CreateIndexTemplateRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                CreateIndexTemplateRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            CreateIndexTemplateRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetIndexTemplateRequest,
                    GetIndexTemplateResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetIndexTemplateRequest,
                GetIndexTemplateResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                GetIndexTemplateRequest,
                Response = GetIndexTemplateResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetIndexTemplateRequest,
                GetIndexTemplateResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<GetIndexTemplateRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FindIndexTemplateMatchesRequest,
                    FindIndexTemplateMatchesResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                FindIndexTemplateMatchesRequest,
                FindIndexTemplateMatchesResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                FindIndexTemplateMatchesRequest,
                Response = FindIndexTemplateMatchesResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                FindIndexTemplateMatchesRequest,
                FindIndexTemplateMatchesResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            FindIndexTemplateMatchesRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListIndexTemplatesRequest,
                    ListIndexTemplatesResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListIndexTemplatesRequest,
                ListIndexTemplatesResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                ListIndexTemplatesRequest,
                Response = ListIndexTemplatesResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                ListIndexTemplatesRequest,
                ListIndexTemplatesResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            ListIndexTemplatesRequest,
        >>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteIndexTemplatesRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteIndexTemplatesRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service: tower::Service<
                DeleteIndexTemplatesRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                DeleteIndexTemplatesRequest,
                EmptyResponse,
                crate::metastore::MetastoreError,
            >,
        >>::Service as tower::Service<
            DeleteIndexTemplatesRequest,
        >>::Future: Send + 'static,
    {
        self.create_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.update_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.index_metadata_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.indexes_metadata_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_indexes_metadata_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_index_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_splits_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.stage_splits_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.publish_splits_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.mark_splits_for_deletion_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_splits_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.add_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.update_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.toggle_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_source_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.reset_source_checkpoint_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.last_delete_opstamp_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.create_delete_task_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.update_splits_delete_opstamp_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_delete_tasks_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_stale_splits_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.open_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.acquire_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.prune_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_shards_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.create_index_template_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.get_index_template_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.find_index_template_matches_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.list_index_templates_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.delete_index_templates_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_create_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    CreateIndexRequest,
                    CreateIndexResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                CreateIndexRequest,
                Response = CreateIndexResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CreateIndexRequest>>::Future: Send + 'static,
    {
        self.create_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_update_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateIndexRequest,
                    IndexMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                UpdateIndexRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<UpdateIndexRequest>>::Future: Send + 'static,
    {
        self.update_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_index_metadata_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    IndexMetadataRequest,
                    IndexMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                IndexMetadataRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IndexMetadataRequest>>::Future: Send + 'static,
    {
        self.index_metadata_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_indexes_metadata_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    IndexesMetadataRequest,
                    IndexesMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                IndexesMetadataRequest,
                Response = IndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IndexesMetadataRequest>>::Future: Send + 'static,
    {
        self.indexes_metadata_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_indexes_metadata_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListIndexesMetadataRequest,
                    ListIndexesMetadataResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListIndexesMetadataRequest,
                Response = ListIndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ListIndexesMetadataRequest,
        >>::Future: Send + 'static,
    {
        self.list_indexes_metadata_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteIndexRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteIndexRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteIndexRequest>>::Future: Send + 'static,
    {
        self.delete_index_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListSplitsRequest,
                    MetastoreServiceStream<ListSplitsResponse>,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListSplitsRequest,
                Response = MetastoreServiceStream<ListSplitsResponse>,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListSplitsRequest>>::Future: Send + 'static,
    {
        self.list_splits_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_stage_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    StageSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                StageSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<StageSplitsRequest>>::Future: Send + 'static,
    {
        self.stage_splits_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_publish_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PublishSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                PublishSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PublishSplitsRequest>>::Future: Send + 'static,
    {
        self.publish_splits_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_mark_splits_for_deletion_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    MarkSplitsForDeletionRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                MarkSplitsForDeletionRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            MarkSplitsForDeletionRequest,
        >>::Future: Send + 'static,
    {
        self.mark_splits_for_deletion_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteSplitsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSplitsRequest>>::Future: Send + 'static,
    {
        self.delete_splits_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_add_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AddSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                AddSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AddSourceRequest>>::Future: Send + 'static,
    {
        self.add_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_update_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                UpdateSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<UpdateSourceRequest>>::Future: Send + 'static,
    {
        self.update_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_toggle_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ToggleSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ToggleSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ToggleSourceRequest>>::Future: Send + 'static,
    {
        self.toggle_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteSourceRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSourceRequest>>::Future: Send + 'static,
    {
        self.delete_source_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_reset_source_checkpoint_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ResetSourceCheckpointRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ResetSourceCheckpointRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ResetSourceCheckpointRequest,
        >>::Future: Send + 'static,
    {
        self.reset_source_checkpoint_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_last_delete_opstamp_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    LastDeleteOpstampRequest,
                    LastDeleteOpstampResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                LastDeleteOpstampRequest,
                Response = LastDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<LastDeleteOpstampRequest>>::Future: Send + 'static,
    {
        self.last_delete_opstamp_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_create_delete_task_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteQuery,
                    DeleteTask,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteQuery,
                Response = DeleteTask,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteQuery>>::Future: Send + 'static,
    {
        self.create_delete_task_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_update_splits_delete_opstamp_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    UpdateSplitsDeleteOpstampRequest,
                    UpdateSplitsDeleteOpstampResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                UpdateSplitsDeleteOpstampRequest,
                Response = UpdateSplitsDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            UpdateSplitsDeleteOpstampRequest,
        >>::Future: Send + 'static,
    {
        self.update_splits_delete_opstamp_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_delete_tasks_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListDeleteTasksRequest,
                    ListDeleteTasksResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListDeleteTasksRequest,
                Response = ListDeleteTasksResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListDeleteTasksRequest>>::Future: Send + 'static,
    {
        self.list_delete_tasks_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_stale_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListStaleSplitsRequest,
                    ListSplitsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListStaleSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListStaleSplitsRequest>>::Future: Send + 'static,
    {
        self.list_stale_splits_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    OpenShardsRequest,
                    OpenShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                OpenShardsRequest,
                Response = OpenShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenShardsRequest>>::Future: Send + 'static,
    {
        self.open_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_acquire_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    AcquireShardsRequest,
                    AcquireShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                AcquireShardsRequest,
                Response = AcquireShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AcquireShardsRequest>>::Future: Send + 'static,
    {
        self.acquire_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteShardsRequest,
                    DeleteShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteShardsRequest,
                Response = DeleteShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteShardsRequest>>::Future: Send + 'static,
    {
        self.delete_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_prune_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    PruneShardsRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                PruneShardsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PruneShardsRequest>>::Future: Send + 'static,
    {
        self.prune_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListShardsRequest,
                    ListShardsResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
    {
        self.list_shards_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_create_index_template_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    CreateIndexTemplateRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                CreateIndexTemplateRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            CreateIndexTemplateRequest,
        >>::Future: Send + 'static,
    {
        self.create_index_template_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_get_index_template_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetIndexTemplateRequest,
                    GetIndexTemplateResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                GetIndexTemplateRequest,
                Response = GetIndexTemplateResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<GetIndexTemplateRequest>>::Future: Send + 'static,
    {
        self.get_index_template_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_find_index_template_matches_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FindIndexTemplateMatchesRequest,
                    FindIndexTemplateMatchesResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                FindIndexTemplateMatchesRequest,
                Response = FindIndexTemplateMatchesResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            FindIndexTemplateMatchesRequest,
        >>::Future: Send + 'static,
    {
        self.find_index_template_matches_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_list_index_templates_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    ListIndexTemplatesRequest,
                    ListIndexTemplatesResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                ListIndexTemplatesRequest,
                Response = ListIndexTemplatesResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ListIndexTemplatesRequest,
        >>::Future: Send + 'static,
    {
        self.list_index_templates_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_delete_index_templates_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    DeleteIndexTemplatesRequest,
                    EmptyResponse,
                    crate::metastore::MetastoreError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteIndexTemplatesRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            DeleteIndexTemplatesRequest,
        >>::Future: Send + 'static,
    {
        self.delete_index_templates_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> MetastoreServiceClient
    where
        T: MetastoreService,
    {
        let inner_client = InnerMetastoreServiceClient(std::sync::Arc::new(instance));
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> MetastoreServiceClient {
        let client = MetastoreServiceClient::from_channel(
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
    ) -> MetastoreServiceClient {
        let client = MetastoreServiceClient::from_balance_channel(
            balance_channel,
            max_message_size,
        );
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> MetastoreServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        MetastoreServiceMailbox<A>: MetastoreService,
    {
        let inner_client = InnerMetastoreServiceClient(
            std::sync::Arc::new(MetastoreServiceMailbox::new(mailbox)),
        );
        self.build_from_inner_client(inner_client)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn build_from_mock(self, mock: MockMetastoreService) -> MetastoreServiceClient {
        let client = MetastoreServiceClient::from_mock(mock);
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    fn build_from_inner_client(
        self,
        inner_client: InnerMetastoreServiceClient,
    ) -> MetastoreServiceClient {
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
        let index_metadata_svc = self
            .index_metadata_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let indexes_metadata_svc = self
            .indexes_metadata_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let list_indexes_metadata_svc = self
            .list_indexes_metadata_layers
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
        let list_splits_svc = self
            .list_splits_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let stage_splits_svc = self
            .stage_splits_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let publish_splits_svc = self
            .publish_splits_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let mark_splits_for_deletion_svc = self
            .mark_splits_for_deletion_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let delete_splits_svc = self
            .delete_splits_layers
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
        let reset_source_checkpoint_svc = self
            .reset_source_checkpoint_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let last_delete_opstamp_svc = self
            .last_delete_opstamp_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let create_delete_task_svc = self
            .create_delete_task_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let update_splits_delete_opstamp_svc = self
            .update_splits_delete_opstamp_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let list_delete_tasks_svc = self
            .list_delete_tasks_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let list_stale_splits_svc = self
            .list_stale_splits_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let open_shards_svc = self
            .open_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let acquire_shards_svc = self
            .acquire_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let delete_shards_svc = self
            .delete_shards_layers
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
        let list_shards_svc = self
            .list_shards_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let create_index_template_svc = self
            .create_index_template_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let get_index_template_svc = self
            .get_index_template_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let find_index_template_matches_svc = self
            .find_index_template_matches_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let list_index_templates_svc = self
            .list_index_templates_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let delete_index_templates_svc = self
            .delete_index_templates_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = MetastoreServiceTowerServiceStack {
            inner: inner_client,
            create_index_svc,
            update_index_svc,
            index_metadata_svc,
            indexes_metadata_svc,
            list_indexes_metadata_svc,
            delete_index_svc,
            list_splits_svc,
            stage_splits_svc,
            publish_splits_svc,
            mark_splits_for_deletion_svc,
            delete_splits_svc,
            add_source_svc,
            update_source_svc,
            toggle_source_svc,
            delete_source_svc,
            reset_source_checkpoint_svc,
            last_delete_opstamp_svc,
            create_delete_task_svc,
            update_splits_delete_opstamp_svc,
            list_delete_tasks_svc,
            list_stale_splits_svc,
            open_shards_svc,
            acquire_shards_svc,
            delete_shards_svc,
            prune_shards_svc,
            list_shards_svc,
            create_index_template_svc,
            get_index_template_svc,
            find_index_template_matches_svc,
            list_index_templates_svc,
            delete_index_templates_svc,
        };
        MetastoreServiceClient::new(tower_svc_stack)
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
pub struct MetastoreServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::metastore::MetastoreError>,
}
impl<A: quickwit_actors::Actor> MetastoreServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for MetastoreServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for MetastoreServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::metastore::MetastoreError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::metastore::MetastoreError;
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
impl<A> MetastoreService for MetastoreServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    MetastoreServiceMailbox<
        A,
    >: tower::Service<
            CreateIndexRequest,
            Response = CreateIndexResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<CreateIndexResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            UpdateIndexRequest,
            Response = IndexMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<IndexMetadataResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            IndexMetadataRequest,
            Response = IndexMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<IndexMetadataResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            IndexesMetadataRequest,
            Response = IndexesMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<IndexesMetadataResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListIndexesMetadataRequest,
            Response = ListIndexesMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                ListIndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            DeleteIndexRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListSplitsRequest,
            Response = MetastoreServiceStream<ListSplitsResponse>,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                MetastoreServiceStream<ListSplitsResponse>,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            StageSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            PublishSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            MarkSplitsForDeletionRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            AddSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            UpdateSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ToggleSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ResetSourceCheckpointRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            LastDeleteOpstampRequest,
            Response = LastDeleteOpstampResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                LastDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            DeleteQuery,
            Response = DeleteTask,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<DeleteTask, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            UpdateSplitsDeleteOpstampRequest,
            Response = UpdateSplitsDeleteOpstampResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                UpdateSplitsDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            ListDeleteTasksRequest,
            Response = ListDeleteTasksResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListDeleteTasksResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListStaleSplitsRequest,
            Response = ListSplitsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListSplitsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            OpenShardsRequest,
            Response = OpenShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<OpenShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            AcquireShardsRequest,
            Response = AcquireShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<AcquireShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteShardsRequest,
            Response = DeleteShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<DeleteShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            PruneShardsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListShardsRequest,
            Response = ListShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            CreateIndexTemplateRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            GetIndexTemplateRequest,
            Response = GetIndexTemplateResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                GetIndexTemplateResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            FindIndexTemplateMatchesRequest,
            Response = FindIndexTemplateMatchesResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                FindIndexTemplateMatchesResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            ListIndexTemplatesRequest,
            Response = ListIndexTemplatesResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                ListIndexTemplatesResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            DeleteIndexTemplatesRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >,
{
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.clone().call(request).await
    }
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.clone().call(request).await
    }
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.clone().call(request).await
    }
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexesMetadataResponse> {
        self.clone().call(request).await
    }
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.clone().call(request).await
    }
    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.clone().call(request).await
    }
    async fn stage_splits(
        &self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn delete_splits(
        &self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn add_source(
        &self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn update_source(
        &self,
        request: UpdateSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn toggle_source(
        &self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn delete_source(
        &self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.clone().call(request).await
    }
    async fn create_delete_task(
        &self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.clone().call(request).await
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.clone().call(request).await
    }
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.clone().call(request).await
    }
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.clone().call(request).await
    }
    async fn open_shards(
        &self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.clone().call(request).await
    }
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.clone().call(request).await
    }
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.clone().call(request).await
    }
    async fn prune_shards(
        &self,
        request: PruneShardsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn list_shards(
        &self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.clone().call(request).await
    }
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<GetIndexTemplateResponse> {
        self.clone().call(request).await
    }
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> crate::metastore::MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.clone().call(request).await
    }
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexTemplatesResponse> {
        self.clone().call(request).await
    }
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.clone().call(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if self.inner.is_disconnected() {
            anyhow::bail!("actor `{}` is disconnected", self.inner.actor_instance_id())
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![
            quickwit_common::uri::Uri::from_str(& format!("actor://localhost/{}", self
            .inner.actor_instance_id())).expect("URI should be valid")
        ]
    }
}
#[derive(Debug, Clone)]
pub struct MetastoreServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> MetastoreServiceGrpcClientAdapter<T> {
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
impl<T> MetastoreService
for MetastoreServiceGrpcClientAdapter<
    metastore_service_grpc_client::MetastoreServiceGrpcClient<T>,
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
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.inner
            .clone()
            .create_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                CreateIndexRequest::rpc_name(),
            ))
    }
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner
            .clone()
            .update_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                UpdateIndexRequest::rpc_name(),
            ))
    }
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner
            .clone()
            .index_metadata(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                IndexMetadataRequest::rpc_name(),
            ))
    }
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexesMetadataResponse> {
        self.inner
            .clone()
            .indexes_metadata(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                IndexesMetadataRequest::rpc_name(),
            ))
    }
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.inner
            .clone()
            .list_indexes_metadata(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListIndexesMetadataRequest::rpc_name(),
            ))
    }
    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .delete_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteIndexRequest::rpc_name(),
            ))
    }
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.inner
            .clone()
            .list_splits(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream
                    .map_err(|status| crate::error::grpc_status_to_service_error(
                        status,
                        ListSplitsRequest::rpc_name(),
                    ))
            })
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListSplitsRequest::rpc_name(),
            ))
    }
    async fn stage_splits(
        &self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .stage_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                StageSplitsRequest::rpc_name(),
            ))
    }
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .publish_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                PublishSplitsRequest::rpc_name(),
            ))
    }
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .mark_splits_for_deletion(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                MarkSplitsForDeletionRequest::rpc_name(),
            ))
    }
    async fn delete_splits(
        &self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .delete_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteSplitsRequest::rpc_name(),
            ))
    }
    async fn add_source(
        &self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .add_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                AddSourceRequest::rpc_name(),
            ))
    }
    async fn update_source(
        &self,
        request: UpdateSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .update_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                UpdateSourceRequest::rpc_name(),
            ))
    }
    async fn toggle_source(
        &self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .toggle_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ToggleSourceRequest::rpc_name(),
            ))
    }
    async fn delete_source(
        &self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .delete_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteSourceRequest::rpc_name(),
            ))
    }
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .reset_source_checkpoint(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ResetSourceCheckpointRequest::rpc_name(),
            ))
    }
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.inner
            .clone()
            .last_delete_opstamp(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                LastDeleteOpstampRequest::rpc_name(),
            ))
    }
    async fn create_delete_task(
        &self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.inner
            .clone()
            .create_delete_task(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteQuery::rpc_name(),
            ))
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.inner
            .clone()
            .update_splits_delete_opstamp(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                UpdateSplitsDeleteOpstampRequest::rpc_name(),
            ))
    }
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.inner
            .clone()
            .list_delete_tasks(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListDeleteTasksRequest::rpc_name(),
            ))
    }
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner
            .clone()
            .list_stale_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListStaleSplitsRequest::rpc_name(),
            ))
    }
    async fn open_shards(
        &self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.inner
            .clone()
            .open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                OpenShardsRequest::rpc_name(),
            ))
    }
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.inner
            .clone()
            .acquire_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                AcquireShardsRequest::rpc_name(),
            ))
    }
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.inner
            .clone()
            .delete_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteShardsRequest::rpc_name(),
            ))
    }
    async fn prune_shards(
        &self,
        request: PruneShardsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .prune_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                PruneShardsRequest::rpc_name(),
            ))
    }
    async fn list_shards(
        &self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.inner
            .clone()
            .list_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListShardsRequest::rpc_name(),
            ))
    }
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .create_index_template(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                CreateIndexTemplateRequest::rpc_name(),
            ))
    }
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> crate::metastore::MetastoreResult<GetIndexTemplateResponse> {
        self.inner
            .clone()
            .get_index_template(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                GetIndexTemplateRequest::rpc_name(),
            ))
    }
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> crate::metastore::MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.inner
            .clone()
            .find_index_template_matches(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                FindIndexTemplateMatchesRequest::rpc_name(),
            ))
    }
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexTemplatesResponse> {
        self.inner
            .clone()
            .list_index_templates(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                ListIndexTemplatesRequest::rpc_name(),
            ))
    }
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .clone()
            .delete_index_templates(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                DeleteIndexTemplatesRequest::rpc_name(),
            ))
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if self.connection_addrs_rx.borrow().len() == 0 {
            anyhow::bail!("no server currently available")
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.connection_addrs_rx
            .borrow()
            .iter()
            .flat_map(|addr| quickwit_common::uri::Uri::from_str(
                &format!("grpc://{addr}/{}.{}", "quickwit.metastore", "MetastoreService"),
            ))
            .collect()
    }
}
#[derive(Debug)]
pub struct MetastoreServiceGrpcServerAdapter {
    inner: InnerMetastoreServiceClient,
}
impl MetastoreServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: MetastoreService,
    {
        Self {
            inner: InnerMetastoreServiceClient(std::sync::Arc::new(instance)),
        }
    }
}
#[async_trait::async_trait]
impl metastore_service_grpc_server::MetastoreServiceGrpc
for MetastoreServiceGrpcServerAdapter {
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        self.inner
            .0
            .create_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn update_index(
        &self,
        request: tonic::Request<UpdateIndexRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        self.inner
            .0
            .update_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        self.inner
            .0
            .index_metadata(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn indexes_metadata(
        &self,
        request: tonic::Request<IndexesMetadataRequest>,
    ) -> Result<tonic::Response<IndexesMetadataResponse>, tonic::Status> {
        self.inner
            .0
            .indexes_metadata(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list_indexes_metadata(
        &self,
        request: tonic::Request<ListIndexesMetadataRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadataResponse>, tonic::Status> {
        self.inner
            .0
            .list_indexes_metadata(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    type ListSplitsStream = quickwit_common::ServiceStream<
        tonic::Result<ListSplitsResponse>,
    >;
    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<Self::ListSplitsStream>, tonic::Status> {
        self.inner
            .0
            .list_splits(request.into_inner())
            .await
            .map(|stream| tonic::Response::new(
                stream.map_err(crate::error::grpc_error_to_grpc_status),
            ))
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn stage_splits(
        &self,
        request: tonic::Request<StageSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .stage_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .publish_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .mark_splits_for_deletion(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_splits(
        &self,
        request: tonic::Request<DeleteSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn add_source(
        &self,
        request: tonic::Request<AddSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .add_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn update_source(
        &self,
        request: tonic::Request<UpdateSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .update_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn toggle_source(
        &self,
        request: tonic::Request<ToggleSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .toggle_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_source(
        &self,
        request: tonic::Request<DeleteSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn reset_source_checkpoint(
        &self,
        request: tonic::Request<ResetSourceCheckpointRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .reset_source_checkpoint(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn last_delete_opstamp(
        &self,
        request: tonic::Request<LastDeleteOpstampRequest>,
    ) -> Result<tonic::Response<LastDeleteOpstampResponse>, tonic::Status> {
        self.inner
            .0
            .last_delete_opstamp(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn create_delete_task(
        &self,
        request: tonic::Request<DeleteQuery>,
    ) -> Result<tonic::Response<DeleteTask>, tonic::Status> {
        self.inner
            .0
            .create_delete_task(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: tonic::Request<UpdateSplitsDeleteOpstampRequest>,
    ) -> Result<tonic::Response<UpdateSplitsDeleteOpstampResponse>, tonic::Status> {
        self.inner
            .0
            .update_splits_delete_opstamp(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list_delete_tasks(
        &self,
        request: tonic::Request<ListDeleteTasksRequest>,
    ) -> Result<tonic::Response<ListDeleteTasksResponse>, tonic::Status> {
        self.inner
            .0
            .list_delete_tasks(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list_stale_splits(
        &self,
        request: tonic::Request<ListStaleSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.inner
            .0
            .list_stale_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn open_shards(
        &self,
        request: tonic::Request<OpenShardsRequest>,
    ) -> Result<tonic::Response<OpenShardsResponse>, tonic::Status> {
        self.inner
            .0
            .open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn acquire_shards(
        &self,
        request: tonic::Request<AcquireShardsRequest>,
    ) -> Result<tonic::Response<AcquireShardsResponse>, tonic::Status> {
        self.inner
            .0
            .acquire_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_shards(
        &self,
        request: tonic::Request<DeleteShardsRequest>,
    ) -> Result<tonic::Response<DeleteShardsResponse>, tonic::Status> {
        self.inner
            .0
            .delete_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn prune_shards(
        &self,
        request: tonic::Request<PruneShardsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .prune_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list_shards(
        &self,
        request: tonic::Request<ListShardsRequest>,
    ) -> Result<tonic::Response<ListShardsResponse>, tonic::Status> {
        self.inner
            .0
            .list_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn create_index_template(
        &self,
        request: tonic::Request<CreateIndexTemplateRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .create_index_template(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn get_index_template(
        &self,
        request: tonic::Request<GetIndexTemplateRequest>,
    ) -> Result<tonic::Response<GetIndexTemplateResponse>, tonic::Status> {
        self.inner
            .0
            .get_index_template(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn find_index_template_matches(
        &self,
        request: tonic::Request<FindIndexTemplateMatchesRequest>,
    ) -> Result<tonic::Response<FindIndexTemplateMatchesResponse>, tonic::Status> {
        self.inner
            .0
            .find_index_template_matches(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn list_index_templates(
        &self,
        request: tonic::Request<ListIndexTemplatesRequest>,
    ) -> Result<tonic::Response<ListIndexTemplatesResponse>, tonic::Status> {
        self.inner
            .0
            .list_index_templates(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn delete_index_templates(
        &self,
        request: tonic::Request<DeleteIndexTemplatesRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .0
            .delete_index_templates(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
}
/// Generated client implementations.
pub mod metastore_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Metastore meant to manage Quickwit's indexes, their splits and delete tasks.
    ///
    /// I. Index and splits management.
    ///
    /// Quickwit needs a way to ensure that we can cleanup unused files,
    /// and this process needs to be resilient to any fail-stop failures.
    /// We rely on atomically transitioning the status of splits.
    ///
    /// The split state goes through the following life cycle:
    /// 1. `Staged`
    ///   - Start uploading the split files.
    /// 2. `Published`
    ///   - Uploading the split files is complete and the split is searchable.
    /// 3. `MarkedForDeletion`
    ///   - Mark the split for deletion.
    ///
    /// If a split has a file in the storage, it MUST be registered in the metastore,
    /// and its state can be as follows:
    /// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
    /// - `Published`: The split is ready and published.
    /// - `MarkedForDeletion`: The split is marked for deletion.
    ///
    /// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
    /// schedule for deletion all the staged splits. A client may not necessarily remove files from
    /// storage right after marking it for deletion. A CLI client may delete files right away, but a
    /// more serious deployment should probably only delete those files after a grace period so that the
    /// running search queries can complete.
    ///
    /// II. Delete tasks management.
    ///
    /// A delete task is defined on a given index and by a search query. It can be
    /// applied to all the splits of the index.
    ///
    /// Quickwit needs a way to track that a delete task has been applied to a split. This is ensured
    /// by two mechanisms:
    /// - On creation of a delete task, we give to the task a monotically increasing opstamp (uniqueness
    ///   and monotonically increasing must be true at the index level).
    /// - When a delete task is executed on a split, that is when the documents matched by the search
    ///   query are removed from the splits, we update the split's `delete_opstamp` to the value of the
    ///   task's opstamp. This marks the split as "up-to-date" regarding this delete task. If new delete
    ///   tasks are added, we will know that we need to run these delete tasks on the splits as its
    ///   `delete_optstamp` will be inferior to the `opstamp` of the new tasks.
    ///
    /// For splits created after a given delete task, Quickwit's indexing ensures that these splits
    /// are created with a `delete_opstamp` equal the latest opstamp of the tasks of the
    /// corresponding index.
    #[derive(Debug, Clone)]
    pub struct MetastoreServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetastoreServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> MetastoreServiceGrpcClient<T>
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
        ) -> MetastoreServiceGrpcClient<InterceptedService<T, F>>
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
            MetastoreServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        ///
        /// This API creates a new index in the metastore.
        /// An error will occur if an index that already exists in the storage is specified.
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
                "/quickwit.metastore.MetastoreService/CreateIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "CreateIndex"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Update an index.
        pub async fn update_index(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateIndexRequest>,
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
                "/quickwit.metastore.MetastoreService/UpdateIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "UpdateIndex"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Returns the `IndexMetadata` of an index identified by its IndexID or its IndexUID.
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
                "/quickwit.metastore.MetastoreService/IndexMetadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "IndexMetadata",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Fetches the metadata of a list of indexes identified by their Index IDs or UIDs.
        pub async fn indexes_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexesMetadataResponse>,
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
                "/quickwit.metastore.MetastoreService/IndexesMetadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "IndexesMetadata",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets an indexes metadatas.
        pub async fn list_indexes_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadataResponse>,
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
                "/quickwit.metastore.MetastoreService/ListIndexesMetadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ListIndexesMetadata",
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
                "/quickwit.metastore.MetastoreService/DeleteIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "DeleteIndex"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Streams splits from index.
        pub async fn list_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ListSplitsResponse>>,
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
                "/quickwit.metastore.MetastoreService/ListSplits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "ListSplits"),
                );
            self.inner.server_streaming(req, path, codec).await
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
                "/quickwit.metastore.MetastoreService/StageSplits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "StageSplits"),
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
                "/quickwit.metastore.MetastoreService/PublishSplits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "PublishSplits",
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
                "/quickwit.metastore.MetastoreService/MarkSplitsForDeletion",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "MarkSplitsForDeletion",
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
                "/quickwit.metastore.MetastoreService/DeleteSplits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "DeleteSplits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Adds a source.
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
                "/quickwit.metastore.MetastoreService/AddSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "AddSource"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Updates a source.
        pub async fn update_source(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSourceRequest>,
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
                "/quickwit.metastore.MetastoreService/UpdateSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "UpdateSource",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Toggles (turns on or off) source.
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
                "/quickwit.metastore.MetastoreService/ToggleSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ToggleSource",
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
                "/quickwit.metastore.MetastoreService/DeleteSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "DeleteSource",
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
                "/quickwit.metastore.MetastoreService/ResetSourceCheckpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ResetSourceCheckpoint",
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
                "/quickwit.metastore.MetastoreService/LastDeleteOpstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "LastDeleteOpstamp",
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
                "/quickwit.metastore.MetastoreService/CreateDeleteTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "CreateDeleteTask",
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
                "/quickwit.metastore.MetastoreService/UpdateSplitsDeleteOpstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "UpdateSplitsDeleteOpstamp",
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
                "/quickwit.metastore.MetastoreService/ListDeleteTasks",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ListDeleteTasks",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
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
                "/quickwit.metastore.MetastoreService/ListStaleSplits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ListStaleSplits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Shard API
        ///
        /// Note that for the file-backed metastore implementation, the requests are not processed atomically.
        /// Indeed, each request comprises one or more subrequests that target different indexes and sources processed
        /// independently. Responses list the requests that succeeded or failed in the fields `successes` and
        /// `failures`.
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
        ///
        /// If a requested shard is missing, this method does not return an error. It should simply return the list of
        /// shards that were actually acquired.
        ///
        /// For this reason, AcquireShards.acquire_shards may return less subresponse than there was in the request.
        /// Also they may be returned in any order.
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
        /// Deletes a set of shards. This RPC deletes the shards from the metastore.
        /// If the shard did not exist to begin with, the operation is successful and does not return any error.
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
        /// Deletes outdated shards. This RPC deletes the shards from the metastore.
        pub async fn prune_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::PruneShardsRequest>,
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
                "/quickwit.metastore.MetastoreService/PruneShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "PruneShards"),
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
        /// Creates an index template.
        pub async fn create_index_template(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateIndexTemplateRequest>,
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
                "/quickwit.metastore.MetastoreService/CreateIndexTemplate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "CreateIndexTemplate",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Fetches an index template.
        pub async fn get_index_template(
            &mut self,
            request: impl tonic::IntoRequest<super::GetIndexTemplateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetIndexTemplateResponse>,
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
                "/quickwit.metastore.MetastoreService/GetIndexTemplate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "GetIndexTemplate",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Finds matching index templates.
        pub async fn find_index_template_matches(
            &mut self,
            request: impl tonic::IntoRequest<super::FindIndexTemplateMatchesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FindIndexTemplateMatchesResponse>,
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
                "/quickwit.metastore.MetastoreService/FindIndexTemplateMatches",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "FindIndexTemplateMatches",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Returns all the index templates.
        pub async fn list_index_templates(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexTemplatesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexTemplatesResponse>,
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
                "/quickwit.metastore.MetastoreService/ListIndexTemplates",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "ListIndexTemplates",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes index templates.
        pub async fn delete_index_templates(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteIndexTemplatesRequest>,
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
                "/quickwit.metastore.MetastoreService/DeleteIndexTemplates",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "DeleteIndexTemplates",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod metastore_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MetastoreServiceGrpcServer.
    #[async_trait]
    pub trait MetastoreServiceGrpc: Send + Sync + 'static {
        /// Creates an index.
        ///
        /// This API creates a new index in the metastore.
        /// An error will occur if an index that already exists in the storage is specified.
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        >;
        /// Update an index.
        async fn update_index(
            &self,
            request: tonic::Request<super::UpdateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
            tonic::Status,
        >;
        /// Returns the `IndexMetadata` of an index identified by its IndexID or its IndexUID.
        async fn index_metadata(
            &self,
            request: tonic::Request<super::IndexMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
            tonic::Status,
        >;
        /// Fetches the metadata of a list of indexes identified by their Index IDs or UIDs.
        async fn indexes_metadata(
            &self,
            request: tonic::Request<super::IndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexesMetadataResponse>,
            tonic::Status,
        >;
        /// Gets an indexes metadatas.
        async fn list_indexes_metadata(
            &self,
            request: tonic::Request<super::ListIndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadataResponse>,
            tonic::Status,
        >;
        /// Deletes an index
        async fn delete_index(
            &self,
            request: tonic::Request<super::DeleteIndexRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Server streaming response type for the ListSplits method.
        type ListSplitsStream: futures_core::Stream<
                Item = std::result::Result<super::ListSplitsResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Streams splits from index.
        async fn list_splits(
            &self,
            request: tonic::Request<super::ListSplitsRequest>,
        ) -> std::result::Result<tonic::Response<Self::ListSplitsStream>, tonic::Status>;
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
        /// Adds a source.
        async fn add_source(
            &self,
            request: tonic::Request<super::AddSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Updates a source.
        async fn update_source(
            &self,
            request: tonic::Request<super::UpdateSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Toggles (turns on or off) source.
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
        /// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        async fn list_stale_splits(
            &self,
            request: tonic::Request<super::ListStaleSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        /// Shard API
        ///
        /// Note that for the file-backed metastore implementation, the requests are not processed atomically.
        /// Indeed, each request comprises one or more subrequests that target different indexes and sources processed
        /// independently. Responses list the requests that succeeded or failed in the fields `successes` and
        /// `failures`.
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
        ///
        /// If a requested shard is missing, this method does not return an error. It should simply return the list of
        /// shards that were actually acquired.
        ///
        /// For this reason, AcquireShards.acquire_shards may return less subresponse than there was in the request.
        /// Also they may be returned in any order.
        async fn acquire_shards(
            &self,
            request: tonic::Request<super::AcquireShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AcquireShardsResponse>,
            tonic::Status,
        >;
        /// Deletes a set of shards. This RPC deletes the shards from the metastore.
        /// If the shard did not exist to begin with, the operation is successful and does not return any error.
        async fn delete_shards(
            &self,
            request: tonic::Request<super::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardsResponse>,
            tonic::Status,
        >;
        /// Deletes outdated shards. This RPC deletes the shards from the metastore.
        async fn prune_shards(
            &self,
            request: tonic::Request<super::PruneShardsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        async fn list_shards(
            &self,
            request: tonic::Request<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
            tonic::Status,
        >;
        /// Creates an index template.
        async fn create_index_template(
            &self,
            request: tonic::Request<super::CreateIndexTemplateRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Fetches an index template.
        async fn get_index_template(
            &self,
            request: tonic::Request<super::GetIndexTemplateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetIndexTemplateResponse>,
            tonic::Status,
        >;
        /// Finds matching index templates.
        async fn find_index_template_matches(
            &self,
            request: tonic::Request<super::FindIndexTemplateMatchesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FindIndexTemplateMatchesResponse>,
            tonic::Status,
        >;
        /// Returns all the index templates.
        async fn list_index_templates(
            &self,
            request: tonic::Request<super::ListIndexTemplatesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexTemplatesResponse>,
            tonic::Status,
        >;
        /// Deletes index templates.
        async fn delete_index_templates(
            &self,
            request: tonic::Request<super::DeleteIndexTemplatesRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
    }
    /// Metastore meant to manage Quickwit's indexes, their splits and delete tasks.
    ///
    /// I. Index and splits management.
    ///
    /// Quickwit needs a way to ensure that we can cleanup unused files,
    /// and this process needs to be resilient to any fail-stop failures.
    /// We rely on atomically transitioning the status of splits.
    ///
    /// The split state goes through the following life cycle:
    /// 1. `Staged`
    ///   - Start uploading the split files.
    /// 2. `Published`
    ///   - Uploading the split files is complete and the split is searchable.
    /// 3. `MarkedForDeletion`
    ///   - Mark the split for deletion.
    ///
    /// If a split has a file in the storage, it MUST be registered in the metastore,
    /// and its state can be as follows:
    /// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
    /// - `Published`: The split is ready and published.
    /// - `MarkedForDeletion`: The split is marked for deletion.
    ///
    /// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
    /// schedule for deletion all the staged splits. A client may not necessarily remove files from
    /// storage right after marking it for deletion. A CLI client may delete files right away, but a
    /// more serious deployment should probably only delete those files after a grace period so that the
    /// running search queries can complete.
    ///
    /// II. Delete tasks management.
    ///
    /// A delete task is defined on a given index and by a search query. It can be
    /// applied to all the splits of the index.
    ///
    /// Quickwit needs a way to track that a delete task has been applied to a split. This is ensured
    /// by two mechanisms:
    /// - On creation of a delete task, we give to the task a monotically increasing opstamp (uniqueness
    ///   and monotonically increasing must be true at the index level).
    /// - When a delete task is executed on a split, that is when the documents matched by the search
    ///   query are removed from the splits, we update the split's `delete_opstamp` to the value of the
    ///   task's opstamp. This marks the split as "up-to-date" regarding this delete task. If new delete
    ///   tasks are added, we will know that we need to run these delete tasks on the splits as its
    ///   `delete_optstamp` will be inferior to the `opstamp` of the new tasks.
    ///
    /// For splits created after a given delete task, Quickwit's indexing ensures that these splits
    /// are created with a `delete_opstamp` equal the latest opstamp of the tasks of the
    /// corresponding index.
    #[derive(Debug)]
    pub struct MetastoreServiceGrpcServer<T: MetastoreServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetastoreServiceGrpc> MetastoreServiceGrpcServer<T> {
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
    for MetastoreServiceGrpcServer<T>
    where
        T: MetastoreServiceGrpc,
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
                "/quickwit.metastore.MetastoreService/CreateIndex" => {
                    #[allow(non_camel_case_types)]
                    struct CreateIndexSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::CreateIndexRequest>
                    for CreateIndexSvc<T> {
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
                "/quickwit.metastore.MetastoreService/UpdateIndex" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateIndexSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::UpdateIndexRequest>
                    for UpdateIndexSvc<T> {
                        type Response = super::IndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateIndexRequest>,
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
                "/quickwit.metastore.MetastoreService/IndexMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct IndexMetadataSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::IndexMetadataRequest>
                    for IndexMetadataSvc<T> {
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
                        let method = IndexMetadataSvc(inner);
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
                "/quickwit.metastore.MetastoreService/IndexesMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct IndexesMetadataSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::IndexesMetadataRequest>
                    for IndexesMetadataSvc<T> {
                        type Response = super::IndexesMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexesMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).indexes_metadata(request).await
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
                        let method = IndexesMetadataSvc(inner);
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
                "/quickwit.metastore.MetastoreService/ListIndexesMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct ListIndexesMetadataSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListIndexesMetadataRequest>
                    for ListIndexesMetadataSvc<T> {
                        type Response = super::ListIndexesMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexesMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_indexes_metadata(request).await
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
                        let method = ListIndexesMetadataSvc(inner);
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
                "/quickwit.metastore.MetastoreService/DeleteIndex" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteIndexSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteIndexRequest>
                    for DeleteIndexSvc<T> {
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
                "/quickwit.metastore.MetastoreService/ListSplits" => {
                    #[allow(non_camel_case_types)]
                    struct ListSplitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::ServerStreamingService<super::ListSplitsRequest>
                    for ListSplitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type ResponseStream = T::ListSplitsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
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
                        let method = ListSplitsSvc(inner);
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
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/StageSplits" => {
                    #[allow(non_camel_case_types)]
                    struct StageSplitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::StageSplitsRequest>
                    for StageSplitsSvc<T> {
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
                        let method = StageSplitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/PublishSplits" => {
                    #[allow(non_camel_case_types)]
                    struct PublishSplitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::PublishSplitsRequest>
                    for PublishSplitsSvc<T> {
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
                        let method = PublishSplitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/MarkSplitsForDeletion" => {
                    #[allow(non_camel_case_types)]
                    struct MarkSplitsForDeletionSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::MarkSplitsForDeletionRequest>
                    for MarkSplitsForDeletionSvc<T> {
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
                        let method = MarkSplitsForDeletionSvc(inner);
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
                "/quickwit.metastore.MetastoreService/DeleteSplits" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSplitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteSplitsRequest>
                    for DeleteSplitsSvc<T> {
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
                        let method = DeleteSplitsSvc(inner);
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
                "/quickwit.metastore.MetastoreService/AddSource" => {
                    #[allow(non_camel_case_types)]
                    struct AddSourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::AddSourceRequest>
                    for AddSourceSvc<T> {
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
                "/quickwit.metastore.MetastoreService/UpdateSource" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::UpdateSourceRequest>
                    for UpdateSourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateSourceRequest>,
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
                "/quickwit.metastore.MetastoreService/ToggleSource" => {
                    #[allow(non_camel_case_types)]
                    struct ToggleSourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ToggleSourceRequest>
                    for ToggleSourceSvc<T> {
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
                "/quickwit.metastore.MetastoreService/DeleteSource" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteSourceRequest>
                    for DeleteSourceSvc<T> {
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
                "/quickwit.metastore.MetastoreService/ResetSourceCheckpoint" => {
                    #[allow(non_camel_case_types)]
                    struct ResetSourceCheckpointSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ResetSourceCheckpointRequest>
                    for ResetSourceCheckpointSvc<T> {
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
                        let method = ResetSourceCheckpointSvc(inner);
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
                "/quickwit.metastore.MetastoreService/LastDeleteOpstamp" => {
                    #[allow(non_camel_case_types)]
                    struct LastDeleteOpstampSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::LastDeleteOpstampRequest>
                    for LastDeleteOpstampSvc<T> {
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
                        let method = LastDeleteOpstampSvc(inner);
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
                "/quickwit.metastore.MetastoreService/CreateDeleteTask" => {
                    #[allow(non_camel_case_types)]
                    struct CreateDeleteTaskSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteQuery>
                    for CreateDeleteTaskSvc<T> {
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
                        let method = CreateDeleteTaskSvc(inner);
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
                "/quickwit.metastore.MetastoreService/UpdateSplitsDeleteOpstamp" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSplitsDeleteOpstampSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<
                        super::UpdateSplitsDeleteOpstampRequest,
                    > for UpdateSplitsDeleteOpstampSvc<T> {
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
                        let method = UpdateSplitsDeleteOpstampSvc(inner);
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
                "/quickwit.metastore.MetastoreService/ListDeleteTasks" => {
                    #[allow(non_camel_case_types)]
                    struct ListDeleteTasksSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListDeleteTasksRequest>
                    for ListDeleteTasksSvc<T> {
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
                        let method = ListDeleteTasksSvc(inner);
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
                "/quickwit.metastore.MetastoreService/ListStaleSplits" => {
                    #[allow(non_camel_case_types)]
                    struct ListStaleSplitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListStaleSplitsRequest>
                    for ListStaleSplitsSvc<T> {
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
                        let method = ListStaleSplitsSvc(inner);
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
                    struct OpenShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
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
                    struct AcquireShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
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
                "/quickwit.metastore.MetastoreService/DeleteShards" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
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
                "/quickwit.metastore.MetastoreService/PruneShards" => {
                    #[allow(non_camel_case_types)]
                    struct PruneShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::PruneShardsRequest>
                    for PruneShardsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PruneShardsRequest>,
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
                "/quickwit.metastore.MetastoreService/ListShards" => {
                    #[allow(non_camel_case_types)]
                    struct ListShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
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
                "/quickwit.metastore.MetastoreService/CreateIndexTemplate" => {
                    #[allow(non_camel_case_types)]
                    struct CreateIndexTemplateSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::CreateIndexTemplateRequest>
                    for CreateIndexTemplateSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateIndexTemplateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).create_index_template(request).await
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
                        let method = CreateIndexTemplateSvc(inner);
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
                "/quickwit.metastore.MetastoreService/GetIndexTemplate" => {
                    #[allow(non_camel_case_types)]
                    struct GetIndexTemplateSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::GetIndexTemplateRequest>
                    for GetIndexTemplateSvc<T> {
                        type Response = super::GetIndexTemplateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetIndexTemplateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_index_template(request).await
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
                        let method = GetIndexTemplateSvc(inner);
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
                "/quickwit.metastore.MetastoreService/FindIndexTemplateMatches" => {
                    #[allow(non_camel_case_types)]
                    struct FindIndexTemplateMatchesSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::FindIndexTemplateMatchesRequest>
                    for FindIndexTemplateMatchesSvc<T> {
                        type Response = super::FindIndexTemplateMatchesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::FindIndexTemplateMatchesRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).find_index_template_matches(request).await
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
                        let method = FindIndexTemplateMatchesSvc(inner);
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
                "/quickwit.metastore.MetastoreService/ListIndexTemplates" => {
                    #[allow(non_camel_case_types)]
                    struct ListIndexTemplatesSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListIndexTemplatesRequest>
                    for ListIndexTemplatesSvc<T> {
                        type Response = super::ListIndexTemplatesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexTemplatesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_index_templates(request).await
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
                        let method = ListIndexTemplatesSvc(inner);
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
                "/quickwit.metastore.MetastoreService/DeleteIndexTemplates" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteIndexTemplatesSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteIndexTemplatesRequest>
                    for DeleteIndexTemplatesSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteIndexTemplatesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_index_templates(request).await
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
                        let method = DeleteIndexTemplatesSvc(inner);
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
    impl<T: MetastoreServiceGrpc> Clone for MetastoreServiceGrpcServer<T> {
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
    impl<T: MetastoreServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetastoreServiceGrpc> tonic::server::NamedService
    for MetastoreServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.metastore.MetastoreService";
    }
}
