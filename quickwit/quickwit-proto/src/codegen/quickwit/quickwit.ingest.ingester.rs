#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetainShardsForSource {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetainShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub retain_shards_for_sources: ::prost::alloc::vec::Vec<RetainShardsForSource>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetainShardsResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::CommitTypeV2", tag = "3")]
    pub commit_type: i32,
    #[prost(message, repeated, tag = "4")]
    pub subrequests: ::prost::alloc::vec::Vec<PersistSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistSubrequest {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "5")]
    pub doc_batch: ::core::option::Option<super::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistResponse {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub successes: ::prost::alloc::vec::Vec<PersistSuccess>,
    #[prost(message, repeated, tag = "3")]
    pub failures: ::prost::alloc::vec::Vec<PersistFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistSuccess {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "5")]
    pub replication_position_inclusive: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistFailure {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(enumeration = "PersistFailureReason", tag = "5")]
    pub reason: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SynReplicationMessage {
    #[prost(oneof = "syn_replication_message::Message", tags = "1, 2, 3")]
    pub message: ::core::option::Option<syn_replication_message::Message>,
}
/// Nested message and enum types in `SynReplicationMessage`.
pub mod syn_replication_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        OpenRequest(super::OpenReplicationStreamRequest),
        #[prost(message, tag = "2")]
        InitRequest(super::InitReplicaRequest),
        #[prost(message, tag = "3")]
        ReplicateRequest(super::ReplicateRequest),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckReplicationMessage {
    #[prost(oneof = "ack_replication_message::Message", tags = "1, 2, 3")]
    pub message: ::core::option::Option<ack_replication_message::Message>,
}
/// Nested message and enum types in `AckReplicationMessage`.
pub mod ack_replication_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        OpenResponse(super::OpenReplicationStreamResponse),
        #[prost(message, tag = "2")]
        InitResponse(super::InitReplicaResponse),
        #[prost(message, tag = "3")]
        ReplicateResponse(super::ReplicateResponse),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenReplicationStreamRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub follower_id: ::prost::alloc::string::String,
    /// Position of the request in the replication stream.
    #[prost(uint64, tag = "3")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenReplicationStreamResponse {
    /// Position of the response in the replication stream. It should match the position of the request.
    #[prost(uint64, tag = "1")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitReplicaRequest {
    #[prost(message, optional, tag = "1")]
    pub replica_shard: ::core::option::Option<super::Shard>,
    #[prost(uint64, tag = "2")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitReplicaResponse {
    #[prost(uint64, tag = "1")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub follower_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::CommitTypeV2", tag = "3")]
    pub commit_type: i32,
    #[prost(message, repeated, tag = "4")]
    pub subrequests: ::prost::alloc::vec::Vec<ReplicateSubrequest>,
    /// Position of the request in the replication stream.
    #[prost(uint64, tag = "5")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateSubrequest {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "5")]
    pub from_position_exclusive: ::core::option::Option<crate::types::Position>,
    #[prost(message, optional, tag = "6")]
    pub to_position_inclusive: ::core::option::Option<crate::types::Position>,
    #[prost(message, optional, tag = "7")]
    pub doc_batch: ::core::option::Option<super::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateResponse {
    #[prost(string, tag = "1")]
    pub follower_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub successes: ::prost::alloc::vec::Vec<ReplicateSuccess>,
    #[prost(message, repeated, tag = "3")]
    pub failures: ::prost::alloc::vec::Vec<ReplicateFailure>,
    /// Position of the response in the replication stream. It should match the position of the request.
    #[prost(uint64, tag = "4")]
    pub replication_seqno: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateSuccess {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "5")]
    pub replication_position_inclusive: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateFailure {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(enumeration = "ReplicateFailureReason", tag = "5")]
    pub reason: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateShardsRequest {
    #[prost(string, tag = "1")]
    pub ingester_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub subrequests: ::prost::alloc::vec::Vec<TruncateShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    /// The position up to which the shard should be truncated (inclusive).
    #[prost(message, optional, tag = "4")]
    pub truncate_up_to_position_inclusive: ::core::option::Option<
        crate::types::Position,
    >,
}
/// TODO
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateShardsResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenFetchStreamRequest {
    #[prost(string, tag = "1")]
    pub client_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "5")]
    pub from_position_exclusive: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchMessage {
    #[prost(oneof = "fetch_message::Message", tags = "1, 2")]
    pub message: ::core::option::Option<fetch_message::Message>,
}
/// Nested message and enum types in `FetchMessage`.
pub mod fetch_message {
    #[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Message {
        #[prost(message, tag = "1")]
        Payload(super::FetchPayload),
        #[prost(message, tag = "2")]
        Eof(super::FetchEof),
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchPayload {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "4")]
    pub mrecord_batch: ::core::option::Option<super::MRecordBatch>,
    #[prost(message, optional, tag = "5")]
    pub from_position_exclusive: ::core::option::Option<crate::types::Position>,
    #[prost(message, optional, tag = "6")]
    pub to_position_inclusive: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchEof {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(message, optional, tag = "4")]
    pub eof_position: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub shards: ::prost::alloc::vec::Vec<super::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitShardsResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub shards: ::prost::alloc::vec::Vec<super::ShardIds>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {
    #[prost(string, tag = "1")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecommissionRequest {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecommissionResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenObservationStreamRequest {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObservationMessage {
    #[prost(string, tag = "1")]
    pub node_id: ::prost::alloc::string::String,
    #[prost(enumeration = "IngesterStatus", tag = "2")]
    pub status: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PersistFailureReason {
    Unspecified = 0,
    ShardNotFound = 1,
    ShardClosed = 2,
    RateLimited = 3,
    ResourceExhausted = 4,
}
impl PersistFailureReason {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PersistFailureReason::Unspecified => "PERSIST_FAILURE_REASON_UNSPECIFIED",
            PersistFailureReason::ShardNotFound => {
                "PERSIST_FAILURE_REASON_SHARD_NOT_FOUND"
            }
            PersistFailureReason::ShardClosed => "PERSIST_FAILURE_REASON_SHARD_CLOSED",
            PersistFailureReason::RateLimited => "PERSIST_FAILURE_REASON_RATE_LIMITED",
            PersistFailureReason::ResourceExhausted => {
                "PERSIST_FAILURE_REASON_RESOURCE_EXHAUSTED"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PERSIST_FAILURE_REASON_UNSPECIFIED" => Some(Self::Unspecified),
            "PERSIST_FAILURE_REASON_SHARD_NOT_FOUND" => Some(Self::ShardNotFound),
            "PERSIST_FAILURE_REASON_SHARD_CLOSED" => Some(Self::ShardClosed),
            "PERSIST_FAILURE_REASON_RATE_LIMITED" => Some(Self::RateLimited),
            "PERSIST_FAILURE_REASON_RESOURCE_EXHAUSTED" => Some(Self::ResourceExhausted),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReplicateFailureReason {
    Unspecified = 0,
    ShardNotFound = 1,
    ShardClosed = 2,
    ResourceExhausted = 4,
}
impl ReplicateFailureReason {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReplicateFailureReason::Unspecified => "REPLICATE_FAILURE_REASON_UNSPECIFIED",
            ReplicateFailureReason::ShardNotFound => {
                "REPLICATE_FAILURE_REASON_SHARD_NOT_FOUND"
            }
            ReplicateFailureReason::ShardClosed => {
                "REPLICATE_FAILURE_REASON_SHARD_CLOSED"
            }
            ReplicateFailureReason::ResourceExhausted => {
                "REPLICATE_FAILURE_REASON_RESOURCE_EXHAUSTED"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "REPLICATE_FAILURE_REASON_UNSPECIFIED" => Some(Self::Unspecified),
            "REPLICATE_FAILURE_REASON_SHARD_NOT_FOUND" => Some(Self::ShardNotFound),
            "REPLICATE_FAILURE_REASON_SHARD_CLOSED" => Some(Self::ShardClosed),
            "REPLICATE_FAILURE_REASON_RESOURCE_EXHAUSTED" => {
                Some(Self::ResourceExhausted)
            }
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IngesterStatus {
    Unspecified = 0,
    /// The ingester is ready and accepts read and write requests.
    Ready = 1,
    /// The ingester is being decommissioned. It accepts read requests but rejects write requests
    /// (open shards, persist, and replicate requests). It will transition to `Decommissioned` once
    /// all shards are fully indexed.
    Decommissioning = 2,
    /// The ingester no longer accepts read and write requests. It does not hold any data and can
    /// be safely removed from the cluster.
    Decommissioned = 3,
}
impl IngesterStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IngesterStatus::Unspecified => "INGESTER_STATUS_UNSPECIFIED",
            IngesterStatus::Ready => "INGESTER_STATUS_READY",
            IngesterStatus::Decommissioning => "INGESTER_STATUS_DECOMMISSIONING",
            IngesterStatus::Decommissioned => "INGESTER_STATUS_DECOMMISSIONED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INGESTER_STATUS_UNSPECIFIED" => Some(Self::Unspecified),
            "INGESTER_STATUS_READY" => Some(Self::Ready),
            "INGESTER_STATUS_DECOMMISSIONING" => Some(Self::Decommissioning),
            "INGESTER_STATUS_DECOMMISSIONED" => Some(Self::Decommissioned),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
pub type IngesterServiceStream<T> = quickwit_common::ServiceStream<
    crate::ingest::IngestV2Result<T>,
>;
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IngesterService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// Persists batches of documents to primary shards hosted on a leader.
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest::IngestV2Result<PersistResponse>;
    /// Opens a replication stream from a leader to a follower.
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<AckReplicationMessage>>;
    /// Streams records from a leader or a follower. The client can optionally specify a range of positions to fetch,
    /// otherwise the stream will go undefinitely or until the shard is closed.
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<FetchMessage>>;
    /// Streams status updates, called "observations", from an ingester.
    async fn open_observation_stream(
        &mut self,
        request: OpenObservationStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<ObservationMessage>>;
    /// Creates and initializes a set of newly opened shards. This RPC is called by the control plane on leaders.
    async fn init_shards(
        &mut self,
        request: InitShardsRequest,
    ) -> crate::ingest::IngestV2Result<InitShardsResponse>;
    /// Only retain the shards that are listed in the request.
    /// Other shards are deleted.
    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> crate::ingest::IngestV2Result<RetainShardsResponse>;
    /// Truncates a set of shards at the given positions. This RPC is called by indexers on leaders AND followers.
    async fn truncate_shards(
        &mut self,
        request: TruncateShardsRequest,
    ) -> crate::ingest::IngestV2Result<TruncateShardsResponse>;
    /// Closes a set of shards. This RPC is called by the control plane.
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::ingest::IngestV2Result<CloseShardsResponse>;
    /// Pings an ingester to check if it is ready to host shards and serve requests.
    async fn ping(
        &mut self,
        request: PingRequest,
    ) -> crate::ingest::IngestV2Result<PingResponse>;
    /// Decommissions the ingester.
    async fn decommission(
        &mut self,
        request: DecommissionRequest,
    ) -> crate::ingest::IngestV2Result<DecommissionResponse>;
}
dyn_clone::clone_trait_object!(IngesterService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIngesterService {
    fn clone(&self) -> Self {
        MockIngesterService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IngesterServiceClient {
    inner: Box<dyn IngesterService>,
}
impl IngesterServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngesterService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockIngesterService > (),
            "`MockIngesterService` must be wrapped in a `MockIngesterServiceWrapper`. Use `MockIngesterService::from(mock)` to instantiate the client."
        );
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
    ) -> ingester_service_grpc_server::IngesterServiceGrpcServer<
        IngesterServiceGrpcServerAdapter,
    > {
        let adapter = IngesterServiceGrpcServerAdapter::new(self.clone());
        ingester_service_grpc_server::IngesterServiceGrpcServer::new(adapter)
            .max_decoding_message_size(10 * 1024 * 1024)
            .max_encoding_message_size(10 * 1024 * 1024)
    }
    pub fn from_channel(
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> Self {
        let (_, connection_keys_watcher) = tokio::sync::watch::channel(
            std::collections::HashSet::from_iter([addr]),
        );
        let adapter = IngesterServiceGrpcClientAdapter::new(
            ingester_service_grpc_client::IngesterServiceGrpcClient::new(channel),
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> IngesterServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = ingester_service_grpc_client::IngesterServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(20 * 1024 * 1024)
            .max_encoding_message_size(20 * 1024 * 1024);
        let adapter = IngesterServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngesterServiceMailbox<A>: IngesterService,
    {
        IngesterServiceClient::new(IngesterServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IngesterServiceTowerBlockBuilder {
        IngesterServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIngesterService {
        MockIngesterService::new()
    }
}
#[async_trait::async_trait]
impl IngesterService for IngesterServiceClient {
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest::IngestV2Result<PersistResponse> {
        self.inner.persist(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.inner.open_replication_stream(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<FetchMessage>> {
        self.inner.open_fetch_stream(request).await
    }
    async fn open_observation_stream(
        &mut self,
        request: OpenObservationStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.inner.open_observation_stream(request).await
    }
    async fn init_shards(
        &mut self,
        request: InitShardsRequest,
    ) -> crate::ingest::IngestV2Result<InitShardsResponse> {
        self.inner.init_shards(request).await
    }
    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> crate::ingest::IngestV2Result<RetainShardsResponse> {
        self.inner.retain_shards(request).await
    }
    async fn truncate_shards(
        &mut self,
        request: TruncateShardsRequest,
    ) -> crate::ingest::IngestV2Result<TruncateShardsResponse> {
        self.inner.truncate_shards(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::ingest::IngestV2Result<CloseShardsResponse> {
        self.inner.close_shards(request).await
    }
    async fn ping(
        &mut self,
        request: PingRequest,
    ) -> crate::ingest::IngestV2Result<PingResponse> {
        self.inner.ping(request).await
    }
    async fn decommission(
        &mut self,
        request: DecommissionRequest,
    ) -> crate::ingest::IngestV2Result<DecommissionResponse> {
        self.inner.decommission(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod ingester_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockIngesterServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockIngesterService>>,
    }
    #[async_trait::async_trait]
    impl IngesterService for MockIngesterServiceWrapper {
        async fn persist(
            &mut self,
            request: super::PersistRequest,
        ) -> crate::ingest::IngestV2Result<super::PersistResponse> {
            self.inner.lock().await.persist(request).await
        }
        async fn open_replication_stream(
            &mut self,
            request: quickwit_common::ServiceStream<super::SynReplicationMessage>,
        ) -> crate::ingest::IngestV2Result<
            IngesterServiceStream<super::AckReplicationMessage>,
        > {
            self.inner.lock().await.open_replication_stream(request).await
        }
        async fn open_fetch_stream(
            &mut self,
            request: super::OpenFetchStreamRequest,
        ) -> crate::ingest::IngestV2Result<IngesterServiceStream<super::FetchMessage>> {
            self.inner.lock().await.open_fetch_stream(request).await
        }
        async fn open_observation_stream(
            &mut self,
            request: super::OpenObservationStreamRequest,
        ) -> crate::ingest::IngestV2Result<
            IngesterServiceStream<super::ObservationMessage>,
        > {
            self.inner.lock().await.open_observation_stream(request).await
        }
        async fn init_shards(
            &mut self,
            request: super::InitShardsRequest,
        ) -> crate::ingest::IngestV2Result<super::InitShardsResponse> {
            self.inner.lock().await.init_shards(request).await
        }
        async fn retain_shards(
            &mut self,
            request: super::RetainShardsRequest,
        ) -> crate::ingest::IngestV2Result<super::RetainShardsResponse> {
            self.inner.lock().await.retain_shards(request).await
        }
        async fn truncate_shards(
            &mut self,
            request: super::TruncateShardsRequest,
        ) -> crate::ingest::IngestV2Result<super::TruncateShardsResponse> {
            self.inner.lock().await.truncate_shards(request).await
        }
        async fn close_shards(
            &mut self,
            request: super::CloseShardsRequest,
        ) -> crate::ingest::IngestV2Result<super::CloseShardsResponse> {
            self.inner.lock().await.close_shards(request).await
        }
        async fn ping(
            &mut self,
            request: super::PingRequest,
        ) -> crate::ingest::IngestV2Result<super::PingResponse> {
            self.inner.lock().await.ping(request).await
        }
        async fn decommission(
            &mut self,
            request: super::DecommissionRequest,
        ) -> crate::ingest::IngestV2Result<super::DecommissionResponse> {
            self.inner.lock().await.decommission(request).await
        }
    }
    impl From<MockIngesterService> for IngesterServiceClient {
        fn from(mock: MockIngesterService) -> Self {
            let mock_wrapper = MockIngesterServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            IngesterServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<PersistRequest> for Box<dyn IngesterService> {
    type Response = PersistResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PersistRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.persist(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<quickwit_common::ServiceStream<SynReplicationMessage>>
for Box<dyn IngesterService> {
    type Response = IngesterServiceStream<AckReplicationMessage>;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_replication_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<OpenFetchStreamRequest> for Box<dyn IngesterService> {
    type Response = IngesterServiceStream<FetchMessage>;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: OpenFetchStreamRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_fetch_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<OpenObservationStreamRequest> for Box<dyn IngesterService> {
    type Response = IngesterServiceStream<ObservationMessage>;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: OpenObservationStreamRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_observation_stream(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<InitShardsRequest> for Box<dyn IngesterService> {
    type Response = InitShardsResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: InitShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.init_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<RetainShardsRequest> for Box<dyn IngesterService> {
    type Response = RetainShardsResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: RetainShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.retain_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<TruncateShardsRequest> for Box<dyn IngesterService> {
    type Response = TruncateShardsResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: TruncateShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.truncate_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<CloseShardsRequest> for Box<dyn IngesterService> {
    type Response = CloseShardsResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: CloseShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.close_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<PingRequest> for Box<dyn IngesterService> {
    type Response = PingResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PingRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.ping(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DecommissionRequest> for Box<dyn IngesterService> {
    type Response = DecommissionResponse;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DecommissionRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.decommission(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IngesterServiceTowerBlock {
    inner: Box<dyn IngesterService>,
    persist_svc: quickwit_common::tower::BoxService<
        PersistRequest,
        PersistResponse,
        crate::ingest::IngestV2Error,
    >,
    open_replication_stream_svc: quickwit_common::tower::BoxService<
        quickwit_common::ServiceStream<SynReplicationMessage>,
        IngesterServiceStream<AckReplicationMessage>,
        crate::ingest::IngestV2Error,
    >,
    open_fetch_stream_svc: quickwit_common::tower::BoxService<
        OpenFetchStreamRequest,
        IngesterServiceStream<FetchMessage>,
        crate::ingest::IngestV2Error,
    >,
    open_observation_stream_svc: quickwit_common::tower::BoxService<
        OpenObservationStreamRequest,
        IngesterServiceStream<ObservationMessage>,
        crate::ingest::IngestV2Error,
    >,
    init_shards_svc: quickwit_common::tower::BoxService<
        InitShardsRequest,
        InitShardsResponse,
        crate::ingest::IngestV2Error,
    >,
    retain_shards_svc: quickwit_common::tower::BoxService<
        RetainShardsRequest,
        RetainShardsResponse,
        crate::ingest::IngestV2Error,
    >,
    truncate_shards_svc: quickwit_common::tower::BoxService<
        TruncateShardsRequest,
        TruncateShardsResponse,
        crate::ingest::IngestV2Error,
    >,
    close_shards_svc: quickwit_common::tower::BoxService<
        CloseShardsRequest,
        CloseShardsResponse,
        crate::ingest::IngestV2Error,
    >,
    ping_svc: quickwit_common::tower::BoxService<
        PingRequest,
        PingResponse,
        crate::ingest::IngestV2Error,
    >,
    decommission_svc: quickwit_common::tower::BoxService<
        DecommissionRequest,
        DecommissionResponse,
        crate::ingest::IngestV2Error,
    >,
}
impl Clone for IngesterServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            persist_svc: self.persist_svc.clone(),
            open_replication_stream_svc: self.open_replication_stream_svc.clone(),
            open_fetch_stream_svc: self.open_fetch_stream_svc.clone(),
            open_observation_stream_svc: self.open_observation_stream_svc.clone(),
            init_shards_svc: self.init_shards_svc.clone(),
            retain_shards_svc: self.retain_shards_svc.clone(),
            truncate_shards_svc: self.truncate_shards_svc.clone(),
            close_shards_svc: self.close_shards_svc.clone(),
            ping_svc: self.ping_svc.clone(),
            decommission_svc: self.decommission_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IngesterService for IngesterServiceTowerBlock {
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest::IngestV2Result<PersistResponse> {
        self.persist_svc.ready().await?.call(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.open_replication_stream_svc.ready().await?.call(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<FetchMessage>> {
        self.open_fetch_stream_svc.ready().await?.call(request).await
    }
    async fn open_observation_stream(
        &mut self,
        request: OpenObservationStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.open_observation_stream_svc.ready().await?.call(request).await
    }
    async fn init_shards(
        &mut self,
        request: InitShardsRequest,
    ) -> crate::ingest::IngestV2Result<InitShardsResponse> {
        self.init_shards_svc.ready().await?.call(request).await
    }
    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> crate::ingest::IngestV2Result<RetainShardsResponse> {
        self.retain_shards_svc.ready().await?.call(request).await
    }
    async fn truncate_shards(
        &mut self,
        request: TruncateShardsRequest,
    ) -> crate::ingest::IngestV2Result<TruncateShardsResponse> {
        self.truncate_shards_svc.ready().await?.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::ingest::IngestV2Result<CloseShardsResponse> {
        self.close_shards_svc.ready().await?.call(request).await
    }
    async fn ping(
        &mut self,
        request: PingRequest,
    ) -> crate::ingest::IngestV2Result<PingResponse> {
        self.ping_svc.ready().await?.call(request).await
    }
    async fn decommission(
        &mut self,
        request: DecommissionRequest,
    ) -> crate::ingest::IngestV2Result<DecommissionResponse> {
        self.decommission_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IngesterServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    persist_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            PersistRequest,
            PersistResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_replication_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            quickwit_common::ServiceStream<SynReplicationMessage>,
            IngesterServiceStream<AckReplicationMessage>,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_fetch_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            OpenFetchStreamRequest,
            IngesterServiceStream<FetchMessage>,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_observation_stream_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            OpenObservationStreamRequest,
            IngesterServiceStream<ObservationMessage>,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    init_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            InitShardsRequest,
            InitShardsResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    retain_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            RetainShardsRequest,
            RetainShardsResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    truncate_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            TruncateShardsRequest,
            TruncateShardsResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    close_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            CloseShardsRequest,
            CloseShardsResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    ping_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            PingRequest,
            PingResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
    #[allow(clippy::type_complexity)]
    decommission_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngesterService>,
            DecommissionRequest,
            DecommissionResponse,
            crate::ingest::IngestV2Error,
        >,
    >,
}
impl IngesterServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                PersistRequest,
                Response = PersistResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PersistRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                quickwit_common::ServiceStream<SynReplicationMessage>,
                Response = IngesterServiceStream<AckReplicationMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                OpenFetchStreamRequest,
                Response = IngesterServiceStream<FetchMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenFetchStreamRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                OpenObservationStreamRequest,
                Response = IngesterServiceStream<ObservationMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            OpenObservationStreamRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                InitShardsRequest,
                Response = InitShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<InitShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                RetainShardsRequest,
                Response = RetainShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<RetainShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                TruncateShardsRequest,
                Response = TruncateShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<TruncateShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                CloseShardsRequest,
                Response = CloseShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CloseShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                PingRequest,
                Response = PingResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PingRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                DecommissionRequest,
                Response = DecommissionResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DecommissionRequest>>::Future: Send + 'static,
    {
        self.persist_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
            .open_replication_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .open_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .open_observation_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .init_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .retain_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .truncate_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .close_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self.ping_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.decommission_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn persist_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                PersistRequest,
                Response = PersistResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PersistRequest>>::Future: Send + 'static,
    {
        self.persist_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn open_replication_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                quickwit_common::ServiceStream<SynReplicationMessage>,
                Response = IngesterServiceStream<AckReplicationMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
        >>::Future: Send + 'static,
    {
        self
            .open_replication_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn open_fetch_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                OpenFetchStreamRequest,
                Response = IngesterServiceStream<FetchMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenFetchStreamRequest>>::Future: Send + 'static,
    {
        self
            .open_fetch_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn open_observation_stream_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                OpenObservationStreamRequest,
                Response = IngesterServiceStream<ObservationMessage>,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            OpenObservationStreamRequest,
        >>::Future: Send + 'static,
    {
        self
            .open_observation_stream_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn init_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                InitShardsRequest,
                Response = InitShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<InitShardsRequest>>::Future: Send + 'static,
    {
        self.init_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn retain_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                RetainShardsRequest,
                Response = RetainShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<RetainShardsRequest>>::Future: Send + 'static,
    {
        self.retain_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn truncate_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                TruncateShardsRequest,
                Response = TruncateShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<TruncateShardsRequest>>::Future: Send + 'static,
    {
        self.truncate_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn close_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                CloseShardsRequest,
                Response = CloseShardsResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CloseShardsRequest>>::Future: Send + 'static,
    {
        self.close_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn ping_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                PingRequest,
                Response = PingResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PingRequest>>::Future: Send + 'static,
    {
        self.ping_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn decommission_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngesterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DecommissionRequest,
                Response = DecommissionResponse,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DecommissionRequest>>::Future: Send + 'static,
    {
        self.decommission_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> IngesterServiceClient
    where
        T: IngesterService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> IngesterServiceClient {
        self.build_from_boxed(
            Box::new(IngesterServiceClient::from_channel(addr, channel)),
        )
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> IngesterServiceClient {
        self.build_from_boxed(
            Box::new(IngesterServiceClient::from_balance_channel(balance_channel)),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IngesterServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngesterServiceMailbox<A>: IngesterService,
    {
        self.build_from_boxed(Box::new(IngesterServiceMailbox::new(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IngesterService>,
    ) -> IngesterServiceClient {
        let persist_svc = if let Some(layer) = self.persist_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_replication_stream_svc = if let Some(layer)
            = self.open_replication_stream_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_fetch_stream_svc = if let Some(layer) = self.open_fetch_stream_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_observation_stream_svc = if let Some(layer)
            = self.open_observation_stream_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let init_shards_svc = if let Some(layer) = self.init_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let retain_shards_svc = if let Some(layer) = self.retain_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let truncate_shards_svc = if let Some(layer) = self.truncate_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let close_shards_svc = if let Some(layer) = self.close_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let ping_svc = if let Some(layer) = self.ping_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let decommission_svc = if let Some(layer) = self.decommission_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IngesterServiceTowerBlock {
            inner: boxed_instance.clone(),
            persist_svc,
            open_replication_stream_svc,
            open_fetch_stream_svc,
            open_observation_stream_svc,
            init_shards_svc,
            retain_shards_svc,
            truncate_shards_svc,
            close_shards_svc,
            ping_svc,
            decommission_svc,
        };
        IngesterServiceClient::new(tower_block)
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
pub struct IngesterServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::ingest::IngestV2Error>,
}
impl<A: quickwit_actors::Actor> IngesterServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IngesterServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for IngesterServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::ingest::IngestV2Error: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::ingest::IngestV2Error;
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
impl<A> IngesterService for IngesterServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    IngesterServiceMailbox<
        A,
    >: tower::Service<
            PersistRequest,
            Response = PersistResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<PersistResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            quickwit_common::ServiceStream<SynReplicationMessage>,
            Response = IngesterServiceStream<AckReplicationMessage>,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<
                IngesterServiceStream<AckReplicationMessage>,
                crate::ingest::IngestV2Error,
            >,
        >
        + tower::Service<
            OpenFetchStreamRequest,
            Response = IngesterServiceStream<FetchMessage>,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<
                IngesterServiceStream<FetchMessage>,
                crate::ingest::IngestV2Error,
            >,
        >
        + tower::Service<
            OpenObservationStreamRequest,
            Response = IngesterServiceStream<ObservationMessage>,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<
                IngesterServiceStream<ObservationMessage>,
                crate::ingest::IngestV2Error,
            >,
        >
        + tower::Service<
            InitShardsRequest,
            Response = InitShardsResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<InitShardsResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            RetainShardsRequest,
            Response = RetainShardsResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<RetainShardsResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            TruncateShardsRequest,
            Response = TruncateShardsResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<TruncateShardsResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            CloseShardsRequest,
            Response = CloseShardsResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<CloseShardsResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            PingRequest,
            Response = PingResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<PingResponse, crate::ingest::IngestV2Error>,
        >
        + tower::Service<
            DecommissionRequest,
            Response = DecommissionResponse,
            Error = crate::ingest::IngestV2Error,
            Future = BoxFuture<DecommissionResponse, crate::ingest::IngestV2Error>,
        >,
{
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest::IngestV2Result<PersistResponse> {
        self.call(request).await
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.call(request).await
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<FetchMessage>> {
        self.call(request).await
    }
    async fn open_observation_stream(
        &mut self,
        request: OpenObservationStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.call(request).await
    }
    async fn init_shards(
        &mut self,
        request: InitShardsRequest,
    ) -> crate::ingest::IngestV2Result<InitShardsResponse> {
        self.call(request).await
    }
    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> crate::ingest::IngestV2Result<RetainShardsResponse> {
        self.call(request).await
    }
    async fn truncate_shards(
        &mut self,
        request: TruncateShardsRequest,
    ) -> crate::ingest::IngestV2Result<TruncateShardsResponse> {
        self.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::ingest::IngestV2Result<CloseShardsResponse> {
        self.call(request).await
    }
    async fn ping(
        &mut self,
        request: PingRequest,
    ) -> crate::ingest::IngestV2Result<PingResponse> {
        self.call(request).await
    }
    async fn decommission(
        &mut self,
        request: DecommissionRequest,
    ) -> crate::ingest::IngestV2Result<DecommissionResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IngesterServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> IngesterServiceGrpcClientAdapter<T> {
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
impl<T> IngesterService
for IngesterServiceGrpcClientAdapter<
    ingester_service_grpc_client::IngesterServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn persist(
        &mut self,
        request: PersistRequest,
    ) -> crate::ingest::IngestV2Result<PersistResponse> {
        self.inner
            .persist(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn open_replication_stream(
        &mut self,
        request: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.inner
            .open_replication_stream(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream.map_err(|error| error.into())
            })
            .map_err(|error| error.into())
    }
    async fn open_fetch_stream(
        &mut self,
        request: OpenFetchStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<FetchMessage>> {
        self.inner
            .open_fetch_stream(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream.map_err(|error| error.into())
            })
            .map_err(|error| error.into())
    }
    async fn open_observation_stream(
        &mut self,
        request: OpenObservationStreamRequest,
    ) -> crate::ingest::IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.inner
            .open_observation_stream(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream.map_err(|error| error.into())
            })
            .map_err(|error| error.into())
    }
    async fn init_shards(
        &mut self,
        request: InitShardsRequest,
    ) -> crate::ingest::IngestV2Result<InitShardsResponse> {
        self.inner
            .init_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> crate::ingest::IngestV2Result<RetainShardsResponse> {
        self.inner
            .retain_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn truncate_shards(
        &mut self,
        request: TruncateShardsRequest,
    ) -> crate::ingest::IngestV2Result<TruncateShardsResponse> {
        self.inner
            .truncate_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::ingest::IngestV2Result<CloseShardsResponse> {
        self.inner
            .close_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn ping(
        &mut self,
        request: PingRequest,
    ) -> crate::ingest::IngestV2Result<PingResponse> {
        self.inner
            .ping(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn decommission(
        &mut self,
        request: DecommissionRequest,
    ) -> crate::ingest::IngestV2Result<DecommissionResponse> {
        self.inner
            .decommission(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IngesterServiceGrpcServerAdapter {
    inner: Box<dyn IngesterService>,
}
impl IngesterServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngesterService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl ingester_service_grpc_server::IngesterServiceGrpc
for IngesterServiceGrpcServerAdapter {
    async fn persist(
        &self,
        request: tonic::Request<PersistRequest>,
    ) -> Result<tonic::Response<PersistResponse>, tonic::Status> {
        self.inner
            .clone()
            .persist(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    type OpenReplicationStreamStream = quickwit_common::ServiceStream<
        tonic::Result<AckReplicationMessage>,
    >;
    async fn open_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<SynReplicationMessage>>,
    ) -> Result<tonic::Response<Self::OpenReplicationStreamStream>, tonic::Status> {
        self.inner
            .clone()
            .open_replication_stream({
                let streaming: tonic::Streaming<_> = request.into_inner();
                quickwit_common::ServiceStream::from(streaming)
            })
            .await
            .map(|stream| tonic::Response::new(stream.map_err(|error| error.into())))
            .map_err(|error| error.into())
    }
    type OpenFetchStreamStream = quickwit_common::ServiceStream<
        tonic::Result<FetchMessage>,
    >;
    async fn open_fetch_stream(
        &self,
        request: tonic::Request<OpenFetchStreamRequest>,
    ) -> Result<tonic::Response<Self::OpenFetchStreamStream>, tonic::Status> {
        self.inner
            .clone()
            .open_fetch_stream(request.into_inner())
            .await
            .map(|stream| tonic::Response::new(stream.map_err(|error| error.into())))
            .map_err(|error| error.into())
    }
    type OpenObservationStreamStream = quickwit_common::ServiceStream<
        tonic::Result<ObservationMessage>,
    >;
    async fn open_observation_stream(
        &self,
        request: tonic::Request<OpenObservationStreamRequest>,
    ) -> Result<tonic::Response<Self::OpenObservationStreamStream>, tonic::Status> {
        self.inner
            .clone()
            .open_observation_stream(request.into_inner())
            .await
            .map(|stream| tonic::Response::new(stream.map_err(|error| error.into())))
            .map_err(|error| error.into())
    }
    async fn init_shards(
        &self,
        request: tonic::Request<InitShardsRequest>,
    ) -> Result<tonic::Response<InitShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .init_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn retain_shards(
        &self,
        request: tonic::Request<RetainShardsRequest>,
    ) -> Result<tonic::Response<RetainShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .retain_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn truncate_shards(
        &self,
        request: tonic::Request<TruncateShardsRequest>,
    ) -> Result<tonic::Response<TruncateShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .truncate_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &self,
        request: tonic::Request<CloseShardsRequest>,
    ) -> Result<tonic::Response<CloseShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .close_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        self.inner
            .clone()
            .ping(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn decommission(
        &self,
        request: tonic::Request<DecommissionRequest>,
    ) -> Result<tonic::Response<DecommissionResponse>, tonic::Status> {
        self.inner
            .clone()
            .decommission(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod ingester_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IngesterServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngesterServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> IngesterServiceGrpcClient<T>
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
        ) -> IngesterServiceGrpcClient<InterceptedService<T, F>>
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
            IngesterServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        /// Persists batches of documents to primary shards hosted on a leader.
        pub async fn persist(
            &mut self,
            request: impl tonic::IntoRequest<super::PersistRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PersistResponse>,
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
                "/quickwit.ingest.ingester.IngesterService/Persist",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "Persist",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Opens a replication stream from a leader to a follower.
        pub async fn open_replication_stream(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::SynReplicationMessage,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::AckReplicationMessage>>,
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
                "/quickwit.ingest.ingester.IngesterService/OpenReplicationStream",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "OpenReplicationStream",
                    ),
                );
            self.inner.streaming(req, path, codec).await
        }
        /// Streams records from a leader or a follower. The client can optionally specify a range of positions to fetch,
        /// otherwise the stream will go undefinitely or until the shard is closed.
        pub async fn open_fetch_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::FetchMessage>>,
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
                "/quickwit.ingest.ingester.IngesterService/OpenFetchStream",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "OpenFetchStream",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        /// Streams status updates, called "observations", from an ingester.
        pub async fn open_observation_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenObservationStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ObservationMessage>>,
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
                "/quickwit.ingest.ingester.IngesterService/OpenObservationStream",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "OpenObservationStream",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        /// Creates and initializes a set of newly opened shards. This RPC is called by the control plane on leaders.
        pub async fn init_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::InitShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InitShardsResponse>,
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
                "/quickwit.ingest.ingester.IngesterService/InitShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "InitShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Only retain the shards that are listed in the request.
        /// Other shards are deleted.
        pub async fn retain_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::RetainShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainShardsResponse>,
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
                "/quickwit.ingest.ingester.IngesterService/RetainShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "RetainShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Truncates a set of shards at the given positions. This RPC is called by indexers on leaders AND followers.
        pub async fn truncate_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::TruncateShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TruncateShardsResponse>,
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
                "/quickwit.ingest.ingester.IngesterService/TruncateShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "TruncateShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Closes a set of shards. This RPC is called by the control plane.
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
                "/quickwit.ingest.ingester.IngesterService/CloseShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "CloseShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Pings an ingester to check if it is ready to host shards and serve requests.
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
                "/quickwit.ingest.ingester.IngesterService/Ping",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.ingest.ingester.IngesterService", "Ping"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Decommissions the ingester.
        pub async fn decommission(
            &mut self,
            request: impl tonic::IntoRequest<super::DecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DecommissionResponse>,
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
                "/quickwit.ingest.ingester.IngesterService/Decommission",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.ingester.IngesterService",
                        "Decommission",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingester_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IngesterServiceGrpcServer.
    #[async_trait]
    pub trait IngesterServiceGrpc: Send + Sync + 'static {
        /// Persists batches of documents to primary shards hosted on a leader.
        async fn persist(
            &self,
            request: tonic::Request<super::PersistRequest>,
        ) -> std::result::Result<tonic::Response<super::PersistResponse>, tonic::Status>;
        /// Server streaming response type for the OpenReplicationStream method.
        type OpenReplicationStreamStream: futures_core::Stream<
                Item = std::result::Result<super::AckReplicationMessage, tonic::Status>,
            >
            + Send
            + 'static;
        /// Opens a replication stream from a leader to a follower.
        async fn open_replication_stream(
            &self,
            request: tonic::Request<tonic::Streaming<super::SynReplicationMessage>>,
        ) -> std::result::Result<
            tonic::Response<Self::OpenReplicationStreamStream>,
            tonic::Status,
        >;
        /// Server streaming response type for the OpenFetchStream method.
        type OpenFetchStreamStream: futures_core::Stream<
                Item = std::result::Result<super::FetchMessage, tonic::Status>,
            >
            + Send
            + 'static;
        /// Streams records from a leader or a follower. The client can optionally specify a range of positions to fetch,
        /// otherwise the stream will go undefinitely or until the shard is closed.
        async fn open_fetch_stream(
            &self,
            request: tonic::Request<super::OpenFetchStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::OpenFetchStreamStream>,
            tonic::Status,
        >;
        /// Server streaming response type for the OpenObservationStream method.
        type OpenObservationStreamStream: futures_core::Stream<
                Item = std::result::Result<super::ObservationMessage, tonic::Status>,
            >
            + Send
            + 'static;
        /// Streams status updates, called "observations", from an ingester.
        async fn open_observation_stream(
            &self,
            request: tonic::Request<super::OpenObservationStreamRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::OpenObservationStreamStream>,
            tonic::Status,
        >;
        /// Creates and initializes a set of newly opened shards. This RPC is called by the control plane on leaders.
        async fn init_shards(
            &self,
            request: tonic::Request<super::InitShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::InitShardsResponse>,
            tonic::Status,
        >;
        /// Only retain the shards that are listed in the request.
        /// Other shards are deleted.
        async fn retain_shards(
            &self,
            request: tonic::Request<super::RetainShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RetainShardsResponse>,
            tonic::Status,
        >;
        /// Truncates a set of shards at the given positions. This RPC is called by indexers on leaders AND followers.
        async fn truncate_shards(
            &self,
            request: tonic::Request<super::TruncateShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TruncateShardsResponse>,
            tonic::Status,
        >;
        /// Closes a set of shards. This RPC is called by the control plane.
        async fn close_shards(
            &self,
            request: tonic::Request<super::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CloseShardsResponse>,
            tonic::Status,
        >;
        /// Pings an ingester to check if it is ready to host shards and serve requests.
        async fn ping(
            &self,
            request: tonic::Request<super::PingRequest>,
        ) -> std::result::Result<tonic::Response<super::PingResponse>, tonic::Status>;
        /// Decommissions the ingester.
        async fn decommission(
            &self,
            request: tonic::Request<super::DecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DecommissionResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct IngesterServiceGrpcServer<T: IngesterServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngesterServiceGrpc> IngesterServiceGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IngesterServiceGrpcServer<T>
    where
        T: IngesterServiceGrpc,
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
                "/quickwit.ingest.ingester.IngesterService/Persist" => {
                    #[allow(non_camel_case_types)]
                    struct PersistSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::PersistRequest>
                    for PersistSvc<T> {
                        type Response = super::PersistResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PersistRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).persist(request).await };
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
                        let method = PersistSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/OpenReplicationStream" => {
                    #[allow(non_camel_case_types)]
                    struct OpenReplicationStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::StreamingService<super::SynReplicationMessage>
                    for OpenReplicationStreamSvc<T> {
                        type Response = super::AckReplicationMessage;
                        type ResponseStream = T::OpenReplicationStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::SynReplicationMessage>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).open_replication_stream(request).await
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
                        let method = OpenReplicationStreamSvc(inner);
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
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.ingest.ingester.IngesterService/OpenFetchStream" => {
                    #[allow(non_camel_case_types)]
                    struct OpenFetchStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::ServerStreamingService<
                        super::OpenFetchStreamRequest,
                    > for OpenFetchStreamSvc<T> {
                        type Response = super::FetchMessage;
                        type ResponseStream = T::OpenFetchStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenFetchStreamRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).open_fetch_stream(request).await
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
                        let method = OpenFetchStreamSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/OpenObservationStream" => {
                    #[allow(non_camel_case_types)]
                    struct OpenObservationStreamSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::ServerStreamingService<
                        super::OpenObservationStreamRequest,
                    > for OpenObservationStreamSvc<T> {
                        type Response = super::ObservationMessage;
                        type ResponseStream = T::OpenObservationStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenObservationStreamRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).open_observation_stream(request).await
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
                        let method = OpenObservationStreamSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/InitShards" => {
                    #[allow(non_camel_case_types)]
                    struct InitShardsSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::InitShardsRequest>
                    for InitShardsSvc<T> {
                        type Response = super::InitShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InitShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).init_shards(request).await };
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
                        let method = InitShardsSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/RetainShards" => {
                    #[allow(non_camel_case_types)]
                    struct RetainShardsSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::RetainShardsRequest>
                    for RetainShardsSvc<T> {
                        type Response = super::RetainShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RetainShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).retain_shards(request).await
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
                        let method = RetainShardsSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/TruncateShards" => {
                    #[allow(non_camel_case_types)]
                    struct TruncateShardsSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::TruncateShardsRequest>
                    for TruncateShardsSvc<T> {
                        type Response = super::TruncateShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TruncateShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).truncate_shards(request).await
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
                        let method = TruncateShardsSvc(inner);
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
                "/quickwit.ingest.ingester.IngesterService/CloseShards" => {
                    #[allow(non_camel_case_types)]
                    struct CloseShardsSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
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
                "/quickwit.ingest.ingester.IngesterService/Ping" => {
                    #[allow(non_camel_case_types)]
                    struct PingSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
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
                "/quickwit.ingest.ingester.IngesterService/Decommission" => {
                    #[allow(non_camel_case_types)]
                    struct DecommissionSvc<T: IngesterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngesterServiceGrpc,
                    > tonic::server::UnaryService<super::DecommissionRequest>
                    for DecommissionSvc<T> {
                        type Response = super::DecommissionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DecommissionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).decommission(request).await
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
                        let method = DecommissionSvc(inner);
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
    impl<T: IngesterServiceGrpc> Clone for IngesterServiceGrpcServer<T> {
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
    impl<T: IngesterServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngesterServiceGrpc> tonic::server::NamedService
    for IngesterServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.ingest.ingester.IngesterService";
    }
}
