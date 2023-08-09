#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocBatchV2 {
    #[prost(bytes = "bytes", tag = "1")]
    pub doc_buffer: ::prost::bytes::Bytes,
    #[prost(uint32, repeated, tag = "2")]
    pub doc_lengths: ::prost::alloc::vec::Vec<u32>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shard {
    /// Immutable fields
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
    /// Mutable fields
    #[prost(enumeration = "ShardState", tag = "8")]
    pub shard_state: i32,
    #[prost(uint64, optional, tag = "9")]
    pub replication_position_inclusive: ::core::option::Option<u64>,
    #[prost(string, tag = "10")]
    pub publish_position_inclusive: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "11")]
    pub publish_token: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CommitTypeV2 {
    Auto = 0,
    Wait = 1,
    Force = 2,
}
impl CommitTypeV2 {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CommitTypeV2::Auto => "Auto",
            CommitTypeV2::Wait => "Wait",
            CommitTypeV2::Force => "Force",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Auto" => Some(Self::Auto),
            "Wait" => Some(Self::Wait),
            "Force" => Some(Self::Force),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShardState {
    /// / The shard is open and accepts write requests.
    Open = 0,
    /// / The shard is open and still accepts write requests, but should no longer be advertised to ingester routers.
    /// / It is waiting for the its leader or follower to close it with its final replication position, after which write requests will be rejected.
    Closing = 1,
    /// / The shard is closed and cannot be written to.
    /// / It can be safely deleted if the publish position is superior or equal to the replication position.
    Closed = 2,
}
impl ShardState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShardState::Open => "OPEN",
            ShardState::Closing => "CLOSING",
            ShardState::Closed => "CLOSED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "OPEN" => Some(Self::Open),
            "CLOSING" => Some(Self::Closing),
            "CLOSED" => Some(Self::Closed),
            _ => None,
        }
    }
}
