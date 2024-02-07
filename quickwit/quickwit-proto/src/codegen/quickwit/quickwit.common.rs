/// The corresponding Rust struct \[`crate::types::IndexUid`\] is defined manually and
/// externally provided during code generation (see `build.rs`).
///
/// Modify at your own risk.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexUid {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub incarnation_id: ::prost::alloc::vec::Vec<u8>,
}
