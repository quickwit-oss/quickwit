#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexUid {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub incarnation_id_high: u64,
    #[prost(uint64, tag = "3")]
    pub incarnation_id_low: u64,
}
