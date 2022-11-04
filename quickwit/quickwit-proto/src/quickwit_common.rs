#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetQuery {
    /// List of terms to search.
    #[prost(string, repeated, tag="1")]
    pub terms: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Field to search in.
    #[prost(string, tag="2")]
    pub field_name: ::prost::alloc::string::String,
}
