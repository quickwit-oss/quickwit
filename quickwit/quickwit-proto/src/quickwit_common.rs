#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetQuery {
    /// List of terms to search.
    #[prost(message, repeated, tag="1")]
    pub terms: ::prost::alloc::vec::Vec<Term>,
    /// Field to search in.
    #[prost(string, tag="2")]
    pub field_name: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Term {
    #[prost(oneof="term::Term", tags="1, 2, 3, 4, 5")]
    pub term: ::core::option::Option<term::Term>,
}
/// Nested message and enum types in `Term`.
pub mod term {
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Term {
        #[prost(string, tag="1")]
        Text(::prost::alloc::string::String),
        #[prost(uint64, tag="2")]
        Unsigned(u64),
        #[prost(int64, tag="3")]
        Signed(i64),
        #[prost(double, tag="4")]
        Fp64(f64),
        /// bytes are represented as b64 text
        #[prost(bool, tag="5")]
        Boolean(bool),
    }
}
