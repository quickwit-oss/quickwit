#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
    #[prost(enumeration="ValueType", tag="2")]
    pub v_type: i32,
    #[prost(string, tag="3")]
    pub v_str: ::prost::alloc::string::String,
    #[prost(bool, tag="4")]
    pub v_bool: bool,
    #[prost(int64, tag="5")]
    pub v_int64: i64,
    #[prost(double, tag="6")]
    pub v_float64: f64,
    #[prost(bytes="vec", tag="7")]
    pub v_binary: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    #[prost(message, optional, tag="1")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, repeated, tag="2")]
    pub fields: ::prost::alloc::vec::Vec<KeyValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpanRef {
    #[prost(bytes="vec", tag="1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration="SpanRefType", tag="3")]
    pub ref_type: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Process {
    #[prost(string, tag="1")]
    pub service_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub tags: ::prost::alloc::vec::Vec<KeyValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(bytes="vec", tag="1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag="3")]
    pub operation_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub references: ::prost::alloc::vec::Vec<SpanRef>,
    #[prost(uint32, tag="5")]
    pub flags: u32,
    #[prost(message, optional, tag="6")]
    pub start_time: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="7")]
    pub duration: ::core::option::Option<::prost_types::Duration>,
    #[prost(message, repeated, tag="8")]
    pub tags: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag="9")]
    pub logs: ::prost::alloc::vec::Vec<Log>,
    #[prost(message, optional, tag="10")]
    pub process: ::core::option::Option<Process>,
    #[prost(string, tag="11")]
    pub process_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="12")]
    pub warnings: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Trace {
    #[prost(message, repeated, tag="1")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    #[prost(message, repeated, tag="2")]
    pub process_map: ::prost::alloc::vec::Vec<trace::ProcessMapping>,
    #[prost(string, repeated, tag="3")]
    pub warnings: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `Trace`.
pub mod trace {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ProcessMapping {
        #[prost(string, tag="1")]
        pub process_id: ::prost::alloc::string::String,
        #[prost(message, optional, tag="2")]
        pub process: ::core::option::Option<super::Process>,
    }
}
/// Note that both Span and Batch may contain a Process.
/// This is different from the Thrift model which was only used
/// for transport, because Proto model is also used by the backend
/// as the domain model, where once a batch is received it is split
/// into individual spans which are all processed independently,
/// and therefore they all need a Process. As far as on-the-wire
/// semantics, both Batch and Spans in the same message may contain
/// their own instances of Process, with span.Process taking priority
/// over batch.Process.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Batch {
    #[prost(message, repeated, tag="1")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    #[prost(message, optional, tag="2")]
    pub process: ::core::option::Option<Process>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DependencyLink {
    #[prost(string, tag="1")]
    pub parent: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub child: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub call_count: u64,
    #[prost(string, tag="4")]
    pub source: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ValueType {
    String = 0,
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Binary = 4,
}
impl ValueType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ValueType::String => "STRING",
            ValueType::Bool => "BOOL",
            ValueType::Int64 => "INT64",
            ValueType::Float64 => "FLOAT64",
            ValueType::Binary => "BINARY",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SpanRefType {
    ChildOf = 0,
    FollowsFrom = 1,
}
impl SpanRefType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SpanRefType::ChildOf => "CHILD_OF",
            SpanRefType::FollowsFrom => "FOLLOWS_FROM",
        }
    }
}
