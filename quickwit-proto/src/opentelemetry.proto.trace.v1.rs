/// TracesData represents the traces data that can be stored in a persistent storage,
/// OR can be embedded by other protocols that transfer OTLP traces data but do
/// not implement the OTLP protocol.
///
/// The main difference between this message and collector protocol is that
/// in this message there will not be any "control" or "metadata" specific to
/// OTLP protocol.
///
/// When new fields are added into this message, the OTLP request MUST be updated
/// as well.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TracesData {
    /// An array of ResourceSpans.
    /// For data coming from a single resource this array will typically contain
    /// one element. Intermediary nodes that receive data from multiple origins
    /// typically batch the data before forwarding further and in that case this
    /// array will contain multiple elements.
    #[prost(message, repeated, tag="1")]
    pub resource_spans: ::prost::alloc::vec::Vec<ResourceSpans>,
}
/// A collection of ScopeSpans from a Resource.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceSpans {
    /// The resource for the spans in this message.
    /// If this field is not set then no resource info is known.
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    /// A list of ScopeSpans that originate from a resource.
    #[prost(message, repeated, tag="2")]
    pub scope_spans: ::prost::alloc::vec::Vec<ScopeSpans>,
    /// A list of InstrumentationLibrarySpans that originate from a resource.
    /// This field is deprecated and will be removed after grace period expires on June 15, 2022.
    ///
    /// During the grace period the following rules SHOULD be followed:
    ///
    /// For Binary Protobufs
    /// ====================
    /// Binary Protobuf senders SHOULD NOT set instrumentation_library_spans. Instead
    /// scope_spans SHOULD be set.
    ///
    /// Binary Protobuf receivers SHOULD check if instrumentation_library_spans is set
    /// and scope_spans is not set then the value in instrumentation_library_spans
    /// SHOULD be used instead by converting InstrumentationLibrarySpans into ScopeSpans.
    /// If scope_spans is set then instrumentation_library_spans SHOULD be ignored.
    ///
    /// For JSON
    /// ========
    /// JSON senders that set instrumentation_library_spans field MAY also set
    /// scope_spans to carry the same spans, essentially double-publishing the same data.
    /// Such double-publishing MAY be controlled by a user-settable option.
    /// If double-publishing is not used then the senders SHOULD set scope_spans and
    /// SHOULD NOT set instrumentation_library_spans.
    ///
    /// JSON receivers SHOULD check if instrumentation_library_spans is set and
    /// scope_spans is not set then the value in instrumentation_library_spans
    /// SHOULD be used instead by converting InstrumentationLibrarySpans into ScopeSpans.
    /// If scope_spans is set then instrumentation_library_spans field SHOULD be ignored.
    #[deprecated]
    #[prost(message, repeated, tag="1000")]
    pub instrumentation_library_spans: ::prost::alloc::vec::Vec<InstrumentationLibrarySpans>,
    /// This schema_url applies to the data in the "resource" field. It does not apply
    /// to the data in the "scope_spans" field which have their own schema_url field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A collection of Spans produced by an InstrumentationScope.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeSpans {
    /// The instrumentation scope information for the spans in this message.
    /// Semantically when InstrumentationScope isn't set, it is equivalent with
    /// an empty instrumentation scope name (unknown).
    #[prost(message, optional, tag="1")]
    pub scope: ::core::option::Option<super::super::common::v1::InstrumentationScope>,
    /// A list of Spans that originate from an instrumentation scope.
    #[prost(message, repeated, tag="2")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    /// This schema_url applies to all spans and span events in the "spans" field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A collection of Spans produced by an InstrumentationLibrary.
/// InstrumentationLibrarySpans is wire-compatible with ScopeSpans for binary
/// Protobuf format.
/// This message is deprecated and will be removed on June 15, 2022.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationLibrarySpans {
    /// The instrumentation library information for the spans in this message.
    /// Semantically when InstrumentationLibrary isn't set, it is equivalent with
    /// an empty instrumentation library name (unknown).
    #[prost(message, optional, tag="1")]
    pub instrumentation_library: ::core::option::Option<super::super::common::v1::InstrumentationLibrary>,
    /// A list of Spans that originate from an instrumentation library.
    #[prost(message, repeated, tag="2")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    /// This schema_url applies to all spans and span events in the "spans" field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// Span represents a single operation within a trace. Spans can be
/// nested to form a trace tree. Spans may also be linked to other spans
/// from the same or different trace and form graphs. Often, a trace
/// contains a root span that describes the end-to-end latency, and one
/// or more subspans for its sub-operations. A trace can also contain
/// multiple root spans, or none at all. Spans do not need to be
/// contiguous - there may be gaps or overlaps between spans in a trace.
///
/// The next available field id is 17.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes
    /// is considered invalid.
    ///
    /// This field is semantically required. Receiver should generate new
    /// random trace_id if empty or invalid trace_id was received.
    ///
    /// This field is required.
    #[prost(bytes="vec", tag="1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes is considered
    /// invalid.
    ///
    /// This field is semantically required. Receiver should generate new
    /// random span_id if empty or invalid span_id was received.
    ///
    /// This field is required.
    #[prost(bytes="vec", tag="2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    /// trace_state conveys information about request position in multiple distributed tracing graphs.
    /// It is a trace_state in w3c-trace-context format: <https://www.w3.org/TR/trace-context/#tracestate-header>
    /// See also <https://github.com/w3c/distributed-tracing> for more details about this field.
    #[prost(string, tag="3")]
    pub trace_state: ::prost::alloc::string::String,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[prost(bytes="vec", tag="4")]
    pub parent_span_id: ::prost::alloc::vec::Vec<u8>,
    /// A description of the span's operation.
    ///
    /// For example, the name can be a qualified method name or a file name
    /// and a line number where the operation is called. A best practice is to use
    /// the same display name at the same call point in an application.
    /// This makes it easier to correlate spans in different traces.
    ///
    /// This field is semantically required to be set to non-empty string.
    /// Empty value is equivalent to an unknown span name.
    ///
    /// This field is required.
    #[prost(string, tag="5")]
    pub name: ::prost::alloc::string::String,
    /// Distinguishes between spans generated in a particular context. For example,
    /// two spans with the same name may be distinguished using `CLIENT` (caller)
    /// and `SERVER` (callee) to identify queueing latency associated with the span.
    #[prost(enumeration="span::SpanKind", tag="6")]
    pub kind: i32,
    /// start_time_unix_nano is the start time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution starts. On the server side, this
    /// is the time when the server's application handler starts running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    #[prost(fixed64, tag="7")]
    pub start_time_unix_nano: u64,
    /// end_time_unix_nano is the end time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution ends. On the server side, this
    /// is the time when the server application handler stops running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    #[prost(fixed64, tag="8")]
    pub end_time_unix_nano: u64,
    /// attributes is a collection of key/value pairs. Note, global attributes
    /// like server name can be set using the resource API. Examples of attributes:
    ///
    ///     "/http/user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
    ///     "/http/server_latency": 300
    ///     "abc.com/myattribute": true
    ///     "abc.com/score": 10.239
    ///
    /// The OpenTelemetry API specification further restricts the allowed value types:
    /// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/common.md#attributes>
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="9")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// dropped_attributes_count is the number of attributes that were discarded. Attributes
    /// can be discarded because their keys are too long or because there are too many
    /// attributes. If this value is 0, then no attributes were dropped.
    #[prost(uint32, tag="10")]
    pub dropped_attributes_count: u32,
    /// events is a collection of Event items.
    #[prost(message, repeated, tag="11")]
    pub events: ::prost::alloc::vec::Vec<span::Event>,
    /// dropped_events_count is the number of dropped events. If the value is 0, then no
    /// events were dropped.
    #[prost(uint32, tag="12")]
    pub dropped_events_count: u32,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    #[prost(message, repeated, tag="13")]
    pub links: ::prost::alloc::vec::Vec<span::Link>,
    /// dropped_links_count is the number of dropped links after the maximum size was
    /// enforced. If this value is 0, then no links were dropped.
    #[prost(uint32, tag="14")]
    pub dropped_links_count: u32,
    /// An optional final status for this span. Semantically when Status isn't set, it means
    /// span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    #[prost(message, optional, tag="15")]
    pub status: ::core::option::Option<Status>,
}
/// Nested message and enum types in `Span`.
pub mod span {
    /// Event is a time-stamped annotation of the span, consisting of user-supplied
    /// text description and key-value pairs.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Event {
        /// time_unix_nano is the time the event occurred.
        #[prost(fixed64, tag="1")]
        pub time_unix_nano: u64,
        /// name of the event.
        /// This field is semantically required to be set to non-empty string.
        #[prost(string, tag="2")]
        pub name: ::prost::alloc::string::String,
        /// attributes is a collection of attribute key/value pairs on the event.
        /// Attribute keys MUST be unique (it is not allowed to have more than one
        /// attribute with the same key).
        #[prost(message, repeated, tag="3")]
        pub attributes: ::prost::alloc::vec::Vec<super::super::super::common::v1::KeyValue>,
        /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
        /// then no attributes were dropped.
        #[prost(uint32, tag="4")]
        pub dropped_attributes_count: u32,
    }
    /// A pointer from the current span to another span in the same trace or in a
    /// different trace. For example, this can be used in batching operations,
    /// where a single batch handler processes multiple requests from different
    /// traces or when the handler receives a request from a different project.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        /// A unique identifier of a trace that this linked span is part of. The ID is a
        /// 16-byte array.
        #[prost(bytes="vec", tag="1")]
        pub trace_id: ::prost::alloc::vec::Vec<u8>,
        /// A unique identifier for the linked span. The ID is an 8-byte array.
        #[prost(bytes="vec", tag="2")]
        pub span_id: ::prost::alloc::vec::Vec<u8>,
        /// The trace_state associated with the link.
        #[prost(string, tag="3")]
        pub trace_state: ::prost::alloc::string::String,
        /// attributes is a collection of attribute key/value pairs on the link.
        /// Attribute keys MUST be unique (it is not allowed to have more than one
        /// attribute with the same key).
        #[prost(message, repeated, tag="4")]
        pub attributes: ::prost::alloc::vec::Vec<super::super::super::common::v1::KeyValue>,
        /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
        /// then no attributes were dropped.
        #[prost(uint32, tag="5")]
        pub dropped_attributes_count: u32,
    }
    /// SpanKind is the type of span. Can be used to specify additional relationships between spans
    /// in addition to a parent/child relationship.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SpanKind {
        /// Unspecified. Do NOT use as default.
        /// Implementations MAY assume SpanKind to be INTERNAL when receiving UNSPECIFIED.
        Unspecified = 0,
        /// Indicates that the span represents an internal operation within an application,
        /// as opposed to an operation happening at the boundaries. Default value.
        Internal = 1,
        /// Indicates that the span covers server-side handling of an RPC or other
        /// remote network request.
        Server = 2,
        /// Indicates that the span describes a request to some remote service.
        Client = 3,
        /// Indicates that the span describes a producer sending a message to a broker.
        /// Unlike CLIENT and SERVER, there is often no direct critical path latency relationship
        /// between producer and consumer spans. A PRODUCER span ends when the message was accepted
        /// by the broker while the logical processing of the message might span a much longer time.
        Producer = 4,
        /// Indicates that the span describes consumer receiving a message from a broker.
        /// Like the PRODUCER kind, there is often no direct critical path latency relationship
        /// between producer and consumer spans.
        Consumer = 5,
    }
}
/// The Status type defines a logical error model that is suitable for different
/// programming environments, including REST APIs and RPC APIs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    /// A developer-facing human readable error message.
    #[prost(string, tag="2")]
    pub message: ::prost::alloc::string::String,
    /// The status code.
    #[prost(enumeration="status::StatusCode", tag="3")]
    pub code: i32,
}
/// Nested message and enum types in `Status`.
pub mod status {
    /// For the semantics of status codes see
    /// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#set-status>
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        /// The default status.
        Unset = 0,
        /// The Span has been validated by an Application developers or Operator to have
        /// completed successfully.
        Ok = 1,
        /// The Span contains an error.
        Error = 2,
    }
}
