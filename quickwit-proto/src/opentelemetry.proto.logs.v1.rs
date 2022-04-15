/// LogsData represents the logs data that can be stored in a persistent storage,
/// OR can be embedded by other protocols that transfer OTLP logs data but do not
/// implement the OTLP protocol.
///
/// The main difference between this message and collector protocol is that
/// in this message there will not be any "control" or "metadata" specific to
/// OTLP protocol.
///
/// When new fields are added into this message, the OTLP request MUST be updated
/// as well.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogsData {
    /// An array of ResourceLogs.
    /// For data coming from a single resource this array will typically contain
    /// one element. Intermediary nodes that receive data from multiple origins
    /// typically batch the data before forwarding further and in that case this
    /// array will contain multiple elements.
    #[prost(message, repeated, tag="1")]
    pub resource_logs: ::prost::alloc::vec::Vec<ResourceLogs>,
}
/// A collection of ScopeLogs from a Resource.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceLogs {
    /// The resource for the logs in this message.
    /// If this field is not set then resource info is unknown.
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    /// A list of ScopeLogs that originate from a resource.
    #[prost(message, repeated, tag="2")]
    pub scope_logs: ::prost::alloc::vec::Vec<ScopeLogs>,
    /// A list of InstrumentationLibraryLogs that originate from a resource.
    /// This field is deprecated and will be removed after grace period expires on June 15, 2022.
    ///
    /// During the grace period the following rules SHOULD be followed:
    ///
    /// For Binary Protobufs
    /// ====================
    /// Binary Protobuf senders SHOULD NOT set instrumentation_library_logs. Instead
    /// scope_logs SHOULD be set.
    ///
    /// Binary Protobuf receivers SHOULD check if instrumentation_library_logs is set
    /// and scope_logs is not set then the value in instrumentation_library_logs
    /// SHOULD be used instead by converting InstrumentationLibraryLogs into ScopeLogs.
    /// If scope_logs is set then instrumentation_library_logs SHOULD be ignored.
    ///
    /// For JSON
    /// ========
    /// JSON senders that set instrumentation_library_logs field MAY also set
    /// scope_logs to carry the same logs, essentially double-publishing the same data.
    /// Such double-publishing MAY be controlled by a user-settable option.
    /// If double-publishing is not used then the senders SHOULD set scope_logs and
    /// SHOULD NOT set instrumentation_library_logs.
    ///
    /// JSON receivers SHOULD check if instrumentation_library_logs is set and
    /// scope_logs is not set then the value in instrumentation_library_logs
    /// SHOULD be used instead by converting InstrumentationLibraryLogs into ScopeLogs.
    /// If scope_logs is set then instrumentation_library_logs field SHOULD be ignored.
    #[deprecated]
    #[prost(message, repeated, tag="1000")]
    pub instrumentation_library_logs: ::prost::alloc::vec::Vec<InstrumentationLibraryLogs>,
    /// This schema_url applies to the data in the "resource" field. It does not apply
    /// to the data in the "scope_logs" field which have their own schema_url field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A collection of Logs produced by a Scope.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeLogs {
    /// The instrumentation scope information for the logs in this message.
    /// Semantically when InstrumentationScope isn't set, it is equivalent with
    /// an empty instrumentation scope name (unknown).
    #[prost(message, optional, tag="1")]
    pub scope: ::core::option::Option<super::super::common::v1::InstrumentationScope>,
    /// A list of log records.
    #[prost(message, repeated, tag="2")]
    pub log_records: ::prost::alloc::vec::Vec<LogRecord>,
    /// This schema_url applies to all logs in the "logs" field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A collection of Logs produced by an InstrumentationLibrary.
/// InstrumentationLibraryLogs is wire-compatible with ScopeLogs for binary
/// Protobuf format.
/// This message is deprecated and will be removed on June 15, 2022.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationLibraryLogs {
    /// The instrumentation library information for the logs in this message.
    /// Semantically when InstrumentationLibrary isn't set, it is equivalent with
    /// an empty instrumentation library name (unknown).
    #[prost(message, optional, tag="1")]
    pub instrumentation_library: ::core::option::Option<super::super::common::v1::InstrumentationLibrary>,
    /// A list of logs that originate from an instrumentation library.
    #[prost(message, repeated, tag="2")]
    pub log_records: ::prost::alloc::vec::Vec<LogRecord>,
    /// This schema_url applies to all logs in the "logs" field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A log record according to OpenTelemetry Log Data Model:
/// <https://github.com/open-telemetry/oteps/blob/main/text/logs/0097-log-data-model.md>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogRecord {
    /// time_unix_nano is the time when the event occurred.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    /// Value of 0 indicates unknown or missing timestamp.
    #[prost(fixed64, tag="1")]
    pub time_unix_nano: u64,
    /// Time when the event was observed by the collection system.
    /// For events that originate in OpenTelemetry (e.g. using OpenTelemetry Logging SDK)
    /// this timestamp is typically set at the generation time and is equal to Timestamp.
    /// For events originating externally and collected by OpenTelemetry (e.g. using
    /// Collector) this is the time when OpenTelemetry's code observed the event measured
    /// by the clock of the OpenTelemetry code. This field MUST be set once the event is
    /// observed by OpenTelemetry.
    ///
    /// For converting OpenTelemetry log data to formats that support only one timestamp or
    /// when receiving OpenTelemetry log data by recipients that support only one timestamp
    /// internally the following logic is recommended:
    ///   - Use time_unix_nano if it is present, otherwise use observed_time_unix_nano.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    /// Value of 0 indicates unknown or missing timestamp.
    #[prost(fixed64, tag="11")]
    pub observed_time_unix_nano: u64,
    /// Numerical value of the severity, normalized to values described in Log Data Model.
    /// \[Optional\].
    #[prost(enumeration="SeverityNumber", tag="2")]
    pub severity_number: i32,
    /// The severity text (also known as log level). The original string representation as
    /// it is known at the source. \[Optional\].
    #[prost(string, tag="3")]
    pub severity_text: ::prost::alloc::string::String,
    /// A value containing the body of the log record. Can be for example a human-readable
    /// string message (including multi-line) describing the event in a free form or it can
    /// be a structured data composed of arrays and maps of other values. \[Optional\].
    #[prost(message, optional, tag="5")]
    pub body: ::core::option::Option<super::super::common::v1::AnyValue>,
    /// Additional attributes that describe the specific event occurrence. \[Optional\].
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="6")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    #[prost(uint32, tag="7")]
    pub dropped_attributes_count: u32,
    /// Flags, a bit field. 8 least significant bits are the trace flags as
    /// defined in W3C Trace Context specification. 24 most significant bits are reserved
    /// and must be set to 0. Readers must not assume that 24 most significant bits
    /// will be zero and must correctly mask the bits when reading 8-bit trace flag (use
    /// flags & TRACE_FLAGS_MASK). \[Optional\].
    #[prost(fixed32, tag="8")]
    pub flags: u32,
    /// A unique identifier for a trace. All logs from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes
    /// is considered invalid. Can be set for logs that are part of request processing
    /// and have an assigned trace id. \[Optional\].
    #[prost(bytes="vec", tag="9")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes is considered
    /// invalid. Can be set for logs that are part of a particular processing span.
    /// If span_id is present trace_id SHOULD be also present. \[Optional\].
    #[prost(bytes="vec", tag="10")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
}
/// Possible values for LogRecord.SeverityNumber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SeverityNumber {
    /// UNSPECIFIED is the default SeverityNumber, it MUST NOT be used.
    Unspecified = 0,
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}
/// Masks for LogRecord.flags field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LogRecordFlags {
    LogRecordFlagUnspecified = 0,
    LogRecordFlagTraceFlagsMask = 255,
}
