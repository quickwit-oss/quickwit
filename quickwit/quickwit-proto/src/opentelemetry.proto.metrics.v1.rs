/// MetricsData represents the metrics data that can be stored in a persistent
/// storage, OR can be embedded by other protocols that transfer OTLP metrics
/// data but do not implement the OTLP protocol.
///
/// The main difference between this message and collector protocol is that
/// in this message there will not be any "control" or "metadata" specific to
/// OTLP protocol.
///
/// When new fields are added into this message, the OTLP request MUST be updated
/// as well.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricsData {
    /// An array of ResourceMetrics.
    /// For data coming from a single resource this array will typically contain
    /// one element. Intermediary nodes that receive data from multiple origins
    /// typically batch the data before forwarding further and in that case this
    /// array will contain multiple elements.
    #[prost(message, repeated, tag="1")]
    pub resource_metrics: ::prost::alloc::vec::Vec<ResourceMetrics>,
}
/// A collection of ScopeMetrics from a Resource.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceMetrics {
    /// The resource for the metrics in this message.
    /// If this field is not set then no resource info is known.
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    /// A list of metrics that originate from a resource.
    #[prost(message, repeated, tag="2")]
    pub scope_metrics: ::prost::alloc::vec::Vec<ScopeMetrics>,
    /// This schema_url applies to the data in the "resource" field. It does not apply
    /// to the data in the "scope_metrics" field which have their own schema_url field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// A collection of Metrics produced by an Scope.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeMetrics {
    /// The instrumentation scope information for the metrics in this message.
    /// Semantically when InstrumentationScope isn't set, it is equivalent with
    /// an empty instrumentation scope name (unknown).
    #[prost(message, optional, tag="1")]
    pub scope: ::core::option::Option<super::super::common::v1::InstrumentationScope>,
    /// A list of metrics that originate from an instrumentation library.
    #[prost(message, repeated, tag="2")]
    pub metrics: ::prost::alloc::vec::Vec<Metric>,
    /// This schema_url applies to all metrics in the "metrics" field.
    #[prost(string, tag="3")]
    pub schema_url: ::prost::alloc::string::String,
}
/// Defines a Metric which has one or more timeseries.  The following is a
/// brief summary of the Metric data model.  For more details, see:
///
///    <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md>
///
///
/// The data model and relation between entities is shown in the
/// diagram below. Here, "DataPoint" is the term used to refer to any
/// one of the specific data point value types, and "points" is the term used
/// to refer to any one of the lists of points contained in the Metric.
///
/// - Metric is composed of a metadata and data.
/// - Metadata part contains a name, description, unit.
/// - Data is one of the possible types (Sum, Gauge, Histogram, Summary).
/// - DataPoint contains timestamps, attributes, and one of the possible value type
///    fields.
///
///      Metric
///   +------------+
///   |name        |
///   |description |
///   |unit        |     +------------------------------------+
///   |data        |---> |Gauge, Sum, Histogram, Summary, ... |
///   +------------+     +------------------------------------+
///
///     Data [One of Gauge, Sum, Histogram, Summary, ...]
///   +-----------+
///   |...        |  // Metadata about the Data.
///   |points     |--+
///   +-----------+  |
///                  |      +---------------------------+
///                  |      |DataPoint 1                |
///                  v      |+------+------+   +------+ |
///               +-----+   ||label |label |...|label | |
///               |  1  |-->||value1|value2|...|valueN| |
///               +-----+   |+------+------+   +------+ |
///               |  .  |   |+-----+                    |
///               |  .  |   ||value|                    |
///               |  .  |   |+-----+                    |
///               |  .  |   +---------------------------+
///               |  .  |                   .
///               |  .  |                   .
///               |  .  |                   .
///               |  .  |   +---------------------------+
///               |  .  |   |DataPoint M                |
///               +-----+   |+------+------+   +------+ |
///               |  M  |-->||label |label |...|label | |
///               +-----+   ||value1|value2|...|valueN| |
///                         |+------+------+   +------+ |
///                         |+-----+                    |
///                         ||value|                    |
///                         |+-----+                    |
///                         +---------------------------+
///
/// Each distinct type of DataPoint represents the output of a specific
/// aggregation function, the result of applying the DataPoint's
/// associated function of to one or more measurements.
///
/// All DataPoint types have three common fields:
/// - Attributes includes key-value pairs associated with the data point
/// - TimeUnixNano is required, set to the end time of the aggregation
/// - StartTimeUnixNano is optional, but strongly encouraged for DataPoints
///    having an AggregationTemporality field, as discussed below.
///
/// Both TimeUnixNano and StartTimeUnixNano values are expressed as
/// UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
///
/// # TimeUnixNano
///
/// This field is required, having consistent interpretation across
/// DataPoint types.  TimeUnixNano is the moment corresponding to when
/// the data point's aggregate value was captured.
///
/// Data points with the 0 value for TimeUnixNano SHOULD be rejected
/// by consumers.
///
/// # StartTimeUnixNano
///
/// StartTimeUnixNano in general allows detecting when a sequence of
/// observations is unbroken.  This field indicates to consumers the
/// start time for points with cumulative and delta
/// AggregationTemporality, and it should be included whenever possible
/// to support correct rate calculation.  Although it may be omitted
/// when the start time is truly unknown, setting StartTimeUnixNano is
/// strongly encouraged.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metric {
    /// name of the metric, including its DNS name prefix. It must be unique.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// description of the metric, which can be used in documentation.
    #[prost(string, tag="2")]
    pub description: ::prost::alloc::string::String,
    /// unit in which the metric value is reported. Follows the format
    /// described by <http://unitsofmeasure.org/ucum.html.>
    #[prost(string, tag="3")]
    pub unit: ::prost::alloc::string::String,
    /// Data determines the aggregation type (if any) of the metric, what is the
    /// reported value type for the data points, as well as the relatationship to
    /// the time interval over which they are reported.
    #[prost(oneof="metric::Data", tags="5, 7, 9, 10, 11")]
    pub data: ::core::option::Option<metric::Data>,
}
/// Nested message and enum types in `Metric`.
pub mod metric {
    /// Data determines the aggregation type (if any) of the metric, what is the
    /// reported value type for the data points, as well as the relatationship to
    /// the time interval over which they are reported.
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag="5")]
        Gauge(super::Gauge),
        #[prost(message, tag="7")]
        Sum(super::Sum),
        #[prost(message, tag="9")]
        Histogram(super::Histogram),
        #[prost(message, tag="10")]
        ExponentialHistogram(super::ExponentialHistogram),
        #[prost(message, tag="11")]
        Summary(super::Summary),
    }
}
/// Gauge represents the type of a scalar metric that always exports the
/// "current value" for every data point. It should be used for an "unknown"
/// aggregation.
///
/// A Gauge does not support different aggregation temporalities. Given the
/// aggregation is unknown, points cannot be combined using the same
/// aggregation, regardless of aggregation temporalities. Therefore,
/// AggregationTemporality is not included. Consequently, this also means
/// "StartTimeUnixNano" is ignored for all data points.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Gauge {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<NumberDataPoint>,
}
/// Sum represents the type of a scalar metric that is calculated as a sum of all
/// reported measurements over a time interval.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sum {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<NumberDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
    /// If "true" means that the sum is monotonic.
    #[prost(bool, tag="3")]
    pub is_monotonic: bool,
}
/// Histogram represents the type of a metric that is calculated by aggregating
/// as a Histogram of all reported measurements over a time interval.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Histogram {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<HistogramDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
}
/// ExponentialHistogram represents the type of a metric that is calculated by aggregating
/// as a ExponentialHistogram of all reported double measurements over a time interval.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExponentialHistogram {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<ExponentialHistogramDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
}
/// Summary metric data are used to convey quantile summaries,
/// a Prometheus (see: <https://prometheus.io/docs/concepts/metric_types/#summary>)
/// and OpenMetrics (see: <https://github.com/OpenObservability/OpenMetrics/blob/4dbf6075567ab43296eed941037c12951faafb92/protos/prometheus.proto#L45>)
/// data type. These data points cannot always be merged in a meaningful way.
/// While they can be useful in some applications, histogram data points are
/// recommended for new applications.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Summary {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<SummaryDataPoint>,
}
/// NumberDataPoint is a single data point in a timeseries that describes the
/// time-varying scalar value of a metric.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NumberDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from
    /// where this point belongs. The list may be empty (may contain 0 elements).
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="7")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// StartTimeUnixNano is optional but strongly encouraged, see the
    /// the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// TimeUnixNano is required, see the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="5")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
    /// Flags that apply to this specific data point.  See DataPointFlags
    /// for the available flags and their meaning.
    #[prost(uint32, tag="8")]
    pub flags: u32,
    /// The value itself.  A point is considered invalid when one of the recognized
    /// value fields is not present inside this oneof.
    #[prost(oneof="number_data_point::Value", tags="4, 6")]
    pub value: ::core::option::Option<number_data_point::Value>,
}
/// Nested message and enum types in `NumberDataPoint`.
pub mod number_data_point {
    /// The value itself.  A point is considered invalid when one of the recognized
    /// value fields is not present inside this oneof.
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(double, tag="4")]
        AsDouble(f64),
        #[prost(sfixed64, tag="6")]
        AsInt(i64),
    }
}
/// HistogramDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Histogram. A Histogram contains summary statistics
/// for a population of values, it may optionally contain the distribution of
/// those values across a set of buckets.
///
/// If the histogram contains the distribution of values, then both
/// "explicit_bounds" and "bucket counts" fields must be defined.
/// If the histogram does not contain the distribution of values, then both
/// "explicit_bounds" and "bucket_counts" must be omitted and only "count" and
/// "sum" are known.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HistogramDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from
    /// where this point belongs. The list may be empty (may contain 0 elements).
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="9")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// StartTimeUnixNano is optional but strongly encouraged, see the
    /// the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// TimeUnixNano is required, see the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be non-negative. This
    /// value must be equal to the sum of the "count" fields in buckets if a
    /// histogram is provided.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero.
    ///
    /// Note: Sum should only be filled out when measuring non-negative discrete
    /// events, and is assumed to be monotonic over the values of these events.
    /// Negative events *can* be recorded, but sum should not be filled out when
    /// doing so.  This is specifically to enforce compatibility w/ OpenMetrics,
    /// see: <https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#histogram>
    #[prost(double, optional, tag="5")]
    pub sum: ::core::option::Option<f64>,
    /// bucket_counts is an optional field contains the count values of histogram
    /// for each bucket.
    ///
    /// The sum of the bucket_counts must equal the value in the count field.
    ///
    /// The number of elements in bucket_counts array must be by one greater than
    /// the number of elements in explicit_bounds array.
    #[prost(fixed64, repeated, tag="6")]
    pub bucket_counts: ::prost::alloc::vec::Vec<u64>,
    /// explicit_bounds specifies buckets with explicitly defined bounds for values.
    ///
    /// The boundaries for bucket at index i are:
    ///
    /// (-infinity, explicit_bounds\[i]\] for i == 0
    /// (explicit_bounds\[i-1\], explicit_bounds\[i]\] for 0 < i < size(explicit_bounds)
    /// (explicit_bounds\[i-1\], +infinity) for i == size(explicit_bounds)
    ///
    /// The values in the explicit_bounds array must be strictly increasing.
    ///
    /// Histogram buckets are inclusive of their upper boundary, except the last
    /// bucket where the boundary is at infinity. This format is intentionally
    /// compatible with the OpenMetrics histogram definition.
    #[prost(double, repeated, tag="7")]
    pub explicit_bounds: ::prost::alloc::vec::Vec<f64>,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="8")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
    /// Flags that apply to this specific data point.  See DataPointFlags
    /// for the available flags and their meaning.
    #[prost(uint32, tag="10")]
    pub flags: u32,
    /// min is the minimum value over (start_time, end_time].
    #[prost(double, optional, tag="11")]
    pub min: ::core::option::Option<f64>,
    /// max is the maximum value over (start_time, end_time].
    #[prost(double, optional, tag="12")]
    pub max: ::core::option::Option<f64>,
}
/// ExponentialHistogramDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a ExponentialHistogram of double values. A ExponentialHistogram contains
/// summary statistics for a population of values, it may optionally contain the
/// distribution of those values across a set of buckets.
///
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExponentialHistogramDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from
    /// where this point belongs. The list may be empty (may contain 0 elements).
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="1")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// StartTimeUnixNano is optional but strongly encouraged, see the
    /// the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// TimeUnixNano is required, see the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be
    /// non-negative. This value must be equal to the sum of the "bucket_counts"
    /// values in the positive and negative Buckets plus the "zero_count" field.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero.
    ///
    /// Note: Sum should only be filled out when measuring non-negative discrete
    /// events, and is assumed to be monotonic over the values of these events.
    /// Negative events *can* be recorded, but sum should not be filled out when
    /// doing so.  This is specifically to enforce compatibility w/ OpenMetrics,
    /// see: <https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#histogram>
    #[prost(double, optional, tag="5")]
    pub sum: ::core::option::Option<f64>,
    /// scale describes the resolution of the histogram.  Boundaries are
    /// located at powers of the base, where:
    ///
    ///    base = (2^(2^-scale))
    ///
    /// The histogram bucket identified by `index`, a signed integer,
    /// contains values that are greater than (base^index) and
    /// less than or equal to (base^(index+1)).
    ///
    /// The positive and negative ranges of the histogram are expressed
    /// separately.  Negative values are mapped by their absolute value
    /// into the negative range using the same scale as the positive range.
    ///
    /// scale is not restricted by the protocol, as the permissible
    /// values depend on the range of the data.
    #[prost(sint32, tag="6")]
    pub scale: i32,
    /// zero_count is the count of values that are either exactly zero or
    /// within the region considered zero by the instrumentation at the
    /// tolerated degree of precision.  This bucket stores values that
    /// cannot be expressed using the standard exponential formula as
    /// well as values that have been rounded to zero.
    ///
    /// Implementations MAY consider the zero bucket to have probability
    /// mass equal to (zero_count / count).
    #[prost(fixed64, tag="7")]
    pub zero_count: u64,
    /// positive carries the positive range of exponential bucket counts.
    #[prost(message, optional, tag="8")]
    pub positive: ::core::option::Option<exponential_histogram_data_point::Buckets>,
    /// negative carries the negative range of exponential bucket counts.
    #[prost(message, optional, tag="9")]
    pub negative: ::core::option::Option<exponential_histogram_data_point::Buckets>,
    /// Flags that apply to this specific data point.  See DataPointFlags
    /// for the available flags and their meaning.
    #[prost(uint32, tag="10")]
    pub flags: u32,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="11")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
    /// min is the minimum value over (start_time, end_time].
    #[prost(double, optional, tag="12")]
    pub min: ::core::option::Option<f64>,
    /// max is the maximum value over (start_time, end_time].
    #[prost(double, optional, tag="13")]
    pub max: ::core::option::Option<f64>,
}
/// Nested message and enum types in `ExponentialHistogramDataPoint`.
pub mod exponential_histogram_data_point {
    /// Buckets are a set of bucket counts, encoded in a contiguous array
    /// of counts.
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Buckets {
        /// Offset is the bucket index of the first entry in the bucket_counts array.
        /// 
        /// Note: This uses a varint encoding as a simple form of compression.
        #[prost(sint32, tag="1")]
        pub offset: i32,
        /// Count is an array of counts, where count\[i\] carries the count
        /// of the bucket at index (offset+i).  count\[i\] is the count of
        /// values greater than base^(offset+i) and less or equal to than
        /// base^(offset+i+1).
        ///
        /// Note: By contrast, the explicit HistogramDataPoint uses
        /// fixed64.  This field is expected to have many buckets,
        /// especially zeros, so uint64 has been selected to ensure
        /// varint encoding.
        #[prost(uint64, repeated, tag="2")]
        pub bucket_counts: ::prost::alloc::vec::Vec<u64>,
    }
}
/// SummaryDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Summary metric.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SummaryDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from
    /// where this point belongs. The list may be empty (may contain 0 elements).
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="7")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// StartTimeUnixNano is optional but strongly encouraged, see the
    /// the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// TimeUnixNano is required, see the detailed comments above Metric.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be non-negative.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero.
    ///
    /// Note: Sum should only be filled out when measuring non-negative discrete
    /// events, and is assumed to be monotonic over the values of these events.
    /// Negative events *can* be recorded, but sum should not be filled out when
    /// doing so.  This is specifically to enforce compatibility w/ OpenMetrics,
    /// see: <https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#summary>
    #[prost(double, tag="5")]
    pub sum: f64,
    /// (Optional) list of values at different quantiles of the distribution calculated
    /// from the current snapshot. The quantiles must be strictly increasing.
    #[prost(message, repeated, tag="6")]
    pub quantile_values: ::prost::alloc::vec::Vec<summary_data_point::ValueAtQuantile>,
    /// Flags that apply to this specific data point.  See DataPointFlags
    /// for the available flags and their meaning.
    #[prost(uint32, tag="8")]
    pub flags: u32,
}
/// Nested message and enum types in `SummaryDataPoint`.
pub mod summary_data_point {
    /// Represents the value at a given quantile of a distribution.
    ///
    /// To record Min and Max values following conventions are used:
    /// - The 1.0 quantile is equivalent to the maximum value observed.
    /// - The 0.0 quantile is equivalent to the minimum value observed.
    ///
    /// See the following issue for more context:
    /// <https://github.com/open-telemetry/opentelemetry-proto/issues/125>
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValueAtQuantile {
        /// The quantile of a distribution. Must be in the interval
        /// [0.0, 1.0].
        #[prost(double, tag="1")]
        pub quantile: f64,
        /// The value at the given quantile of a distribution.
        ///
        /// Quantile values must NOT be negative.
        #[prost(double, tag="2")]
        pub value: f64,
    }
}
/// A representation of an exemplar, which is a sample input measurement.
/// Exemplars also hold information about the environment when the measurement
/// was recorded, for example the span and trace ID of the active span when the
/// exemplar was recorded.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Exemplar {
    /// The set of key/value pairs that were filtered out by the aggregator, but
    /// recorded alongside the original measurement. Only key/value pairs that were
    /// filtered out by the aggregator should be included
    #[prost(message, repeated, tag="7")]
    pub filtered_attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// time_unix_nano is the exact time when this exemplar was recorded
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub time_unix_nano: u64,
    /// (Optional) Span ID of the exemplar trace.
    /// span_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="4")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    /// (Optional) Trace ID of the exemplar trace.
    /// trace_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="5")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    /// The value of the measurement that was recorded. An exemplar is
    /// considered invalid when one of the recognized value fields is not present
    /// inside this oneof.
    #[prost(oneof="exemplar::Value", tags="3, 6")]
    pub value: ::core::option::Option<exemplar::Value>,
}
/// Nested message and enum types in `Exemplar`.
pub mod exemplar {
    /// The value of the measurement that was recorded. An exemplar is
    /// considered invalid when one of the recognized value fields is not present
    /// inside this oneof.
    #[derive(Serialize, Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(double, tag="3")]
        AsDouble(f64),
        #[prost(sfixed64, tag="6")]
        AsInt(i64),
    }
}
/// AggregationTemporality defines how a metric aggregator reports aggregated
/// values. It describes how those values relate to the time interval over
/// which they are aggregated.
#[derive(Serialize, Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregationTemporality {
    /// UNSPECIFIED is the default AggregationTemporality, it MUST not be used.
    Unspecified = 0,
    /// DELTA is an AggregationTemporality for a metric aggregator which reports
    /// changes since last report time. Successive metrics contain aggregation of
    /// values from continuous and non-overlapping intervals.
    ///
    /// The values for a DELTA metric are based only on the time interval
    /// associated with one measurement cycle. There is no dependency on
    /// previous measurements like is the case for CUMULATIVE metrics.
    ///
    /// For example, consider a system measuring the number of requests that
    /// it receives and reports the sum of these requests every second as a
    /// DELTA metric:
    ///
    ///    1. The system starts receiving at time=t_0.
    ///    2. A request is received, the system measures 1 request.
    ///    3. A request is received, the system measures 1 request.
    ///    4. A request is received, the system measures 1 request.
    ///    5. The 1 second collection cycle ends. A metric is exported for the
    ///       number of requests received over the interval of time t_0 to
    ///       t_0+1 with a value of 3.
    ///    6. A request is received, the system measures 1 request.
    ///    7. A request is received, the system measures 1 request.
    ///    8. The 1 second collection cycle ends. A metric is exported for the
    ///       number of requests received over the interval of time t_0+1 to
    ///       t_0+2 with a value of 2.
    Delta = 1,
    /// CUMULATIVE is an AggregationTemporality for a metric aggregator which
    /// reports changes since a fixed start time. This means that current values
    /// of a CUMULATIVE metric depend on all previous measurements since the
    /// start time. Because of this, the sender is required to retain this state
    /// in some form. If this state is lost or invalidated, the CUMULATIVE metric
    /// values MUST be reset and a new fixed start time following the last
    /// reported measurement time sent MUST be used.
    ///
    /// For example, consider a system measuring the number of requests that
    /// it receives and reports the sum of these requests every second as a
    /// CUMULATIVE metric:
    ///
    ///    1. The system starts receiving at time=t_0.
    ///    2. A request is received, the system measures 1 request.
    ///    3. A request is received, the system measures 1 request.
    ///    4. A request is received, the system measures 1 request.
    ///    5. The 1 second collection cycle ends. A metric is exported for the
    ///       number of requests received over the interval of time t_0 to
    ///       t_0+1 with a value of 3.
    ///    6. A request is received, the system measures 1 request.
    ///    7. A request is received, the system measures 1 request.
    ///    8. The 1 second collection cycle ends. A metric is exported for the
    ///       number of requests received over the interval of time t_0 to
    ///       t_0+2 with a value of 5.
    ///    9. The system experiences a fault and loses state.
    ///    10. The system recovers and resumes receiving at time=t_1.
    ///    11. A request is received, the system measures 1 request.
    ///    12. The 1 second collection cycle ends. A metric is exported for the
    ///       number of requests received over the interval of time t_1 to
    ///       t_0+1 with a value of 1.
    ///
    /// Note: Even though, when reporting changes since last report time, using
    /// CUMULATIVE is valid, it is not recommended. This may cause problems for
    /// systems that do not use start_time to determine when the aggregation
    /// value was reset (e.g. Prometheus).
    Cumulative = 2,
}
impl AggregationTemporality {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregationTemporality::Unspecified => "AGGREGATION_TEMPORALITY_UNSPECIFIED",
            AggregationTemporality::Delta => "AGGREGATION_TEMPORALITY_DELTA",
            AggregationTemporality::Cumulative => "AGGREGATION_TEMPORALITY_CUMULATIVE",
        }
    }
}
/// DataPointFlags is defined as a protobuf 'uint32' type and is to be used as a
/// bit-field representing 32 distinct boolean flags.  Each flag defined in this
/// enum is a bit-mask.  To test the presence of a single flag in the flags of
/// a data point, for example, use an expression like:
///
///    (point.flags & FLAG_NO_RECORDED_VALUE) == FLAG_NO_RECORDED_VALUE
///
#[derive(Serialize, Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DataPointFlags {
    FlagNone = 0,
    /// This DataPoint is valid but has no recorded value.  This value
    /// SHOULD be used to reflect explicitly missing data in a series, as
    /// for an equivalent to the Prometheus "staleness marker".
    FlagNoRecordedValue = 1,
}
impl DataPointFlags {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DataPointFlags::FlagNone => "FLAG_NONE",
            DataPointFlags::FlagNoRecordedValue => "FLAG_NO_RECORDED_VALUE",
        }
    }
}
