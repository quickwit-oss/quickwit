// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use async_trait::async_trait;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::uri::Uri;
use quickwit_config::{ConfigFormat, IndexConfig, load_index_config_from_user_config};
use quickwit_ingest::CommitType;
use quickwit_parquet_engine::schema::REQUIRED_FIELDS;
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::opentelemetry::proto::collector::metrics::v1::metrics_service_server::MetricsService;
use quickwit_proto::opentelemetry::proto::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use quickwit_proto::opentelemetry::proto::metrics::v1::{
    AggregationTemporality, Metric, NumberDataPoint, metric,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use serde_json::Value as JsonValue;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{Span as RuntimeSpan, error, instrument, warn};

use super::arrow_metrics::{ArrowDocBatchV2Builder, ArrowMetricsBatchBuilder};
use super::{OtelSignal, extract_otel_index_id_from_metadata, ingest_doc_batch_v2};
use crate::otlp::extract_attributes;
use crate::otlp::metrics::OTLP_SERVICE_METRICS;

pub const OTEL_METRICS_INDEX_ID: &str = "otel-metrics-v0_9";

/// Minimal index config for the metrics index.
///
/// The parquet pipeline bypasses Tantivy entirely — it writes Arrow/Parquet data
/// directly and queries via DataFusion. The doc mapping is unused; this config
/// exists only so the index can be registered in the metastore for source
/// assignment and lifecycle management.
const OTEL_METRICS_INDEX_CONFIG: &str = r#"
version: 0.8

index_id: ${INDEX_ID}

doc_mapping:
  mode: dynamic

indexing_settings:
  commit_timeout_secs: 15

search_settings:
  default_search_fields: []
"#;

/// Metric type enumeration for efficient storage.
///
/// Stored as UInt8 in Arrow for compact representation.
/// Only a few metric types exist, so this is more efficient than
/// dictionary-encoded strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    /// Gauge metric - instantaneous value
    Gauge = 0,
    /// Sum metric - cumulative or delta sum
    Sum = 1,
    /// Histogram metric (not yet fully supported)
    Histogram = 2,
    /// Exponential histogram metric (not yet fully supported)
    ExponentialHistogram = 3,
    /// Summary metric (not yet fully supported)
    Summary = 4,
}

impl MetricType {
    /// Converts from u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Gauge),
            1 => Some(Self::Sum),
            2 => Some(Self::Histogram),
            3 => Some(Self::ExponentialHistogram),
            4 => Some(Self::Summary),
            _ => None,
        }
    }

    /// Gets the string representation for JSON/Tantivy.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Sum => "sum",
            Self::Histogram => "histogram",
            Self::ExponentialHistogram => "exponential_histogram",
            Self::Summary => "summary",
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OtlpMetricsError {
    #[error("failed to deserialize JSON metric records: `{0}`")]
    Json(#[from] serde_json::Error),
    #[error("failed to deserialize Protobuf metric records: `{0}`")]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<OtlpMetricsError> for tonic::Status {
    fn from(err: OtlpMetricsError) -> Self {
        match &err {
            OtlpMetricsError::InvalidArgument(_) => {
                tonic::Status::invalid_argument(err.to_string())
            }
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

/// Represents a single metric data point document.
#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    pub metric_name: String,
    pub metric_type: MetricType,
    pub timestamp_secs: u64,
    pub value: f64,
    pub tags: HashMap<String, String>,
}

/// Convert nanoseconds to seconds
fn nanos_to_secs(nanos: u64) -> u64 {
    nanos / 1_000_000_000
}

/// Convert a `serde_json::Value` to a plain `String`.
fn json_value_to_string(value: JsonValue) -> String {
    match value {
        JsonValue::String(s) => s,
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

struct ParsedMetrics {
    doc_batch: DocBatchV2,
    num_data_points: u64,
    num_parse_errors: u64,
    error_message: String,
}

#[derive(Clone)]
pub struct OtlpGrpcMetricsService {
    ingest_router: IngestRouterServiceClient,
}

impl OtlpGrpcMetricsService {
    pub fn new(ingest_router: IngestRouterServiceClient) -> Self {
        Self { ingest_router }
    }

    pub fn index_config(default_index_root_uri: &Uri) -> anyhow::Result<IndexConfig> {
        let index_config_str =
            OTEL_METRICS_INDEX_CONFIG.replace("${INDEX_ID}", OTEL_METRICS_INDEX_ID);
        let index_config = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            index_config_str.as_bytes(),
            default_index_root_uri,
        )?;
        Ok(index_config)
    }

    async fn export_inner(
        &mut self,
        request: ExportMetricsServiceRequest,
        index_id: IndexId,
        labels: [&str; 4],
    ) -> Result<ExportMetricsServiceResponse, Status> {
        let ParsedMetrics {
            doc_batch,
            num_data_points,
            num_parse_errors,
            error_message,
        } = run_cpu_intensive({
            let parent_span = RuntimeSpan::current();
            || Self::parse_metrics(request, parent_span)
        })
        .await
        .map_err(|join_error| {
            error!(error=?join_error, "failed to parse metric records");
            Status::internal("failed to parse metric records")
        })??;

        if num_data_points == 0 {
            // Empty request — nothing to ingest, return success.
            return Ok(ExportMetricsServiceResponse {
                partial_success: None,
            });
        }

        if num_data_points == num_parse_errors {
            // All data points were rejected.
            return Err(tonic::Status::internal(error_message));
        }

        let num_bytes = doc_batch.num_bytes() as u64;
        self.store_metrics(index_id, doc_batch).await?;

        OTLP_SERVICE_METRICS
            .ingested_data_points_total
            .with_label_values(labels)
            .inc_by(num_data_points - num_parse_errors);
        OTLP_SERVICE_METRICS
            .ingested_bytes_total
            .with_label_values(labels)
            .inc_by(num_bytes);

        let response = ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: num_parse_errors as i64,
                error_message,
            }),
        };
        Ok(response)
    }

    #[instrument(skip_all, parent = parent_span, fields(num_data_points = Empty, num_bytes = Empty, num_parse_errors = Empty))]
    #[allow(clippy::result_large_err)]
    fn parse_metrics(
        request: ExportMetricsServiceRequest,
        parent_span: RuntimeSpan,
    ) -> tonic::Result<ParsedMetrics> {
        let ParseOtlpResult {
            data_points,
            num_rejected,
        } = parse_otlp_metrics(request);
        let num_data_points = data_points.len() as u64 + num_rejected;

        // Build Arrow RecordBatch from valid data points
        let mut arrow_builder = ArrowMetricsBatchBuilder::with_capacity(data_points.len());
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut doc_uids = Vec::with_capacity(data_points.len());

        for data_point in data_points {
            arrow_builder.append(data_point);
            doc_uids.push(doc_uid_generator.next_doc_uid());
        }

        let record_batch = arrow_builder.finish();

        // Serialize to Arrow IPC format
        let doc_batch = ArrowDocBatchV2Builder::from_record_batch(&record_batch, doc_uids)
            .map_err(|e| {
                error!(error=?e, "failed to serialize Arrow IPC");
                tonic::Status::internal(format!("failed to serialize Arrow IPC: {e}"))
            })?
            .build();

        let current_span = RuntimeSpan::current();
        current_span.record("num_data_points", num_data_points);
        current_span.record("num_bytes", doc_batch.num_bytes());
        current_span.record("num_parse_errors", num_rejected);

        let error_message = if num_rejected > 0 {
            format!(
                "{num_rejected} data point(s) rejected (unsupported temporality or missing \
                 required fields)"
            )
        } else {
            String::new()
        };

        let parsed_metrics = ParsedMetrics {
            doc_batch,
            num_data_points,
            num_parse_errors: num_rejected,
            error_message,
        };
        Ok(parsed_metrics)
    }

    #[instrument(skip_all, fields(num_bytes = doc_batch.num_bytes()))]
    async fn store_metrics(
        &mut self,
        index_id: String,
        doc_batch: DocBatchV2,
    ) -> Result<(), tonic::Status> {
        ingest_doc_batch_v2(
            self.ingest_router.clone(),
            index_id,
            doc_batch,
            CommitType::Auto,
        )
        .await?;
        Ok(())
    }

    async fn export_instrumented(
        &mut self,
        request: ExportMetricsServiceRequest,
        index_id: IndexId,
    ) -> Result<ExportMetricsServiceResponse, Status> {
        let start = std::time::Instant::now();

        let labels = ["metrics", &index_id, "grpc", "protobuf"];

        OTLP_SERVICE_METRICS
            .requests_total
            .with_label_values(labels)
            .inc();

        let (export_res, is_error) =
            match self.export_inner(request, index_id.clone(), labels).await {
                ok @ Ok(_) => (ok, "false"),
                err @ Err(_) => {
                    OTLP_SERVICE_METRICS
                        .request_errors_total
                        .with_label_values(labels)
                        .inc();
                    (err, "true")
                }
            };

        let elapsed = start.elapsed().as_secs_f64();
        let labels = ["metrics", &index_id, "grpc", "protobuf", is_error];
        OTLP_SERVICE_METRICS
            .request_duration_seconds
            .with_label_values(labels)
            .observe(elapsed);

        export_res
    }
}

#[async_trait]
impl MetricsService for OtlpGrpcMetricsService {
    #[instrument(name = "ingest_metrics", skip_all)]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let index_id =
            extract_otel_index_id_from_metadata(request.metadata(), OtelSignal::Metrics)?;
        let request = request.into_inner();
        self.clone()
            .export_instrumented(request, index_id)
            .await
            .map(Response::new)
    }
}

struct ParseOtlpResult {
    data_points: Vec<MetricDataPoint>,
    num_rejected: u64,
}

fn parse_otlp_metrics(request: ExportMetricsServiceRequest) -> ParseOtlpResult {
    let mut data_points = Vec::new();
    let mut num_rejected: u64 = 0;

    for resource_metrics in request.resource_metrics {
        let mut resource_attributes = extract_attributes(
            resource_metrics
                .resource
                .map(|rsrc| rsrc.attributes)
                .unwrap_or_default(),
        );
        let service_name = match resource_attributes.remove("service.name") {
            Some(JsonValue::String(value)) => value,
            _ => "unknown_service".to_string(),
        };

        for scope_metrics in resource_metrics.scope_metrics {
            for metric in scope_metrics.metrics {
                parse_metric(
                    &metric,
                    &service_name,
                    &resource_attributes,
                    &mut data_points,
                    &mut num_rejected,
                );
            }
        }
    }

    ParseOtlpResult {
        data_points,
        num_rejected,
    }
}

fn parse_metric(
    metric: &Metric,
    service_name: &str,
    resource_attributes: &HashMap<String, JsonValue>,
    data_points: &mut Vec<MetricDataPoint>,
    num_rejected: &mut u64,
) {
    let metric_name = metric.name.clone();
    let metric_unit = if metric.unit.is_empty() {
        None
    } else {
        Some(metric.unit.clone())
    };

    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            for dp in &gauge.data_points {
                match create_number_data_point(
                    &metric_name,
                    MetricType::Gauge,
                    &metric_unit,
                    dp,
                    service_name,
                    resource_attributes,
                ) {
                    Ok(Some(data_point)) => data_points.push(data_point),
                    Ok(None) => *num_rejected += 1,
                    Err(err) => {
                        warn!(error = %err, metric_name, "skipping invalid gauge data point");
                        *num_rejected += 1;
                    }
                }
            }
        }
        Some(metric::Data::Sum(sum)) => {
            if sum.aggregation_temporality == AggregationTemporality::Cumulative as i32 {
                warn!(
                    metric_name,
                    "skipping sum metric with cumulative temporality (only delta is supported)"
                );
                *num_rejected += sum.data_points.len() as u64;
                return;
            }

            for dp in &sum.data_points {
                match create_number_data_point(
                    &metric_name,
                    MetricType::Sum,
                    &metric_unit,
                    dp,
                    service_name,
                    resource_attributes,
                ) {
                    Ok(Some(data_point)) => data_points.push(data_point),
                    Ok(None) => *num_rejected += 1,
                    Err(err) => {
                        warn!(error = %err, metric_name, "skipping invalid sum data point");
                        *num_rejected += 1;
                    }
                }
            }
        }
        Some(metric::Data::Histogram(h)) => {
            warn!("histogram metrics not supported, skipping");
            *num_rejected += h.data_points.len() as u64;
        }
        Some(metric::Data::ExponentialHistogram(h)) => {
            warn!("exponential histogram metrics not supported, skipping");
            *num_rejected += h.data_points.len() as u64;
        }
        Some(metric::Data::Summary(s)) => {
            warn!("summary metrics not supported, skipping");
            *num_rejected += s.data_points.len() as u64;
        }
        None => {
            warn!("metric has no data, skipping");
            *num_rejected += 1;
        }
    }
}

fn create_number_data_point(
    metric_name: &str,
    metric_type: MetricType,
    metric_unit: &Option<String>,
    dp: &NumberDataPoint,
    service_name: &str,
    resource_attributes: &HashMap<String, JsonValue>,
) -> Result<Option<MetricDataPoint>, OtlpMetricsError> {
    // Convert timestamps to seconds
    let timestamp_secs = nanos_to_secs(dp.time_unix_nano);

    // Validate: skip data points with empty metric_name or zero timestamp
    if metric_name.is_empty() {
        warn!("skipping data point with empty metric_name");
        return Ok(None);
    }
    if timestamp_secs == 0 {
        warn!("skipping data point with zero timestamp_secs");
        return Ok(None);
    }

    // Extract value as f64
    let value = match &dp.value {
        Some(
            quickwit_proto::opentelemetry::proto::metrics::v1::number_data_point::Value::AsDouble(
                v,
            ),
        ) => *v,
        Some(
            quickwit_proto::opentelemetry::proto::metrics::v1::number_data_point::Value::AsInt(v),
        ) => *v as f64,
        None => 0.0,
    };

    // Start with resource-level attributes as the base tags.
    // Skip keys that collide with fixed column names to avoid duplicate columns.
    let reserved = REQUIRED_FIELDS;
    let mut tags = HashMap::with_capacity(resource_attributes.len() + dp.attributes.len() + 3);
    for (key, json_val) in resource_attributes {
        if !reserved.contains(&key.as_str()) {
            tags.insert(key.clone(), json_value_to_string(json_val.clone()));
        }
    }

    // Data-point attributes override resource attributes.
    let attributes = extract_attributes(dp.attributes.clone());
    for (key, json_val) in attributes {
        if !reserved.contains(&key.as_str()) {
            tags.insert(key, json_value_to_string(json_val));
        }
    }

    // Add metric_unit and start_timestamp_secs using or_insert_with so a
    // data-point attribute with the same name is not silently overwritten.
    if let Some(unit) = metric_unit {
        tags.entry("metric_unit".to_string())
            .or_insert_with(|| unit.clone());
    }

    if dp.start_time_unix_nano != 0 {
        let start_ts = nanos_to_secs(dp.start_time_unix_nano);
        tags.entry("start_timestamp_secs".to_string())
            .or_insert_with(|| start_ts.to_string());
    }

    // Fall back to the resource-level service.name if no data-point-level
    // "service" tag was set. Data-point attributes take precedence.
    tags.entry("service".to_string())
        .or_insert_with(|| service_name.to_string());

    Ok(Some(MetricDataPoint {
        metric_name: metric_name.to_string(),
        metric_type,
        timestamp_secs,
        value,
        tags,
    }))
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::{CreateIndexRequestExt, metastore_for_test};
    use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService};

    use super::*;

    #[test]
    fn test_index_config_is_valid() {
        let index_config =
            OtlpGrpcMetricsService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        assert_eq!(index_config.index_id, OTEL_METRICS_INDEX_ID);
    }

    #[tokio::test]
    async fn test_create_index() {
        let metastore = metastore_for_test();
        let index_config =
            OtlpGrpcMetricsService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        metastore.create_index(create_index_request).await.unwrap();
    }

    #[test]
    fn test_nanos_to_secs() {
        assert_eq!(nanos_to_secs(0), 0);
        assert_eq!(nanos_to_secs(1_000_000_000), 1);
        assert_eq!(nanos_to_secs(1_500_000_000), 1);
        assert_eq!(nanos_to_secs(2_000_000_000), 2);
    }

    fn make_test_gauge_request() -> ExportMetricsServiceRequest {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: "CPU usage percentage".to_string(),
                        unit: "%".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "api".to_string(),
                                            )),
                                        }),
                                    },
                                    KeyValue {
                                        key: "env".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "prod".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(85.5)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn make_test_sum_delta_request() -> ExportMetricsServiceRequest {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            ResourceMetrics, ScopeMetrics, Sum, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "counter-service".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "http.requests".to_string(),
                        description: "HTTP request count".to_string(),
                        unit: "1".to_string(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "server-1".to_string(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsInt(100)),
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                            is_monotonic: true,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn make_test_sum_cumulative_request() -> ExportMetricsServiceRequest {
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            ResourceMetrics, ScopeMetrics, Sum, number_data_point,
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cumulative.counter".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: Vec::new(),
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(50.0)),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_parse_gauge_metrics() {
        let request = make_test_gauge_request();
        let data_points = parse_otlp_metrics(request).data_points;

        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "cpu.usage");
        assert_eq!(dp.metric_type, MetricType::Gauge);
        assert_eq!(dp.tags.get("metric_unit").map(|s| s.as_str()), Some("%"));
        assert_eq!(dp.timestamp_secs, 2);
        assert_eq!(
            dp.tags.get("start_timestamp_secs").map(|s| s.as_str()),
            Some("1")
        );
        assert_eq!(dp.value, 85.5);
        // Data-point attribute "service" takes precedence over resource-level service.name.
        assert_eq!(dp.tags.get("service").map(|s| s.as_str()), Some("api"));
        assert_eq!(dp.tags.get("env").map(|s| s.as_str()), Some("prod"));
    }

    #[test]
    fn test_parse_sum_delta_metrics() {
        let request = make_test_sum_delta_request();
        let data_points = parse_otlp_metrics(request).data_points;

        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "http.requests");
        assert_eq!(dp.metric_type, MetricType::Sum);
        assert_eq!(dp.tags.get("metric_unit").map(|s| s.as_str()), Some("1"));
        assert_eq!(dp.timestamp_secs, 2);
        assert_eq!(dp.value, 100.0); // int converted to f64
        assert_eq!(dp.tags.get("host").map(|s| s.as_str()), Some("server-1"));
        assert_eq!(
            dp.tags.get("service").map(|s| s.as_str()),
            Some("counter-service")
        );
    }

    #[test]
    fn test_reject_cumulative_temporality() {
        let request = make_test_sum_cumulative_request();
        let result = parse_otlp_metrics(request);

        // Cumulative sums are skipped (not a hard error) so other metrics in the same
        // request can still be processed. The rejected count is incremented instead.
        assert_eq!(result.data_points.len(), 0);
        assert_eq!(result.num_rejected, 1);
    }

    /// Test parsing metrics with various attribute types
    #[test]
    fn test_parse_metrics_with_various_attribute_types() {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("test".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "int_attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(42)),
                            }),
                        },
                        KeyValue {
                            key: "bool_attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::BoolValue(true)),
                            }),
                        },
                        KeyValue {
                            key: "double_attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::DoubleValue(std::f64::consts::PI)),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test.metric".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "string_tag".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "value".to_string(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 1);

        let dp = &data_points[0];
        assert_eq!(dp.tags.get("service").map(|s| s.as_str()), Some("test"));

        // Verify data point attributes are in tags as strings
        assert_eq!(dp.tags.get("string_tag").map(|s| s.as_str()), Some("value"));
    }

    /// Test metrics with empty and missing values
    #[test]
    fn test_parse_metrics_with_empty_values() {
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None, // No resource
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "minimal.metric".to_string(),
                        description: String::new(),
                        unit: String::new(), // Empty unit
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: Vec::new(),  // No attributes
                                start_time_unix_nano: 0, // No start time
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(0.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 1);

        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "minimal.metric");
        assert_eq!(
            dp.tags.get("service").map(|s| s.as_str()),
            Some("unknown_service")
        );
        // No metric_unit tag when unit is empty
        assert!(!dp.tags.contains_key("metric_unit"));
        // No start_timestamp_secs tag when start time is 0
        assert!(!dp.tags.contains_key("start_timestamp_secs"));
        // Only "service" should be in tags (no attributes, no unit, no start time)
        assert_eq!(dp.tags.len(), 1);
    }

    /// Test that data points with empty metric_name are skipped
    #[test]
    fn test_skip_empty_metric_name() {
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: String::new(), // Empty name
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: Vec::new(),
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 0);
    }

    /// Test that data points with zero timestamp are skipped
    #[test]
    fn test_skip_zero_timestamp() {
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test.metric".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: Vec::new(),
                                start_time_unix_nano: 0,
                                time_unix_nano: 0, // Zero timestamp
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 0);
    }

    #[test]
    fn test_resource_attributes_propagated() {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "my-service".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "env".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("prod".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "region".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("us-east-1".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: Vec::new(), // no data-point attributes
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        assert_eq!(dp.tags.get("env").map(|s| s.as_str()), Some("prod"));
        assert_eq!(dp.tags.get("region").map(|s| s.as_str()), Some("us-east-1"));
        assert_eq!(
            dp.tags.get("service").map(|s| s.as_str()),
            Some("my-service")
        );
    }

    #[test]
    fn test_data_point_attributes_override_resource_attributes() {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "my-service".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "env".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "resource-env".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "host".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "resource-host".to_string(),
                                )),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    // Override "env" from resource
                                    KeyValue {
                                        key: "env".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "dp-env".to_string(),
                                            )),
                                        }),
                                    },
                                    // Override "service" from resource service.name
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "dp-service".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        // Data-point "env" overrides resource "env"
        assert_eq!(dp.tags.get("env").map(|s| s.as_str()), Some("dp-env"));
        // Data-point "service" overrides resource service.name
        assert_eq!(
            dp.tags.get("service").map(|s| s.as_str()),
            Some("dp-service")
        );
        // Resource "host" is preserved (not overridden by data-point)
        assert_eq!(
            dp.tags.get("host").map(|s| s.as_str()),
            Some("resource-host")
        );
    }

    #[test]
    fn test_reserved_keys_stripped_from_tags() {
        use quickwit_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
        use quickwit_proto::opentelemetry::proto::metrics::v1::{
            Gauge, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        use quickwit_proto::opentelemetry::proto::resource::v1::Resource;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "my-service".to_string(),
                                )),
                            }),
                        },
                        // Reserved key at resource level — should be dropped
                        KeyValue {
                            key: "metric_name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("injected".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    // Reserved key at data-point level — should be dropped
                                    KeyValue {
                                        key: "timestamp_secs".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "999".to_string(),
                                            )),
                                        }),
                                    },
                                    // Non-reserved key — should be kept
                                    KeyValue {
                                        key: "env".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "prod".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: Vec::new(),
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let data_points = parse_otlp_metrics(request).data_points;
        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        // Reserved keys should not appear in tags
        assert!(!dp.tags.contains_key("metric_name"));
        assert!(!dp.tags.contains_key("timestamp_secs"));
        // Non-reserved keys should be present
        assert_eq!(dp.tags.get("env").map(|s| s.as_str()), Some("prod"));
        assert_eq!(
            dp.tags.get("service").map(|s| s.as_str()),
            Some("my-service")
        );
    }
}
