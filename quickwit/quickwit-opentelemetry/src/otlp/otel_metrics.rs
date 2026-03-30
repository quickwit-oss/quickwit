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
///
/// TODO: As a temporary hack, we are including a timestamp_field, so that
/// we can pass the retention policy validation.
const OTEL_METRICS_INDEX_CONFIG: &str = r#"
version: 0.8

index_id: ${INDEX_ID}

doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      fast: true
  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 15

retention:
  period: 30 days
  schedule: hourly

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

/// Represents a single metric data point document
#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    // Metric identity
    pub metric_name: String,
    pub metric_type: MetricType,
    pub metric_unit: Option<String>,

    // Timestamps (seconds granularity)
    pub timestamp_secs: u64,
    pub start_timestamp_secs: Option<u64>,

    // Value (f64 only)
    pub value: f64,

    // Explicit tag columns
    pub tag_service: Option<String>,
    pub tag_env: Option<String>,
    pub tag_datacenter: Option<String>,
    pub tag_region: Option<String>,
    pub tag_host: Option<String>,

    // Dynamic tags (remaining attributes)
    pub attributes: HashMap<String, JsonValue>,

    // Resource metadata
    pub service_name: String,
    pub resource_attributes: HashMap<String, JsonValue>,
}

struct ExplicitTags {
    service: Option<String>,
    env: Option<String>,
    datacenter: Option<String>,
    region: Option<String>,
    host: Option<String>,
}

fn extract_string_tag(attributes: &mut HashMap<String, JsonValue>, key: &str) -> Option<String> {
    attributes.remove(key).and_then(|v| match v {
        JsonValue::String(s) => Some(s),
        _ => None,
    })
}

fn extract_explicit_tags(attributes: &mut HashMap<String, JsonValue>) -> ExplicitTags {
    ExplicitTags {
        service: extract_string_tag(attributes, "service"),
        env: extract_string_tag(attributes, "env"),
        datacenter: extract_string_tag(attributes, "datacenter"),
        region: extract_string_tag(attributes, "region"),
        host: extract_string_tag(attributes, "host"),
    }
}

/// Convert nanoseconds to seconds
fn nanos_to_secs(nanos: u64) -> u64 {
    nanos / 1_000_000_000
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

        if num_data_points == num_parse_errors {
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
        let data_points = parse_otlp_metrics(request)?;
        let num_data_points = data_points.len() as u64;

        // Build Arrow RecordBatch from data points
        let mut arrow_builder = ArrowMetricsBatchBuilder::with_capacity(num_data_points as usize);
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut doc_uids = Vec::with_capacity(num_data_points as usize);

        for data_point in &data_points {
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
        current_span.record("num_parse_errors", 0u64);

        let parsed_metrics = ParsedMetrics {
            doc_batch,
            num_data_points,
            num_parse_errors: 0,
            error_message: String::new(),
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

fn parse_otlp_metrics(
    request: ExportMetricsServiceRequest,
) -> Result<Vec<MetricDataPoint>, OtlpMetricsError> {
    let mut data_points = Vec::new();

    for resource_metrics in request.resource_metrics {
        let mut resource_attributes = extract_attributes(
            resource_metrics
                .resource
                .clone()
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
                )?;
            }
        }
    }

    Ok(data_points)
}

fn parse_metric(
    metric: &Metric,
    service_name: &str,
    resource_attributes: &HashMap<String, JsonValue>,
    data_points: &mut Vec<MetricDataPoint>,
) -> Result<(), OtlpMetricsError> {
    let metric_name = metric.name.clone();
    let metric_unit = if metric.unit.is_empty() {
        None
    } else {
        Some(metric.unit.clone())
    };

    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            for dp in &gauge.data_points {
                let data_point = create_number_data_point(
                    &metric_name,
                    MetricType::Gauge,
                    &metric_unit,
                    dp,
                    service_name,
                    resource_attributes,
                )?;
                data_points.push(data_point);
            }
        }
        Some(metric::Data::Sum(sum)) => {
            // Only support DELTA temporality
            if sum.aggregation_temporality == AggregationTemporality::Cumulative as i32 {
                return Err(OtlpMetricsError::InvalidArgument(
                    "cumulative aggregation temporality is not supported, only delta is supported"
                        .to_string(),
                ));
            }

            for dp in &sum.data_points {
                let data_point = create_number_data_point(
                    &metric_name,
                    MetricType::Sum,
                    &metric_unit,
                    dp,
                    service_name,
                    resource_attributes,
                )?;
                data_points.push(data_point);
            }
        }
        Some(metric::Data::Histogram(_)) => {
            warn!("histogram metrics not supported, skipping");
        }
        Some(metric::Data::ExponentialHistogram(_)) => {
            warn!("exponential histogram metrics not supported, skipping");
        }
        Some(metric::Data::Summary(_)) => {
            warn!("summary metrics not supported, skipping");
        }
        None => {
            warn!("metric has no data, skipping");
        }
    }

    Ok(())
}

fn create_number_data_point(
    metric_name: &str,
    metric_type: MetricType,
    metric_unit: &Option<String>,
    dp: &NumberDataPoint,
    service_name: &str,
    resource_attributes: &HashMap<String, JsonValue>,
) -> Result<MetricDataPoint, OtlpMetricsError> {
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

    // Extract attributes and explicit tags
    let mut attributes = extract_attributes(dp.attributes.clone());
    let explicit_tags = extract_explicit_tags(&mut attributes);

    // Convert timestamps to seconds
    let timestamp_secs = nanos_to_secs(dp.time_unix_nano);
    let start_timestamp_secs = if dp.start_time_unix_nano != 0 {
        Some(nanos_to_secs(dp.start_time_unix_nano))
    } else {
        None
    };

    Ok(MetricDataPoint {
        metric_name: metric_name.to_string(),
        metric_type,
        metric_unit: metric_unit.clone(),
        timestamp_secs,
        start_timestamp_secs,
        value,
        tag_service: explicit_tags.service,
        tag_env: explicit_tags.env,
        tag_datacenter: explicit_tags.datacenter,
        tag_region: explicit_tags.region,
        tag_host: explicit_tags.host,
        attributes,
        service_name: service_name.to_string(),
        resource_attributes: resource_attributes.clone(),
    })
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
        let retention = index_config
            .retention_policy_opt
            .expect("retention policy should be set");
        assert_eq!(
            retention.retention_period().unwrap(),
            std::time::Duration::from_secs(30 * 24 * 3600)
        );
        assert_eq!(retention.evaluation_schedule, "hourly");
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

    #[test]
    fn test_extract_explicit_tags() {
        let mut attributes = HashMap::from([
            ("service".to_string(), JsonValue::String("api".to_string())),
            ("env".to_string(), JsonValue::String("prod".to_string())),
            (
                "datacenter".to_string(),
                JsonValue::String("us-east".to_string()),
            ),
            (
                "region".to_string(),
                JsonValue::String("us-east-1".to_string()),
            ),
            (
                "host".to_string(),
                JsonValue::String("server-1".to_string()),
            ),
            (
                "custom_tag".to_string(),
                JsonValue::String("custom_value".to_string()),
            ),
        ]);

        let explicit_tags = extract_explicit_tags(&mut attributes);

        assert_eq!(explicit_tags.service, Some("api".to_string()));
        assert_eq!(explicit_tags.env, Some("prod".to_string()));
        assert_eq!(explicit_tags.datacenter, Some("us-east".to_string()));
        assert_eq!(explicit_tags.region, Some("us-east-1".to_string()));
        assert_eq!(explicit_tags.host, Some("server-1".to_string()));

        // custom_tag should remain in attributes
        assert_eq!(attributes.len(), 1);
        assert!(attributes.contains_key("custom_tag"));
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
        let data_points = parse_otlp_metrics(request).unwrap();

        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "cpu.usage");
        assert_eq!(dp.metric_type, MetricType::Gauge);
        assert_eq!(dp.metric_unit, Some("%".to_string()));
        assert_eq!(dp.timestamp_secs, 2);
        assert_eq!(dp.start_timestamp_secs, Some(1));
        assert_eq!(dp.value, 85.5);
        assert_eq!(dp.tag_service, Some("api".to_string()));
        assert_eq!(dp.tag_env, Some("prod".to_string()));
        assert_eq!(dp.service_name, "test-service");
    }

    #[test]
    fn test_parse_sum_delta_metrics() {
        let request = make_test_sum_delta_request();
        let data_points = parse_otlp_metrics(request).unwrap();

        assert_eq!(data_points.len(), 1);
        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "http.requests");
        assert_eq!(dp.metric_type, MetricType::Sum);
        assert_eq!(dp.metric_unit, Some("1".to_string()));
        assert_eq!(dp.timestamp_secs, 2);
        assert_eq!(dp.value, 100.0); // int converted to f64
        assert_eq!(dp.tag_host, Some("server-1".to_string()));
        assert_eq!(dp.service_name, "counter-service");
    }

    #[test]
    fn test_reject_cumulative_temporality() {
        let request = make_test_sum_cumulative_request();
        let result = parse_otlp_metrics(request);

        assert!(result.is_err());
        match result.unwrap_err() {
            OtlpMetricsError::InvalidArgument(_) => {}
            err => panic!("unexpected error type: {:?}", err),
        }
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

        let data_points = parse_otlp_metrics(request).unwrap();
        assert_eq!(data_points.len(), 1);

        let dp = &data_points[0];
        assert_eq!(dp.service_name, "test");

        // Verify resource attributes contain the non-service.name attributes
        assert!(dp.resource_attributes.contains_key("int_attr"));
        assert!(dp.resource_attributes.contains_key("bool_attr"));
        assert!(dp.resource_attributes.contains_key("double_attr"));

        // Verify data point attributes
        assert!(dp.attributes.contains_key("string_tag"));
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

        let data_points = parse_otlp_metrics(request).unwrap();
        assert_eq!(data_points.len(), 1);

        let dp = &data_points[0];
        assert_eq!(dp.metric_name, "minimal.metric");
        assert_eq!(dp.service_name, "unknown_service"); // Default value
        assert!(dp.metric_unit.is_none());
        assert!(dp.start_timestamp_secs.is_none());
        assert!(dp.tag_service.is_none());
        assert!(dp.tag_env.is_none());
        assert!(dp.attributes.is_empty());
        assert!(dp.resource_attributes.is_empty());
    }
}
