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
use std::time::Instant;

use bytes::Bytes;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::{rate_limited_error, rate_limited_warn};
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_metrics::{counter, histogram, label_values};
use quickwit_opentelemetry::otlp::{
    ArrowDocBatchV2Builder, ArrowMetricsBatchBuilder, MetricDataPoint, MetricType,
};
use quickwit_parquet_engine::ingest::{ArrowSketchBatchBuilder, SketchDataPoint};
use quickwit_parquet_engine::schema::REQUIRED_FIELDS;
use quickwit_parquet_engine::schema::sketch_fields::SketchParquetField;
use quickwit_processing::DatadogLogMsg;
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestResponseV2, IngestRouterService,
    IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error};
use quickwit_proto::types::DocUidGenerator;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;
use tracing::debug;
use warp::{Filter, Rejection};

use super::{
    BYOC_INGEST_BYTES_TOTAL, BYOC_INGEST_REQUEST_DURATION_SECONDS, BYOC_INGEST_REQUESTS_TOTAL,
    BYOC_INGEST_UNMATCHED_EVENTS_TOTAL, SIGNAL, SIGNAL_STATUS_CODE,
};
use crate::datadog_api::{IndexRouter, custom_field_accessor, get_error_code, tag_accessor};
use crate::decompression::get_body_bytes;
use crate::ingest_api::lines;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

const BYOC_METRICS_INDEX: &str = "datadog-metrics";
const BYOC_SKETCHES_INDEX: &str = "datadog-sketches";
const BYOC_TRACES_INDEX: &str = "datadog-spans";

#[derive(Debug, thiserror::Error)]
enum ByocApiError {
    #[error("failed to ingest events")]
    IngestError(IngestV2Error),
    #[error("failed to ingest events")]
    IngestFailure(IngestFailureReason),
    #[error("failed to deserialize event: {0}")]
    JsonSerde(String),
    #[cfg(not(feature = "metrics"))]
    #[error("Pomsky was compiled without the `metrics` feature")]
    MetricsNotSupported,
    #[error("task panicked while {0}")]
    Panic(String),
}

impl ServiceError for ByocApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::IngestError(error) => error.error_code(),
            Self::IngestFailure(failure_reason) => get_error_code(*failure_reason),
            Self::JsonSerde(_) => ServiceErrorCode::BadRequest,
            #[cfg(not(feature = "metrics"))]
            Self::MetricsNotSupported => ServiceErrorCode::Unimplemented,
            Self::Panic(_) => ServiceErrorCode::Internal,
        }
    }
}

pub(crate) fn byoc_api_handlers(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    byoc_logs_handler(ingest_router.clone(), index_router)
        .or(byoc_metrics_handler(ingest_router.clone()))
        .or(byoc_sketches_handler(ingest_router.clone()))
        .or(byoc_temp_metrics_handler(ingest_router.clone()))
        .or(byoc_traces_handler(ingest_router))
        .boxed()
}

fn byoc_logs_handler(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "datadog" / "v1" / "byoc" / "logs")
        .and(warp::post())
        .and(get_body_bytes())
        .and(with_arg(ingest_router))
        .and(with_arg(index_router))
        .then(byoc_ingest_logs)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

/// Log event as received from Pomsky intake. For now, it closely matches the Datadog agent log
/// format but may evolve over time as we add support for additional sources.
///
/// The `ddtags` field carries the comma-separated list of tags provided by the Datadog agent.
/// Example: "filename:driver.log,dirname:/var/log/appgate"
///
/// The `tags` field carries the tags added by Pomsky intake (host/resource tags enrichment).
/// Example: {"env": "prod", "region": "us-east-1"}
///
/// The `timestamp` field carries the timestamp of the log event in ISO 8601 format.
/// Example: "2021-01-01T00:00:00.000Z"
#[derive(Debug, Deserialize)]
struct VectorLog {
    #[serde(rename = "ddsource", default)]
    ddsource_opt: Option<String>,
    /// Comma-separated list of tags. A tag can be a key-value pair or just a key.
    /// Example: "filename:driver.log,dirname:/var/log/appgate"
    #[serde(rename = "ddtags", default)]
    ddtags_opt: Option<String>,
    #[serde(rename = "host", default)]
    hostname_opt: Option<String>,
    message: String,
    #[serde(rename = "service", default)]
    service_opt: Option<String>,
    #[serde(rename = "status", default)]
    status_opt: Option<String>,
    #[serde(rename = "tags", default)]
    tags_opt: Option<JsonValue>,
    #[serde(rename = "timestamp", default)]
    timestamp_iso8601_opt: Option<String>,
}

impl TryFrom<VectorLog> for DatadogLogMsg {
    type Error = ByocApiError;

    fn try_from(log: VectorLog) -> Result<Self, Self::Error> {
        let ddtags = build_processing_ddtags(log.ddtags_opt, log.tags_opt);

        let timestamp_opt = log
            .timestamp_iso8601_opt
            .map(|timestamp| OffsetDateTime::parse(&timestamp, &Iso8601::DEFAULT))
            .transpose()
            .map_err(|error| {
                ByocApiError::IngestError(IngestV2Error::Internal(format!(
                    "failed to parse timestamp: {error}"
                )))
            })?;

        let message = DatadogLogMsg {
            ddsource: log.ddsource_opt,
            ddtags,
            hostname: log.hostname_opt,
            message: quickwit_processing::MessageValue::Str(log.message),
            service: log.service_opt,
            status: log.status_opt,
            timestamp: timestamp_opt,
        };
        Ok(message)
    }
}

/// Builds a list of tags for the Pomchi [`DatadogLogMsg`] from the [`VectorLog`] `ddtags` and
/// `tags` fields.
fn build_processing_ddtags(ddtags_opt: Option<String>, tags_opt: Option<JsonValue>) -> Vec<String> {
    let mut accumulator = Vec::new();

    if let Some(ddtags) = ddtags_opt {
        for ddtag in ddtags.split(',') {
            let trimmed_ddtag = ddtag.trim();
            if !trimmed_ddtag.is_empty() {
                accumulator.push(trimmed_ddtag.to_string());
            }
        }
    }
    if let Some(tags) = tags_opt {
        unnest_tags(tags, &mut accumulator);
    }
    accumulator
}

fn unnest_tags(object: JsonValue, tags: &mut Vec<String>) {
    unnest_tags_inner(object, &mut Vec::new(), tags);
}

fn unnest_tags_inner(value: JsonValue, parent: &mut Vec<String>, tags: &mut Vec<String>) {
    match value {
        JsonValue::Object(obj) => {
            for (key, value) in obj {
                parent.push(key);
                unnest_tags_inner(value, parent, tags);
                parent.pop();
            }
        }
        JsonValue::String(value_str) if !parent.is_empty() => {
            tags.push(build_tag(parent, &value_str));
        }
        JsonValue::String(_) => {
            rate_limited_warn!(
                limit_per_min = 6,
                "received unexpected top-level string in log tags"
            );
        }
        _ => {
            rate_limited_warn!(
                limit_per_min = 6,
                "received unexpected value type (array, boolean, or number) in log tags"
            );
        }
    }
}

/// Builds a `"part0.part1…:value"` tag string in a single allocation of exactly the right size.
fn build_tag(parts: &[String], value: &str) -> String {
    let key_len =
        parts.iter().map(|part| part.len()).sum::<usize>() + parts.len().saturating_sub(1);
    let tag_len = key_len + 1 + value.len();

    let mut tag = String::with_capacity(tag_len);

    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            tag.push('.');
        }
        tag.push_str(part);
    }
    tag.push(':');
    tag.push_str(value);
    debug_assert_eq!(tag_len, tag.len());
    tag
}

async fn byoc_ingest_logs(
    body: Body,
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> Result<(), ByocApiError> {
    debug!("received logs request from intake");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(limit_per_min = 6, "received empty logs request from intake");
        record_metrics("log", &Ok(()), start, 0);
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;

    let (subrequests_opt, num_unmatched_events) =
        run_cpu_intensive(move || build_subrequests(&body.content, index_router))
            .await
            .map_err(|_panicked| ByocApiError::Panic("processing logs request".to_string()))??;

    let Some(subrequests) = subrequests_opt else {
        rate_limited_warn!(limit_per_min = 6, "received empty logs request from intake");
        record_metrics("log", &Ok(()), start, 0);
        return Ok(());
    };
    if num_unmatched_events > 0 {
        rate_limited_warn!(
            limit_per_min = 6,
            "discarded {num_unmatched_events} log events with no matching routing rule"
        );
        let signal_labels = label_values!(SIGNAL => "log");
        counter!(parent: BYOC_INGEST_UNMATCHED_EVENTS_TOTAL, labels: [signal_labels])
            .inc_by(num_unmatched_events);
    }
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests,
    };
    let ingest_result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response, "logs"),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics("log", &ingest_result, start, num_bytes);
    ingest_result
}

fn build_subrequests(
    body: &Bytes,
    index_router: IndexRouter,
) -> Result<(Option<Vec<IngestSubrequest>>, u64), ByocApiError> {
    let mut buffer = Vec::new();
    let mut doc_batch_builders: HashMap<&str, DocBatchV2Builder> = HashMap::new();
    let mut doc_uid_generator = DocUidGenerator::default();
    let mut num_unmatched_events = 0;

    let index_router_guard = index_router.get_router();

    for log_json in lines(body) {
        let vector_log: VectorLog = serde_json::from_slice(log_json)
            .map_err(|error| ByocApiError::JsonSerde(error.to_string()))?;

        let datadog_log = DatadogLogMsg::try_from(vector_log)?;

        let Some(index_id) = index_router_guard.resolve_index(
            &tag_accessor(&datadog_log),
            &custom_field_accessor(&datadog_log),
        ) else {
            num_unmatched_events += 1;
            continue;
        };
        serde_json::to_writer(&mut buffer, &datadog_log)
            .expect("`DatadogLogMsg` should be JSON serializable");

        doc_batch_builders
            .entry(index_id)
            .or_default()
            .add_doc(doc_uid_generator.next_doc_uid(), &buffer);

        buffer.clear();
    }
    let subrequests: Vec<IngestSubrequest> = doc_batch_builders
        .into_iter()
        .enumerate()
        .filter_map(|(subrequest_id, (index_id, doc_batch_builder))| {
            let doc_batch = doc_batch_builder.build()?;
            let subrequest = IngestSubrequest {
                subrequest_id: subrequest_id as u32,
                index_id: index_id.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                doc_batch: Some(doc_batch),
            };
            Some(subrequest)
        })
        .collect();

    if subrequests.is_empty() {
        return Ok((None, num_unmatched_events));
    }
    Ok((Some(subrequests), num_unmatched_events))
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

fn byoc_metrics_handler(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "datadog" / "v1" / "byoc" / "metrics")
        .and(warp::post())
        .and(get_body_bytes())
        .and(with_arg(ingest_router))
        .then(byoc_ingest_metrics)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

#[cfg(not(feature = "metrics"))]
async fn byoc_ingest_metrics(
    _body: Body,
    _ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    Err(ByocApiError::MetricsNotSupported)
}

#[cfg(feature = "metrics")]
async fn byoc_ingest_metrics(
    body: Body,
    ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    debug!("received metrics request from intake");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(
            limit_per_min = 6,
            "received empty metrics request from intake"
        );
        record_metrics("metric", &Ok(()), start, 0);
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;
    let doc_lengths = vec![body.content.len() as u32];
    let doc_batch = quickwit_proto::ingest::DocBatchV2 {
        doc_uids: vec![quickwit_proto::types::DocUid::random()],
        doc_buffer: body.content,
        doc_lengths,
        doc_format: quickwit_proto::ingest::DocFormat::ArrowIpc as i32,
    };
    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id: BYOC_METRICS_INDEX.to_string(),
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let ingest_result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response, "metrics"),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics("metric", &ingest_result, start, num_bytes);
    ingest_result
}

// ---------------------------------------------------------------------------
// Sketches
// ---------------------------------------------------------------------------

fn byoc_sketches_handler(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "datadog" / "v1" / "byoc" / "sketches")
        .and(warp::post())
        .and(get_body_bytes())
        .and(with_arg(ingest_router))
        .then(byoc_ingest_sketches)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

#[cfg(not(feature = "metrics"))]
async fn byoc_ingest_sketches(
    _body: Body,
    _ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    Err(ByocApiError::MetricsNotSupported)
}

#[cfg(feature = "metrics")]
async fn byoc_ingest_sketches(
    body: Body,
    ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    debug!("received sketches request from intake");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(
            limit_per_min = 6,
            "received empty sketches request from intake"
        );
        record_metrics("sketch", &Ok(()), start, 0);
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;
    let doc_lengths = vec![body.content.len() as u32];
    let doc_batch = quickwit_proto::ingest::DocBatchV2 {
        doc_uids: vec![quickwit_proto::types::DocUid::random()],
        doc_buffer: body.content,
        doc_lengths,
        doc_format: quickwit_proto::ingest::DocFormat::ArrowIpc as i32,
    };
    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id: BYOC_SKETCHES_INDEX.to_string(),
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let ingest_result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response, "sketches"),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics("sketch", &ingest_result, start, num_bytes);
    ingest_result
}

// ---------------------------------------------------------------------------
// Traces
// ---------------------------------------------------------------------------

fn byoc_traces_handler(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "datadog" / "v1" / "byoc" / "traces")
        .and(warp::post())
        .and(get_body_bytes())
        .and(with_arg(ingest_router))
        .then(byoc_ingest_traces)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

async fn byoc_ingest_traces(
    body: Body,
    ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    debug!("received traces request from intake");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(
            limit_per_min = 6,
            "received empty traces request from intake"
        );
        record_metrics("trace", &Ok(()), start, 0);
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;

    let mut doc_batch_builder = DocBatchV2Builder::default();
    let mut doc_uid_generator = DocUidGenerator::default();

    for span_json in lines(&body.content) {
        doc_batch_builder.add_doc(doc_uid_generator.next_doc_uid(), span_json);
    }
    let Some(doc_batch) = doc_batch_builder.build() else {
        rate_limited_warn!(
            limit_per_min = 6,
            "received empty traces request from intake"
        );
        record_metrics("trace", &Ok(()), start, 0);
        return Ok(());
    };
    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id: BYOC_TRACES_INDEX.to_string(),
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let ingest_result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response, "metrics"),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics("trace", &ingest_result, start, num_bytes);
    ingest_result
}

// ---------------------------------------------------------------------------
// Temp Metrics (temporary endpoint — will be removed once pomsky-intake is deployed)
// ---------------------------------------------------------------------------

fn byoc_temp_metrics_handler(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "datadog" / "v1" / "byoc" / "temp-metrics")
        .and(warp::post())
        .and(get_body_bytes())
        .and(with_arg(ingest_router))
        .then(byoc_ingest_temp_metrics)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

async fn byoc_ingest_temp_metrics(
    body: Body,
    ingest_router: IngestRouterServiceClient,
) -> Result<(), ByocApiError> {
    debug!("received metrics request");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(limit_per_min = 6, "received empty metrics request");
        record_metrics("metric", &Ok(()), start, 0);
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;

    let subrequests = quickwit_common::thread_pool::run_cpu_intensive(move || {
        let parsed = try_parse_vector_payload(&body)?;
        let mut subrequests = Vec::with_capacity(2);
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut subrequest_id = 0u32;

        // Build metrics subrequest.
        if !parsed.metrics.is_empty() {
            let mut arrow_builder = ArrowMetricsBatchBuilder::with_capacity(parsed.metrics.len());
            let mut doc_uids = Vec::with_capacity(parsed.metrics.len());
            for dp in parsed.metrics {
                arrow_builder.append(dp);
                doc_uids.push(doc_uid_generator.next_doc_uid());
            }
            let record_batch = arrow_builder.finish();
            let doc_batch = ArrowDocBatchV2Builder::from_record_batch(&record_batch, doc_uids)
                .map_err(|error| {
                    ByocApiError::IngestError(IngestV2Error::Internal(format!(
                        "failed to serialize metrics Arrow IPC: {error}"
                    )))
                })?
                .build();
            subrequests.push(IngestSubrequest {
                subrequest_id,
                index_id: BYOC_METRICS_INDEX.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                doc_batch: Some(doc_batch),
            });
            subrequest_id += 1;
        }

        // Build sketches subrequest.
        if !parsed.sketches.is_empty() {
            let mut sketch_builder = ArrowSketchBatchBuilder::with_capacity(parsed.sketches.len());
            let mut sketch_doc_uids = Vec::with_capacity(parsed.sketches.len());
            for dp in parsed.sketches {
                sketch_builder.append(dp);
                sketch_doc_uids.push(doc_uid_generator.next_doc_uid());
            }
            let record_batch = sketch_builder.finish();
            let doc_batch =
                ArrowDocBatchV2Builder::from_record_batch(&record_batch, sketch_doc_uids)
                    .map_err(|error| {
                        ByocApiError::IngestError(IngestV2Error::Internal(format!(
                            "failed to serialize sketches Arrow IPC: {error}"
                        )))
                    })?
                    .build();
            subrequests.push(IngestSubrequest {
                subrequest_id,
                index_id: BYOC_SKETCHES_INDEX.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                doc_batch: Some(doc_batch),
            });
        }

        Ok(subrequests)
    })
    .await
    .map_err(|_| {
        ByocApiError::IngestError(IngestV2Error::Internal(
            "task panicked while parsing metrics payload".to_string(),
        ))
    })??;

    if subrequests.is_empty() {
        record_metrics("metric", &Ok(()), start, 0);
        return Ok(());
    }

    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests,
    };
    let ingest_result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response, "temp metrics"),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics("metric", &ingest_result, start, num_bytes);
    ingest_result
}

#[derive(Debug, Deserialize)]
struct VectorNumericValue {
    value: f64,
}

#[derive(Debug, Deserialize)]
struct VectorSketchBins {
    k: Vec<i16>,
    n: Vec<u16>,
}

#[derive(Debug, Deserialize)]
struct VectorAgentDDSketch {
    bins: VectorSketchBins,
    count: u32,
    sum: f64,
    min: f64,
    max: f64,
}

/// Vector's `MetricSketch` enum has no `rename_all`, so the variant is PascalCase.
#[derive(Debug, Deserialize)]
enum VectorMetricSketch {
    AgentDDSketch(VectorAgentDDSketch),
}

/// Matches `MetricValue::Sketch { sketch: MetricSketch }` — the variant has a field named `sketch`.
#[derive(Debug, Deserialize)]
struct VectorSketchValue {
    sketch: VectorMetricSketch,
}

/// A single metric as emitted by Vector's native metric format.
/// May contain a counter, gauge, or sketch value.
#[derive(Debug, Deserialize)]
struct VectorMetricMsg {
    name: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    timestamp: Option<String>,
    #[serde(default)]
    counter: Option<VectorNumericValue>,
    #[serde(default)]
    gauge: Option<VectorNumericValue>,
    #[serde(default)]
    sketch: Option<VectorSketchValue>,
}

struct ParsedVectorPayload {
    metrics: Vec<MetricDataPoint>,
    sketches: Vec<SketchDataPoint>,
}

fn parse_iso8601_to_secs(ts: &str) -> Option<u64> {
    let dt = OffsetDateTime::parse(ts, &Iso8601::DEFAULT).ok()?;
    u64::try_from(dt.unix_timestamp()).ok()
}

fn parse_timestamp(name: &str, ts: &Option<String>) -> Result<u64, ByocApiError> {
    match ts {
        Some(ts) => parse_iso8601_to_secs(ts).ok_or_else(|| {
            ByocApiError::IngestError(IngestV2Error::Internal(format!(
                "failed to parse timestamp '{ts}'"
            )))
        }),
        None => Err(ByocApiError::IngestError(IngestV2Error::Internal(format!(
            "metric '{}' is missing timestamp",
            name
        )))),
    }
}

fn vector_msg_to_data_point(msg: VectorMetricMsg) -> Result<MetricDataPoint, ByocApiError> {
    let (metric_type, value) = if let Some(counter) = msg.counter {
        (MetricType::Sum, counter.value)
    } else if let Some(gauge) = msg.gauge {
        (MetricType::Gauge, gauge.value)
    } else {
        return Err(ByocApiError::IngestError(IngestV2Error::Internal(format!(
            "metric '{}' has no counter or gauge value",
            msg.name
        ))));
    };

    let timestamp_secs = parse_timestamp(&msg.name, &msg.timestamp)?;

    // TODO: Will drop customer tags that are in REQUIRED_FIELDS. Fine for now.
    let tags: HashMap<String, String> = msg
        .tags
        .into_iter()
        .filter(|(k, _)| !REQUIRED_FIELDS.contains(&k.as_str()))
        .collect();

    Ok(MetricDataPoint {
        metric_name: msg.name,
        metric_type,
        timestamp_secs,
        value,
        tags,
    })
}

fn vector_msg_to_sketch_data_point(
    msg: VectorMetricMsg,
    sketch: VectorSketchValue,
) -> Result<SketchDataPoint, ByocApiError> {
    let timestamp_secs = parse_timestamp(&msg.name, &msg.timestamp)?;
    let VectorMetricSketch::AgentDDSketch(dd) = sketch.sketch;

    let sketch_reserved: Vec<&str> = SketchParquetField::all().iter().map(|f| f.name()).collect();
    let tags: HashMap<String, String> = msg
        .tags
        .into_iter()
        .filter(|(k, _)| !sketch_reserved.contains(&k.as_str()))
        .collect();

    Ok(SketchDataPoint {
        metric_name: msg.name,
        timestamp_secs,
        count: u64::from(dd.count),
        sum: dd.sum,
        min: dd.min,
        max: dd.max,
        flags: 0,
        keys: dd.bins.k,
        counts: dd.bins.n.into_iter().map(u64::from).collect(),
        tags,
    })
}

fn classify_vector_msg(
    mut msg: VectorMetricMsg,
    payload: &mut ParsedVectorPayload,
) -> Result<(), ByocApiError> {
    if msg.name.is_empty() {
        return Err(ByocApiError::IngestError(IngestV2Error::Internal(
            "metric has empty name".to_string(),
        )));
    }

    if let Some(sketch) = msg.sketch.take() {
        payload
            .sketches
            .push(vector_msg_to_sketch_data_point(msg, sketch)?);
    } else if msg.counter.is_some() || msg.gauge.is_some() {
        payload.metrics.push(vector_msg_to_data_point(msg)?);
    } else {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 6,
            name = msg.name,
            "skipping metric with no counter, gauge, or sketch value"
        );
    }
    Ok(())
}

/// Parse a Vector payload into metric and sketch data points.
fn try_parse_vector_payload(body: &Body) -> Result<ParsedVectorPayload, ByocApiError> {
    let mut payload = ParsedVectorPayload {
        metrics: Vec::new(),
        sketches: Vec::new(),
    };

    // Try JSON array
    if let Ok(messages) = serde_json::from_slice::<Vec<VectorMetricMsg>>(&body.content) {
        for msg in messages {
            classify_vector_msg(msg, &mut payload)?;
        }
        return Ok(payload);
    }

    // Try newline-delimited JSON
    {
        let content = std::str::from_utf8(&body.content).unwrap_or("");
        let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.len() > 1 {
            for line in &lines {
                let msg: VectorMetricMsg = serde_json::from_str(line).map_err(|error| {
                    ByocApiError::IngestError(IngestV2Error::Internal(format!(
                        "failed to parse NDJSON line: {error}"
                    )))
                })?;
                classify_vector_msg(msg, &mut payload)?;
            }
            return Ok(payload);
        }
    }

    // Try single JSON object
    if let Ok(msg) = serde_json::from_slice::<VectorMetricMsg>(&body.content) {
        classify_vector_msg(msg, &mut payload)?;
        return Ok(payload);
    }

    Err(ByocApiError::IngestError(IngestV2Error::Internal(
        "failed to parse metrics payload as JSON array, NDJSON, or single object".to_string(),
    )))
}

/// Processes the response of an ingest request. Returns the first failure if any.
fn process_ingest_response(response: IngestResponseV2, signal: &str) -> Result<(), ByocApiError> {
    if response.failures.is_empty() {
        return Ok(());
    }
    // Log every failure for visibility, then surface the first as the error code.
    // The caller (pomsky-intake) will retry the whole batch on error, which will cause duplicates
    // for subrequests that succeeded. At-least-once is acceptable for now.
    for failure in &response.failures {
        rate_limited_error!(
            limit_per_min = 6,
            index_id = failure.index_id,
            reason = ?failure.reason(),
            "failed to ingest {signal} intake request"
        );
    }
    Err(ByocApiError::IngestFailure(response.failures[0].reason()))
}

fn record_metrics(
    signal: &'static str,
    result: &Result<(), ByocApiError>,
    start: Instant,
    num_bytes: u64,
) {
    let status_code = match result {
        Ok(()) => http::StatusCode::OK,
        Err(error) => error.error_code().http_status_code(),
    };
    let request_labels = label_values!(
        SIGNAL_STATUS_CODE => signal,
        status_code.as_str().to_string()
    );
    counter!(parent: BYOC_INGEST_REQUESTS_TOTAL, labels: [request_labels]).inc();
    histogram!(parent: BYOC_INGEST_REQUEST_DURATION_SECONDS, labels: [request_labels])
        .observe(start.elapsed().as_secs_f64());

    let signal_labels = label_values!(SIGNAL => signal);
    counter!(parent: BYOC_INGEST_BYTES_TOTAL, labels: [signal_labels]).inc_by(num_bytes);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::unnest_tags;

    #[test]
    fn test_unnest() {
        let mut tags = Vec::new();
        unnest_tags(
            json!({
                "env": "prod",
                "dmi": {"product": {"serial_number": "abc"}},
            }),
            &mut tags,
        );
        tags.sort();
        assert_eq!(tags, ["dmi.product.serial_number:abc", "env:prod",]);
    }

    mod build_tag {
        use super::super::build_tag;

        fn parts(keys: &[&str]) -> Vec<String> {
            keys.iter().map(|s| s.to_string()).collect()
        }

        #[test]
        fn test_single_part() {
            assert_eq!(build_tag(&parts(&["env"]), "prod"), "env:prod");
        }

        #[test]
        fn test_nested_parts() {
            assert_eq!(
                build_tag(&parts(&["dmi", "product", "serial_number"]), "abc123"),
                "dmi.product.serial_number:abc123"
            );
        }

        #[test]
        fn test_multibyte_value() {
            // Ensures byte-accurate capacity for non-ASCII values.
            assert_eq!(build_tag(&parts(&["currency"]), "€"), "currency:€");
        }
    }

    mod build_subrequests {
        use bytes::Bytes;

        use super::super::{ByocApiError, build_subrequests};
        use crate::datadog_api::IndexRouter;

        fn catch_all_router(index_id: &str) -> IndexRouter {
            IndexRouter::for_test(&[("*", index_id)])
        }

        fn ndjson(lines: &[&str]) -> Bytes {
            Bytes::from(lines.join("\n"))
        }

        #[test]
        fn test_empty_body_returns_none() {
            let body = Bytes::new();
            let router = catch_all_router("datadog");
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            assert!(subrequests.is_none());
            assert_eq!(unmatched, 0);
        }

        #[test]
        fn test_malformed_json_returns_error() {
            let body = Bytes::from("not-json");
            let router = catch_all_router("datadog");
            let error = build_subrequests(&body, router).unwrap_err();
            assert!(matches!(error, ByocApiError::JsonSerde(_)));
        }

        #[test]
        fn test_single_log_routed_to_index() {
            let body = ndjson(&[r#"{"message":"hello"}"#]);
            let router = catch_all_router("datadog");
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            let subrequests = subrequests.unwrap();
            assert_eq!(unmatched, 0);
            assert_eq!(subrequests.len(), 1);
            assert_eq!(subrequests[0].index_id, "datadog");
            let batch = subrequests[0].doc_batch.as_ref().unwrap();
            assert_eq!(batch.doc_lengths.len(), 1);
        }

        #[test]
        fn test_multiple_logs_batched_per_index() {
            let body = ndjson(&[r#"{"message":"a"}"#, r#"{"message":"b"}"#]);
            let router = catch_all_router("datadog");
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            let subrequests = subrequests.unwrap();
            assert_eq!(unmatched, 0);
            // Same index → one subrequest containing both docs.
            assert_eq!(subrequests.len(), 1);
            assert_eq!(subrequests[0].index_id, "datadog");
            let batch = subrequests[0].doc_batch.as_ref().unwrap();
            assert_eq!(batch.doc_lengths.len(), 2);
        }

        #[test]
        fn test_no_matching_index_returns_none() {
            let body = ndjson(&[r#"{"message":"hello"}"#]);
            let router = IndexRouter::for_test(&[]);
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            assert!(subrequests.is_none());
            assert_eq!(unmatched, 1);
        }

        #[test]
        fn test_logs_routed_to_multiple_indexes() {
            let router = IndexRouter::for_test(&[
                ("service:app-a", "index-a"),
                ("service:app-b", "index-b"),
            ]);
            let body = ndjson(&[
                r#"{"message":"from-a","service":"app-a"}"#,
                r#"{"message":"from-b","service":"app-b"}"#,
            ]);
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            let mut subrequests = subrequests.unwrap();
            assert_eq!(unmatched, 0);
            subrequests.sort_by(|left, right| left.index_id.cmp(&right.index_id));
            assert_eq!(subrequests.len(), 2);

            assert_eq!(subrequests[0].index_id, "index-a");
            let batch_a = subrequests[0].doc_batch.as_ref().unwrap();
            assert_eq!(batch_a.doc_lengths.len(), 1);

            assert_eq!(subrequests[1].index_id, "index-b");
            let batch_b = subrequests[1].doc_batch.as_ref().unwrap();
            assert_eq!(batch_b.doc_lengths.len(), 1);
        }

        #[test]
        fn test_partial_match_counts_unmatched() {
            // One log matches, one doesn't — check both the subrequest and the unmatched count.
            let router = IndexRouter::for_test(&[("service:known", "datadog")]);
            let body = ndjson(&[
                r#"{"message":"routed","service":"known"}"#,
                r#"{"message":"dropped","service":"unknown"}"#,
            ]);
            let (subrequests, unmatched) = build_subrequests(&body, router).unwrap();
            let subrequests = subrequests.unwrap();
            assert_eq!(unmatched, 1);
            assert_eq!(subrequests.len(), 1);
            let batch = subrequests[0].doc_batch.as_ref().unwrap();
            assert_eq!(batch.doc_lengths.len(), 1);
        }
    }
}
