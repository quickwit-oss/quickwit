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

use metrics::Counter;
use quickwit_common::dd_metrics::{DDCounters, DDHistograms};
use quickwit_common::rate_limited_warn;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_opentelemetry::otlp::{
    ArrowDocBatchV2Builder, ArrowMetricsBatchBuilder, MetricDataPoint, MetricType,
};
use quickwit_parquet_engine::ingest::{ArrowSketchBatchBuilder, SketchDataPoint};
use quickwit_parquet_engine::schema::REQUIRED_FIELDS;
use quickwit_parquet_engine::schema::sketch_fields::SketchParquetField;
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestResponseV2, IngestRouterService,
    IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2, DocFormat, IngestV2Error};
use quickwit_proto::types::{DocUid, DocUidGenerator};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;
use tracing::debug;
use warp::{Filter, Rejection};

use super::BYOC_METRICS;
use crate::datadog_api::{IndexRouter, get_error_code};
use crate::decompression::get_body_bytes;
use crate::ingest_api::lines;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

const BYOC_METRICS_INDEX: &str = "datadog-metrics";
const BYOC_SKETCHES_INDEX: &str = "datadog-sketches";
const BYOC_TRACES_INDEX: &str = "datadog-spans";

#[derive(Debug, thiserror::Error)]
pub enum ByocApiError {
    #[error("failed to ingest events")]
    IngestError(IngestV2Error),
    #[error("failed to ingest events")]
    IngestFailure(IngestFailureReason),
}

impl ServiceError for ByocApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::IngestError(error) => error.error_code(),
            Self::IngestFailure(failure_reason) => get_error_code(*failure_reason),
        }
    }
}

pub(crate) fn byoc_api_handlers(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    byoc_logs_handler(ingest_router.clone(), index_router)
        .or(byoc_metrics_handler(ingest_router.clone()))
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

async fn byoc_ingest_logs(
    body: Body,
    _ingest_router: IngestRouterServiceClient,
    _index_router: IndexRouter,
) -> Result<(), ByocApiError> {
    debug!("received logs request from intake");
    let start = Instant::now();

    if body.content.is_empty() {
        rate_limited_warn!(limit_per_min = 6, "received empty logs request from intake");
        record_metrics(
            &BYOC_METRICS.log_requests_total,
            &BYOC_METRICS.log_request_duration_seconds,
            &BYOC_METRICS.log_bytes_total,
            &Ok(()),
            start,
            0,
        );
        return Ok(());
    }
    // TODO(guilload): Implement log ingestion.
    record_metrics(
        &BYOC_METRICS.log_requests_total,
        &BYOC_METRICS.log_request_duration_seconds,
        &BYOC_METRICS.log_bytes_total,
        &Ok(()),
        start,
        body.content.len() as u64,
    );
    Ok(())
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
        record_metrics(
            &BYOC_METRICS.metric_requests_total,
            &BYOC_METRICS.metric_request_duration_seconds,
            &BYOC_METRICS.metric_bytes_total,
            &Ok(()),
            start,
            0,
        );
        return Ok(());
    }
    let num_bytes = body.content.len() as u64;
    let doc_lengths = vec![body.content.len() as u32];
    let doc_batch = DocBatchV2 {
        doc_uids: vec![DocUid::random()],
        doc_buffer: body.content,
        doc_lengths,
        doc_format: DocFormat::ArrowIpc as i32,
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
    let result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics(
        &BYOC_METRICS.metric_requests_total,
        &BYOC_METRICS.metric_request_duration_seconds,
        &BYOC_METRICS.metric_bytes_total,
        &result,
        start,
        num_bytes,
    );
    result
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
        record_metrics(
            &BYOC_METRICS.trace_requests_total,
            &BYOC_METRICS.trace_request_duration_seconds,
            &BYOC_METRICS.trace_bytes_total,
            &Ok(()),
            start,
            0,
        );
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
        record_metrics(
            &BYOC_METRICS.trace_requests_total,
            &BYOC_METRICS.trace_request_duration_seconds,
            &BYOC_METRICS.trace_bytes_total,
            &Ok(()),
            start,
            0,
        );
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
    let result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics(
        &BYOC_METRICS.trace_requests_total,
        &BYOC_METRICS.trace_request_duration_seconds,
        &BYOC_METRICS.trace_bytes_total,
        &result,
        start,
        num_bytes,
    );
    result
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
        record_metrics(
            &BYOC_METRICS.metric_requests_total,
            &BYOC_METRICS.metric_request_duration_seconds,
            &BYOC_METRICS.metric_bytes_total,
            &Ok(()),
            start,
            0,
        );
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
        record_metrics(
            &BYOC_METRICS.metric_requests_total,
            &BYOC_METRICS.metric_request_duration_seconds,
            &BYOC_METRICS.metric_bytes_total,
            &Ok(()),
            start,
            0,
        );
        return Ok(());
    }

    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests,
    };
    let result = match ingest_router.ingest(request).await {
        Ok(response) => process_ingest_response(response),
        Err(error) => Err(ByocApiError::IngestError(error)),
    };
    record_metrics(
        &BYOC_METRICS.metric_requests_total,
        &BYOC_METRICS.metric_request_duration_seconds,
        &BYOC_METRICS.metric_bytes_total,
        &result,
        start,
        num_bytes,
    );
    result
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
fn process_ingest_response(response: IngestResponseV2) -> Result<(), ByocApiError> {
    if response.failures.is_empty() {
        return Ok(());
    }
    let failure_reason = response.failures[0].reason();
    Err(ByocApiError::IngestFailure(failure_reason))
}

fn record_metrics(
    requests_total: &DDCounters,
    request_duration: &DDHistograms,
    bytes_total: &Counter,
    result: &Result<(), ByocApiError>,
    start: Instant,
    num_bytes: u64,
) {
    let status_code = match result {
        Ok(()) => http::StatusCode::OK,
        Err(error) => error.error_code().http_status_code(),
    };
    let status_code_str = status_code.as_str();

    requests_total.get(status_code_str).increment(1);
    request_duration
        .get(status_code_str)
        .record(start.elapsed().as_secs_f64());
    bytes_total.increment(num_bytes);
}
