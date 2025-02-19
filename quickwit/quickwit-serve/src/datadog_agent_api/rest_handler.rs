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

use core::fmt;

use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use quickwit_common::rate_limited_error;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::types::{DocUidGenerator, IndexId};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::de::Visitor;
use serde::{self, Deserialize, Deserializer, Serialize};
use tracing::{debug, error};
use warp::{Filter, Rejection};

use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{with_arg, Body, BodyFormat};

const DATADOG_AGENT_INDEX_ID: &str = "datadog";

#[derive(utoipa::OpenApi)]
#[openapi(paths(datadog_agent_logs,))]
pub struct DDGAgentApi;

pub(crate) fn datadog_ingest_api_handlers(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_agent_logs(ingest_router.clone()).boxed()
}

/// Based on vector agent logs endpoint:
/// https://github.com/vectordotdev/vector/blob/450de36904f3d1524057e8cdb736941194da8d22/src/sources/datadog_agent/mod.rs#L499
pub(crate) fn datadog_agent_filter() -> impl Filter<Extract = (Body,), Error = Rejection> + Clone {
    let path_filter = warp::path!("api" / "v1" / "input")
        .or(warp::path!("api" / "v2" / "logs"))
        .unify();
    path_filter
        .and(warp::post())
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/json",
        ))
        .and(get_body_bytes())
}

#[utoipa::path(
    post,
    tag = "Datadog Agent Logs",
    path = "/api/v2/logs",
    request_body(content = String, description = "Datadog agent JSON message", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = bool),
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
    )
)]
pub(crate) fn datadog_agent_logs(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_agent_filter()
        .and(with_arg(ingest_router))
        .and(warp::post())
        .then(|body, ingest_router| async move {
            datadog_agent_ingest_logs(ingest_router, DATADOG_AGENT_INDEX_ID.to_string(), body).await
        })
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

#[derive(Debug, Clone, thiserror::Error, Serialize)]
pub enum DatadogAgentApiError {
    #[error("invalid datadog agent request: {0}")]
    InvalidPayload(String),
    #[error("error when ingesting payload: {0}")]
    Ingest(String),
}

impl ServiceError for DatadogAgentApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            DatadogAgentApiError::InvalidPayload(_) => ServiceErrorCode::BadRequest,
            DatadogAgentApiError::Ingest(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "datadog internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
        }
    }
}

async fn datadog_agent_ingest_logs(
    ingest_router: IngestRouterServiceClient,
    index_id: IndexId,
    body: Body,
) -> Result<(), DatadogAgentApiError> {
    if body.content.is_empty() || body.content.as_ref() == b"{}" {
        // The datadog agent may send an empty payload as a keep alive
        // https://github.com/DataDog/datadog-agent/blob/5a6c5dd75a2233fbf954e38ddcc1484df4c21a35/pkg/logs/client/http/destination.go#L52
        debug!(
            message = "Empty payload ignored.",
            internal_log_rate_limit = true
        );
        return Ok(());
    }

    // TODO: We could just validate + get the byte bounds of each object instead of the more
    // expensive serde_json rountrip.
    // e.g. Vec<RawValue> + validation
    let messages: Vec<AgentLogMsg> = serde_json::from_slice(&body.content).map_err(|error| {
        DatadogAgentApiError::InvalidPayload(format!("Error parsing JSON: {:?}", error))
    })?;

    let mut doc_batch_builder = DocBatchV2Builder::default();
    let mut doc_uid_generator = DocUidGenerator::default();

    for doc in messages {
        doc_batch_builder.add_doc(
            doc_uid_generator.next_doc_uid(),
            serde_json::to_string(&doc).unwrap().as_bytes(),
        );
    }
    let doc_batch = doc_batch_builder.build();

    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch,
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router
        .ingest(request)
        .await
        .map_err(|err| DatadogAgentApiError::Ingest(err.to_string()))?;
    for failure in response.failures.iter() {
        error!(
            message = "Failed to ingest logs.",
            internal_log_rate_limit = true,
            error = ?failure
        );
    }
    if !response.failures.is_empty() {
        return Err(DatadogAgentApiError::Ingest(format!(
            "Failed to ingest logs {:?}.",
            response.failures
        )));
    }
    Ok(())
}

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLogMsg {
    pub message: String,
    pub status: Option<String>,
    #[serde(
        deserialize_with = "ts_milliseconds::deserialize",
        serialize_with = "ts_milliseconds::serialize"
    )]
    pub timestamp: DateTime<Utc>,
    pub hostname: String,
    pub service: String,
    pub ddsource: String,
    // Instead of `Bytes`, we now store a `Vec<String>` for ddtags
    #[serde(deserialize_with = "deserialize_ddtags")]
    pub ddtags: Vec<String>,
}

/// A visitor to parse a comma-separated string into `Vec<String>`.
pub struct CommaSplitVisitor;

impl<'de> Visitor<'de> for CommaSplitVisitor {
    type Value = Vec<String>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a comma-separated string")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        self.visit_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where E: serde::de::Error {
        self.visit_str(&value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        let parts = value.split(',').map(str::to_string).collect();
        Ok(parts)
    }
}

/// A helper function so you can do `deserializer.deserialize_str(CommaSplitVisitor)`.
pub fn deserialize_ddtags<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_str(CommaSplitVisitor)
}
