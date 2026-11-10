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

use quickwit_common::{rate_limited_debug, rate_limited_error};
use quickwit_index_management::IndexServiceError;
use quickwit_ingest::IngestServiceError;
use quickwit_proto::ServiceError;
use quickwit_proto::ingest::IngestV2Error;
use quickwit_search::SearchError;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use warp::hyper::StatusCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchError {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub error: ElasticsearchErrorCause,
}

/// Subset of the Elasticsearch `ErrorCause` that Quickwit populates.
#[derive(Debug, Clone, Deserialize)]
pub struct ElasticsearchErrorCause {
    pub reason: String,
    #[serde(rename = "type")]
    pub ty: Option<String>,
}

// Always emits `caused_by` and `stack_trace` as null for backward compatibility with the
// `elasticsearch_dsl::ErrorCause` format we used to return.
impl Serialize for ElasticsearchErrorCause {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ErrorCause", 4)?;
        state.serialize_field("caused_by", &None::<()>)?;
        state.serialize_field("reason", &self.reason)?;
        state.serialize_field("stack_trace", &None::<()>)?;
        state.serialize_field("type", &self.ty)?;
        state.end()
    }
}

impl ElasticsearchError {
    pub fn new(
        status: StatusCode,
        reason: String,
        exception_opt: Option<ElasticException>,
    ) -> Self {
        if status.is_server_error() {
            rate_limited_error!(limit_per_min=10, status=%status, "http request failed with server error: {reason}");
        } else if !status.is_success() {
            rate_limited_debug!(limit_per_min=10, status=%status, "http request failed: {reason}");
        }
        ElasticsearchError {
            status,
            error: ElasticsearchErrorCause {
                reason,
                ty: exception_opt.map(|exception| exception.as_str().to_string()),
            },
        }
    }

    fn from_service_error(service_error: impl ServiceError) -> Self {
        ElasticsearchError {
            status: service_error.error_code().http_status_code(),
            error: ElasticsearchErrorCause {
                reason: service_error.to_string(),
                ty: None,
            },
        }
    }
}

impl From<SearchError> for ElasticsearchError {
    fn from(search_error: SearchError) -> Self {
        Self::from_service_error(search_error)
    }
}

impl From<IngestServiceError> for ElasticsearchError {
    fn from(ingest_service_error: IngestServiceError) -> Self {
        Self::from_service_error(ingest_service_error)
    }
}

impl From<IngestV2Error> for ElasticsearchError {
    fn from(ingest_error: IngestV2Error) -> Self {
        Self::from_service_error(ingest_error)
    }
}

impl From<IndexServiceError> for ElasticsearchError {
    fn from(index_service_error: IndexServiceError) -> Self {
        Self::from_service_error(index_service_error)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ElasticException {
    #[serde(rename = "action_request_validation_exception")]
    ActionRequestValidation,
    #[serde(rename = "document_parsing_exception")]
    DocumentParsing,
    // This is an exception proper to Quickwit.
    #[serde(rename = "internal_exception")]
    Internal,
    #[serde(rename = "illegal_argument_exception")]
    IllegalArgument,
    #[serde(rename = "index_not_found_exception")]
    IndexNotFound,
    // This is an exception proper to Quickwit.
    #[serde(rename = "rate_limited_exception")]
    RateLimited,
    // This is an exception proper to Quickwit.
    #[serde(rename = "source_not_found_exception")]
    SourceNotFound,
    #[serde(rename = "timeout_exception")]
    Timeout,
}

impl ElasticException {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ActionRequestValidation => "action_request_validation_exception",
            Self::DocumentParsing => "document_parsing_exception",
            Self::Internal => "internal_exception",
            Self::RateLimited => "rate_limited_exception",
            Self::IllegalArgument => "illegal_argument_exception",
            Self::IndexNotFound => "index_not_found_exception",
            Self::SourceNotFound => "source_not_found_exception",
            Self::Timeout => "timeout_exception",
        }
    }
}

#[cfg(test)]
mod tests {
    use elasticsearch_dsl::ErrorCause;

    use super::*;

    #[test]
    fn test_elasticsearch_error_cause_serialization_matches_error_cause() {
        let error_cause = ElasticsearchErrorCause {
            reason: "index `foo` not found".to_string(),
            ty: Some("index_not_found_exception".to_string()),
        };
        let expected_error_cause = ErrorCause {
            reason: Some("index `foo` not found".to_string()),
            caused_by: None,
            root_cause: Vec::new(),
            stack_trace: None,
            suppressed: Vec::new(),
            ty: Some("index_not_found_exception".to_string()),
            additional_details: Default::default(),
        };
        assert_eq!(
            serde_json::to_string(&error_cause).unwrap(),
            serde_json::to_string(&expected_error_cause).unwrap()
        );
        let error_cause_json = serde_json::to_string(&error_cause).unwrap();
        let deserialized: ElasticsearchErrorCause =
            serde_json::from_str(&error_cause_json).unwrap();
        assert_eq!(deserialized.reason, error_cause.reason);
        assert_eq!(deserialized.ty, error_cause.ty);
    }
}
