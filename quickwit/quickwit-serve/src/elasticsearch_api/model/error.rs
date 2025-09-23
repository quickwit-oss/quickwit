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

use elasticsearch_dsl::search::ErrorCause;
use quickwit_common::{rate_limited_debug, rate_limited_error};
use quickwit_index_management::IndexServiceError;
use quickwit_ingest::IngestServiceError;
use quickwit_proto::ServiceError;
use quickwit_proto::ingest::IngestV2Error;
use quickwit_search::SearchError;
use serde::{Deserialize, Serialize};
use warp::hyper::StatusCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchError {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub error: ErrorCause,
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
            error: ErrorCause {
                reason: Some(reason),
                caused_by: None,
                root_cause: Vec::new(),
                stack_trace: None,
                suppressed: Vec::new(),
                ty: exception_opt.map(|exception| exception.as_str().to_string()),
                additional_details: Default::default(),
            },
        }
    }
}

impl From<SearchError> for ElasticsearchError {
    fn from(search_error: SearchError) -> Self {
        let status = search_error.error_code().http_status_code();
        // Fill only reason field to keep it simple.
        let reason = ErrorCause {
            reason: Some(search_error.to_string()),
            caused_by: None,
            root_cause: Vec::new(),
            stack_trace: None,
            suppressed: Vec::new(),
            ty: None,
            additional_details: Default::default(),
        };
        ElasticsearchError {
            status,
            error: reason,
        }
    }
}

impl From<IngestServiceError> for ElasticsearchError {
    fn from(ingest_service_error: IngestServiceError) -> Self {
        let status = ingest_service_error.error_code().http_status_code();

        let reason = ErrorCause {
            reason: Some(ingest_service_error.to_string()),
            caused_by: None,
            root_cause: Vec::new(),
            stack_trace: None,
            suppressed: Vec::new(),
            ty: None,
            additional_details: Default::default(),
        };
        ElasticsearchError {
            status,
            error: reason,
        }
    }
}

impl From<IngestV2Error> for ElasticsearchError {
    fn from(ingest_error: IngestV2Error) -> Self {
        let status = ingest_error.error_code().http_status_code();

        let reason = ErrorCause {
            reason: Some(ingest_error.to_string()),
            caused_by: None,
            root_cause: Vec::new(),
            stack_trace: None,
            suppressed: Vec::new(),
            ty: None,
            additional_details: Default::default(),
        };
        ElasticsearchError {
            status,
            error: reason,
        }
    }
}

impl From<IndexServiceError> for ElasticsearchError {
    fn from(ingest_error: IndexServiceError) -> Self {
        let status = ingest_error.error_code().http_status_code();

        let reason = ErrorCause {
            reason: Some(ingest_error.to_string()),
            caused_by: None,
            root_cause: Vec::new(),
            stack_trace: None,
            suppressed: Vec::new(),
            ty: None,
            additional_details: Default::default(),
        };
        ElasticsearchError {
            status,
            error: reason,
        }
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
