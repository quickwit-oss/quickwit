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

use std::path::PathBuf;
use std::time::Duration;

use quickwit_serve::{RestIngestResponse, RestParseFailure};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::{ApiError, Error, ErrorResponsePayload};

#[derive(Debug)]
pub struct ApiResponse {
    inner: reqwest::Response,
}

impl ApiResponse {
    pub fn new(inner: reqwest::Response) -> Self {
        Self { inner }
    }
    /// Get the HTTP status code of the response
    pub fn status_code(&self) -> StatusCode {
        self.inner.status()
    }

    /// Checks status and returns error if appropriate.
    pub async fn check(self) -> Result<(), Error> {
        if self.inner.status().is_client_error() || self.inner.status().is_server_error() {
            return Err(self.api_error().await);
        }
        Ok(())
    }

    async fn extract_error_message(self) -> Option<String> {
        let error_body_bytes = self.inner.bytes().await.ok()?;
        let error_body_text = std::str::from_utf8(&error_body_bytes).ok()?;
        if let Ok(error_payload) = serde_json::from_str::<ErrorResponsePayload>(error_body_text) {
            Some(error_payload.message)
        } else {
            Some(error_body_text.to_string())
        }
    }

    async fn api_error(self) -> Error {
        let code = self.inner.status();
        let error_message = self.extract_error_message().await;
        Error::from(ApiError {
            message: error_message,
            code,
        })
    }

    pub async fn deserialize<T: DeserializeOwned>(self) -> Result<T, Error> {
        if self.inner.status().is_client_error() || self.inner.status().is_server_error() {
            Err(self.api_error().await)
        } else {
            let object = self.inner.json::<T>().await?;
            Ok(object)
        }
    }
}

/// A cousin of [`quickwit_search::SearchResponseRest`] that implements [`Deserialize`]
///
/// This version of the response is necessary because
/// `serde_json_borrow::OwnedValue` is not deserializeable.
#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub struct SearchResponseRestClient {
    pub num_hits: u64,
    pub hits: Vec<JsonValue>,
    pub snippets: Option<Vec<JsonValue>>,
    pub elapsed_time_micros: u64,
    pub errors: Vec<String>,
    pub aggregations: Option<JsonValue>,
}

#[derive(Clone)]
pub enum IngestSource {
    Str(String),
    File(PathBuf),
    Stdin,
}

#[derive(Debug, PartialEq, Default)]
pub struct CumulatedIngestResponse {
    pub num_docs_for_processing: u64,
    pub num_ingested_docs: Option<u64>,
    pub num_rejected_docs: Option<u64>,
    pub parse_failures: Option<Vec<RestParseFailure>>,
    pub num_too_many_requests: u64,
}

impl CumulatedIngestResponse {
    /// Aggregates ingest counts and errors.
    pub fn merge(self, other: RestIngestResponse) -> Self {
        Self {
            num_docs_for_processing: self.num_docs_for_processing + other.num_docs_for_processing,
            num_ingested_docs: apply_op(self.num_ingested_docs, other.num_ingested_docs, |a, b| {
                a + b
            }),
            num_rejected_docs: apply_op(self.num_rejected_docs, other.num_rejected_docs, |a, b| {
                a + b
            }),
            parse_failures: apply_op(self.parse_failures, other.parse_failures, |a, b| {
                a.into_iter().chain(b).collect()
            }),
            num_too_many_requests: self.num_too_many_requests,
        }
    }
}

fn apply_op<T>(a: Option<T>, b: Option<T>, f: impl Fn(T, T) -> T) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(f(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// A structure that represent a timeout. Unlike Duration it can also represent an infinite or no
/// timeout value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Timeout {
    duration: Duration,
}

const SECS_PER_MIN: u64 = 60;
const MINS_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

impl Timeout {
    /// Creates a new timeout from duration
    pub const fn new(duration: Duration) -> Timeout {
        Timeout { duration }
    }

    /// Creates a new timeout from seconds
    pub const fn from_secs(secs: u64) -> Timeout {
        Timeout {
            duration: Duration::from_secs(secs),
        }
    }

    /// Creates a new timeout from minutes
    pub const fn from_mins(mins: u64) -> Timeout {
        Self::from_secs(mins * SECS_PER_MIN)
    }

    /// Creates a new timeout from hours
    pub const fn from_hours(hours: u64) -> Timeout {
        Self::from_secs(hours * SECS_PER_MIN * MINS_PER_HOUR)
    }

    /// Creates a new timeout from days
    pub const fn from_days(days: u64) -> Timeout {
        Self::from_secs(days * SECS_PER_MIN * MINS_PER_HOUR * HOURS_PER_DAY)
    }

    /// Creates a new infinite timeout
    pub const fn none() -> Timeout {
        Timeout {
            duration: Duration::MAX,
        }
    }

    /// Converts timeout into Some(Duration) or None if it is infinite.
    pub fn as_duration_opt(&self) -> Option<Duration> {
        if self.duration != Duration::MAX {
            Some(self.duration)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use quickwit_proto::ingest::ParseFailureReason;

    use super::*;

    #[test]
    fn test_merge_responses() {
        let mut merged_response = CumulatedIngestResponse::default();
        let response1 = RestIngestResponse {
            num_docs_for_processing: 10,
            num_ingested_docs: Some(5),
            num_rejected_docs: Some(2),
            parse_failures: Some(vec![RestParseFailure {
                message: "error1".to_string(),
                document: "doc1".to_string(),
                reason: ParseFailureReason::InvalidJson,
            }]),
        };
        let response2 = RestIngestResponse {
            num_docs_for_processing: 15,
            num_ingested_docs: Some(10),
            num_rejected_docs: Some(3),
            parse_failures: Some(vec![RestParseFailure {
                message: "error2".to_string(),
                document: "doc2".to_string(),
                reason: ParseFailureReason::InvalidJson,
            }]),
        };
        merged_response = merged_response.merge(response1);
        merged_response = merged_response.merge(response2);
        assert_eq!(merged_response.num_docs_for_processing, 25);
        assert_eq!(merged_response.num_ingested_docs.unwrap(), 15);
        assert_eq!(merged_response.num_rejected_docs.unwrap(), 5);
        assert_eq!(
            merged_response.parse_failures.unwrap(),
            vec![
                RestParseFailure {
                    message: "error1".to_string(),
                    document: "doc1".to_string(),
                    reason: ParseFailureReason::InvalidJson,
                },
                RestParseFailure {
                    message: "error2".to_string(),
                    document: "doc2".to_string(),
                    reason: ParseFailureReason::InvalidJson,
                }
            ]
        );
    }
}
