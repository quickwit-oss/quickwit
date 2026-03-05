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

use std::collections::BTreeMap;

use bytes::Bytes;
use quickwit_ingest::{IngestResponse, IngestServiceError};
use quickwit_proto::ingest::router::IngestResponseV2;
use quickwit_proto::ingest::{DocBatchV2, ParseFailureReason};
use quickwit_proto::types::DocUid;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct RestParseFailure {
    pub message: String,
    pub document: String,
    pub reason: ParseFailureReason,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, utoipa::ToSchema)]
pub struct RestIngestResponse {
    /// Number of rows in the request payload
    pub num_docs_for_processing: u64,
    /// Number of docs successfully ingested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_ingested_docs: Option<u64>, // TODO(#5604) remove Option
    /// Number of docs rejected because of parsing errors
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rejected_docs: Option<u64>, // TODO(#5604) remove Option
    /// Detailed description of parsing errors (available if the path param
    /// `detailed_response` is set to `true`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_failures: Option<Vec<RestParseFailure>>,
}

impl RestIngestResponse {
    pub(crate) fn from_ingest_v1(ingest_response: IngestResponse) -> Self {
        Self {
            num_docs_for_processing: ingest_response.num_docs_for_processing,
            ..Default::default()
        }
    }

    /// Converts [`IngestResponseV2`] into [`RestIngestResponse`].
    ///
    /// Generates a detailed failure description (`parse_failures`) if
    /// `doc_batch_clone_opt.is_some()`
    pub(crate) fn from_ingest_v2(
        mut ingest_response: IngestResponseV2,
        doc_batch_clone_opt: Option<&DocBatchV2>,
        num_docs_for_processing: u64,
    ) -> Result<Self, IngestServiceError> {
        let num_responses = ingest_response.successes.len() + ingest_response.failures.len();
        if num_responses != 1 {
            return Err(IngestServiceError::Internal(format!(
                "expected a single failure/success, got {num_responses}",
            )));
        }
        if let Some(failure_resp) = ingest_response.failures.pop() {
            return Err(failure_resp.into());
        }
        let success_resp = ingest_response.successes.pop().unwrap();

        let mut resp = Self {
            num_docs_for_processing,
            num_ingested_docs: Some(success_resp.num_ingested_docs as u64),
            num_rejected_docs: Some(success_resp.parse_failures.len() as u64),
            parse_failures: None,
        };
        if let Some(doc_batch) = doc_batch_clone_opt {
            let docs: BTreeMap<DocUid, Bytes> = doc_batch.docs().collect();
            let mut parse_failures = Vec::with_capacity(success_resp.parse_failures.len());
            for failure in success_resp.parse_failures {
                let doc = docs.get(&failure.doc_uid()).ok_or_else(|| {
                    IngestServiceError::Internal(format!(
                        "failed doc_uid {} not found in the original doc batch",
                        failure.doc_uid()
                    ))
                })?;
                parse_failures.push(RestParseFailure {
                    reason: failure.reason(),
                    message: failure.message,
                    document: String::from_utf8(doc.to_vec()).unwrap(),
                });
            }
            resp.parse_failures = Some(parse_failures);
        }
        Ok(resp)
    }

    /// Aggregates ingest counts and errors.
    pub fn merge(self, other: Self) -> Self {
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
#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ParseFailure;
    use quickwit_proto::ingest::router::{IngestFailure, IngestFailureReason, IngestSuccess};
    use quickwit_proto::types::IndexUid;

    use super::*;

    #[test]
    fn test_from_ingest_v1() {
        let ingest_response = IngestResponse {
            num_docs_for_processing: 10,
        };
        let rest_response = RestIngestResponse::from_ingest_v1(ingest_response);
        assert_eq!(rest_response.num_docs_for_processing, 10);
        assert_eq!(rest_response.num_ingested_docs, None);
        assert_eq!(rest_response.num_rejected_docs, None);
        assert_eq!(rest_response.parse_failures, None);
    }

    #[test]
    fn test_from_ingest_v2_success() {
        let success_resp = IngestResponseV2 {
            successes: vec![IngestSuccess {
                subrequest_id: 0,
                index_uid: Some(IndexUid::new_with_random_ulid("myindex")),
                source_id: String::from("mysource"),
                shard_id: Some("myshard".into()),
                replication_position_inclusive: None,
                num_ingested_docs: 5,
                parse_failures: vec![],
            }],
            failures: vec![],
        };
        let rest_response = RestIngestResponse::from_ingest_v2(success_resp, None, 10).unwrap();
        assert_eq!(rest_response.num_docs_for_processing, 10);
        assert_eq!(rest_response.num_ingested_docs, Some(5));
        assert_eq!(rest_response.num_rejected_docs, Some(0));
        assert_eq!(rest_response.parse_failures, None);
    }

    #[test]
    fn test_from_ingest_v2_partial_success() {
        let success_resp = IngestResponseV2 {
            successes: vec![IngestSuccess {
                subrequest_id: 0,
                index_uid: Some(IndexUid::new_with_random_ulid("myindex")),
                source_id: String::from("mysource"),
                shard_id: Some("myshard".into()),
                replication_position_inclusive: None,
                num_ingested_docs: 5,
                parse_failures: vec![ParseFailure {
                    doc_uid: Some(DocUid::for_test(42)),
                    message: "error".to_string(),
                    reason: ParseFailureReason::InvalidJson.into(),
                }],
            }],
            failures: vec![],
        };
        let rest_response = RestIngestResponse::from_ingest_v2(success_resp, None, 10).unwrap();
        assert_eq!(rest_response.num_docs_for_processing, 10);
        assert_eq!(rest_response.num_ingested_docs, Some(5));
        assert_eq!(rest_response.num_rejected_docs, Some(1));
        assert_eq!(rest_response.parse_failures, None);
    }

    #[test]
    fn test_from_ingest_v2_failure() {
        let failure_resp = IngestResponseV2 {
            successes: vec![],
            failures: vec![IngestFailure {
                subrequest_id: 0,
                index_id: String::from("myindex"),
                source_id: String::from("mysource"),
                reason: IngestFailureReason::SourceNotFound.into(),
            }],
        };
        let result = RestIngestResponse::from_ingest_v2(failure_resp, None, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_responses() {
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
        let merged_response = response1.merge(response2);
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
