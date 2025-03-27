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
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::router::{IngestFailure, IngestFailureReason, IngestSuccess};
    use quickwit_proto::ingest::ParseFailure;
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
}
