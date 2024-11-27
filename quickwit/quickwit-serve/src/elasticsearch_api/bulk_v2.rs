// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::time::Instant;

use hyper::StatusCode;
use quickwit_common::rate_limited_error;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::IngestRequestV2Builder;
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestResponseV2, IngestRouterService, IngestRouterServiceClient,
};
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::types::{DocUid, IndexId};
use serde::{Deserialize, Serialize};

use super::model::ElasticException;
use crate::elasticsearch_api::model::{BulkAction, ElasticBulkOptions, ElasticsearchError};
use crate::ingest_api::lines;
use crate::Body;

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct ElasticBulkResponse {
    #[serde(rename = "took")]
    pub took_millis: u64,
    pub errors: bool,
    #[serde(rename = "items")]
    pub actions: Vec<ElasticBulkAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ElasticBulkAction {
    #[serde(rename = "create")]
    Create(ElasticBulkItem),
    #[serde(rename = "index")]
    Index(ElasticBulkItem),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ElasticBulkItem {
    #[serde(rename = "_index")]
    pub index_id: IndexId,
    #[serde(rename = "_id")]
    pub es_doc_id: Option<String>,
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub error: Option<ElasticBulkError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ElasticBulkError {
    #[serde(rename = "index")]
    pub index_id: Option<IndexId>,
    #[serde(rename = "type")]
    pub exception: ElasticException,
    pub reason: String,
}

type ElasticDocId = String;

#[derive(Debug)]
struct DocHandle {
    doc_position: usize,
    doc_uid: DocUid,
    es_doc_id: Option<ElasticDocId>,
    // Whether the document failed to parse. When the struct is instantiated, this value is set to
    // `false` and then mutated if the ingest response contains a parse failure for this document.
    is_parse_failure: bool,
}

pub(crate) async fn elastic_bulk_ingest_v2(
    default_index_id: Option<IndexId>,
    body: Body,
    bulk_options: ElasticBulkOptions,
    ingest_router: IngestRouterServiceClient,
) -> Result<ElasticBulkResponse, ElasticsearchError> {
    let now = Instant::now();
    let mut ingest_request_builder = IngestRequestV2Builder::default();
    let mut lines = lines(&body.content).enumerate();
    let mut per_subrequest_doc_handles: HashMap<u32, Vec<DocHandle>> = HashMap::new();
    let mut action_count = 0;
    while let Some((line_no, line)) = lines.next() {
        let action = serde_json::from_slice::<BulkAction>(line).map_err(|error| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                format!("Malformed action/metadata line [{}]: {error}", line_no + 1),
                Some(ElasticException::IllegalArgument),
            )
        })?;
        let (_, doc) = lines.next().ok_or_else(|| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                "Validation Failed: 1: no requests added;".to_string(),
                Some(ElasticException::ActionRequestValidation),
            )
        })?;
        let meta = action.into_meta();
        // When ingesting into `/my-index/_bulk`, if `_index` is set to something other than
        // `my-index`, ES honors it and creates the doc for the requested index. That is,
        // `my-index` is a default value in case `_index`` is missing, but not a constraint on
        // each sub-action.
        let index_id = meta
            .index_id
            .or_else(|| default_index_id.clone())
            .ok_or_else(|| {
                ElasticsearchError::new(
                    StatusCode::BAD_REQUEST,
                    "Validation Failed: 1: index is missing;".to_string(),
                    Some(ElasticException::ActionRequestValidation),
                )
            })?;
        let (subrequest_id, doc_uid) = ingest_request_builder.add_doc(index_id, doc);

        let doc_handle = DocHandle {
            doc_position: action_count,
            doc_uid,
            es_doc_id: meta.es_doc_id,
            is_parse_failure: false,
        };
        action_count += 1;
        per_subrequest_doc_handles
            .entry(subrequest_id)
            .or_default()
            .push(doc_handle);
    }
    let commit_type: CommitTypeV2 = bulk_options.refresh.into();

    let ingest_request_opt = ingest_request_builder.build(INGEST_V2_SOURCE_ID, commit_type);

    let Some(ingest_request) = ingest_request_opt else {
        return Ok(ElasticBulkResponse::default());
    };
    let ingest_response = ingest_router.ingest(ingest_request).await.map_err(|err| {
        rate_limited_error!(limit_per_min=6, err=?err, "router error");
        err
    })?;
    make_elastic_bulk_response_v2(
        ingest_response,
        per_subrequest_doc_handles,
        now,
        action_count,
    )
}

fn make_elastic_bulk_response_v2(
    ingest_response_v2: IngestResponseV2,
    mut per_subrequest_doc_handles: HashMap<u32, Vec<DocHandle>>,
    now: Instant,
    action_count: usize,
) -> Result<ElasticBulkResponse, ElasticsearchError> {
    let mut positioned_actions: Vec<(usize, ElasticBulkAction)> = Vec::with_capacity(action_count);
    let mut errors = false;

    // Populate the items for each `IngestSuccess` subresponse. They may be partially successful and
    // contain some parse failures.
    for success in ingest_response_v2.successes {
        let index_id = success
            .index_uid
            .map(|index_uid| index_uid.index_id)
            .expect("`index_uid` should be a required field");

        // Find the doc handles for the subresponse.
        let mut doc_handles = remove_doc_handles(
            &mut per_subrequest_doc_handles,
            success.subrequest_id,
        )
        .inspect_err(|_| {
            rate_limited_error!(limit_per_min=6, index_id=%index_id, "could not find subrequest id");
        })?;
        doc_handles.sort_unstable_by(|left, right| left.doc_uid.cmp(&right.doc_uid));

        // Populate the response items with one error per parse failure.
        for parse_failure in success.parse_failures {
            errors = true;

            let failed_doc_uid = parse_failure.doc_uid();
            let doc_handle_idx = doc_handles
                .binary_search_by_key(&failed_doc_uid, |doc_handle| doc_handle.doc_uid)
                .map_err(|_| {
                    rate_limited_error!(limit_per_min=6, doc_uid=%failed_doc_uid, "could not find doc_uid from parse failure");
                    ElasticsearchError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "could not find doc `{}` in bulk request",
                            parse_failure.doc_uid()
                        ),
                        None,
                    )
                })?;
            let doc_handle = &mut doc_handles[doc_handle_idx];
            doc_handle.is_parse_failure = true;

            let error = ElasticBulkError {
                index_id: Some(index_id.clone()),
                exception: ElasticException::DocumentParsing,
                reason: parse_failure.message,
            };
            let item = ElasticBulkItem {
                index_id: index_id.clone(),
                es_doc_id: doc_handle.es_doc_id.take(),
                status: StatusCode::BAD_REQUEST,
                error: Some(error),
            };
            let action = ElasticBulkAction::Index(item);
            positioned_actions.push((doc_handle.doc_position, action));
        }
        // Populate the remaining successful items.
        for mut doc_handle in doc_handles {
            if doc_handle.is_parse_failure {
                continue;
            }
            let item = ElasticBulkItem {
                index_id: index_id.clone(),
                es_doc_id: doc_handle.es_doc_id.take(),
                status: StatusCode::CREATED,
                error: None,
            };
            let action = ElasticBulkAction::Index(item);
            positioned_actions.push((doc_handle.doc_position, action));
        }
    }
    // Repeat the operation for each `IngestFailure` subresponse.
    for failure in ingest_response_v2.failures {
        errors = true;

        // Find the doc handles for the subrequest.
        let doc_handles =
            remove_doc_handles(&mut per_subrequest_doc_handles, failure.subrequest_id)
                .inspect_err(|_| {
                    rate_limited_error!(
                        limit_per_min = 6,
                        subrequest = failure.subrequest_id,
                        "failed to find error subrequest"
                    );
                })?;

        // Populate the response items with one error per doc handle.
        let (exception, reason, status) = match failure.reason() {
            IngestFailureReason::IndexNotFound => (
                ElasticException::IndexNotFound,
                format!("no such index [{}]", failure.index_id),
                StatusCode::NOT_FOUND,
            ),
            IngestFailureReason::SourceNotFound => (
                ElasticException::SourceNotFound,
                format!("no such source [{}]", failure.index_id),
                StatusCode::NOT_FOUND,
            ),
            IngestFailureReason::Timeout => (
                ElasticException::Timeout,
                format!("timeout [{}]", failure.index_id),
                StatusCode::REQUEST_TIMEOUT,
            ),
            IngestFailureReason::ShardRateLimited => (
                ElasticException::RateLimited,
                format!("shard rate limiting [{}]", failure.index_id),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            IngestFailureReason::NoShardsAvailable => (
                ElasticException::RateLimited,
                format!("no shards available [{}]", failure.index_id),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            reason => {
                let pretty_reason = reason
                    .as_str_name()
                    .strip_prefix("INGEST_FAILURE_REASON_")
                    .unwrap_or("")
                    .replace('_', " ")
                    .to_ascii_lowercase();
                (
                    ElasticException::Internal,
                    format!("{} error [{}]", pretty_reason, failure.index_id),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        };
        for mut doc_handle in doc_handles {
            let error = ElasticBulkError {
                index_id: Some(failure.index_id.clone()),
                exception,
                reason: reason.clone(),
            };
            let item = ElasticBulkItem {
                index_id: failure.index_id.clone(),
                es_doc_id: doc_handle.es_doc_id.take(),
                status,
                error: Some(error),
            };
            let action = ElasticBulkAction::Index(item);
            positioned_actions.push((doc_handle.doc_position, action));
        }
    }
    assert!(
        per_subrequest_doc_handles.is_empty(),
        "doc handles should be empty"
    );

    assert_eq!(
        positioned_actions.len(),
        action_count,
        "request and response action count should match"
    );
    positioned_actions.sort_unstable_by_key(|(idx, _)| *idx);
    let actions = positioned_actions
        .into_iter()
        .map(|(_, action)| action)
        .collect();

    let took_millis = now.elapsed().as_millis() as u64;

    let bulk_response = ElasticBulkResponse {
        took_millis,
        errors,
        actions,
    };
    Ok(bulk_response)
}

fn remove_doc_handles(
    per_subrequest_doc_handles: &mut HashMap<u32, Vec<DocHandle>>,
    subrequest_id: u32,
) -> Result<Vec<DocHandle>, ElasticsearchError> {
    per_subrequest_doc_handles
        .remove(&subrequest_id)
        .ok_or_else(|| {
            ElasticsearchError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not find subrequest `{subrequest_id}` in bulk request"),
                None,
            )
        })
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestFailureReason, IngestResponseV2, IngestSuccess,
        MockIngestRouterService,
    };
    use quickwit_proto::ingest::{ParseFailure, ParseFailureReason};
    use quickwit_proto::types::{IndexUid, Position, ShardId};
    use warp::{Filter, Rejection, Reply};

    use super::*;
    use crate::elasticsearch_api::bulk_v2::ElasticBulkResponse;
    use crate::elasticsearch_api::filter::elastic_bulk_filter;
    use crate::elasticsearch_api::make_elastic_api_response;
    use crate::elasticsearch_api::model::ElasticsearchError;
    use crate::format::extract_format_from_qs;
    use crate::with_arg;

    impl ElasticBulkAction {
        fn index_id(&self) -> &IndexId {
            match self {
                ElasticBulkAction::Create(item) => &item.index_id,
                ElasticBulkAction::Index(item) => &item.index_id,
            }
        }

        fn es_doc_id(&self) -> Option<&str> {
            match self {
                ElasticBulkAction::Create(item) => item.es_doc_id.as_deref(),
                ElasticBulkAction::Index(item) => item.es_doc_id.as_deref(),
            }
        }

        fn status(&self) -> StatusCode {
            match self {
                ElasticBulkAction::Create(item) => item.status,
                ElasticBulkAction::Index(item) => item.status,
            }
        }

        fn error(&self) -> Option<&ElasticBulkError> {
            match self {
                ElasticBulkAction::Create(item) => item.error.as_ref(),
                ElasticBulkAction::Index(item) => item.error.as_ref(),
            }
        }
    }

    fn es_compat_bulk_handler_v2(
        ingest_router: IngestRouterServiceClient,
    ) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
        elastic_bulk_filter()
            .and(with_arg(ingest_router))
            .then(|body, bulk_options, ingest_router| {
                elastic_bulk_ingest_v2(None, body, bulk_options, ingest_router)
            })
            .and(extract_format_from_qs())
            .map(make_elastic_api_response)
    }

    #[tokio::test]
    async fn test_bulk_api_happy_path() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 2);
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::Auto);

                let mut subrequests = ingest_request.subrequests;
                subrequests.sort_by(|left, right| left.index_id.cmp(&right.index_id));

                assert_eq!(subrequests[0].subrequest_id, 0);
                assert_eq!(subrequests[0].index_id, "my-index-1");
                assert_eq!(subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[0].doc_batch.as_ref().unwrap().num_docs(), 2);
                assert_eq!(subrequests[0].doc_batch.as_ref().unwrap().num_bytes(), 104);

                assert_eq!(subrequests[1].subrequest_id, 1);
                assert_eq!(subrequests[1].index_id, "my-index-2");
                assert_eq!(subrequests[1].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[1].doc_batch.as_ref().unwrap().num_docs(), 1);
                assert_eq!(subrequests[1].doc_batch.as_ref().unwrap().num_bytes(), 52);

                Ok(IngestResponseV2 {
                    successes: vec![
                        IngestSuccess {
                            subrequest_id: 0,
                            index_uid: Some(IndexUid::for_test("my-index-1", 0)),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(1u64)),
                            num_ingested_docs: 2,
                            parse_failures: Vec::new(),
                        },
                        IngestSuccess {
                            subrequest_id: 1,
                            index_uid: Some(IndexUid::for_test("my-index-2", 0)),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(0u64)),
                            num_ingested_docs: 1,
                            parse_failures: Vec::new(),
                        },
                    ],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"
            {"create": {"_index": "my-index-1", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
            {"create": {"_index": "my-index-2", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
            {"create": {"_index": "my-index-1"}}
            {"ts": 2, "message": "my-message-2"}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);

        let bulk_response: ElasticBulkResponse = serde_json::from_slice(response.body()).unwrap();
        assert!(!bulk_response.errors);

        let mut items = bulk_response
            .actions
            .into_iter()
            .map(|action| match action {
                ElasticBulkAction::Create(item) => item,
                ElasticBulkAction::Index(item) => item,
            })
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 3);

        items.sort_by(|left, right| {
            left.index_id
                .cmp(&right.index_id)
                .then(left.es_doc_id.cmp(&right.es_doc_id))
        });
        assert_eq!(items[0].index_id, "my-index-1");
        assert!(items[0].es_doc_id.is_none());
        assert_eq!(items[0].status, StatusCode::CREATED);

        assert_eq!(items[1].index_id, "my-index-1");
        assert_eq!(items[1].es_doc_id.as_ref().unwrap(), "1");
        assert_eq!(items[1].status, StatusCode::CREATED);

        assert_eq!(items[2].index_id, "my-index-2");
        assert_eq!(items[2].es_doc_id.as_ref().unwrap(), "1");
        assert_eq!(items[2].status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_bulk_api_accepts_empty_requests() {
        let ingest_router = IngestRouterServiceClient::mocked();
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body("")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);

        let bulk_response: ElasticBulkResponse = serde_json::from_slice(response.body()).unwrap();
        assert!(!bulk_response.errors)
    }

    #[tokio::test]
    async fn test_bulk_api_ignores_blank_lines() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::Auto);

                let subrequest_0 = &ingest_request.subrequests[0];

                assert_eq!(subrequest_0.index_id, "my-index-1");
                assert_eq!(subrequest_0.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequest_0.doc_batch.as_ref().unwrap().num_docs(), 1);
                assert_eq!(subrequest_0.doc_batch.as_ref().unwrap().num_bytes(), 52);

                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: Some(IndexUid::for_test("my-index-1", 0)),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        num_ingested_docs: 1,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"

            {"create": {"_index": "my-index-1", "_id" : "1"}}

            {"ts": 1, "message": "my-message-1"}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);

        let bulk_response: ElasticBulkResponse = serde_json::from_slice(response.body()).unwrap();
        assert!(!bulk_response.errors);
    }

    #[tokio::test]
    async fn test_bulk_api_handles_malformed_requests() {
        let ingest_router = IngestRouterServiceClient::mocked();
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"
            {"create": {"_index": "my-index-1", "_id" : "1"},}
            {"ts": 1, "message": "my-message-1"}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 400);

        let es_error: ElasticsearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(
            reason,
            "Malformed action/metadata line [1]: expected value at line 1 column 60"
        );

        let payload = r#"
            {"create": {"_index": "my-index-1", "_id" : "1"}}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 400);

        let es_error: ElasticsearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(reason, "Validation Failed: 1: no requests added;");

        let payload = r#"
            {"create": {"_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 400);

        let es_error: ElasticsearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(reason, "Validation Failed: 1: index is missing;");
    }

    #[tokio::test]
    async fn test_bulk_api_index_not_found() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 2);
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::Auto);

                let mut subrequests = ingest_request.subrequests;
                subrequests.sort_by(|left, right| left.index_id.cmp(&right.index_id));

                assert_eq!(subrequests[0].subrequest_id, 0);
                assert_eq!(subrequests[0].index_id, "my-index-1");
                assert_eq!(subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[0].doc_batch.as_ref().unwrap().num_docs(), 2);

                assert_eq!(subrequests[1].subrequest_id, 1);
                assert_eq!(subrequests[1].index_id, "my-index-2");
                assert_eq!(subrequests[1].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[1].doc_batch.as_ref().unwrap().num_docs(), 1);

                Ok(IngestResponseV2 {
                    successes: Vec::new(),
                    failures: vec![
                        IngestFailure {
                            subrequest_id: 0,
                            index_id: "my-index-1".to_string(),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            reason: IngestFailureReason::IndexNotFound as i32,
                        },
                        IngestFailure {
                            subrequest_id: 1,
                            index_id: "my-index-2".to_string(),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            reason: IngestFailureReason::IndexNotFound as i32,
                        },
                    ],
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"
            {"index": {"_index": "my-index-1", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
            {"index": {"_index": "my-index-1"}}
            {"ts": 2, "message": "my-message-1"}
            {"index": {"_index": "my-index-2", "_id" : "1"}}
            {"ts": 3, "message": "my-message-2"}
        "#;
        let response = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);

        let bulk_response: ElasticBulkResponse = serde_json::from_slice(response.body()).unwrap();
        assert!(bulk_response.errors);
        assert_eq!(bulk_response.actions.len(), 3);
    }

    #[test]
    fn test_make_elastic_bulk_response_v2() {
        let response = make_elastic_bulk_response_v2(
            IngestResponseV2::default(),
            HashMap::new(),
            Instant::now(),
            0,
        )
        .unwrap();

        assert!(!response.errors);
        assert!(response.actions.is_empty());

        let ingest_response_v2 = IngestResponseV2 {
            successes: vec![IngestSuccess {
                subrequest_id: 0,
                index_uid: Some(IndexUid::for_test("test-index-foo", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                replication_position_inclusive: Some(Position::offset(0u64)),
                num_ingested_docs: 1,
                parse_failures: vec![ParseFailure {
                    doc_uid: Some(DocUid::for_test(1)),
                    reason: ParseFailureReason::InvalidJson as i32,
                    message: "failed to parse JSON document".to_string(),
                }],
            }],
            failures: vec![IngestFailure {
                subrequest_id: 1,
                index_id: "test-index-bar".to_string(),
                source_id: "test-source".to_string(),
                reason: IngestFailureReason::IndexNotFound as i32,
            }],
        };
        let per_request_doc_handles = HashMap::from_iter([
            (
                0,
                vec![
                    DocHandle {
                        doc_position: 0,
                        doc_uid: DocUid::for_test(0),
                        es_doc_id: Some("0".to_string()),
                        is_parse_failure: false,
                    },
                    DocHandle {
                        doc_position: 1,
                        doc_uid: DocUid::for_test(1),
                        es_doc_id: Some("1".to_string()),
                        is_parse_failure: false,
                    },
                ],
            ),
            (
                1,
                vec![DocHandle {
                    doc_position: 2,
                    doc_uid: DocUid::for_test(2),
                    es_doc_id: Some("2".to_string()),
                    is_parse_failure: false,
                }],
            ),
        ]);
        let response = make_elastic_bulk_response_v2(
            ingest_response_v2,
            per_request_doc_handles,
            Instant::now(),
            3,
        )
        .unwrap();

        assert!(response.errors);
        assert_eq!(response.actions.len(), 3);

        assert_eq!(response.actions[0].index_id(), "test-index-foo");
        assert_eq!(response.actions[0].es_doc_id(), Some("0"));
        assert_eq!(response.actions[0].status(), StatusCode::CREATED);
        assert!(response.actions[0].error().is_none());

        assert_eq!(response.actions[1].index_id(), "test-index-foo");
        assert_eq!(response.actions[1].es_doc_id(), Some("1"));
        assert_eq!(response.actions[1].status(), StatusCode::BAD_REQUEST);

        let error = response.actions[1].error().unwrap();
        assert_eq!(error.index_id.as_ref().unwrap(), "test-index-foo");
        assert_eq!(error.exception, ElasticException::DocumentParsing);
        assert_eq!(error.reason, "failed to parse JSON document");

        assert_eq!(response.actions[2].index_id(), "test-index-bar");
        assert_eq!(response.actions[2].es_doc_id(), Some("2"));
        assert_eq!(response.actions[2].status(), StatusCode::NOT_FOUND);

        let error = response.actions[2].error().unwrap();
        assert_eq!(error.index_id.as_ref().unwrap(), "test-index-bar");
        assert_eq!(error.exception, ElasticException::IndexNotFound);
        assert_eq!(error.reason, "no such index [test-index-bar]");
    }

    #[tokio::test]
    async fn test_refresh_param() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::WaitFor);
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: Some(IndexUid::for_test("my-index-1", 0)),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(1u64)),
                        num_ingested_docs: 2,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"
            {"create": {"_index": "my-index-1", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
        "#;
        warp::test::request()
            .path("/_elastic/_bulk?refresh=wait_for")
            .method("POST")
            .body(payload)
            .reply(&handler)
            .await;
    }
}
