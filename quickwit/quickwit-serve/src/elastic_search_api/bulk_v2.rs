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

use std::time::Instant;

use bytes::Bytes;
use hyper::StatusCode;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::IngestRequestV2Builder;
use quickwit_proto::ingest::router::{IngestRouterService, IngestRouterServiceClient};
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::types::IndexId;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::elastic_search_api::model::{BulkAction, ElasticBulkOptions, ElasticSearchError};
use crate::ingest_api::lines;

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct ElasticBulkResponse {
    #[serde(rename = "took")]
    pub took_millis: u64,
    pub errors: bool,
}

pub(crate) async fn elastic_bulk_ingest_v2(
    default_index_id: Option<IndexId>,
    body: Bytes,
    bulk_options: ElasticBulkOptions,
    mut ingest_router: IngestRouterServiceClient,
) -> Result<ElasticBulkResponse, ElasticSearchError> {
    let now = Instant::now();
    let mut ingest_request_builder = IngestRequestV2Builder::default();
    let mut lines = lines(&body).enumerate();

    while let Some((line_no, line)) = lines.next() {
        let action = serde_json::from_slice::<BulkAction>(line).map_err(|error| {
            ElasticSearchError::new(
                StatusCode::BAD_REQUEST,
                format!("unsupported or malformed action on line #{line_no}: `{error}`"),
            )
        })?;
        let (_, source) = lines.next().ok_or_else(|| {
            ElasticSearchError::new(
                StatusCode::BAD_REQUEST,
                format!("associated source data with action on line #{line_no} is missing"),
            )
        })?;
        // When ingesting into `/my-index/_bulk`, if `_index` is set to something other than
        // `my-index`, ES honors it and creates the doc for the requested index. That is,
        // `my-index` is a default value in case `_index`` is missing, but not a constraint on
        // each sub-action.
        let index_id = action
            .into_index_id()
            .or_else(|| default_index_id.clone())
            .ok_or_else(|| {
                ElasticSearchError::new(
                    StatusCode::BAD_REQUEST,
                    format!("`_index` field of action on line #{line_no} is missing"),
                )
            })?;
        ingest_request_builder.add_doc(index_id, source);
    }
    let commit_type: CommitTypeV2 = bulk_options.refresh.into();

    if commit_type != CommitTypeV2::Auto {
        warn!("ingest API v2 does not support the `refresh` parameter (yet)");
    }
    let ingest_request_opt = ingest_request_builder.build(INGEST_V2_SOURCE_ID, commit_type);

    if let Some(ingest_request) = ingest_request_opt {
        let ingest_response_v2 = ingest_router.ingest(ingest_request).await?;
        let took_millis = now.elapsed().as_millis() as u64;
        let errors = !ingest_response_v2.failures.is_empty();
        let bulk_response = ElasticBulkResponse {
            took_millis,
            errors,
        };
        Ok(bulk_response)
    } else {
        Ok(ElasticBulkResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestFailureReason, IngestResponseV2, IngestSuccess,
    };
    use quickwit_proto::types::{Position, ShardId};
    use warp::{Filter, Rejection, Reply};

    use super::*;
    use crate::elastic_search_api::bulk_v2::ElasticBulkResponse;
    use crate::elastic_search_api::filter::elastic_bulk_filter;
    use crate::elastic_search_api::make_elastic_api_response;
    use crate::elastic_search_api::model::ElasticSearchError;
    use crate::format::extract_format_from_qs;
    use crate::with_arg;

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
        let mut ingest_router_mock = IngestRouterServiceClient::mock();
        ingest_router_mock
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 3);
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::Auto);

                let mut subrequests = ingest_request.subrequests;
                assert_eq!(subrequests[0].subrequest_id, 0);
                assert_eq!(subrequests[1].subrequest_id, 1);
                assert_eq!(subrequests[2].subrequest_id, 2);

                subrequests.sort_by(|left, right| left.index_id.cmp(&right.index_id));

                assert_eq!(subrequests[0].index_id, "my-index-1");
                assert_eq!(subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[0].doc_batch.as_ref().unwrap().num_docs(), 2);
                assert_eq!(subrequests[0].doc_batch.as_ref().unwrap().num_bytes(), 96);

                assert_eq!(subrequests[1].index_id, "my-index-2");
                assert_eq!(subrequests[1].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[1].doc_batch.as_ref().unwrap().num_docs(), 1);
                assert_eq!(subrequests[1].doc_batch.as_ref().unwrap().num_bytes(), 48);

                assert_eq!(subrequests[2].index_id, "my-index-3");
                assert_eq!(subrequests[2].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequests[2].doc_batch.as_ref().unwrap().num_docs(), 1);
                assert_eq!(subrequests[2].doc_batch.as_ref().unwrap().num_bytes(), 48);

                Ok(IngestResponseV2 {
                    successes: vec![
                        IngestSuccess {
                            subrequest_id: 0,
                            index_uid: "my-index-1:0".to_string(),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(1u64)),
                        },
                        IngestSuccess {
                            subrequest_id: 1,
                            index_uid: "my-index-2:0".to_string(),
                            source_id: INGEST_V2_SOURCE_ID.to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(0u64)),
                        },
                    ],
                    failures: vec![IngestFailure {
                        subrequest_id: 2,
                        index_id: "my-index-3".to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        reason: IngestFailureReason::IndexNotFound as i32,
                    }],
                })
            });
        let ingest_router = IngestRouterServiceClient::from(ingest_router_mock);
        let handler = es_compat_bulk_handler_v2(ingest_router);

        let payload = r#"
            {"create": {"_index": "my-index-1", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
            {"create": {"_index": "my-index-2", "_id" : "1"}}
            {"ts": 1, "message": "my-message-1"}
            {"create": {"_index": "my-index-1"}}
            {"ts": 2, "message": "my-message-2"}
            {"create": {"_index": "my-index-3"}}
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
        assert!(bulk_response.errors);
    }

    #[tokio::test]
    async fn test_bulk_api_accepts_empty_requests() {
        let ingest_router = IngestRouterServiceClient::from(IngestRouterServiceClient::mock());
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
        let mut ingest_router_mock = IngestRouterServiceClient::mock();
        ingest_router_mock
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.commit_type(), CommitTypeV2::Auto);

                let subrequest_0 = &ingest_request.subrequests[0];

                assert_eq!(subrequest_0.index_id, "my-index-1");
                assert_eq!(subrequest_0.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequest_0.doc_batch.as_ref().unwrap().num_docs(), 1);
                assert_eq!(subrequest_0.doc_batch.as_ref().unwrap().num_bytes(), 48);

                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: "my-index-1:0".to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from(ingest_router_mock);
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
        let ingest_router = IngestRouterServiceClient::from(IngestRouterServiceClient::mock());
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

        let es_error: ElasticSearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(
            reason,
            "unsupported or malformed action on line #0: `expected value at line 1 column 60`"
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

        let es_error: ElasticSearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(
            reason,
            "associated source data with action on line #0 is missing"
        );

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

        let es_error: ElasticSearchError = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);

        let reason = es_error.error.reason.unwrap();
        assert_eq!(reason, "`_index` field of action on line #0 is missing");
    }
}
