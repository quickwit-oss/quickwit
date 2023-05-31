// Copyright (C) 2023 Quickwit, Inc.
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

mod bulk;
mod filter;
mod model;
mod rest_handler;

use std::sync::Arc;

use bulk::{es_compat_bulk_handler, es_compat_index_bulk_handler};
use quickwit_ingest::IngestServiceClient;
use quickwit_search::SearchService;
use rest_handler::{
    es_compat_index_multi_search_handler, es_compat_index_search_handler, es_compat_search_handler,
};
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

/// Setup Elasticsearch API handlers
///
/// This is where all newly supported Elasticsearch handlers
/// should be registered.
pub fn elastic_api_handlers(
    search_service: Arc<dyn SearchService>,
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    es_compat_search_handler(search_service.clone())
        .or(es_compat_index_search_handler(search_service.clone()))
        .or(es_compat_index_multi_search_handler(search_service))
        .or(es_compat_bulk_handler(ingest_service.clone()))
        .or(es_compat_index_bulk_handler(ingest_service))
    // Register newly created handlers here.
}

/// Helper type needed by the Elasticsearch endpoints.
/// Control how the total number of hits should be tracked.
///
/// When set to `Track` with a value `true`, the response will always track the number of hits that
/// match the query accurately.
///
/// When set to `Count` with an integer value `n`, the response accurately tracks the total
/// hit count that match the query up to `n` documents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrackTotalHits {
    /// Track the number of hits that match the query accurately.
    Track(bool),
    /// Track the number of hits up to the specified value.
    Count(i64),
}

impl From<bool> for TrackTotalHits {
    fn from(b: bool) -> Self {
        TrackTotalHits::Track(b)
    }
}

impl From<i64> for TrackTotalHits {
    fn from(i: i64) -> Self {
        TrackTotalHits::Count(i)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mockall::predicate;
    use quickwit_ingest::{IngestApiService, IngestServiceClient};
    use quickwit_search::MockSearchService;

    use super::model::ElasticSearchError;
    use crate::elastic_search_api::model::MultiSearchResponse;

    fn ingest_service_client() -> IngestServiceClient {
        let universe = quickwit_actors::Universe::new();
        let (ingest_service_mailbox, _) = universe.create_test_mailbox::<IngestApiService>();
        IngestServiceClient::from_mailbox(ingest_service_mailbox)
    }

    #[tokio::test]
    async fn test_msearch_api_return_200_responses() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .with(predicate::function(
                |search_request: &quickwit_proto::SearchRequest| {
                    (search_request.index_id == "index-1"
                        && search_request.start_offset == 5
                        && search_request.max_hits == 20)
                        || (search_request.index_id == "index-2"
                            && search_request.start_offset == 0
                            && search_request.max_hits == 10)
                },
            ))
            .returning(|_| Ok(Default::default()));
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"query":"test"}}, "from": 5, "size": 20}
            {"index":"index-2"}
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let string_body = String::from_utf8(resp.body().to_vec()).unwrap();
        let es_msearch_response: MultiSearchResponse = serde_json::from_str(&string_body).unwrap();
        assert_eq!(es_msearch_response.responses.len(), 2);
        for response in es_msearch_response.responses {
            assert_eq!(response.status, 200);
            assert_eq!(response.error, None);
            assert!(response.response.is_some())
        }
    }

    #[tokio::test]
    async fn test_msearch_api_return_one_500_and_one_200_responses() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|search_request| {
                if search_request.index_id == "index-1" {
                    Ok(Default::default())
                } else {
                    Err(quickwit_search::SearchError::InternalError(
                        "something bad happened".to_string(),
                    ))
                }
            });
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"query":"test"}}, "from": 5, "size": 10}
            {"index":"index-2"}
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let es_msearch_response: MultiSearchResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(es_msearch_response.responses.len(), 2);
        assert_eq!(es_msearch_response.responses[0].status, 200);
        assert!(es_msearch_response.responses[0].error.is_none());
        assert_eq!(es_msearch_response.responses[1].status, 500);
        assert!(es_msearch_response.responses[1].response.is_none());
        let error_cause = es_msearch_response.responses[1].error.as_ref().unwrap();
        assert_eq!(
            error_cause.reason.as_ref().unwrap(),
            "Internal error: `something bad happened`."
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_malformed_request_header() {
        let mock_search_service = MockSearchService::new();
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index":"index-1"
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticSearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: Failed to parse request header"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_malformed_request_body() {
        let mock_search_service = MockSearchService::new();
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"bad":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticSearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: Failed to parse request body"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_only_a_header_request() {
        let mock_search_service = MockSearchService::new();
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index":"index-1"}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticSearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: Expect request body after request header"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_no_index() {
        let mock_search_service = MockSearchService::new();
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {}
            {"query":{"query_string":{"bad":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticSearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error.error.reason.unwrap().starts_with(
            "Invalid argument: `_msearch` must define one `index` in the request header"
        ));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_multiple_indexes() {
        let mock_search_service = MockSearchService::new();
        let es_search_api_handler =
            super::elastic_api_handlers(Arc::new(mock_search_service), ingest_service_client());
        let msearch_payload = r#"
            {"index": ["index-1", "index-2"]}
            {"query":{"query_string":{"bad":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticSearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: Searching only one index is supported for now."));
    }
}
