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

//! This module implements the search endpoint for the CloudPrem UI.

use std::sync::Arc;

use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine};
use quickwit_proto::search::{
    CountHits, Hit, PartialHit, SearchRequest, SearchResponse, SortField,
};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tracing::debug;
use warp::Filter;
use warp::reject::Rejection;

use super::{CloudPremUiError, CloudPremUiResult, SortOrder, try_into_query_ast};
use crate::cloudprem::CLOUDPREM_INDEX_ID_PATTERN;
use crate::rest_api_response::into_rest_api_response;
use crate::{BodyFormat, with_arg};

#[derive(serde::Deserialize)]
struct ListQueryString {
    query: String,
    #[serde(alias = "from_ts", default)]
    from_timestamp_inclusive_millis: Option<i64>,
    #[serde(alias = "to_ts", default)]
    to_timestamp_exclusive_millis: Option<i64>,
    #[serde(default)]
    max_hits: u64,
    #[serde(default)]
    search_after: Option<String>,
}

impl ListQueryString {
    fn try_into_search_request(self) -> CloudPremUiResult<SearchRequest> {
        let index_id_patterns = vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()];
        let query_ast = try_into_query_ast(
            &self.query,
            self.from_timestamp_inclusive_millis,
            self.to_timestamp_exclusive_millis,
        )?;
        let query_ast_json = serde_json::to_string(&query_ast)?;

        let search_after = self
            .search_after
            .filter(|s| !s.is_empty())
            .map(|s| {
                BASE64_URL_SAFE_NO_PAD
                    .decode(s.as_bytes())
                    .map_err(|e| CloudPremUiError::Invalid(format!("invalid search after: {}", e)))
                    .and_then(|s| Ok(serde_json::from_slice::<PartialHit>(&s)?))
            })
            .transpose()?;
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns,
            query_ast: query_ast_json,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: self.max_hits,
            start_offset: 0,
            aggregation_request: None,
            sort_fields: vec![SortField {
                field_name: "timestamp".to_string(),
                sort_order: SortOrder::Desc as i32,
                sort_datetime_format: None,
            }],
            scroll_ttl_secs: None,
            search_after,
            snippet_fields: Vec::new(),
            count_hits: CountHits::CountAll as i32,
            ignore_missing_indexes: false,
            skip_aggregation_finalization: false,
            enable_request_batching: false,
        };
        Ok(search_request)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct EventItem {
    #[serde(rename = "id")]
    event_id: String,
    event: JsonValue,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ListResponse {
    metadata: ListResponseMetadata,
    events: Vec<EventItem>,
}

impl ListResponse {
    fn try_from_search_response(
        search_response: SearchResponse,
        max_hits: u64,
    ) -> CloudPremUiResult<Self> {
        let paging = if (search_response.hits.len() as u64) < max_hits {
            None
        } else {
            Some(Paging {
                after: (search_response.hits.last().map(|hit| {
                    BASE64_URL_SAFE_NO_PAD.encode(
                        serde_json::to_vec(&hit.partial_hit)
                            .expect("serializing PartialHit should not fail"),
                    )
                })),
            })
        };

        let metadata = ListResponseMetadata {
            is_loading: false,
            paging,
            count: Some(search_response.num_hits),
        };

        let events: Vec<EventItem> = search_response
            .hits
            .into_iter()
            .map(try_into_cloudprem_ui_event_item)
            .collect::<CloudPremUiResult<_>>()?;
        let response = ListResponse { metadata, events };
        Ok(response)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ListResponseMetadata {
    paging: Option<Paging>,
    #[serde(default = "false_fn", rename = "isLoading")]
    is_loading: bool,
    count: Option<u64>,
}

fn false_fn() -> bool {
    false
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Paging {
    after: Option<String>,
}

const EVENT_ID_FIELD: &str = "id";

fn try_into_cloudprem_ui_event_item(hit: Hit) -> CloudPremUiResult<EventItem> {
    let event: JsonValue = serde_json::from_str(&hit.json)
        .map_err(|error| CloudPremUiError::Internal(Box::new(error)))?;

    let JsonValue::Object(event_obj) = &event else {
        panic!("event should be a JSON object");
    };
    let event_id = match event_obj.get(EVENT_ID_FIELD) {
        Some(JsonValue::String(event_id)) => event_id.clone(),
        Some(json_value) => panic!("event ID should be a string, got {:?}", json_value),
        _ => panic!("event ID field should be present"),
    };
    let event_item = EventItem { event_id, event };
    Ok(event_item)
}

pub(crate) fn search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("cloudprem" / "api" / "v1" / "search")
        .and(warp::get())
        .and(warp::query::<ListQueryString>())
        .and(with_arg(search_service))
        .then(cloudprem_ui_search)
        .then(|response_result| {
            futures::future::ready(into_rest_api_response(response_result, BodyFormat::Json))
        })
}

#[utoipa::path(
    post,
    tag = "CloudPrem Search",
    path = "/cloudprem/api/v1/search",
    request_body = CloudPremUiRequest,
    responses(
        (status = 200, description = "The search request was successfully executed.", body = CloudPremUiResponse)
    ),
)]
/// Executes a search request issued by the CloudPrem UI.
async fn cloudprem_ui_search(
    cloudprem_ui_qs: ListQueryString,
    search_service: Arc<dyn SearchService>,
) -> CloudPremUiResult<ListResponse> {
    debug!(
        cloudprem_ui_qs.query,
        from_ts = cloudprem_ui_qs.from_timestamp_inclusive_millis,
        to_ts = cloudprem_ui_qs.to_timestamp_exclusive_millis,
        cloudprem_ui_qs.max_hits,
        "received search request"
    );
    let search_request = cloudprem_ui_qs.try_into_search_request()?;
    let max_hits = search_request.max_hits;
    let search_response = search_service.root_search(search_request).await?;
    let cloudprem_ui_response = ListResponse::try_from_search_response(search_response, max_hits)?;
    debug!("returned {} events", cloudprem_ui_response.events.len(),);
    Ok(cloudprem_ui_response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_search::MockSearchService;

    use super::*;

    #[tokio::test]
    async fn test_cloudprem_ui_search() {
        let hit = Hit {
            index_id: "test-index".to_string(),
            json: r#"{"id":"event-1","timestamp":1759325269270,"message":"test error"}"#
                .to_string(),
            partial_hit: Some(PartialHit {
                sort_value: None,
                sort_value2: None,
                split_id: "split-1".to_string(),
                segment_ord: 0,
                doc_id: 1,
            }),
            snippet: None,
        };
        let search_response = SearchResponse {
            hits: vec![hit],
            num_hits: 1,
            elapsed_time_micros: 100,
            errors: Vec::new(),
            scroll_id: None,
            aggregation_postcard: None,
            failed_splits: Vec::new(),
            num_successful_splits: 0,
        };
        let mut search_service = MockSearchService::new();
        search_service
            .expect_root_search()
            .returning(move |search_request| {
                assert_eq!(search_request.max_hits, 100);
                assert_eq!(
                    search_request.query_ast,
                    r#"{"type":"bool","must":[{"type":"bool","should":[{"type":"full_text","field":"message","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"full_text","field":"error","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}]},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
                );
                assert_eq!(search_request.search_after, None);
                Ok(search_response.clone())
            });
        let bound_cloudprem_ui_search_handler = search_handler(Arc::new(search_service));

        let response = warp::test::request()
            .path(
                "/cloudprem/api/v1/search?query=error&from_ts=1759325269270&to_ts=1759326169270&\
                 max_hits=100&search_after=",
            )
            .method("GET")
            .reply(&bound_cloudprem_ui_search_handler)
            .await;

        assert_eq!(response.status(), 200);

        let body = response.body();
        let list_response: ListResponse = serde_json::from_slice(body).unwrap();
        let event_object = list_response.events[0].event.as_object().unwrap();

        assert!(list_response.metadata.paging.is_none());
        assert_eq!(list_response.metadata.count, Some(1));
        assert!(!list_response.metadata.is_loading);
        assert_eq!(event_object.get("id").unwrap(), "event-1");
        assert_eq!(event_object.get("message").unwrap(), "test error");
    }

    #[tokio::test]
    async fn test_cloudprem_ui_search_with_pagination() {
        let request_partial_hit = PartialHit {
            sort_value: None,
            sort_value2: None,
            split_id: "split-1".to_string(),
            segment_ord: 0,
            doc_id: 1,
        };
        let request_search_after =
            BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_vec(&request_partial_hit).unwrap());

        let response_partial_hit = PartialHit {
            sort_value: None,
            sort_value2: None,
            split_id: "split-2".to_string(),
            segment_ord: 0,
            doc_id: 2,
        };
        let response_search_after =
            BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_vec(&response_partial_hit).unwrap());

        let hit = Hit {
            index_id: "test-index".to_string(),
            json: r#"{"id":"event-1","timestamp":1759325269270,"message":"test error"}"#
                .to_string(),
            partial_hit: Some(response_partial_hit),
            snippet: None,
        };

        let search_response = SearchResponse {
            hits: vec![hit],
            num_hits: 1,
            elapsed_time_micros: 100,
            errors: Vec::new(),
            scroll_id: None,
            aggregation_postcard: None,
            failed_splits: Vec::new(),
            num_successful_splits: 0,
        };
        let mut search_service = MockSearchService::new();
        search_service
            .expect_root_search()
            .returning(move |search_request| {
                assert_eq!(search_request.max_hits, 1);
                assert_eq!(
                    search_request.query_ast,
                    r#"{"type":"bool","must":[{"type":"bool","should":[{"type":"full_text","field":"message","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"full_text","field":"error","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}]},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
                );
                assert_eq!(
                    search_request.search_after,
                    Some(request_partial_hit.clone())
                );
                Ok(search_response.clone())
            });
        let bound_cloudprem_ui_search_handler = search_handler(Arc::new(search_service));

        let response = warp::test::request()
            .path(&format!(
                "/cloudprem/api/v1/search?query=error&from_ts=1759325269270&to_ts=1759326169270&\
                 max_hits=1&search_after={}",
                request_search_after
            ))
            .method("GET")
            .reply(&bound_cloudprem_ui_search_handler)
            .await;

        assert_eq!(response.status(), 200);

        let body = response.body();
        let list_response: ListResponse = serde_json::from_slice(body).unwrap();
        let event_object = list_response.events[0].event.as_object().unwrap();

        assert_eq!(
            list_response.metadata.paging.unwrap().after.unwrap(),
            response_search_after
        );
        assert_eq!(list_response.metadata.count, Some(1));
        assert!(!list_response.metadata.is_loading);
        assert_eq!(event_object.get("id").unwrap(), "event-1");
        assert_eq!(event_object.get("message").unwrap(), "test error");
    }

    #[tokio::test]
    async fn test_cloudprem_ui_search_invalid_search_after() {
        let search_service = MockSearchService::new();
        let bound_cloudprem_ui_search_handler = search_handler(Arc::new(search_service));
        let response = warp::test::request()
            .path(
                "/cloudprem/api/v1/search?query=error&from_ts=1759325269270&to_ts=1759326169270&\
                 max_hits=1&search_after=not-base64",
            )
            .method("GET")
            .reply(&bound_cloudprem_ui_search_handler)
            .await;

        assert_eq!(response.status(), 400);
    }
}
