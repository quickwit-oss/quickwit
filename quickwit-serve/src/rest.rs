/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::convert::Infallible;
use std::sync::Arc;

use grpc::SearchResult;
use quickwit_proto as grpc;
use quickwit_search::SearchService;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use warp::hyper::StatusCode;
use warp::reply;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

use crate::ApiError;

fn default_max_hits() -> u64 {
    20
}

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequestQueryString {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
    /// Maximum number of hits to return (by default 20).
    #[serde(default = "default_max_hits")]
    pub max_hits: u64,
    // First hit to return. Together with num_hits, this parameter
    // can be used for pagination.
    //
    // E.g.
    // The results with rank [start_offset..start_offset + max_hits) are returned
    #[serde(default)] // Default to 0. (We are 0-indexed)
    pub start_offset: u64,
}

/// SearchResultsJson represents the result returned by the rest search API
/// and is meant to be serialized into Json.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResultJson {
    num_hits: u64,
    hits: Vec<serde_json::Value>,
    num_microsecs: u64,
}

impl From<SearchResult> for SearchResultJson {
    fn from(search_result: SearchResult) -> Self {
        let hits: Vec<serde_json::Value> = search_result
            .hits
            .into_iter()
            .map(|hit| match serde_json::from_str(&hit.json) {
                Ok(hit_json) => hit_json,
                Err(invalid_json) => {
                    error!(err=?invalid_json, "Invalid json in hit");
                    serde_json::json!({})
                }
            })
            .collect();
        SearchResultJson {
            num_hits: search_result.num_hits,
            hits,
            num_microsecs: search_result.elapsed_time_micros,
        }
    }
}

async fn search_endpoint<TSearchService: SearchService>(
    index_id: String,
    search_request: SearchRequestQueryString,
    search_service: &TSearchService,
) -> Result<SearchResultJson, ApiError> {
    let search_request = grpc::SearchRequest {
        index_id,
        query: search_request.query,
        start_timestamp: search_request.start_timestamp,
        end_timestamp: search_request.end_timestamp,
        max_hits: search_request.max_hits,
        start_offset: search_request.start_offset,
    };
    let search_result = search_service.root_search(search_request).await?;
    let search_result_json = SearchResultJson::from(search_result);
    Ok(search_result_json)
}

fn search_filter(
) -> impl Filter<Extract = (String, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "search")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

async fn search<TSearchService: SearchService>(
    index_id: String,
    request: SearchRequestQueryString,
    search_service: Arc<TSearchService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id=%index_id,request=?request, "search");
    Ok(make_reply(
        search_endpoint(index_id, request, &*search_service).await,
    ))
}

/// Rest search handler.
///
/// It parses the search request from the
pub fn search_handler<TSearchService: SearchService>(
    search_service: Arc<TSearchService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    search_filter()
        .and(warp::any().map(move || search_service.clone()))
        .and_then(search)
        .recover(recover_fn)
}

fn make_reply<T: serde::Serialize>(result: Result<T, ApiError>) -> reply::WithStatus<reply::Json> {
    match result {
        Ok(success) => reply::with_status(reply::json(&success), StatusCode::OK),
        Err(error) => reply::with_status(reply::json(&error), error.http_status_code()),
    }
}

/// This function returns a formated error based on the given rejection reason.
async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    // TODO handle more errors.
    match rejection.find::<serde_qs::Error>() {
        Some(err) => {
            // The querystring was incorrect.
            Ok(make_reply(Err::<(), ApiError>(ApiError::InvalidArgument(
                err.to_string(),
            ))))
        }
        None => Ok(make_reply(Err::<(), ApiError>(ApiError::NotFound))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_json_diff::assert_json_include;
    use mockall::predicate;
    use quickwit_search::MockSearchService;
    use serde_json::json;

    #[test]
    fn test_serialize_search_results() -> anyhow::Result<()> {
        let search_results = SearchResultJson {
            num_hits: 55,
            hits: Vec::new(),
            num_microsecs: 0u64,
        };
        let search_results_json: serde_json::Value = serde_json::to_value(&search_results)?;
        let expected_json: serde_json::Value = json!({
            "numHits": 55,
            "hits": [],
            "numMicrosecs": 0
        });
        assert_json_include!(actual: search_results_json, expected: expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple() {
        let rest_search_api_filter = search_filter();
        let (index, req) = warp::test::request()
        .path("/api/v1/quickwit-demo-index/search?query=*&endTimestamp=1450720000&maxHits=10&startOffset=22")
        .filter(&rest_search_api_filter)
        .await
        .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 10,
                start_offset: 22,
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_default_num_hits_default_offset() {
        let rest_search_api_filter = search_filter();
        let (index, req) = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&endTimestamp=1450720000")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 20,
                start_offset: 0,
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_invalid_key() -> anyhow::Result<()> {
        let mock_search_service = MockSearchService::new();
        let rest_search_api_handler = super::search_handler(Arc::new(mock_search_service));
        let resp = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&endUnixTimestamp=1450720000")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let exp_resp_json = serde_json::json!({
            "error": "InvalidArgument(\"failed with reason: unknown field `endUnixTimestamp`, expected one of `query`, `startTimestamp`, `endTimestamp`, `maxHits`, `startOffset`\")"
        });
        assert_eq!(resp_json, exp_resp_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_route_serialize_with_results() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Ok(SearchResult {
                hits: Vec::new(),
                num_hits: 10,
                elapsed_time_micros: 16,
            })
        });
        let rest_search_api_handler = super::search_handler(Arc::new(mock_search_service));
        let resp = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "numHits": 10,
            "hits": [],
            "numMicrosecs": 16,
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_start_offset_and_num_hits_parameter() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .with(predicate::function(
                |search_request: &grpc::SearchRequest| {
                    search_request.start_offset == 5 && search_request.max_hits == 30
                },
            ))
            .returning(|_| Ok(Default::default()));
        let rest_search_api_handler = super::search_handler(Arc::new(mock_search_service));
        assert_eq!(
            warp::test::request()
                .path("/api/v1/quickwit-demo-index/search?query=*&startOffset=5&maxHits=30")
                .reply(&rest_search_api_handler)
                .await
                .status(),
            200
        );
        Ok(())
    }
}
