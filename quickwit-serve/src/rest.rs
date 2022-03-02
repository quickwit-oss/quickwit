// Copyright (C) 2021 Quickwit, Inc.
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

use std::convert::{Infallible, TryFrom};
use std::net::SocketAddr;
use std::sync::Arc;

use futures::stream::StreamExt;
use hyper::header::HeaderValue;
use hyper::HeaderMap;
use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_common::metrics;
use quickwit_doc_mapper::{SortByField, SortOrder};
use quickwit_proto::{OutputFormat, SortOrder as ProtoSortOrder};
use quickwit_search::{SearchResponseRest, SearchService, SearchServiceImpl};
use serde::{de, Deserialize, Deserializer};
use tracing::info;
use warp::hyper::header::CONTENT_TYPE;
use warp::hyper::StatusCode;
use warp::{reply, Filter, Rejection, Reply};

use crate::http_handler::cluster::cluster_handler;
use crate::http_handler::health_check::liveness_check_handler;
use crate::ApiError;

/// Start REST service given a HTTP address and a search service.
pub async fn start_rest_service(
    rest_addr: SocketAddr,
    search_service: Arc<SearchServiceImpl>,
    cluster_service: Arc<ClusterServiceImpl>,
) -> anyhow::Result<()> {
    info!(rest_addr=?rest_addr, "Starting REST service.");
    let request_counter = warp::log::custom(|_| {
        crate::COUNTERS.num_requests.inc();
    });
    let metrics_service = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);
    let rest_routes = liveness_check_handler()
        .or(cluster_handler(cluster_service))
        .or(search_get_handler(search_service.clone()))
        .or(search_post_handler(search_service.clone()))
        .or(search_stream_handler(search_service))
        .or(metrics_service)
        .with(request_counter)
        .recover(recover_fn);
    warp::serve(rest_routes).run(rest_addr).await;
    Ok(())
}

fn default_max_hits() -> u64 {
    20
}

/// Output format for the search results.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    PrettyJson,
}

impl Default for Format {
    fn default() -> Self {
        Format::PrettyJson
    }
}

impl ToString for Format {
    fn to_string(&self) -> String {
        match &self {
            Self::Json => "json".to_string(),
            Self::PrettyJson => "prety-json".to_string(),
        }
    }
}

impl Format {
    fn resp_body<T: serde::Serialize>(self, val: T) -> serde_json::Result<String> {
        match self {
            Format::Json => serde_json::to_string(&val),
            Format::PrettyJson => serde_json::to_string_pretty(&val),
        }
    }

    pub fn make_reply<T: serde::Serialize>(self, result: Result<T, ApiError>) -> impl Reply {
        let status_code: StatusCode;
        let body_json = match result {
            Ok(success) => {
                status_code = StatusCode::OK;
                self.resp_body(success)
            }
            Err(err) => {
                status_code = err.http_status_code();
                self.resp_body(err)
            } //< yeah it is lame it is not formatted, but it should never happen really.
        }
        .unwrap_or_else(|_| {
            tracing::error!("Error: the response serialization failed.");
            "Error: Failed to serialize response.".to_string()
        });
        let reply_with_header = reply::with_header(body_json, CONTENT_TYPE, "application/json");
        reply::with_status(reply_with_header, status_code)
    }
}

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequestQueryString {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    /// The aggregation JSON string.
    pub aggregations: Option<serde_json::Value>,
    // Fields to search on
    #[serde(default)]
    #[serde(rename(deserialize = "searchField"))]
    #[serde(deserialize_with = "from_simple_list")]
    pub search_fields: Option<Vec<String>>,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
    /// Maximum number of hits to return (by default 20).
    #[serde(default = "default_max_hits")]
    pub max_hits: u64,
    /// First hit to return. Together with num_hits, this parameter
    /// can be used for pagination.
    ///
    /// E.g.
    /// The results with rank [start_offset..start_offset + max_hits) are returned
    #[serde(default)] // Default to 0. (We are 0-indexed)
    pub start_offset: u64,
    /// The output format.
    #[serde(default)]
    pub format: Format,
    /// Specifies how documents are sorted.
    #[serde(deserialize_with = "sort_by_field_mini_dsl")]
    #[serde(default)]
    sort_by_field: Option<SortByField>,
}

fn get_proto_search_by(search_request: &SearchRequestQueryString) -> (Option<i32>, Option<String>) {
    if let Some(sort_by_field) = &search_request.sort_by_field {
        let sort_order = match sort_by_field.order {
            SortOrder::Asc => ProtoSortOrder::Asc as i32,
            SortOrder::Desc => ProtoSortOrder::Desc as i32,
        };
        (Some(sort_order), Some(sort_by_field.field_name.to_string()))
    } else {
        (None, None)
    }
}

async fn search_endpoint<TSearchService: SearchService>(
    index_id: String,
    search_request: SearchRequestQueryString,
    search_service: &TSearchService,
) -> Result<SearchResponseRest, ApiError> {
    let (sort_order, sort_by_field) = get_proto_search_by(&search_request);
    let search_request = quickwit_proto::SearchRequest {
        index_id,
        query: search_request.query,
        search_fields: search_request.search_fields.unwrap_or_default(),
        start_timestamp: search_request.start_timestamp,
        end_timestamp: search_request.end_timestamp,
        max_hits: search_request.max_hits,
        start_offset: search_request.start_offset,
        aggregation_request: search_request
            .aggregations
            .map(|agg| serde_json::to_string(&agg).expect("could not serialize serde_json::Value")),
        sort_order,
        sort_by_field,
    };
    let search_response = search_service.root_search(search_request).await?;
    let search_response_rest =
        SearchResponseRest::try_from(search_response).map_err(ApiError::SearchError)?;
    Ok(search_response_rest)
}

fn search_get_filter(
) -> impl Filter<Extract = (String, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "search")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

fn search_post_filter(
) -> impl Filter<Extract = (String, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "search")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::json())
}

async fn search<TSearchService: SearchService>(
    index_id: String,
    search_request: SearchRequestQueryString,
    search_service: Arc<TSearchService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id = %index_id, request =? search_request, "search");
    Ok(search_request
        .format
        .make_reply(search_endpoint(index_id, search_request, &*search_service).await))
}

/// REST GET search handler.
///
/// Parses the search request from the
pub fn search_get_handler<TSearchService: SearchService>(
    search_service: Arc<TSearchService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    search_get_filter()
        .and(warp::any().map(move || search_service.clone()))
        .and_then(search)
}

/// REST POST search handler.
///
/// Parses the search request from the
pub fn search_post_handler<TSearchService: SearchService>(
    search_service: Arc<TSearchService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    search_post_filter()
        .and(warp::any().map(move || search_service.clone()))
        .and_then(search)
}

/// This struct represents the search stream query passed to
/// the REST API.
#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SearchStreamRequestQueryString {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    // Fields to search on.
    #[serde(default)]
    #[serde(rename(deserialize = "searchField"))]
    #[serde(deserialize_with = "from_simple_list")]
    pub search_fields: Option<Vec<String>>,
    /// If set, restricts search to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restricts search to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
    /// The fast field to extract.
    #[serde(deserialize_with = "deserialize_not_empty_string")]
    pub fast_field: String,
    /// The requested output format.
    #[serde(default)]
    pub output_format: OutputFormat,
    #[serde(default)]
    pub partition_by_field: Option<String>,
}

async fn search_stream_endpoint<TSearchService: SearchService>(
    index_id: String,
    search_request: SearchStreamRequestQueryString,
    search_service: &TSearchService,
) -> Result<hyper::Body, ApiError> {
    let request = quickwit_proto::SearchStreamRequest {
        index_id,
        query: search_request.query,
        search_fields: search_request.search_fields.unwrap_or_default(),
        start_timestamp: search_request.start_timestamp,
        end_timestamp: search_request.end_timestamp,
        fast_field: search_request.fast_field,
        output_format: search_request.output_format as i32,
        partition_by_field: search_request.partition_by_field,
    };
    let mut data = search_service.root_search_stream(request).await?;
    let (mut sender, body) = hyper::Body::channel();
    tokio::spawn(async move {
        while let Some(result) = data.next().await {
            match result {
                Ok(bytes) => {
                    if sender.send_data(bytes).await.is_err() {
                        sender.abort();
                        break;
                    }
                }
                Err(error) => {
                    // Add trailer to signal to the client that there is an error. Only works
                    // if the request is made with an http2 client that can read it... and
                    // actually this seems pretty rare, for example `curl` will not show this
                    // trailer. Thus we also call `sender.abort()` so that the
                    // client will see something wrong happened. But he will
                    // need to look at the logs to understand that.
                    tracing::error!(error=%error, "Error when streaming search results.");
                    let header_value_str =
                        format!("Error when streaming search results: {}.", error);
                    let header_value = HeaderValue::from_str(header_value_str.as_str())
                        .unwrap_or_else(|_| HeaderValue::from_static("Search stream error"));
                    let mut trailers = HeaderMap::new();
                    trailers.insert("X-Stream-Error", header_value);
                    let _ = sender.send_trailers(trailers).await;
                    sender.abort();
                    break;
                }
            };
        }
    });
    Ok(body)
}

fn make_streaming_reply(result: Result<hyper::Body, ApiError>) -> impl Reply {
    let status_code: StatusCode;
    let body = match result {
        Ok(body) => {
            status_code = StatusCode::OK;
            warp::reply::Response::new(body)
        }
        Err(err) => {
            status_code = err.http_status_code();
            warp::reply::Response::new(hyper::Body::from(err.message()))
        }
    };
    reply::with_status(body, status_code)
}

async fn search_stream<TSearchService: SearchService>(
    index_id: String,
    request: SearchStreamRequestQueryString,
    search_service: Arc<TSearchService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id=%index_id,request=?request, "search_stream");
    let content_type = match request.output_format {
        OutputFormat::ClickHouseRowBinary => "application/octet-stream",
        OutputFormat::Csv => "text/csv",
    };
    let reply =
        make_streaming_reply(search_stream_endpoint(index_id, request, &*search_service).await);
    let reply_with_header = reply::with_header(reply, CONTENT_TYPE, content_type);
    Ok(reply_with_header)
}

fn search_stream_filter(
) -> impl Filter<Extract = (String, SearchStreamRequestQueryString), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "search" / "stream")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

pub fn search_stream_handler<TSearchService: SearchService>(
    search_service: Arc<TSearchService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    search_stream_filter()
        .and(warp::any().map(move || search_service.clone()))
        .and_then(search_stream)
}

/// This function returns a formated error based on the given rejection reason.
async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    // TODO handle more errors.
    match rejection.find::<serde_qs::Error>() {
        Some(err) => {
            // The querystring was incorrect.
            Ok(
                Format::PrettyJson.make_reply(Err::<(), ApiError>(ApiError::InvalidArgument(
                    err.to_string(),
                ))),
            )
        }
        None => Ok(Format::PrettyJson.make_reply(Err::<(), ApiError>(ApiError::NotFound))),
    }
}

fn sort_by_field_mini_dsl<'de, D>(deserializer: D) -> Result<Option<SortByField>, D::Error>
where D: Deserializer<'de> {
    let string = String::deserialize(deserializer)?;
    Ok(Some(string.into()))
}

fn from_simple_list<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where D: Deserializer<'de> {
    let str_sequence = String::deserialize(deserializer)?;
    Ok(Some(
        str_sequence
            .trim_matches(',')
            .split(',')
            .map(|item| item.to_owned())
            .collect(),
    ))
}

// Deserialize a string field and return and error if it's empty.
// We have 2 issues with this implementation:
// - this is not generic and thus nos sustainable and we may need to
//   use an external crate for validation in the future like
//   this one https://github.com/Keats/validator.
// - the error does not mention the field name and this is not user friendly. There
//   is an external crate that can help https://github.com/dtolnay/path-to-error but
//   I did not find a way to plug it to serde_qs.
// Conclusion: the best way I found to reject a user query that contains an empty
// string on an mandatory field is this serializer.
fn deserialize_not_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(de::Error::custom("Expected a non empty string field."));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;
    use bytes::Bytes;
    use mockall::predicate;
    use quickwit_search::{MockSearchService, SearchError};
    use serde_json::json;

    use super::*;

    #[test]
    fn test_serialize_search_response() -> anyhow::Result<()> {
        let search_response = SearchResponseRest {
            num_hits: 55,
            hits: Vec::new(),
            elapsed_time_micros: 0u64,
            errors: Vec::new(),
            aggregations: None,
        };
        let search_response_json: serde_json::Value = serde_json::to_value(&search_response)?;
        let expected_search_response_json: serde_json::Value = json!({
            "numHits": 55,
            "hits": [],
            "elapsedTimeMicros": 0,
        });
        assert_json_include!(
            actual: search_response_json,
            expected: expected_search_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post() {
        let rest_search_api_filter = search_post_filter();
        let (index, req) = warp::test::request()
            .method("POST")
            .path("/api/v1/quickwit-demo-index/search?query=*&maxHits=10")
            .json(&true)
            .body(r#"{"query": "*", "maxHits":10, "aggregations": {"range":[]} }"#)
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: None,
                start_timestamp: None,
                max_hits: 10,
                format: Format::default(),
                sort_by_field: None,
                aggregations: Some(json!({"range":[]})),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple() {
        let rest_search_api_filter = search_get_filter();
        let (index, req) = warp::test::request()
            .path(
                "/api/v1/quickwit-demo-index/search?query=*&endTimestamp=1450720000&maxHits=10&\
                 startOffset=22",
            )
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: None,
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 10,
                start_offset: 22,
                format: Format::default(),
                sort_by_field: None,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_default_num_hits_default_offset() {
        let rest_search_api_filter = search_get_filter();
        let (index, req) = warp::test::request()
            .path(
                "/api/v1/quickwit-demo-index/search?query=*&endTimestamp=1450720000&\
                 searchField=title,body",
            )
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: Some(vec!["title".to_string(), "body".to_string()]),
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 20,
                start_offset: 0,
                format: Format::default(),
                sort_by_field: None,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_format() {
        let rest_search_api_filter = search_get_filter();
        let (index, req) = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&format=json")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(&index, "quickwit-demo-index");
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 20,
                start_offset: 0,
                format: Format::Json,
                search_fields: None,
                sort_by_field: None,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_sort_by() {
        let rest_search_api_filter = search_get_filter();
        let (_, req) = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&format=json&sortByField=field")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 20,
                start_offset: 0,
                format: Format::Json,
                search_fields: None,
                sort_by_field: Some(SortByField {
                    field_name: "field".to_string(),
                    order: SortOrder::Asc
                }),
                ..Default::default()
            }
        );

        let rest_search_api_filter = search_get_filter();
        let (_, req) = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&format=json&sortByField=+field")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 20,
                start_offset: 0,
                format: Format::Json,
                search_fields: None,
                sort_by_field: Some(SortByField {
                    field_name: "field".to_string(),
                    order: SortOrder::Asc
                }),
                ..Default::default()
            }
        );

        let rest_search_api_filter = search_get_filter();
        let (_, req) = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&format=json&sortByField=-field")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 20,
                start_offset: 0,
                format: Format::Json,
                search_fields: None,
                sort_by_field: Some(SortByField {
                    field_name: "field".to_string(),
                    order: SortOrder::Desc
                }),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_invalid_key() -> anyhow::Result<()> {
        let mock_search_service = MockSearchService::new();
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*&endUnixTimestamp=1450720000")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let exp_resp_json = serde_json::json!({
            "error": "InvalidArgument: failed with reason: unknown field `endUnixTimestamp`, expected one of `query`, `aggregations`, `searchField`, `startTimestamp`, `endTimestamp`, `maxHits`, `startOffset`, `format`, `sortByField`."
        });
        assert_eq!(resp_json, exp_resp_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_route_serialize_with_results() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Ok(quickwit_proto::SearchResponse {
                hits: Vec::new(),
                num_hits: 10,
                elapsed_time_micros: 16,
                errors: vec![],
                ..Default::default()
            })
        });
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/api/v1/quickwit-demo-index/search?query=*")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "numHits": 10,
            "hits": [],
            "elapsedTimeMicros": 16,
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
                |search_request: &quickwit_proto::SearchRequest| {
                    search_request.start_offset == 5 && search_request.max_hits == 30
                },
            ))
            .returning(|_| Ok(Default::default()));
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
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

    #[tokio::test]
    async fn test_rest_search_api_with_index_does_not_exist() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Err(SearchError::IndexDoesNotExist {
                index_id: "not-found-index".to_string(),
            })
        });
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
        assert_eq!(
            warp::test::request()
                .path("/api/v1/index-does-not-exist/search?query=myfield:test")
                .reply(&rest_search_api_handler)
                .await
                .status(),
            404
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_with_wrong_fieldname() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|_| Err(SearchError::InternalError("ty".to_string())));
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
        assert_eq!(
            warp::test::request()
                .path("/api/v1/index-does-not-exist/search?query=myfield:test")
                .reply(&rest_search_api_handler)
                .await
                .status(),
            500
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_with_invalid_query() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|_| Err(SearchError::InvalidQuery("invalid query".to_string())));
        let rest_search_api_handler =
            super::search_get_handler(Arc::new(mock_search_service)).recover(recover_fn);
        assert_eq!(
            warp::test::request()
                .path("/api/v1/my-index/search?query=myfield:test")
                .reply(&rest_search_api_handler)
                .await
                .status(),
            400
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_stream_api() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search_stream()
            .return_once(|_| {
                Ok(Box::pin(futures::stream::iter(vec![
                    Ok(Bytes::from("first row\n")),
                    Ok(Bytes::from("second row")),
                ])))
            });
        let rest_search_stream_api_handler =
            super::search_stream_handler(Arc::new(mock_search_service)).recover(recover_fn);
        let response = warp::test::request()
            .path(
                "/api/v1/my-index/search/stream?query=obama&fastField=external_id&outputFormat=csv",
            )
            .reply(&rest_search_stream_api_handler)
            .await;
        assert_eq!(response.status(), 200);
        let body = String::from_utf8_lossy(response.body());
        assert_eq!(body, "first row\nsecond row");
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_csv() {
        let (index, req) = warp::test::request()
            .path(
                "/api/v1/my-index/search/stream?query=obama&fastField=external_id&outputFormat=csv",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap();
        assert_eq!(&index, "my-index");
        assert_eq!(
            &req,
            &super::SearchStreamRequestQueryString {
                query: "obama".to_string(),
                search_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                fast_field: "external_id".to_string(),
                output_format: OutputFormat::Csv,
                partition_by_field: None,
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_click_house_row_binary() {
        let (index, req) = warp::test::request()
            .path(
                "/api/v1/my-index/search/stream?query=obama&fastField=external_id&\
                 outputFormat=clickHouseRowBinary",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap();
        assert_eq!(&index, "my-index");
        assert_eq!(
            &req,
            &super::SearchStreamRequestQueryString {
                query: "obama".to_string(),
                search_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                fast_field: "external_id".to_string(),
                output_format: OutputFormat::ClickHouseRowBinary,
                partition_by_field: None,
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_error() {
        let rejection = warp::test::request()
            .path(
                "/api/v1/my-index/search/stream?query=obama&fastField=external_id&\
                 outputFormat=click_house_row_binary",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap_err();
        let parse_error = rejection.find::<serde_qs::Error>().unwrap();
        assert_eq!(
            parse_error.to_string(),
            "failed with reason: unknown variant `click_house_row_binary`, expected `csv` or \
             `clickHouseRowBinary`"
        );
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_error_empty_fastfield() {
        let rejection = warp::test::request()
            .path(
                "/api/v1/my-index/search/stream?query=obama&fastField=&\
                 outputFormat=clickHouseRowBinary",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap_err();
        let parse_error = rejection.find::<serde_qs::Error>().unwrap();
        assert_eq!(
            parse_error.to_string(),
            "failed with reason: Expected a non empty string field."
        );
    }
}
