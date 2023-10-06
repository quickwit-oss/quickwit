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

use std::convert::TryFrom;
use std::sync::Arc;

use futures::stream::StreamExt;
use hyper::header::HeaderValue;
use hyper::HeaderMap;
use once_cell::sync::Lazy;
use quickwit_config::validate_index_id_pattern;
use quickwit_proto::search::{OutputFormat, SortField, SortOrder};
use quickwit_proto::ServiceError;
use quickwit_query::query_ast::query_ast_from_user_text;
use quickwit_search::{SearchError, SearchResponseRest, SearchService};
use regex::Regex;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tracing::info;
use warp::hyper::header::CONTENT_TYPE;
use warp::hyper::StatusCode;
use warp::{reply, Filter, Rejection, Reply};

use crate::json_api_response::make_json_api_response;
use crate::simple_list::{from_simple_list, to_simple_list};
use crate::{with_arg, BodyFormat};

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(search_get_handler, search_post_handler, search_stream_handler,),
    components(schemas(
        BodyFormat,
        OutputFormat,
        SearchRequestQueryString,
        SearchResponseRest,
        SortBy,
        SortField,
        SortOrder,
    ),)
)]
pub struct SearchApi;

// Matches index patterns separated by commas or its URL encoded version '%2C'.
static COMMA_SEPARATED_INDEX_PATTERNS_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r",|%2C").expect("the regular expression should compile"));

pub(crate) async fn extract_index_id_patterns(
    comma_separated_index_patterns: String,
) -> Result<Vec<String>, Rejection> {
    let mut index_ids_patterns = Vec::new();
    for index_id_pattern in
        COMMA_SEPARATED_INDEX_PATTERNS_REGEX.split(&comma_separated_index_patterns)
    {
        validate_index_id_pattern(index_id_pattern).map_err(|error| {
            warp::reject::custom(crate::rest::InvalidArgument(error.to_string()))
        })?;
        index_ids_patterns.push(index_id_pattern.to_string());
    }
    assert!(!index_ids_patterns.is_empty());
    Ok(index_ids_patterns)
}

#[derive(Debug, Default, Eq, PartialEq, Deserialize, utoipa::ToSchema)]
pub struct SortBy {
    /// Fields to sort on.
    pub sort_fields: Vec<SortField>,
}

impl SortBy {
    pub fn is_empty(&self) -> bool {
        self.sort_fields.is_empty()
    }
}

impl From<String> for SortBy {
    fn from(sort_by: String) -> Self {
        let mut sort_fields = Vec::new();

        for field_name in sort_by.split(',') {
            if field_name.is_empty() {
                continue;
            }
            let (field_name, sort_order) = if let Some(tail) = field_name.strip_prefix('+') {
                (tail.trim().to_string(), SortOrder::Desc)
            } else if let Some(tail) = field_name.strip_prefix('-') {
                (tail.trim().to_string(), SortOrder::Asc)
            } else {
                let trimmed_field_name = field_name.trim().to_string();

                (trimmed_field_name, SortOrder::Desc)
            };
            let sort_field = SortField {
                field_name,
                sort_order: sort_order as i32,
            };
            sort_fields.push(sort_field);
        }
        Self { sort_fields }
    }
}

pub fn sort_by_mini_dsl<'de, D>(deserializer: D) -> Result<SortBy, D::Error>
where D: Deserializer<'de> {
    let sort_by_mini_dsl = String::deserialize(deserializer)?;
    Ok(SortBy::from(sort_by_mini_dsl))
}

impl Serialize for SortBy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut sort_by_mini_dsl = String::new();

        for sort_field in &self.sort_fields {
            if sort_field.sort_order() == SortOrder::Desc {
                sort_by_mini_dsl.push('-');
            }
            sort_by_mini_dsl.push_str(&sort_field.field_name);
        }
        serializer.serialize_str(&sort_by_mini_dsl)
    }
}

fn default_max_hits() -> u64 {
    20
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
fn deserialize_non_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(de::Error::custom("expected a non-empty string field"));
    }
    Ok(value)
}

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(
    Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::IntoParams, utoipa::ToSchema,
)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct SearchRequestQueryString {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    #[param(value_type = Object)]
    #[schema(value_type = Object)]
    /// The aggregation JSON string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggs: Option<JsonValue>,
    // Fields to search on
    #[param(rename = "search_field")]
    #[schema(rename = "search_field")]
    #[serde(default)]
    #[serde(rename = "search_field")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "to_simple_list")]
    pub search_fields: Option<Vec<String>>,
    /// Fields to extract snippets on.
    #[serde(default)]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "to_simple_list")]
    pub snippet_fields: Option<Vec<String>>,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    /// This timestamp is expressed in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    /// This timestamp is expressed in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
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
    pub format: BodyFormat,
    /// Specifies how documents are sorted.
    #[serde(alias = "sort_by_field")]
    #[serde(deserialize_with = "sort_by_mini_dsl")]
    #[serde(default)]
    #[serde(skip_serializing_if = "SortBy::is_empty")]
    #[param(value_type = String)]
    pub sort_by: SortBy,
}

pub fn search_request_from_api_request(
    index_id_patterns: Vec<String>,
    search_request: SearchRequestQueryString,
) -> Result<quickwit_proto::search::SearchRequest, SearchError> {
    // The query ast below may still contain user input query. The actual
    // parsing of the user query will happen in the root service, and might require
    // the user of the docmapper default fields (which we do not have at this point).
    let query_ast = query_ast_from_user_text(&search_request.query, search_request.search_fields);
    let query_ast_json = serde_json::to_string(&query_ast)?;
    let search_request = quickwit_proto::search::SearchRequest {
        index_id_patterns,
        query_ast: query_ast_json,
        snippet_fields: search_request.snippet_fields.unwrap_or_default(),
        start_timestamp: search_request.start_timestamp,
        end_timestamp: search_request.end_timestamp,
        max_hits: search_request.max_hits,
        start_offset: search_request.start_offset,
        aggregation_request: search_request
            .aggs
            .map(|agg| serde_json::to_string(&agg).expect("could not serialize JsonValue")),
        sort_fields: search_request.sort_by.sort_fields,
        scroll_ttl_secs: None,
    };
    Ok(search_request)
}

async fn search_endpoint(
    index_id_patterns: Vec<String>,
    search_request: SearchRequestQueryString,
    search_service: &dyn SearchService,
) -> Result<SearchResponseRest, SearchError> {
    let search_request = search_request_from_api_request(index_id_patterns, search_request)?;
    let search_response = search_service.root_search(search_request).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}

fn search_get_filter(
) -> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search")
        .and_then(extract_index_id_patterns)
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

fn search_post_filter(
) -> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search")
        .and_then(extract_index_id_patterns)
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::json())
}

async fn search(
    index_id_patterns: Vec<String>,
    search_request: SearchRequestQueryString,
    search_service: Arc<dyn SearchService>,
) -> impl warp::Reply {
    info!(request =? search_request, "search");
    let body_format = search_request.format;
    let result = search_endpoint(index_id_patterns, search_request, &*search_service).await;
    make_json_api_response(result, body_format)
}

#[utoipa::path(
    get,
    tag = "Search",
    path = "/{index_id}/search",
    responses(
        (status = 200, description = "Successfully executed search.", body = SearchResponseRest)
    ),
    params(
        SearchRequestQueryString,
        ("index_id" = String, Path, description = "The index ID to search."),
    )
)]
/// Search Index (GET Variant)
///
/// Parses the search request from the request query string.
pub fn search_get_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_get_filter()
        .and(with_arg(search_service))
        .then(search)
}

#[utoipa::path(
    post,
    tag = "Search",
    path = "/{index_id}/search",
    request_body = SearchRequestQueryString,
    responses(
        (status = 200, description = "Successfully executed search.", body = SearchResponseRest)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to search."),
    )
)]
/// Search Index (POST Variant)
///
/// REST POST search handler.
///
/// Parses the search request from the request body.
pub fn search_post_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_post_filter()
        .and(with_arg(search_service))
        .then(search)
}

#[utoipa::path(
    get,
    tag = "Search",
    path = "/{index_id}/search/stream",
    responses(
        (status = 200, description = "Successfully executed search.")
    ),
    params(
        SearchStreamRequestQueryString,
        ("index_id" = String, Path, description = "The index ID to search."),
    )
)]
/// Stream Search Index
pub fn search_stream_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_stream_filter()
        .and(with_arg(search_service))
        .then(search_stream)
}

/// This struct represents the search stream query passed to
/// the REST API.
#[derive(Deserialize, Debug, Eq, PartialEq, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
struct SearchStreamRequestQueryString {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    // Fields to search on.
    #[param(rename = "search_field")]
    #[serde(default)]
    #[serde(rename(deserialize = "search_field"))]
    #[serde(deserialize_with = "from_simple_list")]
    pub search_fields: Option<Vec<String>>,
    /// Fields to extract snippet on
    #[serde(default)]
    #[serde(rename(deserialize = "snippet_fields"))] // TODO: Was this supposed to be `snippet_field`? - CF
    #[serde(deserialize_with = "from_simple_list")]
    pub snippet_fields: Option<Vec<String>>,
    /// If set, restricts search to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restricts search to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
    /// The fast field to extract.
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub fast_field: String,
    /// The requested output format.
    #[serde(default)]
    pub output_format: OutputFormat,
    #[serde(default)]
    pub partition_by_field: Option<String>,
}

async fn search_stream_endpoint(
    index_id: String,
    search_request: SearchStreamRequestQueryString,
    search_service: &dyn SearchService,
) -> Result<hyper::Body, SearchError> {
    let query_ast = query_ast_from_user_text(&search_request.query, search_request.search_fields);
    let query_ast_json = serde_json::to_string(&query_ast)?;
    let request = quickwit_proto::search::SearchStreamRequest {
        index_id,
        query_ast: query_ast_json,
        snippet_fields: search_request.snippet_fields.unwrap_or_default(),
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
                    tracing::error!(error=?error, "Error when streaming search results.");
                    let header_value_str =
                        format!("Error when streaming search results: {error:?}.");
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

fn make_streaming_reply(result: Result<hyper::Body, SearchError>) -> impl Reply {
    let status_code: StatusCode;
    let body = match result {
        Ok(body) => {
            status_code = StatusCode::OK;
            warp::reply::Response::new(body)
        }
        Err(err) => {
            status_code = err.error_code().to_http_status_code();
            warp::reply::Response::new(hyper::Body::from(err.to_string()))
        }
    };
    reply::with_status(body, status_code)
}

async fn search_stream(
    index_id: String,
    request: SearchStreamRequestQueryString,
    search_service: Arc<dyn SearchService>,
) -> impl warp::Reply {
    info!(index_id=%index_id,request=?request, "search_stream");
    let content_type = match request.output_format {
        OutputFormat::ClickHouseRowBinary => "application/octet-stream",
        OutputFormat::Csv => "text/csv",
    };
    let reply =
        make_streaming_reply(search_stream_endpoint(index_id, request, &*search_service).await);
    reply::with_header(reply, CONTENT_TYPE, content_type)
}

fn search_stream_filter(
) -> impl Filter<Extract = (String, SearchStreamRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search" / "stream")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use bytes::Bytes;
    use mockall::predicate;
    use quickwit_search::{MockSearchService, SearchError};
    use serde_json::{json, Value as JsonValue};

    use super::*;
    use crate::recover_fn;

    fn search_handler(
        mock_search_service: MockSearchService,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
        let mock_search_service_in_arc = Arc::new(mock_search_service);
        search_get_handler(mock_search_service_in_arc.clone())
            .or(search_post_handler(mock_search_service_in_arc.clone()))
            .or(search_stream_handler(mock_search_service_in_arc))
            .recover(recover_fn)
    }

    #[tokio::test]
    async fn test_extract_index_id_patterns() {
        extract_index_id_patterns("my-index".to_string())
            .await
            .unwrap();
        assert_eq!(
            extract_index_id_patterns("my-index-1,my-index-2".to_string())
                .await
                .unwrap(),
            vec!["my-index-1".to_string(), "my-index-2".to_string()]
        );
        assert_eq!(
            extract_index_id_patterns("my-index-1%2Cmy-index-2".to_string())
                .await
                .unwrap(),
            vec!["my-index-1".to_string(), "my-index-2".to_string()]
        );
        extract_index_id_patterns("".to_string()).await.unwrap_err();
        extract_index_id_patterns(" ".to_string())
            .await
            .unwrap_err();
    }

    #[test]
    fn test_serialize_search_response() -> anyhow::Result<()> {
        let search_response = SearchResponseRest {
            num_hits: 55,
            hits: Vec::new(),
            snippets: None,
            elapsed_time_micros: 0u64,
            errors: Vec::new(),
            aggregations: None,
        };
        let search_response_json: JsonValue = serde_json::to_value(search_response)?;
        let expected_search_response_json: JsonValue = json!({
            "num_hits": 55,
            "hits": [],
            "elapsed_time_micros": 0,
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
        let (indexes, req) = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index/search?query=*&max_hits=10")
            .json(&true)
            .body(r#"{"query": "*", "max_hits":10, "aggs": {"range":[]} }"#)
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: None,
                start_timestamp: None,
                max_hits: 10,
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                aggs: Some(json!({"range":[]})),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_multi_indexes() {
        let rest_search_api_filter = search_post_filter();
        let (indexes, req) = warp::test::request()
            .method("POST")
            .path(
                "/quickwit-demo-index,quickwit-demo,quickwit-demo-index-*/search?query=*&\
                 max_hits=10",
            )
            .json(&true)
            .body(r#"{"query": "*", "max_hits":10, "aggs": {"range":[]} }"#)
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(
            indexes,
            vec![
                "quickwit-demo-index".to_string(),
                "quickwit-demo".to_string(),
                "quickwit-demo-index-*".to_string()
            ]
        );
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: None,
                start_timestamp: None,
                max_hits: 10,
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                aggs: Some(json!({"range":[]})),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_multi_indexes_bad_pattern() {
        let rest_search_api_filter = search_post_filter();
        let bad_pattern_rejection = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index**/search?query=*&max_hits=10")
            .json(&true)
            .body(r#"{"query": "*", "max_hits":10, "aggs": {"range":[]} }"#)
            .filter(&rest_search_api_filter)
            .await
            .unwrap_err();
        let rejection = bad_pattern_rejection
            .find::<crate::rest::InvalidArgument>()
            .unwrap();
        assert_eq!(
            rejection.0,
            "index ID pattern `quickwit-demo-index**` is invalid. patterns must not contain \
             multiple consecutive `*`"
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple() {
        let rest_search_api_filter = search_get_filter();
        let (indexes, req) = warp::test::request()
            .path(
                "/quickwit-demo-index/search?query=*&end_timestamp=1450720000&max_hits=10&\
                 start_offset=22",
            )
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: None,
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 10,
                start_offset: 22,
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_default_num_hits_default_offset() {
        let rest_search_api_filter = search_get_filter();
        let (indexes, req) = warp::test::request()
            .path(
                "/quickwit-demo-index/search?query=*&end_timestamp=1450720000&search_field=title,\
                 body",
            )
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                search_fields: Some(vec!["title".to_string(), "body".to_string()]),
                start_timestamp: None,
                end_timestamp: Some(1450720000),
                max_hits: 20,
                start_offset: 0,
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_format() {
        let rest_search_api_filter = search_get_filter();
        let (indexes, req) = warp::test::request()
            .path("/quickwit-demo-index/search?query=*&format=json")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 20,
                start_offset: 0,
                format: BodyFormat::Json,
                search_fields: None,
                sort_by: SortBy::default(),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_sort_by() {
        for (sort_by_query_param, expected_sort_fields) in [
            ("", Vec::new()),
            (",", Vec::new()),
            (
                "field1",
                vec![SortField {
                    field_name: "field1".to_string(),
                    sort_order: SortOrder::Desc as i32,
                }],
            ),
            (
                "+field1",
                vec![SortField {
                    field_name: "field1".to_string(),
                    sort_order: SortOrder::Desc as i32,
                }],
            ),
            (
                "-field1",
                vec![SortField {
                    field_name: "field1".to_string(),
                    sort_order: SortOrder::Asc as i32,
                }],
            ),
            (
                "_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Desc as i32,
                }],
            ),
            (
                "-_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Asc as i32,
                }],
            ),
            (
                "+_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Desc as i32,
                }],
            ),
            (
                "field1,field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Desc as i32,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Desc as i32,
                    },
                ],
            ),
            (
                "+field1,-field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Desc as i32,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Asc as i32,
                    },
                ],
            ),
            (
                "-field1,+field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Asc as i32,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Desc as i32,
                    },
                ],
            ),
        ] {
            let path = format!(
                "/quickwit-demo-index/search?query=*&format=json&sort_by={}",
                sort_by_query_param
            );
            let rest_search_api_filter = search_get_filter();
            let (_, req) = warp::test::request()
                .path(&path)
                .filter(&rest_search_api_filter)
                .await
                .unwrap();

            assert_eq!(
                &req.sort_by.sort_fields, &expected_sort_fields,
                "Expected sort fields `{:?}` for query param `{sort_by_query_param}`, got: {:?}",
                expected_sort_fields, req.sort_by.sort_fields
            );
        }

        let rest_search_api_filter = search_get_filter();
        let (_, req) = warp::test::request()
            .path("/quickwit-demo-index/search?query=*&format=json&sort_by_field=fiel1")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();

        assert_eq!(
            &req.sort_by.sort_fields,
            &[SortField {
                field_name: "fiel1".to_string(),
                sort_order: SortOrder::Desc as i32,
            }],
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_invalid_key() {
        let resp = warp::test::request()
            .path("/quickwit-demo-index/search?query=*&end_unix_timestamp=1450720000")
            .reply(&search_handler(MockSearchService::new()))
            .await;
        assert_eq!(resp.status(), 400);
        let resp_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        assert!(resp_json
            .get("message")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("unknown field `end_unix_timestamp`"));
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_with_invalid_payload() -> anyhow::Result<()> {
        let resp = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index/search?query=*&max_hits=10")
            .json(&true)
            .body(r#"{"query": "*", "bad_param":10, "aggs": {"range":[]} }"#)
            .reply(&search_handler(MockSearchService::new()))
            .await;
        assert_eq!(resp.status(), 400);
        let content = String::from_utf8_lossy(resp.body());
        assert!(content.contains("Request body deserialize error: unknown field `bad_param`"));
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_route_serialize_with_results() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Ok(quickwit_proto::search::SearchResponse {
                hits: Vec::new(),
                num_hits: 10,
                elapsed_time_micros: 16,
                errors: Vec::new(),
                ..Default::default()
            })
        });
        let rest_search_api_handler = search_handler(mock_search_service);
        let resp = warp::test::request()
            .path("/quickwit-demo-index/search?query=*")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "num_hits": 10,
            "hits": [],
            "elapsed_time_micros": 16,
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
                |search_request: &quickwit_proto::search::SearchRequest| {
                    search_request.start_offset == 5 && search_request.max_hits == 30
                },
            ))
            .returning(|_| Ok(Default::default()));
        let rest_search_api_handler = search_handler(mock_search_service);
        assert_eq!(
            warp::test::request()
                .path("/quickwit-demo-index/search?query=*&start_offset=5&max_hits=30")
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
            Err(SearchError::IndexesNotFound {
                index_id_patterns: vec!["not-found-index".to_string()],
            })
        });
        let rest_search_api_handler = search_handler(mock_search_service);
        assert_eq!(
            warp::test::request()
                .path("/index-does-not-exist/search?query=myfield:test")
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
            .returning(|_| Err(SearchError::Internal("ty".to_string())));
        let rest_search_api_handler = search_handler(mock_search_service);
        assert_eq!(
            warp::test::request()
                .path("/index-does-not-exist/search?query=myfield:test")
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
        let rest_search_api_handler = search_handler(mock_search_service);
        let response = warp::test::request()
            .path("/my-index/search?query=myfield:test")
            .reply(&rest_search_api_handler)
            .await;
        assert_eq!(response.status(), 400);
        let body = String::from_utf8_lossy(response.body());
        assert!(body.contains("invalid query"));
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_stream_api() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search_stream()
            .return_once(|_| {
                Ok(Box::pin(futures::stream::iter(vec![
                    Ok(Bytes::from("first row\n")),
                    Ok(Bytes::from("second row")),
                ])))
            });
        let rest_search_stream_api_handler = search_handler(mock_search_service);
        let response = warp::test::request()
            .path(
                "/my-index/search/stream?query=obama&search_field=body&fast_field=external_id&\
                 output_format=csv",
            )
            .reply(&rest_search_stream_api_handler)
            .await;
        assert_eq!(response.status(), 200);
        let body = String::from_utf8_lossy(response.body());
        assert_eq!(body, "first row\nsecond row");
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_csv() {
        let (index, req) = warp::test::request()
            .path("/my-index/search/stream?query=obama&fast_field=external_id&output_format=csv")
            .filter(&super::search_stream_filter())
            .await
            .unwrap();
        assert_eq!(&index, "my-index");
        assert_eq!(
            &req,
            &super::SearchStreamRequestQueryString {
                query: "obama".to_string(),
                search_fields: None,
                snippet_fields: None,
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
                "/my-index/search/stream?query=obama&fast_field=external_id&\
                 output_format=click_house_row_binary",
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
                snippet_fields: None,
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
                "/my-index/search/stream?query=obama&fast_field=external_id&\
                 output_format=ClickHouseRowBinary",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap_err();
        let parse_error = rejection.find::<serde_qs::Error>().unwrap();
        assert_eq!(
            parse_error.to_string(),
            "unknown variant `ClickHouseRowBinary`, expected `csv` or `click_house_row_binary`"
        );
    }

    #[tokio::test]
    async fn test_rest_search_stream_api_error_empty_fastfield() {
        let rejection = warp::test::request()
            .path(
                "/my-index/search/stream?query=obama&fast_field=&\
                 output_format=click_house_row_binary",
            )
            .filter(&super::search_stream_filter())
            .await
            .unwrap_err();
        let parse_error = rejection.find::<serde_qs::Error>().unwrap();
        assert_eq!(parse_error.to_string(), "expected a non-empty string field");
    }

    #[tokio::test]
    async fn test_rest_search_api_route_serialize_results_with_snippet() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Ok(quickwit_proto::search::SearchResponse {
                hits: vec![quickwit_proto::search::Hit {
                    json: r#"{"title": "foo", "body": "foo bar baz"}"#.to_string(),
                    partial_hit: None,
                    snippet: Some(r#"{"title": [], "body": ["foo <em>bar</em> baz"]}"#.to_string()),
                }],
                num_hits: 1,
                elapsed_time_micros: 16,
                errors: Vec::new(),
                ..Default::default()
            })
        });
        let rest_search_api_handler = search_handler(mock_search_service);
        let resp = warp::test::request()
            .path(
                "/quickwit-demo-index/search?query=bar&search_field=title,body&\
                 snippet_fields=title,body",
            )
            .reply(&rest_search_api_handler)
            .await;

        assert_eq!(resp.status(), 200);
        let resp_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "num_hits": 1,
            "hits": [{"title": "foo", "body": "foo bar baz"}],
            "snippets": [{"title": [], "body": ["foo <em>bar</em> baz"]}],
            "elapsed_time_micros": 16,
            "errors": [],
        });
        assert_json_eq!(resp_json, expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_multi_indexes() {
        {
            let mut mock_search_service = MockSearchService::new();
            mock_search_service
                .expect_root_search()
                .with(predicate::function(
                    |search_request: &quickwit_proto::search::SearchRequest| {
                        search_request.index_id_patterns
                            == vec!["quickwit-demo-*".to_string(), "quickwit-demo2".to_string()]
                    },
                ))
                .returning(|_| Ok(Default::default()));
            let rest_search_api_handler = search_handler(mock_search_service);
            assert_eq!(
                warp::test::request()
                    .path("/quickwit-demo-*,quickwit-demo2/search?query=*")
                    .reply(&rest_search_api_handler)
                    .await
                    .status(),
                200
            );
            assert_eq!(
                warp::test::request()
                    .path("/quickwit-demo-*%2Cquickwit-demo2/search?query=*")
                    .reply(&rest_search_api_handler)
                    .await
                    .status(),
                200
            );
        }
        {
            let mut mock_search_service = MockSearchService::new();
            mock_search_service
                .expect_root_search()
                .returning(|_| Ok(Default::default()));
            let rest_search_api_handler = search_handler(mock_search_service);
            assert_eq!(
                warp::test::request()
                    .path("/*/search?query=*")
                    .reply(&rest_search_api_handler)
                    .await
                    .status(),
                200
            );
            let response = warp::test::request()
                .path("/abc!/search?query=*")
                .reply(&rest_search_api_handler)
                .await;
            println!("{:?}", response.body());
            assert_eq!(
                warp::test::request()
                    .path("/abc!/search?query=*")
                    .reply(&rest_search_api_handler)
                    .await
                    .status(),
                400
            );
        }
    }
}
