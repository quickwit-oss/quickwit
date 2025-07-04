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

use std::convert::TryFrom;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Path, Query};
use axum::http::StatusCode as AxumStatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::{Extension, Router};
use futures::stream::StreamExt;
use percent_encoding::percent_decode_str;
use quickwit_config::validate_index_id_pattern;
use quickwit_proto::search::{CountHits, OutputFormat, SortField, SortOrder};
use quickwit_proto::types::IndexId;
use quickwit_query::query_ast::query_ast_from_user_text;
use quickwit_search::{SearchError, SearchPlanResponseRest, SearchResponseRest, SearchService};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value as JsonValue;

use crate::BodyFormat;
use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::{from_simple_list, to_simple_list};

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        search_get,
        search_post,
        search_plan_get,
        search_plan_post,
        search_stream,
    ),
    components(schemas(
        BodyFormat,
        OutputFormat,
        SearchRequestQueryString,
        SearchResponseRest,
        SearchPlanResponseRest,
        SortBy,
        SortField,
        SortOrder,
    ),)
)]
pub struct SearchApi;

pub(crate) async fn extract_index_id_patterns(
    comma_separated_index_id_patterns: String,
) -> Result<Vec<String>, crate::rest::InvalidArgument> {
    let percent_decoded_comma_separated_index_id_patterns =
        percent_decode_str(&comma_separated_index_id_patterns)
            .decode_utf8()
            .map_err(|error| {
                let message = format!(
                    "failed to percent decode comma-separated index ID patterns \
                     `{comma_separated_index_id_patterns}`: {error}"
                );
                crate::rest::InvalidArgument(message)
            })?;
    let mut index_id_patterns = Vec::new();

    for index_id_pattern in percent_decoded_comma_separated_index_id_patterns.split(',') {
        validate_index_id_pattern(index_id_pattern, true)
            .map_err(|error| crate::rest::InvalidArgument(error.to_string()))?;
        index_id_patterns.push(index_id_pattern.to_string());
    }
    assert!(!index_id_patterns.is_empty());
    Ok(index_id_patterns)
}

/// Custom axum extractor for index ID patterns from path parameter
///
/// This extracts the index path parameter and validates index ID patterns.
/// It reuses the existing extract_index_id_patterns function.
///
/// Usage: `IndexPatterns(patterns): IndexPatterns` in handler parameters
#[derive(Debug, Clone)]
pub struct IndexPatterns(pub Vec<String>);

#[axum::async_trait]
impl<S> FromRequestParts<S> for IndexPatterns
where S: Send + Sync
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Extract the index path parameter
        let Path(index): Path<String> =
            Path::from_request_parts(parts, state).await.map_err(|_| {
                (
                    AxumStatusCode::BAD_REQUEST,
                    "Missing index parameter in path",
                )
                    .into_response()
            })?;

        // Use the existing extract_index_id_patterns function
        let index_id_patterns = extract_index_id_patterns(index).await.map_err(|_| {
            (AxumStatusCode::BAD_REQUEST, "Invalid index ID pattern").into_response()
        })?;

        Ok(IndexPatterns(index_id_patterns))
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Deserialize, utoipa::ToSchema)]
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
                sort_datetime_format: None,
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
    Debug,
    Default,
    Eq,
    PartialEq,
    Clone,
    Serialize,
    Deserialize,
    utoipa::IntoParams,
    utoipa::ToSchema,
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
    #[param(value_type = bool)]
    #[schema(value_type = bool)]
    #[serde(with = "count_hits_from_bool")]
    #[serde(default = "count_hits_from_bool::default")]
    pub count_all: CountHits,
    #[param(value_type = bool)]
    #[schema(value_type = bool)]
    #[serde(default)]
    pub allow_failed_splits: bool,
}

mod count_hits_from_bool {
    use quickwit_proto::search::CountHits;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(count_hits: &CountHits, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        if count_hits == &CountHits::Underestimate {
            serializer.serialize_bool(false)
        } else {
            serializer.serialize_none()
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CountHits, D::Error>
    where D: Deserializer<'de> {
        let count_all = Option::<bool>::deserialize(deserializer)?.unwrap_or(true);
        Ok(if count_all {
            CountHits::CountAll
        } else {
            CountHits::Underestimate
        })
    }

    pub fn default() -> CountHits {
        CountHits::CountAll
    }
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
        search_after: None,
        count_hits: search_request.count_all.into(),
    };
    Ok(search_request)
}

async fn search_endpoint(
    index_id_patterns: Vec<String>,
    search_request: SearchRequestQueryString,
    search_service: &dyn SearchService,
) -> Result<SearchResponseRest, SearchError> {
    let allow_failed_splits = search_request.allow_failed_splits;
    let search_request = search_request_from_api_request(index_id_patterns, search_request)?;
    let search_response =
        search_service
            .root_search(search_request)
            .await
            .and_then(|search_response| {
                if !allow_failed_splits || search_response.num_successful_splits == 0 {
                    if let Some(search_error) =
                        SearchError::from_split_errors(&search_response.failed_splits[..])
                    {
                        return Err(search_error);
                    }
                }
                Ok(search_response)
            })?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}

/// This struct represents the search stream query passed to
/// the REST API.
#[derive(Deserialize, Debug, Clone, Eq, PartialEq, utoipa::IntoParams)]
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
    index_id: IndexId,
    search_request: SearchStreamRequestQueryString,
    search_service: &dyn SearchService,
) -> Result<warp::hyper::Body, SearchError> {
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
    let (mut sender, body) = warp::hyper::Body::channel();
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
                    tracing::error!(error=?error, "error when streaming search results");
                    let header_value_str =
                        format!("Error when streaming search results: {error:?}.");
                    let header_value =
                        warp::hyper::header::HeaderValue::from_str(header_value_str.as_str())
                            .unwrap_or_else(|_| {
                                warp::hyper::header::HeaderValue::from_static("Search stream error")
                            });
                    let mut trailers = warp::hyper::HeaderMap::new();
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

/// Creates routes for Search API endpoints
pub fn search_routes(search_service: Arc<dyn SearchService>) -> Router {
    Router::new()
        .route("/:indexes/search", get(search_get).post(search_post))
        .route(
            "/:indexes/search/plan",
            get(search_plan_get).post(search_plan_post),
        )
        .route("/:index/search/stream", get(search_stream))
        .layer(Extension(search_service))
}

#[utoipa::path(
    get,
    tag = "Search",
    path = "/{indexes}/search",
    responses(
        (status = 200, description = "Successfully searched the index.", body = SearchResponseRest)
    ),
    params(
        ("indexes" = String, Path, description = "Comma-separated list of index IDs to search"),
        SearchRequestQueryString
    )
)]
async fn search_get(
    IndexPatterns(index_id_patterns): IndexPatterns,
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    Query(search_request): Query<SearchRequestQueryString>,
) -> impl IntoResponse {
    let result = search_endpoint(
        index_id_patterns,
        search_request.clone(),
        search_service.as_ref(),
    )
    .await;
    into_rest_api_response(result, search_request.format)
}

#[utoipa::path(
    post,
    tag = "Search",
    path = "/{indexes}/search",
    request_body = SearchRequestQueryString,
    responses(
        (status = 200, description = "Successfully searched the index.", body = SearchResponseRest)
    ),
    params(
        ("indexes" = String, Path, description = "Comma-separated list of index IDs to search")
    )
)]
async fn search_post(
    IndexPatterns(index_id_patterns): IndexPatterns,
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    Json(search_request): Json<SearchRequestQueryString>,
) -> impl IntoResponse {
    let result = search_endpoint(
        index_id_patterns,
        search_request.clone(),
        search_service.as_ref(),
    )
    .await;
    into_rest_api_response(result, search_request.format)
}

/// Axum handler for GET /{indexes}/search/plan
#[utoipa::path(
    get,
    tag = "Search",
    path = "/{indexes}/search/plan",
    responses(
        (status = 200, description = "Successfully planned the search query.", body = SearchPlanResponseRest)
    ),
    params(
        ("indexes" = String, Path, description = "Comma-separated list of index IDs to search"),
        SearchRequestQueryString
    )
)]
async fn search_plan_get(
    IndexPatterns(index_id_patterns): IndexPatterns,
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    Query(search_request): Query<SearchRequestQueryString>,
) -> impl IntoResponse {
    let search_request_proto =
        match search_request_from_api_request(index_id_patterns, search_request.clone()) {
            Ok(req) => req,
            Err(err) => {
                return into_rest_api_response::<SearchPlanResponseRest, SearchError>(
                    Err(err),
                    search_request.format,
                );
            }
        };

    let result: Result<SearchPlanResponseRest, SearchError> = async {
        let plan_response = search_service.search_plan(search_request_proto).await?;
        let response = serde_json::from_str(&plan_response.result)?;
        Ok(response)
    }
    .await;

    into_rest_api_response::<SearchPlanResponseRest, SearchError>(result, search_request.format)
}

#[utoipa::path(
    post,
    tag = "Search",
    path = "/{indexes}/search/plan",
    request_body = SearchRequestQueryString,
    responses(
        (status = 200, description = "Successfully planned the search query.", body = SearchPlanResponseRest)
    ),
    params(
        ("indexes" = String, Path, description = "Comma-separated list of index IDs to search")
    )
)]
async fn search_plan_post(
    IndexPatterns(index_id_patterns): IndexPatterns,
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    Json(search_request): Json<SearchRequestQueryString>,
) -> impl IntoResponse {
    let search_request_proto =
        match search_request_from_api_request(index_id_patterns, search_request.clone()) {
            Ok(req) => req,
            Err(err) => {
                return into_rest_api_response::<SearchPlanResponseRest, SearchError>(
                    Err(err),
                    search_request.format,
                );
            }
        };

    let result: Result<SearchPlanResponseRest, SearchError> = async {
        let plan_response = search_service.search_plan(search_request_proto).await?;
        let response = serde_json::from_str(&plan_response.result)?;
        Ok(response)
    }
    .await;

    into_rest_api_response::<SearchPlanResponseRest, SearchError>(result, search_request.format)
}

/// Axum handler for GET /{index}/search/stream
#[utoipa::path(
    get,
    tag = "Search",
    path = "/{index}/search/stream",
    responses(
        (status = 200, description = "Successfully started streaming search results.")
    ),
    params(
        ("index" = String, Path, description = "Index ID to search"),
        SearchStreamRequestQueryString
    )
)]
async fn search_stream(
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    Path(index): Path<String>,
    Query(request): Query<SearchStreamRequestQueryString>,
) -> impl IntoResponse {
    match search_stream_endpoint(index, request, search_service.as_ref()).await {
        Ok(body) => {
            // Convert warp::hyper::Body to axum::body::Body
            axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("content-type", "text/plain")
                .body(axum::body::Body::from_stream(body))
                .unwrap()
        }
        Err(search_error) => {
            let status_code = match search_error {
                SearchError::IndexesNotFound { .. } => axum::http::StatusCode::NOT_FOUND,
                SearchError::InvalidQuery(_) => axum::http::StatusCode::BAD_REQUEST,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };

            axum::response::Response::builder()
                .status(status_code)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&search_error).unwrap_or_else(|_| "{}".to_string()),
                ))
                .unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use axum_test::TestServer;
    use bytes::Bytes;
    use http::StatusCode;
    use mockall::predicate;
    use quickwit_search::{MockSearchService, SearchError};
    use serde_json::{Value as JsonValue, json};

    use super::*;
    // Unused imports removed

    // Deprecated warp search handler removed - all tests now use axum

    fn create_search_test_server(mock_search_service: MockSearchService) -> TestServer {
        let mock_search_service_in_arc = Arc::new(mock_search_service);
        let app = search_routes(mock_search_service_in_arc);
        TestServer::new(app).unwrap()
    }

    #[tokio::test]
    async fn test_extract_index_id_patterns() {
        extract_index_id_patterns("my-index".to_string())
            .await
            .unwrap();
        assert_eq!(
            extract_index_id_patterns("my-index-1,my-index-2%2A".to_string())
                .await
                .unwrap(),
            vec!["my-index-1".to_string(), "my-index-2*".to_string()]
        );
        assert_eq!(
            extract_index_id_patterns("my-index-1%2Cmy-index-%2A".to_string())
                .await
                .unwrap(),
            vec!["my-index-1".to_string(), "my-index-*".to_string()]
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
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .return_once(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);
        let response = server
            .post("/quickwit-demo-index/search")
            .json(&serde_json::json!({
                "query": "*",
                "max_hits": 10,
                "aggs": {"range": []}
            }))
            .await;

        // Should succeed with the mock
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_multi_indexes() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .return_once(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);
        let response = server
            .post("/quickwit-demo-index,quickwit-demo,quickwit-demo-index-*/search")
            .json(&serde_json::json!({
                "query": "*",
                "max_hits": 10,
                "aggs": {"range": []}
            }))
            .await;

        // Should succeed with the mock
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_multi_indexes_bad_pattern() {
        let server = create_search_test_server(MockSearchService::new());
        let response = server
            .post("/quickwit-demo-index**/search")
            .json(&serde_json::json!({
                "query": "*",
                "max_hits": 10,
                "aggs": {"range": []}
            }))
            .await;

        // Should return BAD_REQUEST for invalid index pattern
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body = response.text();

        // Check for the actual error message format returned by axum
        assert!(body.contains("Invalid index ID pattern"));
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .return_once(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);
        let response = server
            .get(
                "/quickwit-demo-index/search?query=*&end_timestamp=1450720000&max_hits=10&\
                 start_offset=22",
            )
            .await;

        // Should succeed with the mock
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_count_all() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .times(2)
            .returning(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);

        // Test count_all=true
        let response = server
            .get("/quickwit-demo-index/search?query=*&count_all=true")
            .await;
        assert_eq!(response.status_code(), StatusCode::OK);

        // Test count_all=false
        let response = server
            .get("/quickwit-demo-index/search?query=*&count_all=false")
            .await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_default_num_hits_default_offset() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .return_once(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);
        let response = server
            .get(
                "/quickwit-demo-index/search?query=*&end_timestamp=1450720000&search_field=title,\
                 body",
            )
            .await;

        // Should succeed with the mock
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_simple_format() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .return_once(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);
        let response = server
            .get("/quickwit-demo-index/search?query=*&format=json")
            .await;

        // Should succeed with the mock
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_sort_by() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .times(7) // 6 test cases + 1 sort_by_field alias test
            .returning(|_| Ok(Default::default()));

        let server = create_search_test_server(mock_search_service);

        // Test various sort_by parameters
        let test_cases = vec![
            "field1",
            "+field1",
            "-field1",
            "field1,field2",
            "+field1,-field2",
            "-field1,+field2",
        ];

        for sort_by_query_param in test_cases {
            let path = format!(
                "/quickwit-demo-index/search?query=*&format=json&sort_by={}",
                sort_by_query_param
            );
            let response = server.get(&path).await;

            // Should succeed with the mock
            assert_eq!(response.status_code(), StatusCode::OK);
        }

        // Test sort_by_field alias
        let response = server
            .get("/quickwit-demo-index/search?query=*&format=json&sort_by_field=field1")
            .await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rest_search_api_route_invalid_key() {
        let server = create_search_test_server(MockSearchService::new());
        let resp = server
            .get("/quickwit-demo-index/search?query=*&end_unix_timestamp=1450720000")
            .await;
        assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
        let resp_text = resp.text();
        assert!(resp_text.contains("unknown field `end_unix_timestamp`"));
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_with_invalid_payload() -> anyhow::Result<()> {
        let server = create_search_test_server(MockSearchService::new());
        let resp = server
            .post("/quickwit-demo-index/search")
            .json(&serde_json::json!({"query": "*", "bad_param":10, "aggs": {"range":[]}}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
        let content = resp.text();
        assert!(content.contains("unknown field `bad_param`"));
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
        let server = create_search_test_server(mock_search_service);
        let resp = server.get("/quickwit-demo-index/search?query=*").await;
        assert_eq!(resp.status_code(), StatusCode::OK);
        let resp_json: JsonValue = resp.json();
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
        let server = create_search_test_server(mock_search_service);
        let resp = server
            .get("/quickwit-demo-index/search?query=*&start_offset=5&max_hits=30")
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_with_index_does_not_exist() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Err(SearchError::IndexesNotFound {
                index_ids: vec!["not-found-index".to_string()],
            })
        });
        let server = create_search_test_server(mock_search_service);
        let resp = server
            .get("/index-does-not-exist/search?query=myfield:test")
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_with_wrong_fieldname() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|_| Err(SearchError::Internal("ty".to_string())));
        let server = create_search_test_server(mock_search_service);
        let resp = server
            .get("/index-does-not-exist/search?query=myfield:test")
            .await;
        assert_eq!(resp.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_search_api_with_invalid_query() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|_| Err(SearchError::InvalidQuery("invalid query".to_string())));
        let server = create_search_test_server(mock_search_service);
        let response = server.get("/my-index/search?query=myfield:test").await;
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body = response.text();
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
        let server = create_search_test_server(mock_search_service);
        let response = server
            .get(
                "/my-index/search/stream?query=obama&search_field=body&fast_field=external_id&\
                 output_format=csv",
            )
            .await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let body = response.text();
        assert_eq!(body, "first row\nsecond row");
    }

    // #[tokio::test]
    // async fn test_rest_search_stream_api_csv() {
    //     let (index, req) = warp::test::request()
    //         .path("/my-index/search/stream?query=obama&fast_field=external_id&output_format=csv")
    //         .filter(&super::search_stream_filter())
    //         .await
    //         .unwrap();
    //     assert_eq!(&index, "my-index");
    //     assert_eq!(
    //         &req,
    //         &super::SearchStreamRequestQueryString {
    //             query: "obama".to_string(),
    //             search_fields: None,
    //             snippet_fields: None,
    //             start_timestamp: None,
    //             end_timestamp: None,
    //             fast_field: "external_id".to_string(),
    //             output_format: OutputFormat::Csv,
    //             partition_by_field: None,
    //         }
    //     );
    // }

    // #[tokio::test]
    // async fn test_rest_search_stream_api_click_house_row_binary() {
    //     let (index, req) = warp::test::request()
    //         .path(
    //             "/my-index/search/stream?query=obama&fast_field=external_id&\
    //              output_format=click_house_row_binary",
    //         )
    //         .filter(&super::search_stream_filter())
    //         .await
    //         .unwrap();
    //     assert_eq!(&index, "my-index");
    //     assert_eq!(
    //         &req,
    //         &super::SearchStreamRequestQueryString {
    //             query: "obama".to_string(),
    //             search_fields: None,
    //             snippet_fields: None,
    //             start_timestamp: None,
    //             end_timestamp: None,
    //             fast_field: "external_id".to_string(),
    //             output_format: OutputFormat::ClickHouseRowBinary,
    //             partition_by_field: None,
    //         }
    //     );
    // }

    // #[tokio::test]
    // async fn test_rest_search_stream_api_error() {
    //     let rejection = warp::test::request()
    //         .path(
    //             "/my-index/search/stream?query=obama&fast_field=external_id&\
    //              output_format=ClickHouseRowBinary",
    //         )
    //         .filter(&super::search_stream_filter())
    //         .await
    //         .unwrap_err();
    //     let parse_error = rejection.find::<serde_qs::Error>().unwrap();
    //     assert_eq!(
    //         parse_error.to_string(),
    //         "unknown variant `ClickHouseRowBinary`, expected `csv` or `click_house_row_binary`"
    //     );
    // }

    // #[tokio::test]
    // async fn test_rest_search_stream_api_error_empty_fastfield() {
    //     let rejection = warp::test::request()
    //         .path(
    //             "/my-index/search/stream?query=obama&fast_field=&\
    //              output_format=click_house_row_binary",
    //         )
    //         .filter(&super::search_stream_filter())
    //         .await
    //         .unwrap_err();
    //     let parse_error = rejection.find::<serde_qs::Error>().unwrap();
    //     assert_eq!(parse_error.to_string(), "expected a non-empty string field");
    // }

    #[tokio::test]
    async fn test_rest_search_api_route_serialize_results_with_snippet() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_root_search().returning(|_| {
            Ok(quickwit_proto::search::SearchResponse {
                hits: vec![quickwit_proto::search::Hit {
                    json: r#"{"title": "foo", "body": "foo bar baz"}"#.to_string(),
                    partial_hit: None,
                    snippet: Some(r#"{"title": [], "body": ["foo <em>bar</em> baz"]}"#.to_string()),
                    index_id: "quickwit-demo-index".to_string(),
                }],
                num_hits: 1,
                elapsed_time_micros: 16,
                errors: Vec::new(),
                ..Default::default()
            })
        });
        let server = create_search_test_server(mock_search_service);
        let resp = server
            .get(
                "/quickwit-demo-index/search?query=bar&search_field=title,body&\
                 snippet_fields=title,body",
            )
            .await;

        assert_eq!(resp.status_code(), StatusCode::OK);
        let resp_json: JsonValue = resp.json();
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
            let server = create_search_test_server(mock_search_service);
            let resp = server
                .get("/quickwit-demo-*,quickwit-demo2/search?query=*")
                .await;
            assert_eq!(resp.status_code(), StatusCode::OK);

            let resp = server
                .get("/quickwit-demo-*%2Cquickwit-demo2/search?query=*")
                .await;
            assert_eq!(resp.status_code(), StatusCode::OK);
        }
        {
            let mut mock_search_service = MockSearchService::new();
            mock_search_service
                .expect_root_search()
                .returning(|_| Ok(Default::default()));
            let server = create_search_test_server(mock_search_service);
            let resp = server.get("/*/search?query=*").await;
            assert_eq!(resp.status_code(), StatusCode::OK);

            let response = server.get("/abc!/search?query=*").await;
            assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        }
    }
}
