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

use percent_encoding::percent_decode_str;
use quickwit_config::validate_index_id_pattern;
use quickwit_proto::search::{CountHits, SortField, SortOrder};
use quickwit_query::query_ast::query_ast_from_user_text;
use quickwit_search::{SearchError, SearchPlanResponseRest, SearchResponseRest, SearchService};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tracing::info;
use warp::{Filter, Rejection};

use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::{from_simple_list, to_simple_list};
use crate::{BodyFormat, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        search_get_handler,
        search_post_handler,
        search_plan_get_handler,
        search_plan_post_handler,
    ),
    components(schemas(
        BodyFormat,
        SearchRequestQueryString,
        SearchResponseRest,
        SearchPlanResponseRest,
        SortBy,
        SortField,
        SortOrder,
    ),)
)]
pub struct SearchApi;

pub(crate) async fn extract_index_id_patterns_default() -> Result<Vec<String>, Rejection> {
    let index_id_patterns = Vec::new();
    Ok(index_id_patterns)
}

pub(crate) async fn extract_index_id_patterns(
    comma_separated_index_id_patterns: String,
) -> Result<Vec<String>, Rejection> {
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
        ignore_missing_indexes: false,
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
                if (!allow_failed_splits || search_response.num_successful_splits == 0)
                    && let Some(search_error) =
                        SearchError::from_split_errors(&search_response.failed_splits[..])
                {
                    return Err(search_error);
                }
                Ok(search_response)
            })?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}

fn search_get_filter()
-> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search")
        .and_then(extract_index_id_patterns)
        .and(warp::get())
        .and(warp::query())
}

fn search_post_filter()
-> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search")
        .and_then(extract_index_id_patterns)
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::json())
}

fn search_plan_get_filter()
-> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search-plan")
        .and_then(extract_index_id_patterns)
        .and(warp::get())
        .and(warp::query())
}

fn search_plan_post_filter()
-> impl Filter<Extract = (Vec<String>, SearchRequestQueryString), Error = Rejection> + Clone {
    warp::path!(String / "search-plan")
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
    into_rest_api_response(result, body_format)
}

async fn search_plan(
    index_id_patterns: Vec<String>,
    search_request: SearchRequestQueryString,
    search_service: Arc<dyn SearchService>,
) -> impl warp::Reply {
    let body_format = search_request.format;
    let result: Result<SearchPlanResponseRest, SearchError> = async {
        let plan_request = search_request_from_api_request(index_id_patterns, search_request)?;
        let plan_response = search_service.search_plan(plan_request).await?;
        let response = serde_json::from_str(&plan_response.result)?;
        Ok(response)
    }
    .await;
    into_rest_api_response(result, body_format)
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
    path = "/{index_id}/search-plan",
    responses(
        (status = 200, description = "Metadata about how a request would be executed.", body = SearchPlanResponseRest)
    ),
    params(
        SearchRequestQueryString,
        ("index_id" = String, Path, description = "The index ID to search."),
    )
)]
/// Plan Query (GET Variant)
///
/// Parses the search request from the request query string.
pub fn search_plan_get_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_plan_get_filter()
        .and(with_arg(search_service))
        .then(search_plan)
}

#[utoipa::path(
    post,
    tag = "Search",
    path = "/{index_id}/search-plan",
    request_body = SearchRequestQueryString,
    responses(
        (status = 200, description = "Metadata about how a request would be executed.", body = SearchPlanResponseRest)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to search."),
    )
)]
/// Plan Query (POST Variant)
///
/// Parses the search request from the request body.
pub fn search_plan_post_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_plan_post_filter()
        .and(with_arg(search_service))
        .then(search_plan)
}

#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use mockall::predicate;
    use quickwit_search::{MockSearchService, SearchError};
    use serde_json::{Value as JsonValue, json};

    use super::*;
    use crate::recover_fn;

    fn search_handler(
        mock_search_service: MockSearchService,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
        let mock_search_service_in_arc = Arc::new(mock_search_service);
        search_get_handler(mock_search_service_in_arc.clone())
            .or(search_post_handler(mock_search_service_in_arc.clone()))
            .or(search_plan_get_handler(mock_search_service_in_arc.clone()))
            .or(search_plan_post_handler(mock_search_service_in_arc.clone()))
            .recover(recover_fn)
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
        let rest_search_api_filter = search_post_filter();
        let (indexes, req) = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index/search")
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
                count_all: CountHits::CountAll,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_multi_indexes() {
        let rest_search_api_filter = search_post_filter();
        let (indexes, req) = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index,quickwit-demo,quickwit-demo-index-*/search")
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
            .path("/quickwit-demo-index**/search")
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
            "index ID pattern `quickwit-demo-index**` is invalid: patterns must not contain \
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
    async fn test_rest_search_api_route_count_all() {
        let rest_search_api_filter = search_get_filter();
        let (indexes, req) = warp::test::request()
            .path("/quickwit-demo-index/search?query=*&count_all=true")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                max_hits: 20,
                count_all: CountHits::CountAll,
                ..Default::default()
            }
        );
        let rest_search_api_filter = search_get_filter();
        let (indexes, req) = warp::test::request()
            .path("/quickwit-demo-index/search?query=*&count_all=false")
            .filter(&rest_search_api_filter)
            .await
            .unwrap();
        assert_eq!(indexes, vec!["quickwit-demo-index".to_string()]);
        assert_eq!(
            &req,
            &super::SearchRequestQueryString {
                query: "*".to_string(),
                format: BodyFormat::default(),
                sort_by: SortBy::default(),
                max_hits: 20,
                count_all: CountHits::Underestimate,
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
                    sort_datetime_format: None,
                }],
            ),
            (
                "+field1",
                vec![SortField {
                    field_name: "field1".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: None,
                }],
            ),
            (
                "-field1",
                vec![SortField {
                    field_name: "field1".to_string(),
                    sort_order: SortOrder::Asc as i32,
                    sort_datetime_format: None,
                }],
            ),
            (
                "_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: None,
                }],
            ),
            (
                "-_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Asc as i32,
                    sort_datetime_format: None,
                }],
            ),
            (
                "+_score",
                vec![SortField {
                    field_name: "_score".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: None,
                }],
            ),
            (
                "field1,field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Desc as i32,
                        sort_datetime_format: None,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Desc as i32,
                        sort_datetime_format: None,
                    },
                ],
            ),
            (
                "+field1,-field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Desc as i32,
                        sort_datetime_format: None,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Asc as i32,
                        sort_datetime_format: None,
                    },
                ],
            ),
            (
                "-field1,+field2",
                vec![
                    SortField {
                        field_name: "field1".to_string(),
                        sort_order: SortOrder::Asc as i32,
                        sort_datetime_format: None,
                    },
                    SortField {
                        field_name: "field2".to_string(),
                        sort_order: SortOrder::Desc as i32,
                        sort_datetime_format: None,
                    },
                ],
            ),
        ] {
            let path = format!(
                "/quickwit-demo-index/search?query=*&format=json&sort_by={sort_by_query_param}"
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
                sort_datetime_format: None,
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
        assert!(
            resp_json
                .get("message")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("Invalid query string")
        );
    }

    #[tokio::test]
    async fn test_rest_search_api_route_post_with_invalid_payload() -> anyhow::Result<()> {
        let resp = warp::test::request()
            .method("POST")
            .path("/quickwit-demo-index/search")
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
                index_ids: vec!["not-found-index".to_string()],
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
            assert_eq!(response.status(), 400);
        }
    }
}
