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

use std::sync::Arc;
use std::time::Instant;

use elasticsearch_dsl::search::{Hit as ElasticHit, SearchResponse as ElasticSearchResponse};
use elasticsearch_dsl::{HitsMetadata, Source, TotalHits, TotalHitsRelation};
use futures::StreamExt;
use hyper::header::CONTENT_TYPE;
use hyper::StatusCode;
use quickwit_proto::{SearchResponse, ServiceErrorCode, SortOrder};
use quickwit_query::query_ast::{QueryAst, UserInputQuery};
use quickwit_query::DefaultOperator;
use quickwit_search::{SearchError, SearchService};
use serde_json::json;
use tracing::info;
use warp::reply::{WithHeader, WithStatus};
use warp::{reply, Filter, Rejection};

use super::filter::elastic_multi_search_filter;
use super::model::{ElasticSearchError, MultiSearchQueryParams, SearchBody, SearchQueryParams};
use crate::elastic_search_api::filter::elastic_index_search_filter;
use crate::elastic_search_api::model::{RequestBody, RequestHeader};
use crate::format::{ApiError, BodyFormat};
use crate::with_arg;

/// GET or POST _elastic/_search
pub fn es_compat_search_handler(
    _search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    super::filter::elastic_search_filter().then(|_params: SearchQueryParams| async move {
        // TODO
        BodyFormat::Json.make_reply_for_err(ApiError {
            code: ServiceErrorCode::NotSupportedYet,
            message: "_elastic/_search is not supported yet. Please try the index search endpoint \
                      (_elastic/{index}/search)"
                .to_string(),
        })
    })
}

/// GET or POST _elastic/{index}/_search
pub fn es_compat_index_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_search_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_search)
        .map(make_rest_reply)
}

fn build_request_for_es_api(
    index_id: String,
    search_params: SearchQueryParams,
    search_body: SearchBody,
) -> Result<quickwit_proto::SearchRequest, ElasticSearchError> {
    let default_operator = search_params
        .default_operator
        .unwrap_or(DefaultOperator::Or);
    // The query string, if present, takes priority over what can be in the request
    // body.
    let query_ast = if let Some(q) = &search_params.q {
        let user_text_query = UserInputQuery {
            user_text: q.to_string(),
            default_fields: None,
            default_operator,
        };
        user_text_query.into()
    } else if let Some(query_dsl) = search_body.query {
        query_dsl
            .try_into()
            .map_err(|err: anyhow::Error| ElasticSearchError::new(400, err))?
    } else {
        QueryAst::MatchAll
    };
    let aggregation_request: Option<String> = if search_body.aggs.is_empty() {
        None
    } else {
        serde_json::to_string(&search_body.aggs).ok()
    };

    let max_hits = search_params.size.or(search_body.size).unwrap_or(10);
    let start_offset = search_params.from.or(search_body.from).unwrap_or(0);

    let sort_fields: Vec<(String, Option<SortOrder>)> = search_params
        .sort_fields()?
        .or_else(|| {
            search_body.sort.map(|sort_orders| {
                sort_orders
                    .into_iter()
                    .map(|sort_order| (sort_order.field, sort_order.value.order))
                    .collect()
            })
        })
        .unwrap_or_default();

    if sort_fields.len() >= 2 {
        return Err(ElasticSearchError::from(SearchError::InvalidArgument(
            format!(
                "Only one search field is supported at the moment. Got {:?}",
                sort_fields
            ),
        )));
    }

    let (sort_by_field, sort_order) =
        if let Some((field_name, sort_order_opt)) = sort_fields.into_iter().next() {
            (Some(field_name), sort_order_opt)
        } else {
            (None, None)
        };

    Ok(quickwit_proto::SearchRequest {
        index_id,
        query_ast: serde_json::to_string(&query_ast).expect("Failed to serialize QueryAst"),
        max_hits,
        start_offset,
        aggregation_request,
        sort_by_field,
        sort_order: sort_order.map(|order| order as i32),
        ..Default::default()
    })
}

async fn es_compat_index_search(
    index_id: String,
    search_params: SearchQueryParams,
    search_body: SearchBody,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticSearchResponse, ElasticSearchError> {
    let start_instant = Instant::now();
    let search_request = build_request_for_es_api(index_id, search_params, search_body)?;
    let search_response: SearchResponse = search_service.root_search(search_request).await?;
    let elapsed = start_instant.elapsed();
    let mut search_response_rest: ElasticSearchResponse =
        convert_to_es_search_response(search_response);
    search_response_rest.took = elapsed.as_millis() as u32;
    Ok(search_response_rest)
}

fn convert_hit(hit: quickwit_proto::Hit) -> ElasticHit {
    let fields: elasticsearch_dsl::Map<String, serde_json::Value> =
        serde_json::from_str(&hit.json).unwrap_or_default();
    ElasticHit {
        fields,
        explanation: None,
        index: "".to_string(),
        id: "".to_string(),
        score: None,
        nested: None,
        source: Source::from_string(hit.json)
            .unwrap_or_else(|_| Source::from_string("{}".to_string()).unwrap()),
        highlight: Default::default(),
        inner_hits: Default::default(),
        matched_queries: Vec::default(),
        sort: Vec::default(),
    }
}

fn convert_to_es_search_response(resp: SearchResponse) -> ElasticSearchResponse {
    let hits: Vec<ElasticHit> = resp.hits.into_iter().map(convert_hit).collect();
    let aggregations: Option<serde_json::Value> = if let Some(aggregation_json) = resp.aggregation {
        serde_json::from_str(&aggregation_json).ok()
    } else {
        None
    };
    ElasticSearchResponse {
        timed_out: false,
        hits: HitsMetadata {
            total: Some(TotalHits {
                value: resp.num_hits,
                relation: TotalHitsRelation::Equal,
            }),
            max_score: None,
            hits,
        },
        aggregations,
        ..Default::default()
    }
}

/// POST _elastic/_msearch
pub fn es_compat_index_multi_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_multi_search_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_multi_search)
        .map(make_rest_reply)
}

async fn es_compat_index_multi_search(
    payload: String,
    multi_search_params: MultiSearchQueryParams,
    search_service: Arc<dyn SearchService>,
) -> Result<serde_json::Value, ElasticSearchError> {
    let mut search_requests = Vec::new();
    let mut payload_lines = lines(&payload);

    while let Some(json_str) = payload_lines.next() {
        let request_header = serde_json::from_str::<RequestHeader>(json_str)
            .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
        let index_ids = request_header
            .index
            .ok_or_else(|| SearchError::InvalidQuery("Index must be present".to_string()))?;
        if index_ids.len() != 1 {
            return Err(ElasticSearchError::from(SearchError::InvalidQuery(
                "Search on only one index is supported".to_string(),
            )));
        }
        let index_id = index_ids.into_iter().next().unwrap();
        let request_body = payload_lines
            .next()
            .ok_or_else(|| {
                SearchError::InvalidQuery("Expect request body after request header".to_string())
            })
            .and_then(|source| {
                serde_json::from_str::<RequestBody>(source)
                    .map_err(|err| SearchError::InvalidQuery(err.to_string()))
            })?;
        let search_query_params = SearchQueryParams {
            allow_no_indices: request_header.allow_no_indices,
            expand_wildcards: request_header.expand_wildcards,
            ignore_unavailable: request_header.ignore_unavailable,
            routing: request_header.routing,
            // FIXME: add sort
            ..Default::default()
        };
        info!("sort {:?}", request_body.sort);
        let search_body = SearchBody {
            query: request_body.query,
            aggs: request_body.aggs,
            from: request_body.from,
            size: request_body.size,
            sort: request_body.sort,
            ..Default::default()
        };
        let es_request = build_request_for_es_api(index_id, search_query_params, search_body)?;
        search_requests.push(es_request);
    }

    let futures = search_requests.into_iter().map(|search_request| async {
        let start_instant = Instant::now();
        let search_response: SearchResponse =
            search_service.clone().root_search(search_request).await?;
        let elapsed = start_instant.elapsed();
        let mut search_response_rest: ElasticSearchResponse =
            convert_to_es_search_response(search_response);
        search_response_rest.took = elapsed.as_millis() as u32;
        Ok::<_, ElasticSearchError>(search_response_rest)
    });

    let max_concurrent_searches =
        multi_search_params.max_concurrent_searches.unwrap_or(10) as usize;
    let search_responses = futures::stream::iter(futures)
        .buffer_unordered(max_concurrent_searches)
        .collect::<Vec<_>>()
        .await;

    let json_responses = search_responses
        .into_iter()
        .map(|search_response| {
            let value = match search_response {
                Ok(search_response) => {
                    let mut json_response =
                        serde_json::to_value(&search_response).map_err(|err| {
                            ElasticSearchError::new(500, anyhow::anyhow!(err.to_string()))
                        })?;
                    json_response
                        .as_object_mut()
                        .unwrap()
                        .insert("status".to_string(), json!(200));
                    Ok::<_, ElasticSearchError>(json_response)
                }
                Err(error) => {
                    info!(error = ?error, "Search error");
                    Ok(serde_json::to_value(&error).expect("msg"))
                }
            };
            value
        })
        .collect::<Result<Vec<_>, _>>()?;
    let responses = serde_json::Value::Array(json_responses);
    let mut root = serde_json::Map::new();
    root.insert("responses".to_string(), responses);
    Ok(serde_json::Value::Object(root))
}

fn lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines().filter_map(|line| {
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            return None;
        }
        Some(line_trimmed)
    })
}

// TODO: add a config variable to set this header
// let reply_with_header = reply::with_header(body_json, "x-elastic-product", "Elasticsearch");
fn make_rest_reply<T: serde::Serialize>(
    result: Result<T, ElasticSearchError>,
) -> WithStatus<WithHeader<String>> {
    match result {
        Ok(success) => {
            let body_json_res = serde_json::to_string(&success);
            match body_json_res {
                Ok(body_json) => {
                    let reply_with_header =
                        reply::with_header(body_json, CONTENT_TYPE, "application/json");
                    reply::with_status(reply_with_header, StatusCode::OK)
                }
                Err(err) => {
                    tracing::error!("Error: the response serialization failed.");
                    let es_error = ElasticSearchError::new(500, anyhow::anyhow!(err));
                    make_rest_error_reply(es_error)
                }
            }
        }
        Err(err) => make_rest_error_reply(err),
    }
}

fn make_rest_error_reply(err: ElasticSearchError) -> WithStatus<WithHeader<String>> {
    let body_json =
        serde_json::to_string(&err).expect("ElasticSearchError serialization should never fail.");
    let reply_with_header = reply::with_header(body_json, CONTENT_TYPE, "application/json");
    reply::with_status(
        reply_with_header,
        StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    )
}
