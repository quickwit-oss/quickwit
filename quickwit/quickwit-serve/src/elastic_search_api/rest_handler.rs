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

use std::collections::BTreeMap;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use elasticsearch_dsl::search::{Hit as ElasticHit, SearchResponse as ElasticSearchResponse};
use elasticsearch_dsl::{HitsMetadata, Source, TotalHits, TotalHitsRelation};
use futures_util::StreamExt;
use hyper::StatusCode;
use itertools::Itertools;
use quickwit_common::truncate_str;
use quickwit_config::{validate_index_id_pattern, NodeConfig};
use quickwit_proto::search::{
    CountHits, ListFieldsResponse, PartialHit, ScrollRequest, SearchResponse, SortByValue,
    SortDatetimeFormat,
};
use quickwit_proto::ServiceErrorCode;
use quickwit_query::query_ast::{QueryAst, UserInputQuery};
use quickwit_query::BooleanOperand;
use quickwit_search::{SearchError, SearchService};
use serde_json::json;
use warp::{Filter, Rejection};

use super::filter::{
    elastic_cluster_info_filter, elastic_index_field_capabilities_filter,
    elastic_index_search_filter, elastic_multi_search_filter, elastic_scroll_filter,
    elastic_search_filter,
};
use super::model::{
    build_list_field_request_for_es_api, convert_to_es_field_capabilities_response,
    ElasticSearchError, FieldCapabilityQueryParams, FieldCapabilityRequestBody,
    FieldCapabilityResponse, MultiSearchHeader, MultiSearchQueryParams, MultiSearchResponse,
    MultiSearchSingleResponse, ScrollQueryParams, SearchBody, SearchQueryParams,
};
use super::{make_elastic_api_response, TrackTotalHits};
use crate::format::BodyFormat;
use crate::json_api_response::{make_json_api_response, ApiError, JsonApiResponse};
use crate::{with_arg, BuildInfo};

/// Elastic compatible cluster info handler.
pub fn es_compat_cluster_info_handler(
    node_config: Arc<NodeConfig>,
    build_info: &'static BuildInfo,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_cluster_info_filter()
        .and(with_arg(node_config.clone()))
        .and(with_arg(build_info))
        .then(
            |config: Arc<NodeConfig>, build_info: &'static BuildInfo| async move {
                warp::reply::json(&json!({
                    "name" : config.node_id,
                    "cluster_name" : config.cluster_id,
                    "version" : {
                        "distribution" : "quickwit",
                        "number" : build_info.version,
                        "build_hash" : build_info.commit_hash,
                        "build_date" : build_info.build_date,
                    }
                }))
            },
        )
}

/// GET or POST _elastic/_search
pub fn es_compat_search_handler(
    _search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_search_filter().then(|_params: SearchQueryParams| async move {
        // TODO
        let api_error = ApiError {
            service_code: ServiceErrorCode::NotSupportedYet,
            message: "_elastic/_search is not supported yet. Please try the index search endpoint \
                      (_elastic/{index}/search)"
                .to_string(),
        };
        make_json_api_response::<(), _>(Err(api_error), BodyFormat::default())
    })
}

/// GET or POST _elastic/{index}/_field_caps
/// TODO: add route handling for _elastic/_field_caps
pub fn es_compat_index_field_capabilities_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_field_capabilities_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_field_capabilities)
        .map(|result| make_elastic_api_response(result, BodyFormat::default()))
}

/// GET or POST _elastic/{index}/_search
pub fn es_compat_index_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_search_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_search)
        .map(|result| make_elastic_api_response(result, BodyFormat::default()))
}

/// GET or POST _elastic/_search/scroll
pub fn es_compat_scroll_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_scroll_filter()
        .and(with_arg(search_service))
        .then(es_scroll)
        .map(|result| make_elastic_api_response(result, BodyFormat::default()))
}

/// POST _elastic/_search
pub fn es_compat_index_multi_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_multi_search_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_multi_search)
        .map(|result: Result<MultiSearchResponse, ElasticSearchError>| {
            let status_code = match &result {
                Ok(_) => StatusCode::OK,
                Err(err) => err.status,
            };
            JsonApiResponse::new(&result, status_code, &BodyFormat::default())
        })
}

fn build_request_for_es_api(
    index_id_patterns: Vec<String>,
    search_params: SearchQueryParams,
    search_body: SearchBody,
) -> Result<(quickwit_proto::search::SearchRequest, bool), ElasticSearchError> {
    let default_operator = search_params.default_operator.unwrap_or(BooleanOperand::Or);
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
            .map_err(|err: anyhow::Error| SearchError::InvalidQuery(err.to_string()))?
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
    let count_hits = match search_params.track_total_hits {
        None => CountHits::Underestimate,
        Some(TrackTotalHits::Track(false)) => CountHits::Underestimate,
        Some(TrackTotalHits::Count(count)) if count <= max_hits as i64 => CountHits::Underestimate,
        Some(TrackTotalHits::Track(true) | TrackTotalHits::Count(_)) => CountHits::CountAll,
    }
    .into();

    let sort_fields: Vec<quickwit_proto::search::SortField> = search_params
        .sort_fields()?
        .or_else(|| search_body.sort.clone())
        .unwrap_or_default()
        .iter()
        .map(|sort_field| quickwit_proto::search::SortField {
            field_name: sort_field.field.to_string(),
            sort_order: sort_field.order as i32,
            sort_datetime_format: sort_field
                .date_format
                .clone()
                .map(|date_format| SortDatetimeFormat::from(date_format) as i32),
        })
        .take_while_inclusive(|sort_field| !is_doc_field(sort_field))
        .collect();
    if sort_fields.len() >= 3 {
        return Err(ElasticSearchError::from(SearchError::InvalidArgument(
            format!("only up to two sort fields supported at the moment. got {sort_fields:?}"),
        )));
    }

    let scroll_duration: Option<Duration> = search_params.parse_scroll_ttl()?;
    let scroll_ttl_secs: Option<u32> = scroll_duration.map(|duration| duration.as_secs() as u32);

    let has_doc_id_field = sort_fields.iter().any(is_doc_field);
    let search_after = partial_hit_from_search_after_param(search_body.search_after, &sort_fields)?;

    Ok((
        quickwit_proto::search::SearchRequest {
            index_id_patterns,
            query_ast: serde_json::to_string(&query_ast).expect("Failed to serialize QueryAst"),
            max_hits,
            start_offset,
            aggregation_request,
            sort_fields,
            start_timestamp: None,
            end_timestamp: None,
            snippet_fields: Vec::new(),
            scroll_ttl_secs,
            search_after,
            count_hits,
        },
        has_doc_id_field,
    ))
}

fn is_doc_field(field: &quickwit_proto::search::SortField) -> bool {
    field.field_name == "_shard_doc" || field.field_name == "_doc"
}

fn partial_hit_from_search_after_param(
    search_after: Vec<serde_json::Value>,
    sort_order: &[quickwit_proto::search::SortField],
) -> Result<Option<PartialHit>, ElasticSearchError> {
    if search_after.is_empty() {
        return Ok(None);
    }
    if search_after.len() != sort_order.len() {
        return Err(ElasticSearchError::new(
            StatusCode::BAD_REQUEST,
            "sort and search_after are of different length".to_string(),
        ));
    }
    let mut parsed_search_after = PartialHit::default();
    for (value, field) in search_after.into_iter().zip(sort_order) {
        if is_doc_field(field) {
            if let Some(value_str) = value.as_str() {
                let address: quickwit_search::GlobalDocAddress =
                    value_str.parse().map_err(|_| {
                        ElasticSearchError::new(
                            StatusCode::BAD_REQUEST,
                            "invalid search_after doc id, must be of form \
                             `{split_id}:{segment_id: u32}:{doc_id: u32}`"
                                .to_string(),
                        )
                    })?;
                parsed_search_after.split_id = address.split;
                parsed_search_after.segment_ord = address.doc_addr.segment_ord;
                parsed_search_after.doc_id = address.doc_addr.doc_id;
                return Ok(Some(parsed_search_after));
            } else {
                return Err(ElasticSearchError::new(
                    StatusCode::BAD_REQUEST,
                    "search_after doc id must be of string type".to_string(),
                ));
            }
        } else {
            let value = SortByValue::try_from_json(value).ok_or_else(|| {
                ElasticSearchError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid search_after field value, expect bool, number or string".to_string(),
                )
            })?;
            // TODO make cleaner once we support Vec
            if parsed_search_after.sort_value.is_none() {
                parsed_search_after.sort_value = Some(value);
            } else {
                parsed_search_after.sort_value2 = Some(value);
            }
        }
    }
    Ok(Some(parsed_search_after))
}

async fn es_compat_index_search(
    index_id_patterns: Vec<String>,
    search_params: SearchQueryParams,
    search_body: SearchBody,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticSearchResponse, ElasticSearchError> {
    let start_instant = Instant::now();
    let (search_request, append_shard_doc) =
        build_request_for_es_api(index_id_patterns, search_params, search_body)?;
    let search_response: SearchResponse = search_service.root_search(search_request).await?;
    let elapsed = start_instant.elapsed();
    let mut search_response_rest: ElasticSearchResponse =
        convert_to_es_search_response(search_response, append_shard_doc);
    search_response_rest.took = elapsed.as_millis() as u32;
    Ok(search_response_rest)
}

async fn es_compat_index_field_capabilities(
    index_id_patterns: Vec<String>,
    search_params: FieldCapabilityQueryParams,
    search_body: FieldCapabilityRequestBody,
    search_service: Arc<dyn SearchService>,
) -> Result<FieldCapabilityResponse, ElasticSearchError> {
    let search_request =
        build_list_field_request_for_es_api(index_id_patterns, search_params, search_body)?;
    let search_response: ListFieldsResponse =
        search_service.root_list_fields(search_request).await?;
    let search_response_rest: FieldCapabilityResponse =
        convert_to_es_field_capabilities_response(search_response);
    Ok(search_response_rest)
}

fn convert_hit(hit: quickwit_proto::search::Hit, append_shard_doc: bool) -> ElasticHit {
    let fields: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(&hit.json).unwrap_or_default();
    let mut sort = Vec::new();
    if let Some(partial_hit) = hit.partial_hit {
        if let Some(sort_value) = partial_hit.sort_value {
            sort.push(sort_value.into_json());
        }
        if let Some(sort_value2) = partial_hit.sort_value2 {
            sort.push(sort_value2.into_json());
        }
        if append_shard_doc {
            sort.push(serde_json::Value::String(
                quickwit_search::GlobalDocAddress::from_partial_hit(&partial_hit).to_string(),
            ));
        }
    }

    ElasticHit {
        fields,
        explanation: None,
        index: hit.index_id,
        id: "".to_string(),
        score: None,
        nested: None,
        source: Source::from_string(hit.json)
            .unwrap_or_else(|_| Source::from_string("{}".to_string()).unwrap()),
        highlight: Default::default(),
        inner_hits: Default::default(),
        matched_queries: Vec::default(),
        sort,
    }
}

async fn es_compat_index_multi_search(
    payload: Bytes,
    multi_search_params: MultiSearchQueryParams,
    search_service: Arc<dyn SearchService>,
) -> Result<MultiSearchResponse, ElasticSearchError> {
    let mut search_requests = Vec::new();
    let str_payload = from_utf8(&payload)
        .map_err(|err| SearchError::InvalidQuery(format!("invalid UTF-8: {}", err)))?;
    let mut payload_lines = str_lines(str_payload);

    while let Some(line) = payload_lines.next() {
        let request_header = serde_json::from_str::<MultiSearchHeader>(line).map_err(|err| {
            SearchError::InvalidArgument(format!(
                "failed to parse request header `{}...`: {}",
                truncate_str(line, 20),
                err
            ))
        })?;
        if request_header.index.is_empty() {
            return Err(ElasticSearchError::from(SearchError::InvalidArgument(
                "`_msearch` request header must define at least one index".to_string(),
            )));
        }
        for index in &request_header.index {
            validate_index_id_pattern(index).map_err(|err| {
                SearchError::InvalidArgument(format!(
                    "request header contains an invalid index: {}",
                    err
                ))
            })?;
        }
        let index_ids_patterns = request_header.index.clone();
        let search_body = payload_lines
            .next()
            .ok_or_else(|| {
                SearchError::InvalidArgument("expect request body after request header".to_string())
            })
            .and_then(|line| {
                serde_json::from_str::<SearchBody>(line).map_err(|err| {
                    SearchError::InvalidArgument(format!(
                        "failed to parse request body `{}...`: {}",
                        truncate_str(line, 20),
                        err
                    ))
                })
            })?;
        let search_query_params = SearchQueryParams::from(request_header);
        let es_request =
            build_request_for_es_api(index_ids_patterns, search_query_params, search_body)?;
        search_requests.push(es_request);
    }
    // TODO: forced to do weird referencing to work around https://github.com/rust-lang/rust/issues/100905
    // otherwise append_shard_doc is captured by ref, and we get lifetime issues
    let futures = search_requests
        .into_iter()
        .map(|(search_request, append_shard_doc)| {
            let search_service = &search_service;
            async move {
                let start_instant = Instant::now();
                let search_response: SearchResponse =
                    search_service.clone().root_search(search_request).await?;
                let elapsed = start_instant.elapsed();
                let mut search_response_rest: ElasticSearchResponse =
                    convert_to_es_search_response(search_response, append_shard_doc);
                search_response_rest.took = elapsed.as_millis() as u32;
                Ok::<_, ElasticSearchError>(search_response_rest)
            }
        });
    let max_concurrent_searches =
        multi_search_params.max_concurrent_searches.unwrap_or(10) as usize;
    let search_responses = futures::stream::iter(futures)
        .buffer_unordered(max_concurrent_searches)
        .collect::<Vec<_>>()
        .await;
    let responses = search_responses
        .into_iter()
        .map(|search_response| match search_response {
            Ok(search_response) => MultiSearchSingleResponse::from(search_response),
            Err(error) => MultiSearchSingleResponse::from(error),
        })
        .collect_vec();
    let multi_search_response = MultiSearchResponse { responses };
    Ok(multi_search_response)
}

async fn es_scroll(
    scroll_query_params: ScrollQueryParams,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticSearchResponse, ElasticSearchError> {
    let start_instant = Instant::now();
    let Some(scroll_id) = scroll_query_params.scroll_id.clone() else {
        return Err(SearchError::InvalidArgument("missing scroll_id".to_string()).into());
    };
    let scroll_ttl_secs: Option<u32> = if let Some(scroll_ttl) = scroll_query_params.scroll {
        let scroll_ttl_duration = humantime::parse_duration(&scroll_ttl)
            .map_err(|_| SearchError::InvalidArgument(format!("Scroll invalid: {}", scroll_ttl)))?;
        Some(scroll_ttl_duration.as_secs() as u32)
    } else {
        None
    };
    let scroll_request = ScrollRequest {
        scroll_id,
        scroll_ttl_secs,
    };
    let search_response: SearchResponse = search_service.scroll(scroll_request).await?;
    // TODO append_shard_doc depends on the initial request, but we don't have access to it
    let mut search_response_rest: ElasticSearchResponse =
        convert_to_es_search_response(search_response, false);
    search_response_rest.took = start_instant.elapsed().as_millis() as u32;
    Ok(search_response_rest)
}

fn convert_to_es_search_response(
    resp: SearchResponse,
    append_shard_doc: bool,
) -> ElasticSearchResponse {
    let hits: Vec<ElasticHit> = resp
        .hits
        .into_iter()
        .map(|hit| convert_hit(hit, append_shard_doc))
        .collect();
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
        scroll_id: resp.scroll_id,
        ..Default::default()
    }
}

pub(crate) fn str_lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;

    use super::partial_hit_from_search_after_param;

    #[test]
    fn test_partial_hit_from_search_after_param_invalid_length() {
        let search_after = vec![serde_json::json!([1])];
        let sort_order = &[];
        let error = partial_hit_from_search_after_param(search_after, sort_order).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.error.reason.unwrap(),
            "sort and search_after are of different length"
        );
    }

    #[test]
    fn test_partial_hit_from_search_after_param_invalid_search_after_value() {
        let search_after = vec![serde_json::json!([1])];
        let sort_order = &[quickwit_proto::search::SortField {
            field_name: "field1".to_string(),
            sort_order: 1,
            sort_datetime_format: None,
        }];
        let error = partial_hit_from_search_after_param(search_after, sort_order).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.error.reason.unwrap(),
            "invalid search_after field value, expect bool, number or string"
        );
    }

    #[test]
    fn test_partial_hit_from_search_after_param_invalid_search_after_doc_id() {
        let search_after = vec![serde_json::json!("split_id:1112")];
        let sort_order = &[quickwit_proto::search::SortField {
            field_name: "_doc".to_string(),
            sort_order: 1,
            sort_datetime_format: None,
        }];
        let error = partial_hit_from_search_after_param(search_after, sort_order).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.error.reason.unwrap(),
            "invalid search_after doc id, must be of form `{split_id}:{segment_id: u32}:{doc_id: \
             u32}`"
        );
    }
}
