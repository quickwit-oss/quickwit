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
use hyper::StatusCode;
use quickwit_proto::{SearchResponse, ServiceErrorCode, SortOrder};
use quickwit_query::query_ast::{QueryAst, UserInputQuery};
use quickwit_query::DefaultOperator;
use quickwit_search::{SearchError, SearchService};
use warp::{Filter, Rejection};

use super::model::{ElasticSearchError, SearchBody, SearchQueryParams};
use crate::elastic_search_api::filter::elastic_index_search_filter;
use crate::format::BodyFormat;
use crate::json_api_response::{make_json_api_response, ApiError, JsonApiResponse};
use crate::with_arg;

/// GET or POST _elastic/_search
pub fn es_compat_search_handler(
    _search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    super::filter::elastic_search_filter().then(|_params: SearchQueryParams| async move {
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

/// GET or POST _elastic/{index}/_search
pub fn es_compat_index_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_search_filter()
        .and(with_arg(search_service))
        .then(es_compat_index_search)
        .map(make_elastic_api_response)
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

fn make_elastic_api_response(
    elasticsearch_result: Result<ElasticSearchResponse, ElasticSearchError>,
) -> JsonApiResponse {
    let status_code = match &elasticsearch_result {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status,
    };
    JsonApiResponse::new(&elasticsearch_result, status_code, &BodyFormat::default())
}
