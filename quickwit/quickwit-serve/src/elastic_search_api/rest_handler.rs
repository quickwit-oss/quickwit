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
use quickwit_proto::{SearchResponse, ServiceErrorCode};
use quickwit_query::quickwit_query_ast::QueryAst;
use quickwit_query::DefaultOperator;
use quickwit_search::{SearchError, SearchService};
use warp::{Filter, Rejection};

use super::model::{SearchBody, SearchQueryParams};
use crate::elastic_search_api::filter::elastic_index_search_filter;
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
        .map(|resp| BodyFormat::Json.make_rest_reply(resp))
}

fn build_request_for_es_api(
    index_id: String,
    search_params: SearchQueryParams,
    search_body: SearchBody,
) -> Result<quickwit_proto::SearchRequest, SearchError> {
    let default_operator = search_params
        .default_operator
        .unwrap_or(DefaultOperator::Or);
    let query_ast = if let Some(q) = &search_params.q {
        quickwit_query::parse_user_query(q, &[], default_operator)?
    } else if let Some(query_dsl) = search_body.query {
        query_dsl
            .convert_to_query_ast(&[])
            .map_err(|err| SearchError::InvalidQuery(err.to_string()))?
    } else {
        QueryAst::MatchAll
    };
    let aggregation_request: Option<String> = if search_body.aggs.is_empty() {
        None
    } else {
        serde_json::to_string(&search_body.aggs).ok()
    };
    Ok(quickwit_proto::SearchRequest {
        index_id,
        query_ast: serde_json::to_string(&query_ast).expect("Failed to serialize QueryAst"),
        max_hits: search_params.size.unwrap_or(10).max(0i64) as u64,
        start_offset: search_params.from.unwrap_or(0).max(0i64) as u64,
        aggregation_request,
        ..Default::default()
    })
}

async fn es_compat_index_search(
    index_id: String,
    search_params: SearchQueryParams,
    search_body: SearchBody,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticSearchResponse, SearchError> {
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
