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
use quickwit_common::simple_list::SimpleList;
use quickwit_proto::SearchResponse;
use quickwit_search::{SearchError, SearchService};
use warp::{Filter, Rejection};

use super::api_specs::SearchQueryParams;
use crate::elastic_search_api::api_specs::{elastic_index_search_filter, elastic_search_filter};
use crate::format::BodyFormat;
use crate::with_arg;

/// GET or POST _elastic/_search
pub fn es_compat_search_handler(
    _search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_search_filter().then(|params: SearchQueryParams| async move {
        // TODO: implement
        let resp = serde_json::json!({
            "index": "all indexes",
            "params": params,
        });
        warp::reply::json(&resp)
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

async fn es_compat_index_search(
    indexes: SimpleList,
    search_params: SearchQueryParams,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticSearchResponse, SearchError> {
    let start_instant = Instant::now();
    if indexes.0.len() != 1 {
        let error_msg = format!("Expected exaclty one index, got {indexes:?}.");
        return Err(SearchError::InvalidArgument(error_msg));
    }
    let index_id: String = indexes.0.into_iter().next().expect(
        "There should be exactly once index in the list, as checked by the if statement above.",
    );
    let search_request = quickwit_proto::SearchRequest {
        index_id,
        query: search_params.q.unwrap_or_else(|| "*".to_string()),
        max_hits: search_params.size.unwrap_or(10).max(0i64) as u64,
        start_offset: search_params.from.unwrap_or(0).max(0i64) as u64,
        ..Default::default()
    };
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
        ..Default::default()
    }
}
