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

use quickwit_common::simple_list::SimpleList;
use bytes::Bytes;
use elasticsearch_dsl::Search;
use quickwit_query::elastic_search_input_to_search_ast;
use quickwit_search::{SearchService, SearchResponseRest, SearchError};
use tracing::info;
use warp::{Filter, Rejection};

use crate::{format::Format, with_arg, search_api::get_proto_search_by};

use super::api_specs::{
    elastic_get_index_search_filter, elastic_get_search_filter, elastic_post_index_search_filter,
    elastic_post_search_filter, SearchQueryParams,
};

/// GET _elastic/_search
pub fn elastic_get_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_search_filter().then(|params: SearchQueryParams| async move {
        // TODO: implement
        let resp = serde_json::json!({
            "index": "all indexes",
            "params": params,
        });
        warp::reply::json(&resp)
    })
}

/// POST _elastic/_search
pub fn elastic_post_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_post_search_filter().then(|params: SearchQueryParams| async move {
        // TODO: implement
        let resp = serde_json::json!({
            "index": "all indexes",
            "params": params,
        });
        warp::reply::json(&resp)
    })
}

/// GET _elastic/{index}/_search
pub fn elastic_get_index_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_index_search_filter().then(
        |index: SimpleList, params: SearchQueryParams| async move {
            // TODO: implement
            let resp = serde_json::json!({
                "index": index.0,
                "params": params,
            });
            warp::reply::json(&resp)
        },
    )
}

/// POST api/_elastic/{index}/_search
pub fn elastic_post_index_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_post_index_search_filter()
    .and(warp::body::content_length_limit(1024 * 1024))
    .and(warp::filters::body::bytes())
    .and(with_arg(search_service))
    .then(|index: SimpleList, params: SearchQueryParams, body: Bytes, search_service: Arc<dyn SearchService>,| async move {
        info!(index_ids = ?index, params =? params, "elastic-search");
        Format::Json.make_rest_reply(elastic_search_endpoint(index.0, params, body, &*search_service).await)
    })
}

async fn elastic_search_endpoint(
    index_ids: Vec<String>,
    params: SearchQueryParams,
    body: Bytes, 
    search_service: &dyn SearchService,
) -> Result<SearchResponseRest, SearchError> {
    let elastic_search_input = serde_json::from_slice::<Search>(&body)?;
    let search_input_ast = elastic_search_input_to_search_ast(&elastic_search_input)?;
    println!("DEBUG {:?}", search_input_ast);

    //TODO: build search_request using:
    // - elastic_search_input
    // - params
    // - search_input_ast
    let search_request = crate::SearchRequestQueryString {
        query: "foo".to_string(), //TODO: HOT PRIORITY should be `search_input_ast`
        aggs: None,
        search_fields: None,
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 20,
        start_offset: 0,
        format: Format::Json,
        sort_by_field: None, //elastic way
    };

    let (sort_order, sort_by_field) = get_proto_search_by(&search_request);
    
    let search_request = quickwit_proto::SearchRequest {
        index_id: index_ids[0].clone(),
        query: search_request.query,
        search_fields: search_request.search_fields.unwrap_or_default(),
        snippet_fields: search_request.snippet_fields.unwrap_or_default(),
        start_timestamp: search_request.start_timestamp,
        end_timestamp: search_request.end_timestamp,
        max_hits: search_request.max_hits,
        start_offset: search_request.start_offset,
        aggregation_request: search_request
            .aggs
            .map(|agg| serde_json::to_string(&agg)
            .expect("could not serialize JsonValue")),
        sort_order,
        sort_by_field,
    };
    let search_response = search_service.root_search(search_request).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}
