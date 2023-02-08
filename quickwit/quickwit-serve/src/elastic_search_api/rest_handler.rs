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

use quickwit_common::simple_list::SimpleList;
use bytes::Bytes;
use elasticsearch_dsl::Search;
use serde::Deserialize;
use warp::{Filter, Rejection};

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
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_post_index_search_filter()
    .and(warp::body::content_length_limit(1024 * 1024))
    .and(warp::filters::body::bytes())
    .then(
        |index: SimpleList, params: SearchQueryParams, body: Bytes| async move {
            use elasticsearch_dsl::Search;
            let search = serde_json::from_slice::<Search>(&body)?;
            let user_input_ast = elasticsearch_dsl_to_tantivy_query(search)?;

            let err_str = match query {
                Ok(qw) => {
                    println!("EVAN {:?}", qw);
                    None
                },
                Err(err) => Some(err.to_string()),
            };

            
            
            
            // TODO: implement
            let resp = serde_json::json!({
                "index": index.0,
                "params": params,
                "error": err_str,
            });
            warp::reply::json(&resp)
        },
    )
}

fn elasticsearch_dsl_to_tantivy_query(search: Search) -> tantivy::UserInputAst {
    todo!()
}
