// Copyright (C) 2022 Quickwit, Inc.
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
    elastic_post_index_search_filter().then(
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
