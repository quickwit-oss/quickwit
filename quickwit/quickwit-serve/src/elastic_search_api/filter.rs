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

use byte_unit::Byte;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use warp::reject::LengthRequired;
use warp::{Filter, Rejection};

use crate::elastic_search_api::model::{SearchBody, SearchQueryParams};

const BODY_LENGTH_LIMIT: Byte = byte_unit::Byte::from_bytes(1_000_000);

#[utoipa::path(get, tag = "Search", path = "/_search")]
pub(crate) fn elastic_search_filter(
) -> impl Filter<Extract = (SearchQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search")
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

/// Like the warp json filter, but accepts an empty body and interprets it as `T::default`.
fn json_or_empty<T: DeserializeOwned + Send + Default>(
) -> impl Filter<Extract = (T,), Error = Rejection> + Copy {
    warp::body::content_length_limit(BODY_LENGTH_LIMIT.get_bytes())
        .and(warp::body::bytes().and_then(|buf: Bytes| async move {
            if buf.is_empty() {
                return Ok(T::default());
            }
            serde_json::from_slice(&buf)
                .map_err(|err| warp::reject::custom(crate::rest::InvalidJsonRequest(err)))
        }))
        .recover(|rejection: Rejection| async {
            // Not having a header with content length is not an error as long as
            // there are no body.
            if rejection.find::<LengthRequired>().is_some() {
                Ok(T::default())
            } else {
                Err(rejection)
            }
        })
        .unify()
}

#[utoipa::path(get, tag = "Search", path = "/{index}/_search")]
pub(crate) fn elastic_index_search_filter(
) -> impl Filter<Extract = (String, SearchQueryParams, SearchBody), Error = Rejection> + Clone {
    warp::path!("_elastic" / String / "_search")
        .and_then(|comma_separated_indexes: String| async move {
            if comma_separated_indexes.contains(',') {
                return Err(warp::reject::custom(crate::rest::InvalidArgument(format!(
                    "Searching only one index is supported for now. Got \
                     (`{comma_separated_indexes}`)"
                ))));
            }
            let index = comma_separated_indexes.trim();
            if index.is_empty() {
                return Err(warp::reject::custom(crate::rest::InvalidArgument(
                    "Missing index name.".to_string(),
                )));
            }
            Ok(index.to_string())
        })
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
}
