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

use bytes::Bytes;
use bytesize::ByteSize;
use serde::de::DeserializeOwned;
use warp::reject::LengthRequired;
use warp::{Filter, Rejection};

use super::model::{
    FieldCapabilityQueryParams, FieldCapabilityRequestBody, MultiSearchQueryParams,
    SearchQueryParamsCount,
};
use crate::elasticsearch_api::model::{
    ElasticBulkOptions, ScrollQueryParams, SearchBody, SearchQueryParams,
};
use crate::search_api::{extract_index_id_patterns, extract_index_id_patterns_default};

const BODY_LENGTH_LIMIT: ByteSize = ByteSize::mib(1);
const CONTENT_LENGTH_LIMIT: ByteSize = ByteSize::mib(10);

// TODO: Make all elastic endpoint models `utoipa` compatible
// and register them here.
#[derive(utoipa::OpenApi)]
#[openapi(paths(elastic_cluster_info_filter,))]
pub struct ElasticCompatibleApi;

#[utoipa::path(get, tag = "Cluster Info", path = "/_elastic")]
pub(crate) fn elastic_cluster_info_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone
{
    warp::path!("_elastic")
        .and(warp::get().or(warp::head()).unify())
        .and(warp::path::end())
}

#[utoipa::path(get, tag = "Search", path = "/_search")]
pub(crate) fn elasticsearch_filter(
) -> impl Filter<Extract = (SearchQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search")
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/_bulk",
    request_body(content = String, description = "Elasticsearch compatible bulk request body limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
    params(
        ("refresh" = Option<ElasticRefresh>, Query, description = "Force or wait for commit at the end of the indexing operation."),
    )
)]
pub(crate) fn elastic_bulk_filter(
) -> impl Filter<Extract = (Bytes, ElasticBulkOptions), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            CONTENT_LENGTH_LIMIT.as_u64(),
        ))
        .and(warp::body::bytes())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/{index}/_bulk",
    request_body(content = String, description = "Elasticsearch compatible bulk request body limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
    params(
        ("refresh" = Option<ElasticRefresh>, Query, description = "Force or wait for commit at the end of the indexing operation."),
    )
)]
pub(crate) fn elastic_index_bulk_filter(
) -> impl Filter<Extract = (String, Bytes, ElasticBulkOptions), Error = Rejection> + Clone {
    warp::path!("_elastic" / String / "_bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            CONTENT_LENGTH_LIMIT.as_u64(),
        ))
        .and(warp::body::bytes())
        .and(serde_qs::warp::query::<ElasticBulkOptions>(
            serde_qs::Config::default(),
        ))
}

/// Like the warp json filter, but accepts an empty body and interprets it as `T::default`.
fn json_or_empty<T: DeserializeOwned + Send + Default>(
) -> impl Filter<Extract = (T,), Error = Rejection> + Copy {
    warp::body::content_length_limit(BODY_LENGTH_LIMIT.as_u64())
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

#[utoipa::path(get, tag = "Metadata", path = "/{index}/_field_caps")]
pub(crate) fn elastic_index_field_capabilities_filter() -> impl Filter<
    Extract = (
        Vec<String>,
        FieldCapabilityQueryParams,
        FieldCapabilityRequestBody,
    ),
    Error = Rejection,
> + Clone {
    warp::path!("_elastic" / String / "_field_caps")
        .and_then(extract_index_id_patterns)
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
}

#[utoipa::path(get, tag = "Metadata", path = "/_field_caps")]
pub(crate) fn elastic_field_capabilities_filter() -> impl Filter<
    Extract = (
        Vec<String>,
        FieldCapabilityQueryParams,
        FieldCapabilityRequestBody,
    ),
    Error = Rejection,
> + Clone {
    warp::path!("_elastic" / "_field_caps")
        .and_then(extract_index_id_patterns_default)
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
}

#[utoipa::path(get, tag = "Count", path = "/{index}/_count")]
pub(crate) fn elastic_index_count_filter(
) -> impl Filter<Extract = (Vec<String>, SearchQueryParamsCount, SearchBody), Error = Rejection> + Clone
{
    warp::path!("_elastic" / String / "_count")
        .and_then(extract_index_id_patterns)
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
}

#[utoipa::path(get, tag = "Search", path = "/{index}/_search")]
pub(crate) fn elastic_index_search_filter(
) -> impl Filter<Extract = (Vec<String>, SearchQueryParams, SearchBody), Error = Rejection> + Clone
{
    warp::path!("_elastic" / String / "_search")
        .and_then(extract_index_id_patterns)
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
}

#[utoipa::path(post, tag = "Search", path = "/_msearch")]
pub(crate) fn elastic_multi_search_filter(
) -> impl Filter<Extract = (Bytes, MultiSearchQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_msearch")
        .and(warp::body::content_length_limit(BODY_LENGTH_LIMIT.as_u64()))
        .and(warp::body::bytes())
        .and(warp::post())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

fn merge_scroll_body_params(
    from_query_string: ScrollQueryParams,
    from_body: ScrollQueryParams,
) -> ScrollQueryParams {
    ScrollQueryParams {
        scroll: from_query_string.scroll.or(from_body.scroll),
        scroll_id: from_query_string.scroll_id.or(from_body.scroll_id),
    }
}

#[utoipa::path(post, tag = "Search", path = "/_search/scroll")]
pub(crate) fn elastic_scroll_filter(
) -> impl Filter<Extract = (ScrollQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search" / "scroll")
        .and(warp::body::content_length_limit(BODY_LENGTH_LIMIT.as_u64()))
        .and(warp::get().or(warp::post()).unify())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(json_or_empty())
        .map(
            |scroll_query_params: ScrollQueryParams, scroll_body: ScrollQueryParams| {
                merge_scroll_body_params(scroll_query_params, scroll_body)
            },
        )
}
