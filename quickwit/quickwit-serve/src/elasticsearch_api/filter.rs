// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use bytesize::ByteSize;
use serde::de::DeserializeOwned;
use warp::reject::LengthRequired;
use warp::{Filter, Rejection};

use super::model::{
    CatIndexQueryParams, DeleteQueryParams, FieldCapabilityQueryParams, FieldCapabilityRequestBody,
    MultiSearchQueryParams, SearchQueryParamsCount,
};
use crate::Body;
use crate::decompression::get_body_bytes;
use crate::elasticsearch_api::model::{
    ElasticBulkOptions, ScrollQueryParams, SearchBody, SearchQueryParams,
};
use crate::search_api::{extract_index_id_patterns, extract_index_id_patterns_default};

const BODY_LENGTH_LIMIT: ByteSize = ByteSize::mib(1);

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
pub(crate) fn elasticsearch_filter()
-> impl Filter<Extract = (SearchQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search")
        .and(warp::get().or(warp::post()).unify())
        .and(warp::query())
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
    content_length_limit: ByteSize,
) -> impl Filter<Extract = (Body, ElasticBulkOptions), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_bulk")
        .and(warp::post().or(warp::put()).unify())
        .and(warp::body::content_length_limit(
            content_length_limit.as_u64(),
        ))
        .and(get_body_bytes())
        .and(warp::query())
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
    content_length_limit: ByteSize,
) -> impl Filter<Extract = (String, Body, ElasticBulkOptions), Error = Rejection> + Clone {
    warp::path!("_elastic" / String / "_bulk")
        .and(warp::post().or(warp::put()).unify())
        .and(warp::body::content_length_limit(
            content_length_limit.as_u64(),
        ))
        .and(get_body_bytes())
        .and(warp::query::<ElasticBulkOptions>())
}

/// Like the warp json filter, but accepts an empty body and interprets it as `T::default`.
fn json_or_empty<T: DeserializeOwned + Send + Default>()
-> impl Filter<Extract = (T,), Error = Rejection> + Copy {
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
        .and(warp::query())
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
        .and(warp::query())
        .and(json_or_empty())
}

#[utoipa::path(get, tag = "Metadata", path = "/_resolve/index/{index}")]
pub(crate) fn elastic_resolve_index_filter()
-> impl Filter<Extract = (Vec<String>,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_resolve" / "index" / String)
        .and_then(extract_index_id_patterns)
        .and(warp::get())
}

#[utoipa::path(get, tag = "Count", path = "/{index}/_count")]
pub(crate) fn elastic_index_count_filter()
-> impl Filter<Extract = (Vec<String>, SearchQueryParamsCount, SearchBody), Error = Rejection> + Clone
{
    warp::path!("_elastic" / String / "_count")
        .and_then(extract_index_id_patterns)
        .and(warp::get().or(warp::post()).unify())
        .and(warp::query())
        .and(json_or_empty())
}

#[utoipa::path(delete, tag = "Indexes", path = "/{index}")]
pub(crate) fn elastic_delete_index_filter()
-> impl Filter<Extract = (Vec<String>, DeleteQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / String)
        .and(warp::delete())
        .and_then(extract_index_id_patterns)
        .and(warp::query())
}

// No support for any query parameters for now.
#[utoipa::path(get, tag = "Search", path = "/{index}/_stats")]
pub(crate) fn elastic_index_stats_filter()
-> impl Filter<Extract = (Vec<String>,), Error = Rejection> + Clone {
    warp::path!("_elastic" / String / "_stats")
        .and_then(extract_index_id_patterns)
        .and(warp::get())
}

#[utoipa::path(get, tag = "Search", path = "/_stats")]
pub(crate) fn elastic_stats_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_stats").and(warp::get())
}

#[utoipa::path(get, tag = "Search", path = "/_cluster/health")]
pub(crate) fn elastic_cluster_health_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone
{
    warp::path!("_elastic" / "_cluster" / "health").and(warp::get())
}

#[utoipa::path(get, tag = "Search", path = "/_cat/indices/{index}")]
pub(crate) fn elastic_index_cat_indices_filter()
-> impl Filter<Extract = (Vec<String>, CatIndexQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_cat" / "indices" / String)
        .and_then(extract_index_id_patterns)
        .and(warp::get())
        .and(warp::query())
}

#[utoipa::path(get, tag = "Search", path = "/_cat/indices")]
pub(crate) fn elastic_cat_indices_filter()
-> impl Filter<Extract = (CatIndexQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_cat" / "indices")
        .and(warp::get())
        .and(warp::query())
}

#[utoipa::path(get, tag = "Search", path = "/{index}/_search")]
pub(crate) fn elastic_index_search_filter()
-> impl Filter<Extract = (Vec<String>, SearchQueryParams, SearchBody), Error = Rejection> + Clone {
    warp::path!("_elastic" / String / "_search")
        .and_then(extract_index_id_patterns)
        .and(warp::get().or(warp::post()).unify())
        .and(warp::query())
        .and(json_or_empty())
}

#[utoipa::path(post, tag = "Search", path = "/_msearch")]
pub(crate) fn elastic_multi_search_filter()
-> impl Filter<Extract = (Bytes, MultiSearchQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_msearch")
        .and(warp::body::content_length_limit(BODY_LENGTH_LIMIT.as_u64()))
        .and(warp::body::bytes())
        .and(warp::post())
        .and(warp::query())
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
pub(crate) fn elastic_scroll_filter()
-> impl Filter<Extract = (ScrollQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search" / "scroll")
        .and(warp::body::content_length_limit(BODY_LENGTH_LIMIT.as_u64()))
        .and(warp::get().or(warp::post()).unify())
        .and(warp::query())
        .and(json_or_empty())
        .map(
            |scroll_query_params: ScrollQueryParams, scroll_body: ScrollQueryParams| {
                merge_scroll_body_params(scroll_query_params, scroll_body)
            },
        )
}
