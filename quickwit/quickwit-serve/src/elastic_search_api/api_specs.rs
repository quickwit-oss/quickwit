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

use std::str::FromStr;

use quickwit_common::simple_list::{from_simple_list, to_simple_list, SimpleList};
/// This file is auto-generated, any change can be overriden.
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

use super::TrackTotalHits;

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SearchQueryParams {
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    _source: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    _source_excludes: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    _source_includes: Option<Vec<String>>,
    #[serde(default)]
    allow_no_indices: Option<bool>,
    #[serde(default)]
    allow_partial_search_results: Option<bool>,
    #[serde(default)]
    analyze_wildcard: Option<bool>,
    #[serde(default)]
    analyzer: Option<String>,
    #[serde(default)]
    batched_reduce_size: Option<i64>,
    #[serde(default)]
    ccs_minimize_roundtrips: Option<bool>,
    #[serde(default)]
    default_operator: Option<DefaultOperator>,
    #[serde(default)]
    df: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    docvalue_fields: Option<Vec<String>>,
    #[serde(default)]
    error_trace: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    explain: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    filter_path: Option<Vec<String>>,
    #[serde(default)]
    force_synthetic_source: Option<bool>,
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    human: Option<bool>,
    #[serde(default)]
    ignore_throttled: Option<bool>,
    #[serde(default)]
    ignore_unavailable: Option<bool>,
    #[serde(default)]
    lenient: Option<bool>,
    #[serde(default)]
    max_concurrent_shard_requests: Option<i64>,
    #[serde(default)]
    min_compatible_shard_node: Option<String>,
    #[serde(default)]
    pre_filter_shard_size: Option<i64>,
    #[serde(default)]
    preference: Option<String>,
    #[serde(default)]
    pretty: Option<bool>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    request_cache: Option<bool>,
    #[serde(default)]
    rest_total_hits_as_int: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    routing: Option<Vec<String>>,
    #[serde(default)]
    scroll: Option<String>,
    #[serde(default)]
    search_type: Option<SearchType>,
    #[serde(default)]
    seq_no_primary_term: Option<bool>,
    #[serde(default)]
    size: Option<i64>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    sort: Option<Vec<String>>,
    #[serde(default)]
    source: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    stats: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    stored_fields: Option<Vec<String>>,
    #[serde(default)]
    suggest_field: Option<String>,
    #[serde(default)]
    suggest_mode: Option<SuggestMode>,
    #[serde(default)]
    suggest_size: Option<i64>,
    #[serde(default)]
    suggest_text: Option<String>,
    #[serde(default)]
    terminate_after: Option<i64>,
    #[serde(default)]
    timeout: Option<String>,
    #[serde(default)]
    track_scores: Option<bool>,
    #[serde(default)]
    track_total_hits: Option<TrackTotalHits>,
    #[serde(default)]
    typed_keys: Option<bool>,
    #[serde(default)]
    version: Option<bool>,
}
#[doc = "The default operator for query string query (AND or OR)"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum DefaultOperator {
    #[serde(rename = "AND")]
    And,
    #[serde(rename = "OR")]
    Or,
}
impl FromStr for DefaultOperator {
    type Err = &'static str;
    fn from_str(value_str: &str) -> Result<Self, Self::Err> {
        match value_str {
            "AND" => Ok(Self::And),
            "OR" => Ok(Self::Or),
            _ => Err("unknown enum variant"),
        }
    }
}
impl ToString for DefaultOperator {
    fn to_string(&self) -> String {
        match &self {
            Self::And => "AND".to_string(),
            Self::Or => "OR".to_string(),
        }
    }
}
#[doc = "Whether to expand wildcard expression to concrete indices that are open, closed or both."]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum ExpandWildcards {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "hidden")]
    Hidden,
    #[serde(rename = "none")]
    None,
    #[serde(rename = "all")]
    All,
}
impl FromStr for ExpandWildcards {
    type Err = &'static str;
    fn from_str(value_str: &str) -> Result<Self, Self::Err> {
        match value_str {
            "open" => Ok(Self::Open),
            "closed" => Ok(Self::Closed),
            "hidden" => Ok(Self::Hidden),
            "none" => Ok(Self::None),
            "all" => Ok(Self::All),
            _ => Err("unknown enum variant"),
        }
    }
}
impl ToString for ExpandWildcards {
    fn to_string(&self) -> String {
        match &self {
            Self::Open => "open".to_string(),
            Self::Closed => "closed".to_string(),
            Self::Hidden => "hidden".to_string(),
            Self::None => "none".to_string(),
            Self::All => "all".to_string(),
        }
    }
}
#[doc = "Search operation type"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum SearchType {
    #[serde(rename = "query_then_fetch")]
    QueryThenFetch,
    #[serde(rename = "dfs_query_then_fetch")]
    DfsQueryThenFetch,
}
impl FromStr for SearchType {
    type Err = &'static str;
    fn from_str(value_str: &str) -> Result<Self, Self::Err> {
        match value_str {
            "query_then_fetch" => Ok(Self::QueryThenFetch),
            "dfs_query_then_fetch" => Ok(Self::DfsQueryThenFetch),
            _ => Err("unknown enum variant"),
        }
    }
}
impl ToString for SearchType {
    fn to_string(&self) -> String {
        match &self {
            Self::QueryThenFetch => "query_then_fetch".to_string(),
            Self::DfsQueryThenFetch => "dfs_query_then_fetch".to_string(),
        }
    }
}
#[doc = "Specify suggest mode"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum SuggestMode {
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "popular")]
    Popular,
    #[serde(rename = "always")]
    Always,
}
impl FromStr for SuggestMode {
    type Err = &'static str;
    fn from_str(value_str: &str) -> Result<Self, Self::Err> {
        match value_str {
            "missing" => Ok(Self::Missing),
            "popular" => Ok(Self::Popular),
            "always" => Ok(Self::Always),
            _ => Err("unknown enum variant"),
        }
    }
}
impl ToString for SuggestMode {
    fn to_string(&self) -> String {
        match &self {
            Self::Missing => "missing".to_string(),
            Self::Popular => "popular".to_string(),
            Self::Always => "always".to_string(),
        }
    }
}
#[utoipa::path(get, tag = "Search", path = "/_search")]
pub(crate) fn elastic_get_search_filter(
) -> impl Filter<Extract = (SearchQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}
#[utoipa::path(post, tag = "Search", path = "/_search")]
pub(crate) fn elastic_post_search_filter(
) -> impl Filter<Extract = (SearchQueryParams,), Error = Rejection> + Clone {
    warp::path!("_elastic" / "_search")
        .and(warp::post())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}
#[utoipa::path(get, tag = "Search", path = "/{index}/_search")]
pub(crate) fn elastic_get_index_search_filter(
) -> impl Filter<Extract = (SimpleList, SearchQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / SimpleList / "_search")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}
#[utoipa::path(post, tag = "Search", path = "/{index}/_search")]
pub(crate) fn elastic_post_index_search_filter(
) -> impl Filter<Extract = (SimpleList, SearchQueryParams), Error = Rejection> + Clone {
    warp::path!("_elastic" / SimpleList / "_search")
        .and(warp::post())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}
