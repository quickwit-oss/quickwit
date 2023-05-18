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

use elasticsearch_dsl::search::SearchResponse as ElasticSearchResponse;
use elasticsearch_dsl::ErrorCause;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use serde_with::formats::PreferMany;
use serde_with::{serde_as, OneOrMany};

use super::search_query_params::ExpandWildcards;
use super::ElasticSearchError;
use crate::simple_list::{from_simple_list, to_simple_list};

// Multi search doc: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiSearchQueryParams {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub ccs_minimize_roundtrips: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_throttled: Option<bool>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde(default)]
    pub max_concurrent_searches: Option<u64>,
    #[serde(default)]
    pub max_concurrent_shard_requests: Option<i64>,
    #[serde(default)]
    pub pre_filter_shard_size: Option<i64>,
    #[serde(default)]
    pub rest_total_hits_as_int: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub routing: Option<Vec<String>>,
    #[serde(default)]
    pub typed_keys: Option<bool>,
}

#[serde_as]
#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct MultiSearchHeader {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    pub index: Vec<String>,
    #[serde(default)]
    pub preference: Option<String>,
    #[serde(default)]
    pub request_cache: Option<bool>,
    #[serde(default)]
    pub routing: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct MultiSearchResponse {
    pub responses: Vec<MultiSearchSingleResponse>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MultiSearchSingleResponse {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub response: Option<ElasticSearchResponse>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorCause>,
}

impl From<ElasticSearchResponse> for MultiSearchSingleResponse {
    fn from(response: ElasticSearchResponse) -> Self {
        MultiSearchSingleResponse {
            status: StatusCode::OK,
            response: Some(response),
            error: None,
        }
    }
}

impl From<ElasticSearchError> for MultiSearchSingleResponse {
    fn from(error: ElasticSearchError) -> Self {
        MultiSearchSingleResponse {
            status: error.status,
            response: None,
            error: Some(error.error),
        }
    }
}
