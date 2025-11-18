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

use elasticsearch_dsl::ErrorCause;
use serde::{Deserialize, Serialize};
use serde_with::formats::PreferMany;
use serde_with::{OneOrMany, serde_as};
use warp::hyper::StatusCode;

use super::ElasticsearchError;
use super::search_query_params::ExpandWildcards;
use super::search_response::ElasticsearchResponse;
use crate::simple_list::{from_simple_list, to_simple_list};

// Multi search doc: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html

#[serde_as]
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
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    /// Additional filters to be applied to the query.
    /// Useful for permissions and other use cases.
    /// This is not part of the official Elasticsearch API.
    ///
    /// This will set extra_filters on the search request.
    pub extra_filters: Option<Vec<String>>,
    #[serde(default)]
    pub ignore_throttled: Option<bool>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    /// List of indexes to search.
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default, rename = "index")]
    pub indexes: Vec<String>,
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
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    /// This is not part of the official Elasticsearch API.
    /// This will set source_excludes on the search request.
    pub _source_excludes: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    /// This is not part of the official Elasticsearch API.
    /// This will set source_includes on the search request.
    pub _source_includes: Option<Vec<String>>,
    #[serde(default)]
    pub typed_keys: Option<bool>,
}

#[serde_as]
#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiSearchHeader {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default, rename = "index")]
    pub indexes: Vec<String>,
    #[serde(default)]
    pub preference: Option<String>,
    #[serde(default)]
    pub request_cache: Option<bool>,
    #[serde(default)]
    pub routing: Option<Vec<String>>,
}

impl MultiSearchHeader {
    pub fn apply_query_param_defaults(&mut self, defaults: &MultiSearchQueryParams) {
        if self.allow_no_indices.is_none() {
            self.allow_no_indices = defaults.allow_no_indices;
        }
        if self.expand_wildcards.is_none() {
            self.expand_wildcards = defaults.expand_wildcards.clone();
        }
        if self.ignore_unavailable.is_none() {
            self.ignore_unavailable = defaults.ignore_unavailable;
        }
        if self.indexes.is_empty() {
            self.indexes = defaults.indexes.clone();
        }
        if self.routing.is_none() {
            self.routing = defaults.routing.clone();
        }
    }
}

#[derive(Serialize)]
pub struct MultiSearchResponse {
    pub responses: Vec<MultiSearchSingleResponse>,
}

#[derive(Serialize, Debug)]
pub struct MultiSearchSingleResponse {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub response: Option<ElasticsearchResponse>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorCause>,
}

impl From<ElasticsearchResponse> for MultiSearchSingleResponse {
    fn from(response: ElasticsearchResponse) -> Self {
        MultiSearchSingleResponse {
            status: StatusCode::OK,
            response: Some(response),
            error: None,
        }
    }
}

impl From<ElasticsearchError> for MultiSearchSingleResponse {
    fn from(error: ElasticsearchError) -> Self {
        MultiSearchSingleResponse {
            status: error.status,
            response: None,
            error: Some(error.error),
        }
    }
}
