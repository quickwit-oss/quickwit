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

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use quickwit_query::BooleanOperand;
use quickwit_search::SearchError;
use serde::{Deserialize, Serialize};

use super::super::TrackTotalHits;
use super::MultiSearchHeader;
use crate::elasticsearch_api::model::{SortField, default_elasticsearch_sort_order};
use crate::simple_list::{from_simple_list, to_simple_list};

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchQueryParams {
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub _source: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub _source_excludes: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub _source_includes: Option<Vec<String>>,
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub allow_partial_search_results: Option<bool>,
    #[serde(default)]
    pub analyze_wildcard: Option<bool>,
    #[serde(default)]
    pub analyzer: Option<String>,
    #[serde(default)]
    pub batched_reduce_size: Option<u64>,
    #[serde(default)]
    pub ccs_minimize_roundtrips: Option<bool>,
    #[serde(default)]
    pub default_operator: Option<BooleanOperand>,
    #[serde(default)]
    pub df: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub docvalue_fields: Option<Vec<String>>,
    #[serde(default)]
    pub error_trace: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub explain: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    /// Additional filters to be applied to the query.
    /// Useful for permissions and other use cases.
    /// This is not part of the official Elasticsearch API.
    pub extra_filters: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub filter_path: Option<Vec<String>>,
    #[serde(default)]
    pub force_synthetic_source: Option<bool>,
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub human: Option<bool>,
    #[serde(default)]
    pub ignore_throttled: Option<bool>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde(default)]
    pub lenient: Option<bool>,
    #[serde(default)]
    pub max_concurrent_shard_requests: Option<u64>,
    #[serde(default)]
    pub min_compatible_shard_node: Option<String>,
    #[serde(default)]
    pub pre_filter_shard_size: Option<u64>,
    #[serde(default)]
    pub preference: Option<String>,
    #[serde(default)]
    pub pretty: Option<bool>,
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub request_cache: Option<bool>,
    #[serde(default)]
    pub rest_total_hits_as_int: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub routing: Option<Vec<String>>,
    #[serde(default)]
    pub scroll: Option<String>,
    #[serde(default)]
    pub seq_no_primary_term: Option<bool>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub sort: Option<Vec<String>>,
    #[serde(default)]
    pub stats: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub stored_fields: Option<Vec<String>>,
    #[serde(default)]
    pub suggest_field: Option<String>,
    #[serde(default)]
    pub suggest_mode: Option<SuggestMode>,
    #[serde(default)]
    pub suggest_size: Option<u64>,
    #[serde(default)]
    pub suggest_text: Option<String>,
    #[serde(default)]
    pub terminate_after: Option<u64>,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub track_scores: Option<bool>,
    #[serde(default)]
    pub track_total_hits: Option<TrackTotalHits>,
    #[serde(default)]
    pub typed_keys: Option<bool>,
    #[serde(default)]
    pub version: Option<bool>,
}

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchQueryParamsCount {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub analyze_wildcard: Option<bool>,
    #[serde(default)]
    pub analyzer: Option<String>,
    #[serde(default)]
    pub default_operator: Option<BooleanOperand>,
    #[serde(default)]
    pub df: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_throttled: Option<bool>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde(default)]
    pub lenient: Option<bool>,
    #[serde(default)]
    pub max_concurrent_shard_requests: Option<u64>,
    #[serde(default)]
    pub preference: Option<String>,
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub request_cache: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub routing: Option<Vec<String>>,
}
impl From<SearchQueryParamsCount> for SearchQueryParams {
    fn from(value: SearchQueryParamsCount) -> Self {
        SearchQueryParams {
            allow_no_indices: value.allow_no_indices,
            analyze_wildcard: value.analyze_wildcard,
            analyzer: value.analyzer,
            default_operator: value.default_operator,
            df: value.df,
            expand_wildcards: value.expand_wildcards,
            ignore_throttled: value.ignore_throttled,
            ignore_unavailable: value.ignore_unavailable,
            preference: value.preference,
            q: value.q,
            request_cache: value.request_cache,
            routing: value.routing,
            size: Some(0),
            ..Default::default()
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteQueryParams {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde(default)]
    pub master_timeout: Option<String>,
    #[serde(default)]
    pub timeout: Option<String>,
}

/// Parses a string as if it was a json value string.
fn parse_str_like_json<T: serde::de::DeserializeOwned>(s: &str) -> Option<T> {
    let json_value = serde_json::Value::String(s.to_string());
    serde_json::from_value::<T>(json_value).ok()
}

// Parse a single sort field parameter from ES sort query string parameter.
fn parse_sort_field_str(sort_field_str: &str) -> Result<SortField, SearchError> {
    if let Some((field, order_str)) = sort_field_str.split_once(':') {
        let order = parse_str_like_json(order_str).ok_or_else(|| {
            SearchError::InvalidArgument(format!(
                "invalid sort order `{field}`. expected `asc` or `desc`"
            ))
        })?;
        Ok(SortField {
            field: field.to_string(),
            order,
            date_format: None,
        })
    } else {
        let order = default_elasticsearch_sort_order(sort_field_str);
        Ok(SortField {
            field: sort_field_str.to_string(),
            order,
            date_format: None,
        })
    }
}

impl SearchQueryParams {
    /// Accessor for the list of sort fields passed in the sort query string parameter.
    ///
    /// Returns an error if the sort query string are not in the expected format
    /// (`field:order,field2:order2,...`). Returns `Ok(None)` if the sort query string parameter
    /// is not present.
    #[allow(clippy::type_complexity)]
    pub(crate) fn sort_fields(&self) -> Result<Option<Vec<SortField>>, SearchError> {
        let Some(sort_fields_str) = self.sort.as_ref() else {
            return Ok(None);
        };
        let mut sort_fields: Vec<SortField> = Vec::with_capacity(sort_fields_str.len());
        for sort_field_str in sort_fields_str {
            sort_fields.push(parse_sort_field_str(sort_field_str)?);
        }
        Ok(Some(sort_fields))
    }

    /// Returns the scroll duration supplied by the user.
    ///
    /// This function returns an error if the scroll duration is not in the expected format. (`40s`
    /// etc.)
    pub fn parse_scroll_ttl(&self) -> Result<Option<Duration>, SearchError> {
        let Some(scroll_str) = self.scroll.as_ref() else {
            return Ok(None);
        };
        let duration: Duration = humantime::parse_duration(scroll_str).map_err(|_err| {
            SearchError::InvalidArgument(format!("invalid scroll duration: `{scroll_str}`"))
        })?;
        Ok(Some(duration))
    }

    pub fn allow_partial_search_results(&self) -> bool {
        // By default, elastic search allows partial results.
        self.allow_partial_search_results.unwrap_or(true)
    }
}

#[doc = "Whether to expand wildcard expression to concrete indices that are open, closed or both."]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ExpandWildcards {
    Open,
    Closed,
    Hidden,
    None,
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

impl fmt::Display for ExpandWildcards {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Open => write!(formatter, "open"),
            Self::Closed => write!(formatter, "closed"),
            Self::Hidden => write!(formatter, "hidden"),
            Self::None => write!(formatter, "none"),
            Self::All => write!(formatter, "all"),
        }
    }
}

impl From<MultiSearchHeader> for SearchQueryParams {
    fn from(multi_search_header: MultiSearchHeader) -> Self {
        SearchQueryParams {
            allow_no_indices: multi_search_header.allow_no_indices,
            expand_wildcards: multi_search_header.expand_wildcards,
            ignore_unavailable: multi_search_header.ignore_unavailable,
            routing: multi_search_header.routing,
            request_cache: multi_search_header.request_cache,
            preference: multi_search_header.preference,
            ..Default::default()
        }
    }
}

/// Specify suggest mode
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum SuggestMode {
    Missing,
    Popular,
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

impl fmt::Display for SuggestMode {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Missing => write!(formatter, "missing"),
            Self::Popular => write!(formatter, "popular"),
            Self::Always => write!(formatter, "always"),
        }
    }
}

#[cfg(test)]
mod tests {

    use quickwit_proto::search::SortOrder;

    use super::*;

    #[derive(Deserialize, PartialEq, Eq, Debug)]
    #[serde(rename_all = "snake_case")]
    enum TestEnum {
        FirstItem,
        SecondItem,
    }

    #[test]
    fn test_parse_str_like_json() {
        assert_eq!(
            parse_str_like_json::<TestEnum>("first_item").unwrap(),
            TestEnum::FirstItem
        );
        assert!(parse_str_like_json::<TestEnum>("FirstItem").is_none());
    }

    #[test]
    fn test_sort_order_qs() {
        let sort_order_qs = parse_sort_field_str("timestamp:desc").unwrap();
        assert_eq!(
            sort_order_qs,
            SortField {
                field: "timestamp".to_string(),
                order: SortOrder::Desc,
                date_format: None
            }
        );
        let sort_order_qs = parse_sort_field_str("timestamp:asc").unwrap();
        assert_eq!(
            sort_order_qs,
            SortField {
                field: "timestamp".to_string(),
                order: SortOrder::Asc,
                date_format: None
            }
        );
    }
}
