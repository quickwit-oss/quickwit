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

use quickwit_query::ElasticQueryDsl;
use serde::{Deserialize, Deserializer, Serialize};

use super::{search_query_params::ExpandWildcards, search_body::FieldSort};
use crate::simple_list::{from_simple_list, to_simple_list};

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
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

/// Generic wrapper that allow one or more occurrences of specified type.
///
/// In YAML it will presented or as a value, or as an array:
/// ```yaml
/// one: just a string
/// many:
///   - 1st string
///   - 2nd string
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialOrd, Ord, PartialEq)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    /// NO value
    None(),
    /// Single value
    One(T),
    /// Array of values
    Vec(Vec<T>),
}
impl<T> From<OneOrMany<T>> for Vec<T> {
    fn from(from: OneOrMany<T>) -> Self {
        match from {
            OneOrMany::None() => vec![],
            OneOrMany::One(val) => vec![val],
            OneOrMany::Vec(vec) => vec,
        }
    }
}

pub fn deserialize_type_from<'de: 'a, 'a, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where D: Deserializer<'de> {
    Ok(Some(Vec::from(OneOrMany::deserialize(deserializer)?)))
}

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct RequestHeader {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_type_from")]
    pub index: Option<Vec<String>>,
    #[serde(default)]
    pub preference: Option<String>,
    #[serde(default)]
    pub request_cache: Option<bool>,
    #[serde(default)]
    pub routing: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
pub struct RequestBody {
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub query: Option<ElasticQueryDsl>,
    #[serde(alias = "aggregations")]
    #[serde(default)]
    pub aggs: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub sort: Option<Vec<FieldSort>>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_deserialize_request_header() {
        {
            let json = r#"
            {
                "ignore_unavailable": true,
                "index": "test_index"
            }
            "#;
            let request_header: super::RequestHeader = serde_json::from_str(json).unwrap();
            assert_eq!(request_header.index, Some(vec!["test_index".to_string()]));
            assert_eq!(request_header.ignore_unavailable, Some(true));
        }
        {
            let json = r#"
            {
                "preference": "test_preference",
                "request_cache": true,
                "routing": ["test_routing"]
            }
            "#;
            let request_header: super::RequestHeader = serde_json::from_str(json).unwrap();
            assert_eq!(request_header.index, None);
            assert_eq!(
                request_header.preference,
                Some("test_preference".to_string())
            );
            assert_eq!(request_header.request_cache, Some(true));
            assert_eq!(
                request_header.routing,
                Some(vec!["test_routing".to_string()])
            );
        }
        {
            let json = r#"
            {
                "preference": "test_preference",
                "request_cache": true,
                "routing": ["test_routing"],
                "index": ["test_index"]
            }
            "#;
            let request_header: super::RequestHeader = serde_json::from_str(json).unwrap();
            assert_eq!(request_header.index, Some(vec!["test_index".to_string()]));
            assert_eq!(
                request_header.preference,
                Some("test_preference".to_string())
            );
            assert_eq!(request_header.request_cache, Some(true));
            assert_eq!(
                request_header.routing,
                Some(vec!["test_routing".to_string()])
            );
        }
    }
}
