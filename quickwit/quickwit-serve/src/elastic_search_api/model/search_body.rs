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

use std::collections::BTreeSet;
use std::fmt;

use quickwit_proto::search::SortOrder;
use quickwit_query::{ElasticQueryDsl, OneFieldMap};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::elastic_search_api::model::{default_elasticsearch_sort_order, SortField};
use crate::elastic_search_api::TrackTotalHits;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct FieldSortParams {
    #[serde(default)]
    pub order: Option<SortOrder>,
}

#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
pub struct SearchBody {
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub query: Option<ElasticQueryDsl>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_field_sorts")]
    pub sort: Option<Vec<SortField>>,
    #[serde(default)]
    pub aggs: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub track_total_hits: Option<TrackTotalHits>,
    #[serde(default)]
    pub stored_fields: Option<BTreeSet<String>>,
}

struct FieldSortVecVisitor;

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrMapFieldSort {
    FieldNameOnly(String),
    Sort(OneFieldMap<FieldSortParams>),
}

impl From<StringOrMapFieldSort> for SortField {
    fn from(string_or_map_field_sort: StringOrMapFieldSort) -> Self {
        match string_or_map_field_sort {
            StringOrMapFieldSort::FieldNameOnly(field_name) => {
                let order = default_elasticsearch_sort_order(&field_name);
                SortField {
                    field: field_name,
                    order,
                }
            }
            StringOrMapFieldSort::Sort(sort) => {
                let order = sort
                    .value
                    .order
                    .unwrap_or_else(|| default_elasticsearch_sort_order(&sort.field));
                SortField {
                    field: sort.field,
                    order,
                }
            }
        }
    }
}

impl<'de> Visitor<'de> for FieldSortVecVisitor {
    type Value = Vec<SortField>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("An array containing the sort fields.")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Vec<SortField>, A::Error>
    where A: serde::de::SeqAccess<'de> {
        let mut sort_fields: Vec<SortField> = Vec::new();
        while let Some(field_sort) = seq.next_element::<StringOrMapFieldSort>()? {
            sort_fields.push(field_sort.into());
        }
        Ok(sort_fields)
    }

    fn visit_map<M>(self, mut map: M) -> Result<Vec<SortField>, M::Error>
    where M: MapAccess<'de> {
        let mut sort_fields: Vec<SortField> = Vec::new();
        while let Some((field_sort_key, field_sort_params)) =
            map.next_entry::<String, FieldSortParams>()?
        {
            let sort_order = field_sort_params
                .order
                .unwrap_or_else(|| default_elasticsearch_sort_order(&field_sort_key));
            sort_fields.push(SortField {
                field: field_sort_key,
                order: sort_order,
            });
        }
        Ok(sort_fields)
    }
}

/// ES accepts structs to describe the sort field.
/// In that case the order of apparition in the JSON object matters.
fn deserialize_field_sorts<'de, D>(deserializer: D) -> Result<Option<Vec<SortField>>, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_any(FieldSortVecVisitor).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field_array() {
        let json = r#"
        {
            "sort": [
                { "timestamp": { "order": "desc" } },
                { "uid": { "order": "asc" } },
                { "hello": {}},
                { "_score": {}}
            ]
        }
        "#;
        let search_body: super::SearchBody = serde_json::from_str(json).unwrap();
        let sort_fields = search_body.sort.unwrap();
        assert_eq!(sort_fields.len(), 4);
        assert_eq!(sort_fields[0].field, "timestamp");
        assert_eq!(sort_fields[0].order, SortOrder::Desc);
        assert_eq!(sort_fields[1].field, "uid");
        assert_eq!(sort_fields[1].order, SortOrder::Asc);
        assert_eq!(sort_fields[2].field, "hello");
        assert_eq!(sort_fields[2].order, SortOrder::Asc);
        assert_eq!(sort_fields[3].field, "_score");
        assert_eq!(sort_fields[3].order, SortOrder::Desc);
    }

    #[test]
    fn test_sort_field_obj() {
        let json = r#"
        {
            "sort": {
                "timestamp": { "order": "desc" },
                "uid": { "order": "asc" }
            }
        }
        "#;
        let search_body: super::SearchBody = serde_json::from_str(json).unwrap();
        let field_sorts = search_body.sort.unwrap();
        assert_eq!(field_sorts.len(), 2);
        assert_eq!(field_sorts[0].field, "timestamp");
        assert_eq!(field_sorts[0].order, SortOrder::Desc);
        assert_eq!(field_sorts[1].field, "uid");
        assert_eq!(field_sorts[1].order, SortOrder::Asc);
    }

    #[test]
    fn test_sort_default_orders() {
        let json = r#"
        {
            "sort": [
                "timestamp",
                "uid",
                "_score",
                "_doc"
            ]
        }
        "#;
        let search_body: super::SearchBody = serde_json::from_str(json).unwrap();
        let field_sorts = search_body.sort.unwrap();
        assert_eq!(field_sorts.len(), 4);
        assert_eq!(field_sorts[0].field, "timestamp");
        assert_eq!(field_sorts[0].order, SortOrder::Asc);
        assert_eq!(field_sorts[1].field, "uid");
        assert_eq!(field_sorts[1].order, SortOrder::Asc);
        assert_eq!(field_sorts[2].field, "_score");
        assert_eq!(field_sorts[2].order, SortOrder::Desc);
        assert_eq!(field_sorts[3].field, "_doc");
        assert_eq!(field_sorts[3].order, SortOrder::Asc);
    }
}
