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

use quickwit_proto::search::SortOrder;
use serde::{Deserialize, Serialize};

/// A configured physical sort field. Its logical type is resolved from the split's schema.
/// Currently only raw strings are supported. Missing values sort first ascending, last descending.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexingSortField {
    pub field: String,
    #[serde(default)]
    pub order: SortOrder,
}

/// Index configuration uses field names prefixed with `-` for descending order.
/// The configured declaration does not include a type; split metadata records the resolved type.
pub(super) mod config {
    use quickwit_proto::search::parse_sort_fields;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{IndexingSortField, SortOrder};

    pub fn serialize<S>(
        sort_fields: &[IndexingSortField],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields: Vec<String> = sort_fields
            .iter()
            .map(|sort| match sort.order {
                SortOrder::Asc => sort.field.clone(),
                SortOrder::Desc => format!("-{}", sort.field),
            })
            .collect();
        fields.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<IndexingSortField>, D::Error>
    where D: Deserializer<'de> {
        let fields = Vec::<String>::deserialize(deserializer)?;
        let mut sort_fields = Vec::with_capacity(fields.len());
        let parsed_fields = parse_sort_fields(&fields, SortOrder::Asc);
        for (expression, (field, order)) in fields.iter().zip(parsed_fields) {
            if field.is_empty()
                || field.starts_with(['+', '-'])
                || field.chars().any(char::is_whitespace)
            {
                return Err(serde::de::Error::custom(format!(
                    "invalid sort field `{expression}`: expected a field name optionally prefixed \
                     with `+` or `-`"
                )));
            }
            sort_fields.push(IndexingSortField { field, order });
        }
        Ok(sort_fields)
    }
}
