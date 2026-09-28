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

use quickwit_proto::metastore::SplitSortField;
use quickwit_proto::search::{SortFieldType, SortOrder};
use serde::{Deserialize, Serialize};

/// One resolved field in an [`IndexingSortSchema`](crate::IndexingSortSchema).
/// Missing values sort first ascending and last descending. The type is required even when
/// every document is missing the field.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SortFieldMetadata {
    /// Mapped field path.
    pub field: String,
    /// Physical sort direction.
    pub order: SortOrder,
    /// Logical comparison type, taken from the schema used to write this split.
    #[serde(rename = "type", deserialize_with = "deserialize_sort_field_type")]
    pub field_type: SortFieldType,
}

fn deserialize_sort_field_type<'de, D>(deserializer: D) -> Result<SortFieldType, D::Error>
where D: serde::Deserializer<'de> {
    let field_type = SortFieldType::deserialize(deserializer)?;
    if field_type == SortFieldType::Unspecified {
        return Err(serde::de::Error::custom(
            "sort field type must be specified",
        ));
    }
    Ok(field_type)
}

impl TryFrom<SplitSortField> for SortFieldMetadata {
    type Error = anyhow::Error;

    fn try_from(sort: SplitSortField) -> anyhow::Result<Self> {
        let field_type = SortFieldType::try_from(sort.field_type)?;
        anyhow::ensure!(
            field_type != SortFieldType::Unspecified,
            "missing recovery sort field type for `{}`",
            sort.field
        );
        Ok(Self {
            field: sort.field,
            order: if sort.descending {
                SortOrder::Desc
            } else {
                SortOrder::Asc
            },
            field_type,
        })
    }
}

impl From<&SortFieldMetadata> for SplitSortField {
    fn from(sort: &SortFieldMetadata) -> Self {
        Self {
            field: sort.field.clone(),
            descending: sort.order == SortOrder::Desc,
            field_type: sort.field_type as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field_metadata_roundtrip() {
        for (field_type, json_type) in [
            (SortFieldType::Text, "text"),
            (SortFieldType::I64, "i64"),
            (SortFieldType::U64, "u64"),
            (SortFieldType::F64, "f64"),
            (SortFieldType::Datetime, "datetime"),
            (SortFieldType::Bytes, "bytes"),
            (SortFieldType::Bool, "bool"),
        ] {
            for order in [SortOrder::Asc, SortOrder::Desc] {
                let metadata = SortFieldMetadata {
                    field: "service".to_string(),
                    order,
                    field_type,
                };
                let json = serde_json::to_value(&metadata).unwrap();
                assert_eq!(json["type"], json_type);
                assert_eq!(
                    serde_json::from_value::<SortFieldMetadata>(json).unwrap(),
                    metadata
                );
                let recovery_field = SplitSortField::from(&metadata);
                assert_eq!(
                    SortFieldMetadata::try_from(recovery_field).unwrap(),
                    metadata
                );
            }
        }
    }
}
