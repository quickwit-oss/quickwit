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

use quickwit_proto::metastore::{SplitSortField, SplitSortFieldType};
use quickwit_proto::search::SortOrder;
use serde::{Deserialize, Serialize};

/// Logical comparison type of a physical sort field, resolved from the writer's schema.
/// This is not inferred from document values or from a segment's column encoding.
/// Declaring a type here does not enable indexing configuration support for that type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortValueType {
    /// Raw strings compared by their UTF-8 bytes.
    Text,
    /// Signed 64-bit integers.
    I64,
    /// Unsigned 64-bit integers.
    U64,
    /// 64-bit floating point numbers.
    F64,
    /// Datetimes.
    DateTime,
    /// Lexicographically ordered bytes.
    Bytes,
}

/// Resolved physical ordering of a split. All fields participate in merge compatibility.
/// Missing values sort first ascending and last descending. The type is required even when
/// every document is missing the field. Unsorted splits have an empty list of these declarations.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SortFieldMetadata {
    /// Mapped field path.
    pub field: String,
    /// Physical sort direction.
    pub order: SortOrder,
    /// Logical comparison type, taken from the schema used to write this split.
    #[serde(rename = "type")]
    pub field_type: SortValueType,
}

impl TryFrom<SplitSortField> for SortFieldMetadata {
    type Error = anyhow::Error;

    fn try_from(sort: SplitSortField) -> anyhow::Result<Self> {
        let field_type = match SplitSortFieldType::try_from(sort.field_type)? {
            SplitSortFieldType::Text => SortValueType::Text,
            SplitSortFieldType::I64 => SortValueType::I64,
            SplitSortFieldType::U64 => SortValueType::U64,
            SplitSortFieldType::F64 => SortValueType::F64,
            SplitSortFieldType::Datetime => SortValueType::DateTime,
            SplitSortFieldType::Bytes => SortValueType::Bytes,
            SplitSortFieldType::Unspecified => {
                anyhow::bail!("missing recovery sort field type for `{}`", sort.field);
            }
        };
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
        let field_type = match sort.field_type {
            SortValueType::Text => SplitSortFieldType::Text,
            SortValueType::I64 => SplitSortFieldType::I64,
            SortValueType::U64 => SplitSortFieldType::U64,
            SortValueType::F64 => SplitSortFieldType::F64,
            SortValueType::DateTime => SplitSortFieldType::Datetime,
            SortValueType::Bytes => SplitSortFieldType::Bytes,
        };
        Self {
            field: sort.field.clone(),
            descending: sort.order == SortOrder::Desc,
            field_type: field_type as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field_metadata_roundtrip() {
        for (field_type, json_type) in [
            (SortValueType::Text, "text"),
            (SortValueType::I64, "i64"),
            (SortValueType::U64, "u64"),
            (SortValueType::F64, "f64"),
            (SortValueType::DateTime, "datetime"),
            (SortValueType::Bytes, "bytes"),
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
