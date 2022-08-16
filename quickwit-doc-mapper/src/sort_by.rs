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

use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, FieldType, Schema};
use tantivy::Order as TantivyOrder;

// TODO: Move to `quickwit-config` when `quickwit-config` no longer depends on
// `quickwit-doc-mapper`.

/// Sort order, either ascending or descending.
/// Use `SortOrder::Desc` for top-k queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    /// Ascending order.
    Asc,
    /// Descending order, the default.
    Desc,
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::Desc
    }
}

impl From<SortOrder> for TantivyOrder {
    fn from(order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => TantivyOrder::Asc,
            SortOrder::Desc => TantivyOrder::Desc,
        }
    }
}

/// Specifies how documents are sorted.
/// In case of a tie, the documents are ordered according to descending `(split_id, segment_ord,
/// doc_id)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortByField {
    /// Name of the field to sort by.
    pub field_name: String,
    /// Order to sort by. A usual top-k search implies a descending order.
    pub order: SortOrder,
}

impl From<String> for SortByField {
    fn from(string: String) -> Self {
        let (field_name, order) = if let Some(rest) = string.strip_prefix('+') {
            (rest.trim().to_string(), SortOrder::Asc)
        } else if let Some(rest) = string.strip_prefix('-') {
            (rest.trim().to_string(), SortOrder::Desc)
        } else {
            (string.trim().to_string(), SortOrder::Asc)
        };
        SortByField { field_name, order }
    }
}

/// Specifies how documents are sorted.
/// In case of a tie, the documents are ordered according to descending `(split_id, segment_ord,
/// doc_id)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortBy {
    /// Sort by document ID.
    DocId,
    /// Sort by a specific field. The field must be a fast field.
    FastField {
        /// Name of the field to sort by.
        field_name: String,
        /// Order to sort by. A usual top-k search implies a descending order.
        order: SortOrder,
    },
    /// Sort by BM25 score.
    Score {
        /// Order to sort by. A usual top-k search implies a descending order.
        order: SortOrder,
    },
}

pub(crate) fn validate_sort_by_field_name(
    field_name: &str,
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if field_name == "_score" {
        return validate_sort_by_score(schema, search_fields_opt);
    }
    let sort_by_field = schema
        .get_field(field_name)
        .with_context(|| format!("Unknown sort by field: `{}`", field_name))?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);

    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        bail!(
            "Sort by field on type text is currently not supported `{}`.",
            field_name
        )
    }
    if !sort_by_field_entry.is_fast() {
        bail!(
            "Sort by field must be a fast field, please add the fast property to your field `{}`.",
            field_name
        )
    }

    Ok(())
}

fn validate_sort_by_score(
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if let Some(fields) = search_fields_opt {
        for field in fields {
            if !schema.get_field_entry(*field).has_fieldnorms() {
                bail!(
                    "Fieldnorms for field `{}` is missing. Fieldnorms must be stored for the \
                     field to compute the BM25 score of the documents.",
                    schema.get_field_name(*field)
                )
            }
        }
    }
    Ok(())
}

impl Default for SortBy {
    fn default() -> Self {
        Self::DocId
    }
}

impl From<&SearchRequest> for SortBy {
    fn from(req: &SearchRequest) -> Self {
        if let Some(ref sort_by_field) = req.sort_by_field {
            if *sort_by_field == "_score" {
                return SortBy::Score {
                    order: req
                        .sort_order
                        .map(|sort_order| sort_order.into())
                        .unwrap_or_default(),
                };
            }
            SortBy::FastField {
                field_name: sort_by_field.to_string(),
                order: req
                    .sort_order
                    .map(|sort_order| sort_order.into())
                    .unwrap_or_default(),
            }
        } else {
            SortBy::DocId
        }
    }
}

// Quickwit Proto SortOrder
impl From<i32> for SortOrder {
    fn from(sort: i32) -> Self {
        match sort {
            0 => Self::Asc,
            1 => Self::Desc,
            _ => panic!("unexpected value {}", sort),
        }
    }
}
