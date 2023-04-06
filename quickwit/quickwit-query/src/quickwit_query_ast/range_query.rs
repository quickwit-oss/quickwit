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

use std::ops::Bound;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;

use super::QueryAst;
use crate::json_literal::InterpretUserInput;
use crate::quickwit_query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::quickwit_query_ast::IntoTantivyAst;
use crate::JsonLiteral;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RangeQuery {
    pub field: String,
    pub lower_bound: Bound<JsonLiteral>,
    pub upper_bound: Bound<JsonLiteral>,
}

fn convert_bound<'a, T>(bound: &'a Bound<JsonLiteral>) -> Option<Bound<T>>
where T: InterpretUserInput<'a> {
    match bound {
        Bound::Included(val) => {
            let val = T::interpret(val)?;
            Some(Bound::Included(val))
        }
        Bound::Excluded(val) => {
            let val = T::interpret(val)?;
            Some(Bound::Excluded(val))
        }
        Bound::Unbounded => Some(Bound::Unbounded),
    }
}

impl From<RangeQuery> for QueryAst {
    fn from(range_query: RangeQuery) -> Self {
        QueryAst::Range(range_query)
    }
}

impl IntoTantivyAst for RangeQuery {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        let Some((_field, field_entry, _path)) = super::utils::find_field_or_hit_dynamic(&self.field, schema) else {
            return Ok(TantivyQueryAst::match_none());
        };
        if !field_entry.is_fast() {
            anyhow::bail!(
                "Range queries are only supported for fast fields. (`{}` is not a fast field)",
                field_entry.name()
            );
        }
        Ok(match field_entry.field_type() {
            tantivy::schema::FieldType::Str(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!(
                        "Expected string boundary, for field `{}`.",
                        field_entry.name()
                    )
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!(
                        "Expected string boundary, for field `{}`.",
                        field_entry.name()
                    )
                })?;
                tantivy::query::RangeQuery::new_str_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::U64(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!("Expected u64 boundary, for field `{}`.", field_entry.name())
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!("Expected u64 boundary, for field `{}`.", field_entry.name())
                })?;
                tantivy::query::RangeQuery::new_u64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::I64(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!("Expected i64 boundary, for field `{}`.", field_entry.name())
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!("Expected i64 boundary, for field `{}`.", field_entry.name())
                })?;
                tantivy::query::RangeQuery::new_i64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::F64(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!("Expected f64 boundary, for field `{}`.", field_entry.name())
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!("Expected f64 boundary, for field `{}`.", field_entry.name())
                })?;
                tantivy::query::RangeQuery::new_f64_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::Bool(_) => {
                anyhow::bail!(
                    "Range query on bool field `{}` is forbidden",
                    field_entry.name()
                )
            }
            tantivy::schema::FieldType::Date(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!(
                        "Expected datetime boundary, for field `{}`.",
                        field_entry.name()
                    )
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!(
                        "Expected datetime boundary, for field `{}`.",
                        field_entry.name()
                    )
                })?;
                tantivy::query::RangeQuery::new_date_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
            tantivy::schema::FieldType::Facet(_) => {
                anyhow::bail!(
                    "Range query on facet field `{}` is forbidden",
                    field_entry.name()
                )
            }
            tantivy::schema::FieldType::Bytes(_) => todo!(),
            tantivy::schema::FieldType::JsonObject(_) => todo!(),
            tantivy::schema::FieldType::IpAddr(_) => {
                let lower_bound = convert_bound(&self.lower_bound).with_context(|| {
                    format!("Expected ip boundary, for field `{}`.", field_entry.name())
                })?;
                let upper_bound = convert_bound(&self.upper_bound).with_context(|| {
                    format!("Expected ip boundary, for field `{}`.", field_entry.name())
                })?;
                tantivy::query::RangeQuery::new_ip_bounds(
                    self.field.clone(),
                    lower_bound,
                    upper_bound,
                )
            }
        }
        .into())
    }
}
