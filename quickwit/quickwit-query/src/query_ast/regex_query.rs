// Copyright (C) 2024 Quickwit, Inc.
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

use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};
use tantivy::Term;

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::{AutomatonQuery, JsonPathPrefix, TantivyQueryAst};
use crate::tokenizers::TokenizerManager;
use crate::{find_field_or_hit_dynamic, InvalidQuery};

/// A Wildcard query allows to match 'bond' with a query like 'b*d'.
///
/// At the moment, only wildcard at end of term is supported.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct RegexQuery {
    pub field: String,
    pub regex: String,
}

impl From<RegexQuery> for QueryAst {
    fn from(regex_query: RegexQuery) -> Self {
        Self::Regex(regex_query)
    }
}

impl RegexQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, regex: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            regex: regex.to_string(),
        }
    }
}

impl RegexQuery {
    pub fn to_regex(
        &self,
        schema: &TantivySchema,
    ) -> Result<(Field, Option<Vec<u8>>, String), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type = field_entry.field_type();

        match field_type {
            FieldType::Str(ref text_options) => {
                text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;

                Ok((field, None, self.regex.clone()))
            }
            FieldType::JsonObject(json_options) => {
                json_options.get_text_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;

                let mut term_for_path = Term::from_field_json_path(
                    field,
                    json_path,
                    json_options.is_expand_dots_enabled(),
                );
                term_for_path.append_type_and_str("");

                let value = term_for_path.value();
                // We skip the 1st byte which is a marker to tell this is json. This isn't present
                // in the dictionary
                let byte_path_prefix = value.as_serialized()[1..].to_owned();
                Ok((field, Some(byte_path_prefix), self.regex.clone()))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a regex query on a non-text field".to_string(),
            )),
        }
    }
}

impl BuildTantivyAst for RegexQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, path, regex) = self.to_regex(schema)?;
        let regex = tantivy_fst::Regex::new(&regex).context("failed to parse regex")?;
        let regex_automaton_with_path = JsonPathPrefix {
            prefix: path.unwrap_or_default(),
            automaton: regex,
        };
        let regex_query_with_path = AutomatonQuery {
            field,
            automaton: Arc::new(regex_automaton_with_path),
        };
        Ok(regex_query_with_path.into())
    }
}
