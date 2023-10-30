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

use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, Schema as TantivySchema};
use tantivy::tokenizer::TokenizerManager;
use tantivy::Term;

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::{FullTextParams, TantivyQueryAst};
use crate::{find_field_or_hit_dynamic, InvalidQuery};

/// The TermQuery acts exactly like a FullTextQuery with
/// a raw tokenizer.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct WildcardQuery {
    pub field: String,
    pub value: String,
}

impl From<WildcardQuery> for QueryAst {
    fn from(wildcard_query: WildcardQuery) -> Self {
        Self::Wildcard(wildcard_query)
    }
}

impl WildcardQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, value: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

impl WildcardQuery {
    // TODO this method will probably disappear once we support the full semantic of
    // wildcard queries
    pub fn extract_prefix_term(
        &self,
        schema: &TantivySchema,
    ) -> Result<(Field, Term), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type = field_entry.field_type();

        let prefix = &self.value[..self.value.len() - 1];

        // TODO have a boolean should_lowercase in the tokenizer manager, use it to choose between
        // raw and lowercaser text options
        // match field_type {
        // FieldType::Str(ref text_options) => {
        // let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
        // InvalidQuery::SchemaError(format!(
        // "field {} is not full-text searchable",
        // field_entry.name()
        // ))
        // })?;
        // let terms = self.params.tokenize_text_into_terms(
        // field,
        // &self.phrase,
        // text_field_indexing,
        // tokenizer_manager,
        // )?;
        // if !text_field_indexing.index_option().has_positions() && terms.len() > 1 {
        // return Err(InvalidQuery::SchemaError(
        // "trying to run a phrase prefix query on a field which does not have \
        // positions indexed"
        // .to_string(),
        // ));
        // }
        // Ok((field, terms))
        // }
        // FieldType::JsonObject(json_options) => {
        // let text_field_indexing =
        // json_options.get_text_indexing_options().ok_or_else(|| {
        // InvalidQuery::SchemaError(format!(
        // "field {} is not full-text searchable",
        // field_entry.name()
        // ))
        // })?;
        // let terms = self.params.tokenize_text_into_terms_json(
        // field,
        // json_path,
        // &self.phrase,
        // json_options,
        // tokenizer_manager,
        // )?;
        // if !text_field_indexing.index_option().has_positions() && terms.len() > 1 {
        // return Err(InvalidQuery::SchemaError(
        // "trying to run a PhrasePrefix query on a field which does not have \
        // positions indexed"
        // .to_string(),
        // ));
        // }
        // Ok((field, terms))
        // }
        // _ => Err(InvalidQuery::SchemaError(
        // "trying to run a Wildcard query on a non-text field".to_string(),
        // )),
        // }

        return Ok((field, todo!()));
    }
}

impl BuildTantivyAst for WildcardQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        // TODO handle escaped wildcard and questionmarks?
        let mut chars = self.value.chars();
        if chars.next() != Some('*') {
            todo!("error");
        }
        if chars.any(|c| c == '*' || c == '?') {
            todo!("error again");
        }

        let (_, term) = self.extract_prefix_term(schema)?;

        let mut phrase_prefix_query =
            tantivy::query::PhrasePrefixQuery::new_with_offset(vec![(0, term)]);
        phrase_prefix_query.set_max_expansions(u32::MAX);
        Ok(phrase_prefix_query.into())
    }
}
