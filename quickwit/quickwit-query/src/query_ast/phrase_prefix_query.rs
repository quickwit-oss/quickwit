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
use tantivy::query::PhrasePrefixQuery as TantivyPhrasePrefixQuery;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};
use tantivy::Term;

use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, FullTextParams, QueryAst};
use crate::{find_field_or_hit_dynamic, InvalidQuery};

/// The PhraseQuery node is meant to be tokenized and searched.
///
/// If after tokenization, a single term is emitted, it will naturally be
/// produce a tantivy TermQuery.
/// If not terms is emitted, it will produce a query that match no documents..
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PhrasePrefixQuery {
    pub field: String,
    pub phrase: String,
    pub max_expansions: u32,
    pub analyzer: FullTextParams,
}

impl PhrasePrefixQuery {
    pub fn get_terms(
        &self,
        schema: &TantivySchema,
    ) -> Result<(Field, Vec<(usize, Term)>), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type = field_entry.field_type();

        match field_type {
            FieldType::Str(ref text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "Field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                if !text_field_indexing.index_option().has_positions() {
                    return Err(InvalidQuery::SchemaError(
                        "Trying to run a PhrasePrefix query on a field which does not have \
                         positions indexed."
                            .to_string(),
                    ));
                }

                let terms = self.analyzer.tokenize_text_into_terms(
                    field,
                    &self.phrase,
                    text_field_indexing,
                )?;
                Ok((field, terms))
            }
            FieldType::JsonObject(json_options) => {
                let text_field_indexing =
                    json_options.get_text_indexing_options().ok_or_else(|| {
                        InvalidQuery::SchemaError(format!(
                            "Field {} is not full-text searchable",
                            field_entry.name()
                        ))
                    })?;
                if !text_field_indexing.index_option().has_positions() {
                    return Err(InvalidQuery::SchemaError(
                        "Trying to run a PhrasePrefix query on a field which does not have \
                         positions indexed."
                            .to_string(),
                    ));
                }
                let terms = self.analyzer.tokenize_text_into_terms_json(
                    field,
                    json_path,
                    &self.phrase,
                    json_options,
                )?;
                Ok((field, terms))
            }
            _ => Err(InvalidQuery::SchemaError(
                "Trying to run a PhrasePrefix query on a non-text field.".to_string(),
            )),
        }
    }
}

impl From<PhrasePrefixQuery> for QueryAst {
    fn from(phrase_query: PhrasePrefixQuery) -> Self {
        QueryAst::PhrasePrefix(phrase_query)
    }
}

impl BuildTantivyAst for PhrasePrefixQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (_, terms) = self.get_terms(schema)?;

        if terms.is_empty() {
            if self.analyzer.zero_terms_query.is_none() {
                Ok(TantivyQueryAst::match_none())
            } else {
                Ok(TantivyQueryAst::match_all())
            }
        } else {
            let mut phrase_prefix_query = TantivyPhrasePrefixQuery::new_with_offset(terms);
            phrase_prefix_query.set_max_expansions(self.max_expansions);
            Ok(phrase_prefix_query.into())
        }
    }
}
