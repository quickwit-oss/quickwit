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

use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::query::PhrasePrefixQuery as TantivyPhrasePrefixQuery;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, FullTextParams, QueryAst};
use crate::tokenizers::TokenizerManager;
use crate::{InvalidQuery, find_field_or_hit_dynamic};

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
    pub params: FullTextParams,
    /// Support missing fields
    pub lenient: bool,
}

impl PhrasePrefixQuery {
    pub fn get_terms(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(Field, Vec<(usize, Term)>), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)
            .ok_or_else(|| InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            })?;
        let field_type = field_entry.field_type();

        match field_type {
            FieldType::Str(text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                let terms = self.params.tokenize_text_into_terms(
                    field,
                    &self.phrase,
                    text_field_indexing,
                    tokenizer_manager,
                )?;
                if !text_field_indexing.index_option().has_positions() && terms.len() > 1 {
                    return Err(InvalidQuery::SchemaError(
                        "trying to run a phrase prefix query on a field which does not have \
                         positions indexed"
                            .to_string(),
                    ));
                }
                Ok((field, terms))
            }
            FieldType::JsonObject(json_options) => {
                let text_field_indexing =
                    json_options.get_text_indexing_options().ok_or_else(|| {
                        InvalidQuery::SchemaError(format!(
                            "field {} is not full-text searchable",
                            field_entry.name()
                        ))
                    })?;
                let terms = self.params.tokenize_text_into_terms_json(
                    field,
                    json_path,
                    &self.phrase,
                    json_options,
                    tokenizer_manager,
                )?;
                if !text_field_indexing.index_option().has_positions() && terms.len() > 1 {
                    return Err(InvalidQuery::SchemaError(
                        "trying to run a PhrasePrefix query on a field which does not have \
                         positions indexed"
                            .to_string(),
                    ));
                }
                Ok((field, terms))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a PhrasePrefix query on a non-text field".to_string(),
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
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (_, terms) = match self.get_terms(context.schema, context.tokenizer_manager) {
            Ok(res) => res,
            Err(InvalidQuery::FieldDoesNotExist { .. }) if self.lenient => {
                return Ok(TantivyQueryAst::match_none());
            }
            Err(e) => return Err(e),
        };

        if terms.is_empty() {
            if self.params.zero_terms_query.is_none() {
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
