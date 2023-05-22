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

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::json_utils::JsonTermWriter;
use tantivy::query::{PhraseQuery as TantivyPhraseQuery, TermQuery as TantivyTermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, JsonObjectOptions, Schema as TantivySchema, TextFieldIndexing,
};
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use tantivy::Term;

use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};
use crate::query_ast::utils::full_text_query;
use crate::query_ast::{BuildTantivyAst, QueryAst};
use crate::{get_quickwit_tokenizer_manager, BooleanOperand, InvalidQuery, MatchAllOrNone};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullTextParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<String>,
    pub mode: FullTextMode,
    // How an empty query (no terms after tokenization) should be interpreted.
    // By default we match no documents.
    #[serde(default, skip_serializing_if = "MatchAllOrNone::is_none")]
    pub zero_terms_query: MatchAllOrNone,
}

impl FullTextParams {
    fn text_analyzer(
        &self,
        text_field_indexing: &TextFieldIndexing,
    ) -> anyhow::Result<TextAnalyzer> {
        let tokenizer_name: &str = self
            .tokenizer
            .as_deref()
            .unwrap_or(text_field_indexing.tokenizer());
        get_quickwit_tokenizer_manager()
            .get(tokenizer_name)
            .with_context(|| format!("No tokenizer named `{}` is registered.", tokenizer_name))
    }

    pub(crate) fn tokenize_text_into_terms_json(
        &self,
        field: Field,
        json_path: &str,
        text: &str,
        json_options: &JsonObjectOptions,
    ) -> anyhow::Result<Vec<(usize, Term)>> {
        let text_indexing_options = json_options
            .get_text_indexing_options()
            .with_context(|| format!("Json field text `{}` is not indexed", json_path))?;
        let text_analyzer: TextAnalyzer = self.text_analyzer(text_indexing_options)?;
        let mut token_stream: BoxTokenStream = text_analyzer.token_stream(text);
        let mut tokens = Vec::new();
        let mut term = Term::with_capacity(100);
        let mut json_term_writer = JsonTermWriter::from_field_and_json_path(
            field,
            json_path,
            json_options.is_expand_dots_enabled(),
            &mut term,
        );
        token_stream.process(&mut |token| {
            json_term_writer.set_str(&token.text);
            tokens.push((token.position, json_term_writer.term().clone()));
        });
        Ok(tokens)
    }

    pub(crate) fn tokenize_text_into_terms(
        &self,
        field: Field,
        text: &str,
        text_field_indexing: &TextFieldIndexing,
    ) -> anyhow::Result<Vec<(usize, Term)>> {
        let text_analyzer: TextAnalyzer = self.text_analyzer(text_field_indexing)?;
        let mut token_stream: BoxTokenStream = text_analyzer.token_stream(text);
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            let term: Term = Term::from_field_text(field, &token.text);
            tokens.push((token.position, term));
        });
        Ok(tokens)
    }

    pub(crate) fn make_query(
        &self,
        mut terms: Vec<(usize, Term)>,
        index_record_option: IndexRecordOption,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        if terms.is_empty() {
            return Ok(self.zero_terms_query.into());
        }
        if terms.len() == 1 {
            let term = terms.pop().unwrap().1;
            return Ok(TantivyTermQuery::new(term, IndexRecordOption::WithFreqs).into());
        }
        match self.mode {
            FullTextMode::Bool { operator } => {
                let term_query: Vec<TantivyQueryAst> = terms
                    .into_iter()
                    .map(|(_, term)| TantivyTermQuery::new(term, index_record_option).into())
                    .collect();
                Ok(TantivyBoolQuery::build_clause(operator, term_query).into())
            }
            FullTextMode::Phrase { slop } => {
                let mut phrase_query = TantivyPhraseQuery::new_with_offset(terms);
                phrase_query.set_slop(slop);
                Ok(phrase_query.into())
            }
            FullTextMode::PhraseFallbackToIntersection => {
                if index_record_option.has_positions() {
                    Ok(TantivyPhraseQuery::new_with_offset(terms).into())
                } else {
                    let term_query: Vec<TantivyQueryAst> = terms
                        .into_iter()
                        .map(|(_, term)| TantivyTermQuery::new(term, index_record_option).into())
                        .collect();
                    Ok(TantivyBoolQuery::build_clause(BooleanOperand::And, term_query).into())
                }
            }
        }
    }
}

fn is_zero(val: &u32) -> bool {
    *val == 0u32
}

/// `FullTextMode` describe how we should derive a query from a user sequence of tokens.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FullTextMode {
    // After tokenization, the different tokens should be used to
    // create a boolean clause (conjunction or disjunction based on the operator).
    Bool {
        operator: BooleanOperand,
    },
    // Act as Phrase with slop 0 if the field has positions,
    // otherwise act as an intersection.
    PhraseFallbackToIntersection,
    // After tokenization, the different tokens should be used to create
    // a phrase query.
    //
    // A non-zero slop allows the position of the terms to be slightly off.
    Phrase {
        #[serde(default, skip_serializing_if = "is_zero")]
        slop: u32,
    },
}

impl From<BooleanOperand> for FullTextMode {
    fn from(operator: BooleanOperand) -> Self {
        FullTextMode::Bool { operator }
    }
}

/// The Full Text query is tokenized into a sequence of tokens
/// that will then be searched.
///
/// The `full_text_params` defines what type of match is accepted.
/// The tokens might be transformed into a phrase queries,
/// into a disjunction, or into a conjunction.
///
/// If after tokenization, a single term is emitted, it will naturally be
/// produce a tantivy TermQuery.
///
/// If no terms is emitted, it will produce a query that match all or no documents,
/// depending on `full_text_params.zero_terms_query`.
///
/// Contrary to the user input query, the FullTextQuery does not
/// interpret a boolean query grammar and targets a specific field.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FullTextQuery {
    pub field: String,
    pub text: String,
    pub params: FullTextParams,
}

impl From<FullTextQuery> for QueryAst {
    fn from(full_text_query: FullTextQuery) -> Self {
        QueryAst::FullText(full_text_query)
    }
}

impl BuildTantivyAst for FullTextQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        full_text_query(&self.field, &self.text, &self.params, schema)
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{Schema, TEXT};

    use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
    use crate::query_ast::{BuildTantivyAst, FullTextMode, FullTextQuery};
    use crate::BooleanOperand;

    #[test]
    fn test_zero_terms() {
        let full_text_query = FullTextQuery {
            field: "body".to_string(),
            text: "".to_string(),
            params: super::FullTextParams {
                tokenizer: None,
                mode: BooleanOperand::And.into(),
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
            },
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        assert_eq!(ast.const_predicate(), Some(crate::MatchAllOrNone::MatchAll));
    }

    #[test]
    fn test_phrase_mode_default_tokenizer() {
        let full_text_query = FullTextQuery {
            field: "body".to_string(),
            text: "Hello World!".to_string(),
            params: super::FullTextParams {
                tokenizer: None,
                mode: FullTextMode::Phrase { slop: 1 },
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
            },
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            "PhraseQuery { field: Field(0), phrase_terms: [(0, Term(field=0, type=Str, \
             \"hello\")), (1, Term(field=0, type=Str, \"world\"))], slop: 1 }"
        );
    }

    #[test]
    fn test_full_text_specific_tokenizer() {
        let full_text_query = FullTextQuery {
            field: "body".to_string(),
            text: "Hello world".to_string(),
            params: super::FullTextParams {
                tokenizer: Some("raw".to_string()),
                mode: FullTextMode::Phrase { slop: 1 },
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
            },
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            r#"TermQuery(Term(field=0, type=Str, "Hello world"))"#
        );
    }

    #[test]
    fn test_full_text_bool_mode() {
        let full_text_query = FullTextQuery {
            field: "body".to_string(),
            text: "Hello world".to_string(),
            params: super::FullTextParams {
                tokenizer: None,
                mode: BooleanOperand::And.into(),
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
            },
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let bool_query = ast.as_bool_query().unwrap();
        assert_eq!(bool_query.must.len(), 2);
    }
}
