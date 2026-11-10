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

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::query::{
    PhrasePrefixQuery as TantivyPhrasePrefixQuery, PhraseQuery as TantivyPhraseQuery,
    TermQuery as TantivyTermQuery,
};
use tantivy::schema::{
    Field, FieldType, IndexRecordOption, JsonObjectOptions, Schema as TantivySchema,
    TextFieldIndexing,
};
use tantivy::tokenizer::{TextAnalyzer, TokenStream};

use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};
use crate::query_ast::utils::full_text_query;
use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, QueryAst};
use crate::tokenizers::TokenizerManager;
use crate::{BooleanOperand, InvalidQuery, MatchAllOrNone, find_field_or_hit_dynamic};

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
        tokenizer_manager: &TokenizerManager,
    ) -> anyhow::Result<TextAnalyzer> {
        let tokenizer_name: &str = self
            .tokenizer
            .as_deref()
            .unwrap_or(text_field_indexing.tokenizer());
        tokenizer_manager
            .get_tokenizer(tokenizer_name)
            .with_context(|| format!("no tokenizer named `{tokenizer_name}` is registered"))
    }

    pub(crate) fn tokenize_text_into_terms_json(
        &self,
        field: Field,
        json_path: &str,
        text: &str,
        json_options: &JsonObjectOptions,
        tokenizer_manager: &TokenizerManager,
        is_prefix: bool,
    ) -> anyhow::Result<Vec<(usize, Term)>> {
        let text_indexing_options = json_options
            .get_text_indexing_options()
            .with_context(|| format!("Json field text `{json_path}` is not indexed"))?;
        let mut text_analyzer: TextAnalyzer =
            self.text_analyzer(text_indexing_options, tokenizer_manager)?;
        let mut token_stream = text_analyzer.token_stream(text);
        Ok(collect_terms(&mut token_stream, is_prefix, |text| {
            let mut term =
                Term::from_field_json_path(field, json_path, json_options.is_expand_dots_enabled());
            term.append_type_and_str(text);
            term
        }))
    }

    pub(crate) fn tokenize_text_into_terms(
        &self,
        field: Field,
        text: &str,
        text_field_indexing: &TextFieldIndexing,
        tokenizer_manager: &TokenizerManager,
        is_prefix: bool,
    ) -> anyhow::Result<Vec<(usize, Term)>> {
        let mut text_analyzer: TextAnalyzer =
            self.text_analyzer(text_field_indexing, tokenizer_manager)?;
        let mut token_stream = text_analyzer.token_stream(text);
        Ok(collect_terms(&mut token_stream, is_prefix, |text| {
            Term::from_field_text(field, text)
        }))
    }

    pub(crate) fn make_query(
        &self,
        mut terms: Vec<(usize, Term)>,
        index_record_option: IndexRecordOption,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        if terms.is_empty() {
            return Ok(self.zero_terms_query.into());
        }
        if terms.len() == 1 && !matches!(self.mode, FullTextMode::BoolPrefix { .. }) {
            let term = terms.pop().unwrap().1;
            return Ok(TantivyTermQuery::new(term, IndexRecordOption::WithFreqs).into());
        }
        match self.mode {
            FullTextMode::Bool { operator } => {
                let leaf_queries: Vec<TantivyQueryAst> = terms
                    .into_iter()
                    .map(|(_, term)| TantivyTermQuery::new(term, index_record_option).into())
                    .collect();
                Ok(TantivyBoolQuery::build_clause(operator, leaf_queries).into())
            }
            FullTextMode::BoolPrefix {
                operator,
                max_expansions,
            } => {
                let term_with_prefix = terms.pop();
                let mut leaf_queries: Vec<TantivyQueryAst> = terms
                    .into_iter()
                    .map(|(_, term)| TantivyTermQuery::new(term, index_record_option).into())
                    .collect();
                if let Some(term_with_prefix) = term_with_prefix {
                    let mut phrase_prefix_query =
                        TantivyPhrasePrefixQuery::new_with_offset(vec![term_with_prefix]);
                    phrase_prefix_query.set_max_expansions(max_expansions);
                    leaf_queries.push(phrase_prefix_query.into());
                }
                Ok(TantivyBoolQuery::build_clause(operator, leaf_queries).into())
            }
            FullTextMode::Phrase { slop } => {
                if !index_record_option.has_positions() {
                    return Err(InvalidQuery::SchemaError(
                        "Applied phrase query on field which does not have positions indexed"
                            .to_string(),
                    ));
                }
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

/// Keep the final original token as the prefix, without requiring its parts as
/// exact terms. Tokenizers such as datadog_v2 emit the original first, with a
/// position_length spanning its subsequent parts. Earlier words keep all terms.
fn collect_terms(
    token_stream: &mut dyn TokenStream,
    is_prefix: bool,
    make_term: impl Fn(&str) -> Term,
) -> Vec<(usize, Term)> {
    let mut terms = Vec::new();
    let mut prefix_span: Option<(usize, usize)> = None;
    token_stream.process(&mut |token| {
        if is_prefix && !matches!(prefix_span, Some((_, end)) if token.position < end) {
            prefix_span = (token.position_length > 1)
                .then_some((terms.len(), token.position + token.position_length));
        }
        terms.push((token.position, make_term(&token.text)));
    });
    if let Some((index, _)) = prefix_span {
        terms.truncate(index + 1);
    }
    terms
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
    BoolPrefix {
        operator: BooleanOperand,
        // max_expansions correspond to the fuzzy stop of query evaluation. It's not the same as
        // the max_expansions of a PhrasePrefixQuery, where it's used for the range
        // expansion.
        max_expansions: u32,
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
/// A single token produces a term query, or a prefix query in BoolPrefix mode.
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
    /// Support missing fields
    pub lenient: bool,
}

impl From<FullTextQuery> for QueryAst {
    fn from(full_text_query: FullTextQuery) -> Self {
        QueryAst::FullText(full_text_query)
    }
}

impl BuildTantivyAst for FullTextQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        full_text_query(
            &self.field,
            &self.text,
            &self.params,
            context.schema,
            context.tokenizer_manager,
            self.lenient,
        )
    }
}

impl FullTextQuery {
    /// Returns the last term of the query assuming the query is targeting a string or a Json
    /// field.
    ///
    /// This strange method is used to identify which term range should be warmed up for
    /// phrase prefix queries.
    pub fn get_prefix_term(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Option<Term> {
        if !matches!(self.params.mode, FullTextMode::BoolPrefix { .. }) {
            return None;
        };

        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type: &FieldType = field_entry.field_type();
        match field_type {
            FieldType::Str(text_options) => {
                let text_field_indexing = text_options.get_indexing_options()?;
                let mut terms = self
                    .params
                    .tokenize_text_into_terms(
                        field,
                        &self.text,
                        text_field_indexing,
                        tokenizer_manager,
                        true,
                    )
                    .ok()?;
                let (_pos, term) = terms.pop()?;
                Some(term)
            }
            FieldType::JsonObject(json_options) => {
                let mut terms = self
                    .params
                    .tokenize_text_into_terms_json(
                        field,
                        json_path,
                        &self.text,
                        json_options,
                        tokenizer_manager,
                        true,
                    )
                    .ok()?;
                let (_pos, term) = terms.pop()?;
                Some(term)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{DateOptions, DateTimePrecision, Schema, TEXT};

    use crate::BooleanOperand;
    use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
    use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, FullTextMode, FullTextQuery};

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
            lenient: false,
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
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
            lenient: false,
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
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
            lenient: false,
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
            r#"TermQuery(Term(field=0, type=Str, "Hello world"))"#
        );
    }

    #[test]
    fn test_full_text_datetime() {
        let full_text_query = FullTextQuery {
            field: "ts".to_string(),
            text: "2025-12-13T16:13:12.666777Z".to_string(),
            params: super::FullTextParams {
                tokenizer: Some("raw".to_string()),
                mode: FullTextMode::Phrase { slop: 1 },
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
            },
            lenient: false,
        };
        {
            // indexed, we truncate to the second
            let mut schema_builder = Schema::builder();
            schema_builder.add_date_field(
                "ts",
                DateOptions::default()
                    .set_precision(DateTimePrecision::Milliseconds)
                    .set_fast()
                    .set_indexed(),
            );
            let schema = schema_builder.build();
            let ast: TantivyQueryAst = full_text_query
                .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
                .unwrap();
            let leaf = ast.as_leaf().unwrap();
            assert_eq!(
                &format!("{leaf:?}"),
                r#"TermQuery(Term(field=0, type=Date, 2025-12-13T16:13:12Z))"#
            );
        }
        {
            // not indexed, we truncate to fastfield precision
            let mut schema_builder = Schema::builder();
            schema_builder.add_date_field(
                "ts",
                DateOptions::default()
                    .set_precision(DateTimePrecision::Milliseconds)
                    .set_fast(),
            );
            let schema = schema_builder.build();
            let ast: TantivyQueryAst = full_text_query
                .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
                .unwrap();
            let leaf = ast.as_leaf().unwrap();
            assert_eq!(
                &format!("{leaf:?}"),
                r#"TermQuery(Term(field=0, type=Date, 2025-12-13T16:13:12.666Z))"#
            );
        }
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
            lenient: false,
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let ast: TantivyQueryAst = full_text_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let bool_query = ast.as_bool_query().unwrap();
        assert_eq!(bool_query.must.len(), 2);
    }
}
