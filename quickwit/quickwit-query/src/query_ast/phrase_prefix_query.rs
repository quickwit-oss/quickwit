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

pub struct PhrasePrefixTerms {
    pub field: Field,
    pub term_positions: Vec<(usize, Term)>,
    pub max_expansions: u32,
}

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
    ) -> Result<PhrasePrefixTerms, InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)
            .ok_or_else(|| InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            })?;
        let field_type = field_entry.field_type();

        let (text_indexing_options, term_positions) = match field_type {
            FieldType::Str(text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                let term_positions = self.params.tokenize_text_into_terms(
                    field,
                    &self.phrase,
                    text_field_indexing,
                    tokenizer_manager,
                )?;
                (text_field_indexing, term_positions)
            }
            FieldType::JsonObject(json_options) => {
                let text_field_indexing =
                    json_options.get_text_indexing_options().ok_or_else(|| {
                        InvalidQuery::SchemaError(format!(
                            "field {} is not full-text searchable",
                            field_entry.name()
                        ))
                    })?;
                let term_positions = self.params.tokenize_text_into_terms_json(
                    field,
                    json_path,
                    &self.phrase,
                    json_options,
                    tokenizer_manager,
                )?;
                (text_field_indexing, term_positions)
            }
            _ => {
                return Err(InvalidQuery::SchemaError(
                    "trying to run a PhrasePrefix query on a non-text field".to_string(),
                ));
            }
        };
        if !text_indexing_options.index_option().has_positions() && term_positions.len() > 1 {
            return Err(InvalidQuery::SchemaError(
                "trying to run a PhrasePrefix query on a field which does not have positions \
                 indexed"
                    .to_string(),
            ));
        }
        // A single-token phrase prefix is really just a prefix query. This is
        // especially common when searching a non-tokenized field.
        // In that case, `max_expansions` is not really necessary and we
        // chose to remove this limitation by setting it to u32::MAX.
        //
        // See CLOUDPREM-775.
        let max_expansions = if term_positions.len() == 1 {
            u32::MAX
        } else {
            self.max_expansions
        };
        Ok(PhrasePrefixTerms {
            field,
            term_positions,
            max_expansions,
        })
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
        let phrase_prefix_terms = match self.get_terms(context.schema, context.tokenizer_manager) {
            Ok(res) => res,
            Err(InvalidQuery::FieldDoesNotExist { .. }) if self.lenient => {
                return Ok(TantivyQueryAst::match_none());
            }
            Err(e) => return Err(e),
        };
        if phrase_prefix_terms.term_positions.is_empty() {
            if self.params.zero_terms_query.is_none() {
                Ok(TantivyQueryAst::match_none())
            } else {
                Ok(TantivyQueryAst::match_all())
            }
        } else {
            let mut phrase_prefix_query =
                TantivyPhrasePrefixQuery::new_with_offset(phrase_prefix_terms.term_positions);
            phrase_prefix_query.set_max_expansions(phrase_prefix_terms.max_expansions);
            Ok(phrase_prefix_query.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::collector::Count;
    use tantivy::query::Query as TantivyQuery;
    use tantivy::schema::{
        JsonObjectOptions, Schema as TantivySchema, TEXT, TextFieldIndexing, TextOptions,
    };
    use tantivy::{Index, doc};

    use super::*;
    use crate::MatchAllOrNone;
    use crate::query_ast::{BuildTantivyAstContext, FullTextMode};

    fn raw_text_options() -> TextOptions {
        TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw"))
    }

    fn phrase_prefix_query(field: &str, phrase: &str, max_expansions: u32) -> PhrasePrefixQuery {
        PhrasePrefixQuery {
            field: field.to_string(),
            phrase: phrase.to_string(),
            max_expansions,
            params: FullTextParams {
                tokenizer: None,
                mode: FullTextMode::Phrase { slop: 0 },
                zero_terms_query: MatchAllOrNone::MatchNone,
            },
            lenient: false,
        }
    }

    /// Builds the leaf tantivy query from the AST.
    fn build_tantivy_query(
        query: &PhrasePrefixQuery,
        schema: &TantivySchema,
    ) -> Box<dyn TantivyQuery> {
        let context = BuildTantivyAstContext::for_test(schema);
        let ast = query.build_tantivy_ast_impl(&context).unwrap();
        let TantivyQueryAst::Leaf(tantivy_query) = ast else {
            panic!("expected a leaf tantivy query")
        };
        tantivy_query
    }

    /// Builds the query against `index`'s schema and returns the number of matching docs.
    fn count_matches(query: &PhrasePrefixQuery, index: &Index) -> usize {
        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let ast = query.build_tantivy_ast_impl(&context).unwrap();
        let tantivy_query: Box<dyn TantivyQuery> = ast.into();
        let searcher = index.reader().unwrap().searcher();
        searcher.search(&*tantivy_query, &Count).unwrap()
    }

    #[test]
    fn test_single_token_builds_prefix_only_query() {
        let mut schema_builder = TantivySchema::builder();
        schema_builder.add_text_field("url", raw_text_options());
        let schema = schema_builder.build();

        // A single (raw) token is a plain prefix: the resulting phrase prefix query has
        // no phrase terms, so tantivy runs it as an (uncapped) term range. The actual
        // lifting of `max_expansions` is exercised by the behavioral tests below.
        let query = phrase_prefix_query("url", "/fantasy/", 50);
        let tantivy_query = build_tantivy_query(&query, &schema);
        let phrase_prefix_query = tantivy_query
            .downcast_ref::<TantivyPhrasePrefixQuery>()
            .expect("expected a phrase prefix query");
        assert!(phrase_prefix_query.phrase_terms().is_empty());
    }

    #[test]
    fn test_multi_token_builds_phrase_prefix_query() {
        let mut schema_builder = TantivySchema::builder();
        // `default` tokenizer splits on whitespace and has positions.
        schema_builder.add_text_field("body", tantivy::schema::TEXT);
        let schema = schema_builder.build();

        let query = phrase_prefix_query("body", "foo bar", 42);
        let tantivy_query = build_tantivy_query(&query, &schema);
        let phrase_prefix_query = tantivy_query
            .downcast_ref::<TantivyPhrasePrefixQuery>()
            .expect("expected a phrase prefix query");
        // A genuine multi-token phrase prefix keeps its leading phrase terms.
        assert_eq!(phrase_prefix_query.phrase_terms().len(), 1);
    }

    #[test]
    fn test_single_token_prefix_matches_all_terms_despite_low_max_expansions() {
        let mut schema_builder = TantivySchema::builder();
        let url_field = schema_builder.add_text_field("url", raw_text_options());
        let schema = schema_builder.build();

        let index = tantivy::IndexBuilder::new()
            .schema(schema)
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for url in ["/fantasy/nfl", "/fantasy/mlb", "/fantasy/nba"] {
            index_writer.add_document(doc!(url_field => url)).unwrap();
        }
        index_writer
            .add_document(doc!(url_field => "/news/latest"))
            .unwrap();
        index_writer.commit().unwrap();

        // With max_expansions = 1, a capped phrase prefix would match only 1 term. The
        // single-token prefix lifts the cap and must match all 3 "/fantasy/" documents.
        let query = phrase_prefix_query("url", "/fantasy/", 1);
        assert_eq!(count_matches(&query, &index), 3);
    }

    #[test]
    fn test_single_token_prefix_max_expansions() {
        let mut schema_builder = TantivySchema::builder();
        let url_field = schema_builder.add_text_field("url", raw_text_options());
        let schema = schema_builder.build();

        let index = tantivy::IndexBuilder::new()
            .schema(schema)
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for url in ["/fantasy/some1", "/fantasy/some2", "/fantasy/some3"] {
            index_writer.add_document(doc!(url_field => url)).unwrap();
        }
        index_writer
            .add_document(doc!(url_field => "/news/latest"))
            .unwrap();
        index_writer.commit().unwrap();
        let query = phrase_prefix_query("url", "/fantasy/some", 1);
        assert_eq!(count_matches(&query, &index), 3);
    }

    #[test]
    fn test_multi_token_prefix_max_expansions() {
        let mut schema_builder = TantivySchema::builder();
        let url_field = schema_builder.add_text_field("url", TEXT);
        let schema = schema_builder.build();

        let index = tantivy::IndexBuilder::new()
            .schema(schema)
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for url in ["/fantasy/some1", "/fantasy/some2", "/fantasy/some3"] {
            index_writer.add_document(doc!(url_field => url)).unwrap();
        }
        index_writer
            .add_document(doc!(url_field => "/news/latest"))
            .unwrap();
        index_writer.commit().unwrap();
        let query = phrase_prefix_query("url", "/fantasy/some", 1);
        assert_eq!(count_matches(&query, &index), 1);
    }

    #[test]
    fn test_single_token_prefix_on_json_field_matches_all_terms() {
        let mut schema_builder = TantivySchema::builder();
        let json_options: JsonObjectOptions = raw_text_options().into();
        schema_builder.add_json_field("custom", json_options);
        let schema = schema_builder.build();

        let index = tantivy::IndexBuilder::new()
            .schema(schema.clone())
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for url in [
            "/fantasy/nfl",
            "/fantasy/mlb",
            "/fantasy/nba",
            "/news/latest",
        ] {
            // `parse_json` expects top-level keys to be field names.
            let json_text =
                serde_json::to_string(&serde_json::json!({"custom": {"url": url}})).unwrap();
            let doc = tantivy::TantivyDocument::parse_json(&schema, &json_text).unwrap();
            index_writer.add_document(doc).unwrap();
        }
        index_writer.commit().unwrap();

        let query = phrase_prefix_query("custom.url", "/fantasy/", 1);
        assert_eq!(count_matches(&query, &index), 3);
    }

    #[test]
    fn test_empty_tokenization_matches_none_by_default() {
        let mut schema_builder = TantivySchema::builder();
        schema_builder.add_text_field("body", tantivy::schema::TEXT);
        let schema = schema_builder.build();

        // The `default` tokenizer drops punctuation-only input, yielding zero tokens.
        let query = phrase_prefix_query("body", "!!!", 50);
        let context = BuildTantivyAstContext::for_test(&schema);
        let ast = query.build_tantivy_ast_impl(&context).unwrap();
        assert_eq!(ast, TantivyQueryAst::match_none());
    }
}
