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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::query::{BooleanQuery, ConstScoreQuery};
use tantivy::schema::{FieldType, IndexRecordOption, Schema};

use super::regex_query::json_str_term_prefix;
use super::{AutomatonQuery, JsonPathPrefix, ResolvedRegex, get_or_compile_cached_fst_regex};
use crate::InvalidQuery;
use crate::query_ast::{
    BoolQuery, BuildTantivyAst, BuildTantivyAstContext, QueryAst, TantivyQueryAst, TermQuery,
};
use crate::tokenizers::TokenizerManager;

/// TermSetQuery matches the same document set as if it was a union of
/// the equivalent set of TermQueries.
///
/// The text will be used as is, untokenized.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TermSetQuery {
    pub terms_per_field: BTreeMap<String, BTreeSet<String>>,
}

/// Emit one alternation, not multiple queries. tantivy-fst recursively follows the splits in
/// its instruction graph while compiling, before its DFA state limit can trigger. Keep wide
/// alternations shallow. Capturing groups survive regex-syntax's alternation flattening;
/// tantivy-fst ignores the captures when matching. This is not a limit on the term set.
fn push_literal_alternation(literals: &[&str], pattern: &mut String) {
    const MAX_FLAT_BRANCHES: usize = 128;
    if literals.len() > MAX_FLAT_BRANCHES {
        let (left, right) = literals.split_at(literals.len() / 2);
        pattern.push('(');
        push_literal_alternation(left, pattern);
        pattern.push_str(")|(");
        push_literal_alternation(right, pattern);
        pattern.push(')');
        return;
    }
    for (i, literal) in literals.iter().enumerate() {
        if i != 0 {
            pattern.push('|');
        }
        regex_syntax::escape_into(literal, pattern);
    }
}

impl TermSetQuery {
    fn has_fast_only_field(&self, context: &BuildTantivyAstContext) -> bool {
        for full_path in self.terms_per_field.keys() {
            if let Some((_, field_entry, _)) =
                super::utils::find_field_or_hit_dynamic(full_path, context.schema)
                && field_entry.is_fast()
                && !field_entry.is_indexed()
            {
                return true;
            }
        }
        false
    }

    fn build_bool_query(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let should_clauses = self
            .terms_per_field
            .iter()
            .flat_map(|(full_path, values)| {
                values.iter().map(|value| {
                    QueryAst::Term(TermQuery {
                        field: full_path.to_string(),
                        value: value.to_string(),
                    })
                })
            })
            .collect();

        let bool_query = BoolQuery {
            should: should_clauses,
            ..Default::default()
        };

        bool_query.build_tantivy_ast_impl(context)
    }

    /// Returns the resolved regex automaton when this term set lowers to a literal alternation.
    pub fn to_resolved_regex(
        &self,
        schema: &Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> Option<ResolvedRegex> {
        if self.terms_per_field.len() != 1 {
            return None;
        }
        let context = BuildTantivyAstContext {
            schema,
            tokenizer_manager,
            search_fields: &[],
            with_validation: false,
        };
        let terms = self.make_term_iterator(&context).ok()?;
        self.try_compile_regex(&terms, schema)
            .map(|(resolved, _)| resolved)
    }

    // Work on the indexed terms, not the request strings: term resolution may normalize text
    // or produce several typed terms for a single JSON value.
    fn try_compile_regex(
        &self,
        terms: &HashSet<Term>,
        schema: &Schema,
    ) -> Option<(ResolvedRegex, Arc<tantivy_fst::Regex>)> {
        if terms.len() < 2 || self.terms_per_field.len() != 1 {
            return None;
        }
        let full_path = self.terms_per_field.keys().next()?;
        let (field, entry, path) = super::utils::find_field_or_hit_dynamic(full_path, schema)?;
        let json_path = match entry.field_type() {
            FieldType::Str(_) if entry.is_indexed() => None,
            FieldType::JsonObject(opts) if entry.is_indexed() => {
                Some(json_str_term_prefix(field, path, opts))
            }
            _ => return None,
        };
        let prefix = json_path.as_deref().unwrap_or_default();
        let mut literals = Vec::with_capacity(terms.len());
        for term in terms {
            // The JSON prefix includes both path and type; mixed string/numeric sets fall back
            // intact.
            let bytes = term.serialized_value_bytes().strip_prefix(prefix)?;
            literals.push(std::str::from_utf8(bytes).ok()?);
        }
        literals.sort_unstable(); // Stable cache key across splits and requests.
        let mut pattern = String::new();
        push_literal_alternation(&literals, &mut pattern);
        // A rejected optimization is not an invalid terms query (also in lenient mode).
        let regex = get_or_compile_cached_fst_regex(&pattern)?;
        Some((
            ResolvedRegex {
                field,
                json_path,
                regex: pattern,
            },
            regex,
        ))
    }

    fn build_indexed_query(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let terms = self.make_term_iterator(context)?;
        if let Some((resolved, regex)) = self.try_compile_regex(&terms, context.schema) {
            let automaton = JsonPathPrefix {
                prefix: resolved.json_path.unwrap_or_default(),
                automaton: regex,
            };
            return Ok(AutomatonQuery {
                field: resolved.field,
                automaton: Arc::new(automaton),
            }
            .into());
        }
        let term_queries = terms
            .into_iter()
            .map(|term| {
                Box::new(tantivy::query::TermQuery::new(
                    term,
                    IndexRecordOption::Basic,
                )) as Box<dyn crate::TantivyQuery>
            })
            .collect();
        // Score the union, not each term: matching several terms still contributes only 1.0.
        // Basic postings and query_terms also ensure warmup never requests positions.
        let disjunction = BooleanQuery::union(term_queries);
        Ok(ConstScoreQuery::new(Box::new(disjunction), 1.0).into())
    }

    fn make_term_iterator(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<HashSet<Term>, InvalidQuery> {
        let mut terms: HashSet<Term> = HashSet::default();

        for (full_path, values) in &self.terms_per_field {
            for value in values {
                // Mapping a text (field, value) is non-trivial:
                // It depends on the schema of course, and can actually result in a disjunction of
                // multiple terms if the query targets a dynamic field (due to the
                // different types).
                //
                // Here, we ensure the logic is the same as for a TermQuery, by creating the term
                // query and extracting the terms from the resulting `TermQuery`.
                let term_query = TermQuery {
                    field: full_path.to_string(),
                    value: value.to_string(),
                };
                let ast = term_query.build_tantivy_ast_call(context)?;
                let tantivy_query: Box<dyn crate::TantivyQuery> = ast.simplify().into();
                tantivy_query.query_terms(&mut |term, _| {
                    terms.insert(term.clone());
                });
            }
        }
        Ok(terms)
    }
}

impl BuildTantivyAst for TermSetQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        if self.has_fast_only_field(context) {
            self.build_bool_query(context)
        } else {
            self.build_indexed_query(context)
        }
    }
}

impl From<TermSetQuery> for QueryAst {
    fn from(term_set_query: TermSetQuery) -> Self {
        QueryAst::TermSet(term_set_query)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use tantivy::schema::{FAST, INDEXED, Schema};

    use super::TermSetQuery;
    use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext};

    #[test]
    fn test_term_set_query_with_fast_only_field_returns_bool_query() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("fast_field", FAST);
        let schema = schema_builder.build();

        let terms_per_field = BTreeMap::from([(
            "fast_field".to_string(),
            BTreeSet::from(["1".to_string(), "2".to_string()]),
        )]);
        let term_set_query = TermSetQuery { terms_per_field };

        let tantivy_query_ast = term_set_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();

        let bool_query = tantivy_query_ast
            .as_bool_query()
            .expect("Expected BoolQuery for fast-only field, but got a different query type");
        assert_eq!(bool_query.should.len(), 2);
        assert_eq!(bool_query.must.len(), 0);
        assert_eq!(bool_query.must_not.len(), 0);
        assert_eq!(bool_query.filter.len(), 0);
    }

    #[test]
    fn test_term_set_query_with_indexed_field_uses_disjunction() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("indexed_field", FAST | INDEXED);
        let schema = schema_builder.build();

        let terms_per_field = BTreeMap::from([(
            "indexed_field".to_string(),
            BTreeSet::from(["1".to_string(), "2".to_string()]),
        )]);
        let term_set_query = TermSetQuery { terms_per_field };

        let tantivy_query_ast = term_set_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();

        let leaf = tantivy_query_ast.as_leaf().unwrap();
        let debug_str = format!("{leaf:?}");
        assert!(debug_str.starts_with("Const(score=1, query=BooleanQuery"));
        assert!(!debug_str.contains("TermSetQuery"));
        let mut terms = BTreeSet::new();
        leaf.query_terms(&mut |term, needs_positions| {
            assert!(!needs_positions);
            terms.insert(term.clone());
        });
        assert_eq!(terms.len(), 2);
    }
}
