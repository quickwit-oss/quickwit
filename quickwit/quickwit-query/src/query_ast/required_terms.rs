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

//! Extraction of *required terms*: terms that must all be present in a split for
//! the query to match any document.
//!
//! A leaf search warms up a split by, among other things, loading the posting
//! list of each query term. [`tantivy`] reports whether each term was actually
//! found. If a *required* term is missing (empty posting list), the query
//! provably matches nothing in that split, so the remaining (much more
//! expensive) warmup downloads can be cancelled.
//!
//! The set is a conservative under-approximation: only single-term clauses
//! combined purely by conjunction (`must` / `filter`) qualify. Anything else
//! (`should`, `must_not`, ranges, phrases, term sets, terms on non-indexed
//! fields, ...) is omitted, so a term ends up in the set only when its absence
//! truly implies the whole query is empty.
//!
//! The biggest gap is that a disjunction of terms (e.g. `my_id:(1 OR 2 OR 3)`) is not covered.
//! For this we would need to build a more complex representation of the required terms, ideally an
//! AST that can be evaluated against the set of found terms. This is left for future work.

use std::collections::HashSet;

use tantivy::Term;
use tantivy::query::TermQuery as TantivyTermQuery;
use tantivy::schema::Schema;

use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};

/// Collects the terms that must be present for the (already simplified) query to
/// match any document. See the module docs for the conservative contract.
pub(crate) fn collect_required_terms(
    ast: &TantivyQueryAst,
    schema: &Schema,
    required_terms: &mut HashSet<Term>,
) {
    match ast {
        TantivyQueryAst::Leaf(query) => {
            let Some(term_query) = query.downcast_ref::<TantivyTermQuery>() else {
                return;
            };
            let term = term_query.term();
            if schema.get_field_entry(term.field()).is_indexed() {
                required_terms.insert(term.clone());
            }
        }
        TantivyQueryAst::Bool(bool_query) => {
            collect_required_terms_from_bool(bool_query, schema, required_terms);
        }
        TantivyQueryAst::ConstPredicate(_) => {}
    }
}

fn collect_required_terms_from_bool(
    bool_query: &TantivyBoolQuery,
    schema: &Schema,
    required_terms: &mut HashSet<Term>,
) {
    // Only `must` and `filter` clauses are unconditionally required. `should`
    // (even with `minimum_should_match`) and `must_not` never make a single term
    // mandatory, so we do not descend into them.
    for child in bool_query.must.iter().chain(bool_query.filter.iter()) {
        collect_required_terms(child, schema, required_terms);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tantivy::Term;
    use tantivy::query::TermQuery as TantivyTermQuery;
    use tantivy::schema::{Field, STORED, Schema, TEXT};

    use super::collect_required_terms;
    use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};

    fn term(field: Field, value: &str) -> Term {
        Term::from_field_text(field, value)
    }

    fn term_leaf(field: Field, value: &str) -> TantivyQueryAst {
        TantivyTermQuery::new(term(field, value), Default::default()).into()
    }

    fn required(ast: &TantivyQueryAst, schema: &Schema) -> HashSet<Term> {
        let mut out = HashSet::new();
        collect_required_terms(ast, schema, &mut out);
        out
    }

    #[test]
    fn test_single_indexed_term_is_required() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("f", TEXT);
        let schema = schema_builder.build();

        assert_eq!(
            required(&term_leaf(field, "x"), &schema),
            HashSet::from([term(field, "x")]),
        );
    }

    #[test]
    fn test_term_on_non_indexed_field_is_not_required() {
        let mut schema_builder = Schema::builder();
        let stored_only = schema_builder.add_text_field("stored", STORED);
        let schema = schema_builder.build();

        assert!(required(&term_leaf(stored_only, "x"), &schema).is_empty());
    }

    #[test]
    fn test_must_and_filter_are_required() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("f", TEXT);
        let schema = schema_builder.build();

        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            must: vec![term_leaf(field, "a")],
            filter: vec![term_leaf(field, "b")],
            ..Default::default()
        });
        assert_eq!(
            required(&ast, &schema),
            HashSet::from([term(field, "a"), term(field, "b")]),
        );
    }

    #[test]
    fn test_should_and_must_not_are_not_required() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("f", TEXT);
        let schema = schema_builder.build();

        // `must: [a]`, `should: [b]`, `must_not: [c]` -> only `a` is required.
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            must: vec![term_leaf(field, "a")],
            should: vec![term_leaf(field, "b")],
            must_not: vec![term_leaf(field, "c")],
            ..Default::default()
        });
        assert_eq!(required(&ast, &schema), HashSet::from([term(field, "a")]),);

        // A disjunction alone makes no term required.
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            should: vec![term_leaf(field, "a"), term_leaf(field, "b")],
            ..Default::default()
        });
        assert!(required(&ast, &schema).is_empty());
    }

    #[test]
    fn test_nested_conjunction_collects_recursively() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("f", TEXT);
        let schema = schema_builder.build();

        // must: [ Bool{ must: [a, b] } ] -> both a and b are required.
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            must: vec![TantivyQueryAst::Bool(TantivyBoolQuery {
                must: vec![term_leaf(field, "a"), term_leaf(field, "b")],
                ..Default::default()
            })],
            ..Default::default()
        });
        assert_eq!(
            required(&ast, &schema),
            HashSet::from([term(field, "a"), term(field, "b")]),
        );

        // must: [ Bool{ should: [a, b] } ] -> nothing required (disjunction).
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            must: vec![TantivyQueryAst::Bool(TantivyBoolQuery {
                should: vec![term_leaf(field, "a"), term_leaf(field, "b")],
                ..Default::default()
            })],
            ..Default::default()
        });
        assert!(required(&ast, &schema).is_empty());
    }
}
