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

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tantivy::schema::{IndexRecordOption, Schema as TantivySchema, Type};
use tantivy::Term;

use crate::query_ast::{BuildTantivyAst, QueryAst, TantivyQueryAst, TermQuery};
use crate::tokenizers::TokenizerManager;
use crate::InvalidQuery;

/// TermSetQuery matches the same document set as if it was a union of
/// the equivalent set of TermQueries.
///
/// The text will be used as is, untokenized.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TermSetQuery {
    pub terms_per_field: HashMap<String, BTreeSet<String>>,
}

fn is_term_str(term: &Term) -> bool {
    let val = term.value();
    let typ = val.json_path_type().unwrap_or_else(|| val.typ());
    typ == Type::Str
}

impl TermSetQuery {
    fn make_term_iterator(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(HashSet<Term>, Vec<Vec<Term>>), InvalidQuery> {
        let mut all_terms: HashSet<Term> = HashSet::default();
        let mut intersections: Vec<Vec<Term>> = Vec::new();
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
                let ast = term_query.ast_for_term_extraction(schema, tokenizer_manager)?;
                let tantivy_query: Box<dyn crate::TantivyQuery> = ast.simplify().into();
                let mut terms = Vec::new();
                tantivy_query.query_terms(&mut |term, _| {
                    terms.push(term.clone());
                });

                let str_term_count = terms.iter().filter(|term| is_term_str(term)).count();
                if str_term_count <= 1 {
                    for term in terms {
                        all_terms.insert(term);
                    }
                } else {
                    // we have a string, and it got split into multiple tokens, so we want an
                    // intersection of them
                    let mut phrase = Vec::with_capacity(terms.len());
                    for term in terms {
                        if is_term_str(&term) {
                            phrase.push(term);
                        } else {
                            all_terms.insert(term);
                        }
                    }
                    intersections.push(phrase);
                }
            }
        }
        Ok((all_terms, intersections))
    }
}

impl BuildTantivyAst for TermSetQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        use tantivy::query::{BooleanQuery, Query, TermQuery, TermSetQuery};

        let (terms_it, intersections) = self.make_term_iterator(schema, tokenizer_manager)?;
        let term_set_query = TermSetQuery::new(terms_it);
        if intersections.is_empty() {
            Ok(term_set_query.into())
        } else {
            let mut sub_queries: Vec<Box<dyn Query>> = Vec::with_capacity(intersections.len() + 1);
            sub_queries.push(Box::new(term_set_query));
            for intersection in intersections {
                let terms = intersection
                    .into_iter()
                    .map(|term| {
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>
                    })
                    .collect();
                sub_queries.push(Box::new(BooleanQuery::intersection(terms)));
            }
            Ok(BooleanQuery::union(sub_queries).into())
        }
    }
}

impl From<TermSetQuery> for QueryAst {
    fn from(term_set_query: TermSetQuery) -> Self {
        QueryAst::TermSet(term_set_query)
    }
}
