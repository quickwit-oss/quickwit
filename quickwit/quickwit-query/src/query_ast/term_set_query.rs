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

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::schema::Schema as TantivySchema;

use crate::InvalidQuery;
use crate::query_ast::{BuildTantivyAst, QueryAst, TantivyQueryAst, TermQuery};
use crate::tokenizers::TokenizerManager;

/// TermSetQuery matches the same document set as if it was a union of
/// the equivalent set of TermQueries.
///
/// The text will be used as is, untokenized.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TermSetQuery {
    pub terms_per_field: HashMap<String, BTreeSet<String>>,
}

impl TermSetQuery {
    fn make_term_iterator(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
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
                let ast =
                    term_query.build_tantivy_ast_call(schema, tokenizer_manager, &[], false)?;
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
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let terms_it = self.make_term_iterator(schema, tokenizer_manager)?;
        let term_set_query = tantivy::query::TermSetQuery::new(terms_it);
        Ok(term_set_query.into())
    }
}

impl From<TermSetQuery> for QueryAst {
    fn from(term_set_query: TermSetQuery) -> Self {
        QueryAst::TermSet(term_set_query)
    }
}
