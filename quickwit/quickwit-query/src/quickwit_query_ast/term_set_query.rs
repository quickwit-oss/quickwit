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

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;
use tantivy::Term;

use crate::quickwit_query_ast::{utils, IntoTantivyAst, QueryAst, TantivyQueryAst};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TermSetQuery {
    pub terms_per_field: HashMap<String, HashSet<String>>,
}

impl TermSetQuery {
    fn make_term_iterator(&self, schema: &Schema) -> impl Iterator<Item = Term> {
        let mut terms: HashSet<Term> = HashSet::default();
        for (full_path, values) in &self.terms_per_field {
            for value in values {
                // We simplify the logic by calling compute_query, and extract the resulting terms.
                let ast = utils::compute_query(full_path, value, false, schema);
                let tantivy_query: Box<dyn crate::TantivyQuery> = ast.simplify().into();
                tantivy_query.query_terms(&mut |term, _| {
                    terms.insert(term.clone());
                });
            }
        }
        terms.into_iter()
    }
}

impl IntoTantivyAst for TermSetQuery {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        let terms_it = self.make_term_iterator(schema);
        let term_set_query = tantivy::query::TermSetQuery::new(terms_it);
        Ok(term_set_query.into())
    }
}

impl From<TermSetQuery> for QueryAst {
    fn from(term_set_query: TermSetQuery) -> Self {
        QueryAst::TermSet(term_set_query)
    }
}
