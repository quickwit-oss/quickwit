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
use tantivy::query::BoostQuery;
use tantivy::schema::Schema;

mod bool_query;
mod phrase_query;
mod range_query;
mod tantivy_query_ast;
mod term_query;
mod term_set_query;
mod utils;
mod visitor;

pub use bool_query::BoolQuery;
pub use phrase_query::PhraseQuery;
pub use range_query::RangeQuery;
use tantivy_query_ast::TantivyQueryAst;
pub use term_query::TermQuery;
pub use term_set_query::TermSetQuery;
pub use visitor::QueryAstVisitor;

use crate::NotNaNf32;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueryAst {
    Bool(BoolQuery),
    Term(TermQuery),
    TermSet(TermSetQuery),
    Phrase(PhraseQuery),
    Range(RangeQuery),
    MatchAll,
    MatchNone,
    Boost {
        underlying: Box<QueryAst>,
        boost: NotNaNf32,
    },
}

trait IntoTantivyAst {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst>;
}

impl IntoTantivyAst for QueryAst {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        match self {
            QueryAst::Bool(bool_query) => bool_query.into_tantivy_ast(schema),
            QueryAst::Term(term_query) => term_query.into_tantivy_ast(schema),
            QueryAst::Range(range_query) => range_query.into_tantivy_ast(schema),
            QueryAst::MatchAll => Ok(TantivyQueryAst::match_all()),
            QueryAst::MatchNone => Ok(TantivyQueryAst::match_none()),
            QueryAst::Boost { boost, underlying } => {
                let underlying = underlying.into_tantivy_ast(schema)?;
                let boost_query = BoostQuery::new(underlying.into(), (*boost).into());
                Ok(boost_query.into())
            }
            QueryAst::TermSet(term_set) => term_set.into_tantivy_ast(schema),
            QueryAst::Phrase(phrase_query) => phrase_query.into_tantivy_ast(schema),
        }
    }
}

impl QueryAst {
    pub fn build_tantivy_query(
        &self,
        schema: &Schema,
    ) -> anyhow::Result<Box<dyn crate::TantivyQuery>> {
        let tantivy_query_ast = self.into_tantivy_ast(schema)?;
        Ok(tantivy_query_ast.simplify().into())
    }
}
