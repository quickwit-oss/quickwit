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
use tantivy::schema::Schema;

use super::{IntoTantivyAst, TantivyQueryAst};
use crate::quickwit_query_ast::QueryAst;

/// # Unsupported features
/// - minimum_should_match
/// - named queries
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub struct BoolQuery {
    pub must: Vec<QueryAst>,
    pub must_not: Vec<QueryAst>,
    pub should: Vec<QueryAst>,
    pub filter: Vec<QueryAst>,
}

impl From<BoolQuery> for QueryAst {
    fn from(bool_query: BoolQuery) -> Self {
        QueryAst::Bool(bool_query)
    }
}

impl IntoTantivyAst for BoolQuery {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        let mut boolean_query = super::tantivy_query_ast::TantivyBoolQuery::default();
        for must in &self.must {
            let must_leaf = must.into_tantivy_ast(schema)?;
            boolean_query.must.push(must_leaf);
        }
        for must_not in &self.must_not {
            let must_not_leaf = must_not.into_tantivy_ast(schema)?;
            boolean_query.must_not.push(must_not_leaf);
        }
        for should in &self.should {
            let should_leaf = should.into_tantivy_ast(schema)?;
            boolean_query.should.push(should_leaf);
        }
        for filter in &self.filter {
            let filter_leaf = filter.into_tantivy_ast(schema)?;
            boolean_query.filter.push(filter_leaf);
        }
        Ok(TantivyQueryAst::Bool(boolean_query))
    }
}
