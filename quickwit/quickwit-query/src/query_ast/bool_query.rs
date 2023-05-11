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
use tantivy::schema::Schema as TantivySchema;

use super::{BuildTantivyAst, TantivyQueryAst};
use crate::query_ast::QueryAst;
use crate::InvalidQuery;

/// # Unsupported features
/// - minimum_should_match
/// - named queries
///
/// Edge cases of BooleanQuery are not obvious,
/// and different beahvior could be justified.
///
/// Here we aligne ourselves with ElasticSearch.
/// A boolean query is to be interpreted like a filtering predicate
/// over the set of documents.
///
/// If all clauses are empty, then the full set of documents is returned.
/// Adding a match all must clause does not change the result of a boolean query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub struct BoolQuery {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must: Vec<QueryAst>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must_not: Vec<QueryAst>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub should: Vec<QueryAst>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter: Vec<QueryAst>,
}

impl From<BoolQuery> for QueryAst {
    fn from(bool_query: BoolQuery) -> Self {
        QueryAst::Bool(bool_query)
    }
}

impl BuildTantivyAst for BoolQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let mut boolean_query = super::tantivy_query_ast::TantivyBoolQuery::default();
        for must in &self.must {
            let must_leaf = must.build_tantivy_ast_call(schema, search_fields, with_validation)?;
            boolean_query.must.push(must_leaf);
        }
        for must_not in &self.must_not {
            let must_not_leaf =
                must_not.build_tantivy_ast_call(schema, search_fields, with_validation)?;
            boolean_query.must_not.push(must_not_leaf);
        }
        for should in &self.should {
            let should_leaf =
                should.build_tantivy_ast_call(schema, search_fields, with_validation)?;
            boolean_query.should.push(should_leaf);
        }
        for filter in &self.filter {
            let filter_leaf =
                filter.build_tantivy_ast_call(schema, search_fields, with_validation)?;
            boolean_query.filter.push(filter_leaf);
        }
        Ok(TantivyQueryAst::Bool(boolean_query))
    }
}
