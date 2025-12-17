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

use super::{BuildTantivyAst, BuildTantivyAstContext, TantivyQueryAst};
use crate::InvalidQuery;
use crate::query_ast::QueryAst;

/// # Unsupported features
/// - named queries
///
/// Edge cases of BooleanQuery are not obvious,
/// and different behavior could be justified.
///
/// Here we align ourselves with Elasticsearch.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_should_match: Option<usize>,
}

impl From<BoolQuery> for QueryAst {
    fn from(bool_query: BoolQuery) -> Self {
        QueryAst::Bool(bool_query)
    }
}

impl BuildTantivyAst for BoolQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let mut boolean_query = super::tantivy_query_ast::TantivyBoolQuery {
            minimum_should_match: self.minimum_should_match,
            ..Default::default()
        };
        for must in &self.must {
            let must_leaf = must.build_tantivy_ast_call(context)?;
            boolean_query.must.push(must_leaf);
        }
        for must_not in &self.must_not {
            let must_not_leaf = must_not.build_tantivy_ast_call(context)?;
            boolean_query.must_not.push(must_not_leaf);
        }
        for should in &self.should {
            let should_leaf = should.build_tantivy_ast_call(context)?;
            boolean_query.should.push(should_leaf);
        }
        for filter in &self.filter {
            let filter_leaf = filter.build_tantivy_ast_call(context)?;
            boolean_query.filter.push(filter_leaf);
        }
        Ok(TantivyQueryAst::Bool(boolean_query))
    }
}
