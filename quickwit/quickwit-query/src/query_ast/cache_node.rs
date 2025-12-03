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

/// A node caching the posting list of the inner query.
///
/// This can be used when it's known that some sub-ast might appear in many queries,
/// or that the same query might be run, with various aggregations.
///
/// /!\ Sprinkling this everywhere can lead to performance degradations: the whole posting
/// list of the underlying query will need to be evaluated to build the cache.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct CacheNode {
    pub inner: Box<QueryAst>,
}

impl From<CacheNode> for QueryAst {
    fn from(cache_node: CacheNode) -> Self {
        QueryAst::Cache(cache_node)
    }
}

impl BuildTantivyAst for CacheNode {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        self.inner.build_tantivy_ast_impl(context)
    }
}
