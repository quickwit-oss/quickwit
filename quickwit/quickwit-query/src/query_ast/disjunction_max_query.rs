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
use tantivy::query::DisjunctionMaxQuery as TantivyDisjunctionMaxQuery;

use super::{BuildTantivyAst, BuildTantivyAstContext, QueryAst, TantivyQueryAst};
use crate::{InvalidQuery, MatchAllOrNone};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DisjunctionMaxQuery {
    pub disjuncts: Vec<QueryAst>,
}

impl From<DisjunctionMaxQuery> for QueryAst {
    fn from(disjunction_max_query: DisjunctionMaxQuery) -> Self {
        QueryAst::DisjunctionMax(disjunction_max_query)
    }
}

impl BuildTantivyAst for DisjunctionMaxQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let mut disjuncts = Vec::with_capacity(self.disjuncts.len());
        for disjunct in &self.disjuncts {
            let disjunct = disjunct.build_tantivy_ast_call(context)?.simplify();
            if disjunct.const_predicate() != Some(MatchAllOrNone::MatchNone) {
                disjuncts.push(disjunct);
            }
        }
        match disjuncts.len() {
            0 => Ok(TantivyQueryAst::match_none()),
            1 => Ok(disjuncts.swap_remove(0)),
            _ => {
                let disjuncts = disjuncts.into_iter().map(Into::into).collect();
                Ok(TantivyDisjunctionMaxQuery::new(disjuncts).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_disjunction_max_query_serde_roundtrip() {
        let query_ast: QueryAst = DisjunctionMaxQuery {
            disjuncts: vec![QueryAst::MatchAll, QueryAst::MatchNone],
        }
        .into();
        let serialized = serde_json::to_value(&query_ast).unwrap();
        assert_eq!(
            serialized,
            json!({
                "type": "disjunction_max",
                "disjuncts": [
                    {"type": "match_all"},
                    {"type": "match_none"}
                ]
            })
        );
        assert_eq!(
            serde_json::from_value::<QueryAst>(serialized).unwrap(),
            query_ast
        );
    }

    #[test]
    fn test_disjunction_max_query_simplifies_trivial_disjuncts() {
        let schema = tantivy::schema::Schema::builder().build();
        let context = BuildTantivyAstContext::for_test(&schema);
        let query = DisjunctionMaxQuery {
            disjuncts: vec![QueryAst::MatchNone, QueryAst::MatchAll],
        };
        assert_eq!(
            query.build_tantivy_ast_call(&context).unwrap(),
            TantivyQueryAst::match_all()
        );
    }
}
