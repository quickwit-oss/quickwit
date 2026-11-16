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

use std::convert::Infallible;

use super::{QueryAst, QueryAstVisitor};

/// Node counts in the resolved query, not term values, documents, or executions.
/// Bool clauses (including negation) and boosts count alongside their children.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryFeatureCounts {
    pub bool_count: u64,
    pub term_count: u64,
    pub term_set_count: u64,
    pub field_presence_count: u64,
    pub full_text_count: u64,
    pub phrase_prefix_count: u64,
    pub range_count: u64,
    pub wildcard_count: u64,
    pub regex_count: u64,
    pub match_all_count: u64,
    pub match_none_count: u64,
    pub boost_count: u64,
    pub calc_field_count: u64,
}

impl QueryFeatureCounts {
    pub fn from_query_ast(query_ast: &QueryAst) -> Self {
        let mut features = Self::default();
        features.visit(query_ast).unwrap();
        features
    }
}

impl<'a> QueryAstVisitor<'a> for QueryFeatureCounts {
    type Err = Infallible;

    fn visit(&mut self, query_ast: &'a QueryAst) -> Result<(), Self::Err> {
        match query_ast {
            QueryAst::Bool(query) => {
                self.bool_count += 1;
                self.visit_bool(query)?;
            }
            QueryAst::Boost { underlying, .. } => {
                self.boost_count += 1;
                self.visit(underlying)?;
            }
            QueryAst::Cache(query) => {
                // Cache is an execution optimization, not a query feature.
                // Count only its children.
                self.visit(&query.inner)?;
            }
            QueryAst::Term(_) => self.term_count += 1,
            QueryAst::TermSet(_) => self.term_set_count += 1,
            QueryAst::FieldPresence(_) => self.field_presence_count += 1,
            QueryAst::FullText(_) => self.full_text_count += 1,
            QueryAst::PhrasePrefix(_) => self.phrase_prefix_count += 1,
            QueryAst::Range(_) => self.range_count += 1,
            QueryAst::UserInput(_) => {} // User input is syntax, not a query feature.
            QueryAst::Wildcard(_) => self.wildcard_count += 1,
            QueryAst::Regex(_) => self.regex_count += 1,
            QueryAst::MatchAll => self.match_all_count += 1,
            QueryAst::MatchNone => self.match_none_count += 1,
            QueryAst::CalcField(_) => self.calc_field_count += 1,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_features() {
        let query: QueryAst = serde_json::from_value(serde_json::json!({
            "type": "bool",
            "must": [{"type": "user_input", "user_text": "body:foo OR body:bar",
                      "default_operator": "AND", "lenient": false}],
            "should": [{"type": "term_set", "terms_per_field": {"body": ["foo", "bar"]}}],
            "must_not": [{"type": "regex", "field": "body", "regex": "f.*"}],
            "filter": [{"type": "cache", "inner": {
                "type": "boost", "boost": 2.0, "underlying": {"type": "match_all"}
            }}]
        }))
        .unwrap();
        let resolved = query.parse_user_query(&[]).unwrap();
        assert_eq!(
            QueryFeatureCounts::from_query_ast(&resolved),
            QueryFeatureCounts {
                bool_count: 2,
                full_text_count: 2,
                term_set_count: 1,
                regex_count: 1,
                boost_count: 1,
                match_all_count: 1,
                ..Default::default()
            }
        );
    }
}
