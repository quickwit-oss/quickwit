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

use tantivy::query::{
    AllQuery as TantivyAllQuery, BooleanQuery, ConstScoreQuery as TantivyConstScoreQuery,
    EmptyQuery as TantivyEmptyQuery,
};
use tantivy::query_grammar::Occur;

use crate::{BooleanOperand, MatchAllOrNone, TantivyQuery};

/// This AST point, is only to make it easier to simplify the generated Tantivy query.
/// when we convert a QueryAst into a TantivyQueryAst.
///
/// Let's keep private.
#[derive(Debug)]
pub(crate) enum TantivyQueryAst {
    Bool(TantivyBoolQuery),
    Leaf(Box<dyn TantivyQuery>),
    ConstPredicate(MatchAllOrNone),
}

impl Clone for TantivyQueryAst {
    fn clone(&self) -> Self {
        match self {
            TantivyQueryAst::Bool(bool_query) => TantivyQueryAst::Bool(bool_query.clone()),
            TantivyQueryAst::ConstPredicate(predicate) => {
                TantivyQueryAst::ConstPredicate(*predicate)
            }
            TantivyQueryAst::Leaf(query) => TantivyQueryAst::Leaf(query.box_clone()),
        }
    }
}

impl From<MatchAllOrNone> for TantivyQueryAst {
    fn from(match_all_or_none: MatchAllOrNone) -> Self {
        TantivyQueryAst::ConstPredicate(match_all_or_none)
    }
}

impl PartialEq for TantivyQueryAst {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Leaf(left), Self::Leaf(right)) => {
                let left_str: String = format!("{left:?}");
                let right_str: String = format!("{right:?}");
                left_str == right_str
            }
            (Self::ConstPredicate(left), Self::ConstPredicate(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for TantivyQueryAst {}

impl TantivyQueryAst {
    #[cfg(test)]
    pub(crate) fn as_bool_query(&self) -> Option<&TantivyBoolQuery> {
        match self {
            TantivyQueryAst::Bool(bool) => Some(bool),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn as_leaf(&self) -> Option<&dyn TantivyQuery> {
        match self {
            TantivyQueryAst::Leaf(tantivy_query) => Some(&**tantivy_query),
            _ => None,
        }
    }

    pub(crate) fn const_predicate(&self) -> Option<MatchAllOrNone> {
        if let Self::ConstPredicate(always_or_never) = self {
            Some(*always_or_never)
        } else {
            None
        }
    }

    pub fn match_all() -> Self {
        Self::ConstPredicate(MatchAllOrNone::MatchAll)
    }

    pub fn match_none() -> Self {
        Self::ConstPredicate(MatchAllOrNone::MatchNone)
    }

    pub fn simplify(self) -> TantivyQueryAst {
        match self {
            TantivyQueryAst::Bool(bool_query) => bool_query.simplify(),
            ast => ast,
        }
    }
}

impl<Q: TantivyQuery> From<Q> for TantivyQueryAst {
    fn from(query: Q) -> TantivyQueryAst {
        TantivyQueryAst::Leaf(Box::new(query))
    }
}

impl From<TantivyQueryAst> for Box<dyn TantivyQuery> {
    fn from(boxed_tantivy_query: TantivyQueryAst) -> Box<dyn TantivyQuery> {
        match boxed_tantivy_query {
            TantivyQueryAst::Bool(boolean_query) => boolean_query.into(),
            TantivyQueryAst::Leaf(leaf) => leaf,
            TantivyQueryAst::ConstPredicate(always_or_never_match) => match always_or_never_match {
                MatchAllOrNone::MatchAll => Box::new(TantivyAllQuery),
                MatchAllOrNone::MatchNone => Box::new(TantivyEmptyQuery),
            },
        }
    }
}

// Remove the occurrence of trivial AST in the given list of asts.
//
// If `stop_before_empty` is true, then we will make sure to stop removing asts if it is
// the last element.
// This function may change the order of asts.
fn remove_with_guard(
    asts: &mut Vec<TantivyQueryAst>,
    to_remove: MatchAllOrNone,
    stop_before_empty: bool,
) {
    let mut i = 0;
    while i < asts.len() {
        if stop_before_empty && asts.len() == 1 {
            break;
        }
        if asts[i].const_predicate() == Some(to_remove) {
            asts.swap_remove(i);
        } else {
            i += 1;
        }
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub(crate) struct TantivyBoolQuery {
    pub must: Vec<TantivyQueryAst>,
    pub must_not: Vec<TantivyQueryAst>,
    pub should: Vec<TantivyQueryAst>,
    pub filter: Vec<TantivyQueryAst>,
    pub minimum_should_match: Option<usize>,
}

fn simplify_asts(asts: Vec<TantivyQueryAst>) -> Vec<TantivyQueryAst> {
    asts.into_iter().map(|ast| ast.simplify()).collect()
}

impl TantivyBoolQuery {
    pub fn build_clause(operator: BooleanOperand, children: Vec<TantivyQueryAst>) -> Self {
        match operator {
            BooleanOperand::And => Self {
                must: children,
                ..Default::default()
            },
            BooleanOperand::Or => Self {
                should: children,
                ..Default::default()
            },
        }
    }

    pub fn simplify(mut self) -> TantivyQueryAst {
        // simplify sub branches
        self.must = simplify_asts(self.must);
        self.should = simplify_asts(self.should);
        self.must_not = simplify_asts(self.must_not);
        self.filter = simplify_asts(self.filter);

        for must_children in [&mut self.must, &mut self.filter] {
            for child in must_children {
                if child.const_predicate() == Some(MatchAllOrNone::MatchNone) {
                    return TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchNone);
                }
            }
        }
        if self.should.is_empty()
            && self.must.is_empty()
            && self.filter.is_empty()
            && self.must_not.is_empty()
            && self.minimum_should_match.unwrap_or(0) == 0
        {
            // This is just a convention mimicking Elastic/Commonsearch's behavior.
            return TantivyQueryAst::match_all();
        }

        let mut new_must = Vec::with_capacity(self.must.len());
        for must in self.must {
            let mut must_bool = match must {
                TantivyQueryAst::Bool(bool_query) => bool_query,
                _ => {
                    new_must.push(must);
                    continue;
                }
            };
            if must_bool.should.is_empty() && must_bool.minimum_should_match.is_none() {
                new_must.append(&mut must_bool.must);
                self.filter.append(&mut must_bool.filter);
                self.must_not.append(&mut must_bool.must_not);
            } else {
                new_must.push(TantivyQueryAst::Bool(must_bool));
            }
        }
        self.must = new_must;

        let mut new_filter = Vec::with_capacity(self.filter.len());
        for filter in self.filter {
            let mut filter_bool = match filter {
                TantivyQueryAst::Bool(bool_query) => bool_query,
                _ => {
                    new_filter.push(filter);
                    continue;
                }
            };
            if filter_bool.should.is_empty() && filter_bool.minimum_should_match.is_none() {
                new_filter.append(&mut filter_bool.must);
                new_filter.append(&mut filter_bool.filter);
                // must_not doesn't contribute to score, no need to move it to some filter_not kind
                // of thing
                self.must_not.append(&mut filter_bool.must_not);
            } else {
                new_filter.push(TantivyQueryAst::Bool(filter_bool));
            }
        }
        self.filter = new_filter;

        if self.minimum_should_match.is_none() {
            let mut new_should = Vec::with_capacity(self.should.len());
            for should in self.should {
                let mut should_bool = match should {
                    TantivyQueryAst::Bool(bool_query) => bool_query,
                    _ => {
                        new_should.push(should);
                        continue;
                    }
                };
                if should_bool.must.is_empty()
                    && should_bool.filter.is_empty()
                    && should_bool.must_not.is_empty()
                    && should_bool.minimum_should_match.is_none()
                {
                    new_should.append(&mut should_bool.should);
                } else {
                    new_should.push(TantivyQueryAst::Bool(should_bool));
                }
            }
            self.should = new_should;
        }

        // TODO we could turn must_not(must_not(abc, def)) into should(filter(abc), filter(def)),
        // we can't simply have should(abc, def) because of scoring, and should(filter(abc, def))
        // has a different meaning

        // remove sub-queries which don't impact the result
        remove_with_guard(&mut self.must, MatchAllOrNone::MatchAll, true);
        let mut has_no_positive_ast_so_far = self.must.is_empty();
        remove_with_guard(
            &mut self.filter,
            MatchAllOrNone::MatchAll,
            has_no_positive_ast_so_far,
        );
        has_no_positive_ast_so_far &= self.filter.is_empty();
        if !self.filter.is_empty() {
            // if filter is not empty, we can re-try cleaning must. we can't just check
            // has_no_positive_ast_so_far as it would clean must if must or filter contained
            // something
            remove_with_guard(&mut self.must, MatchAllOrNone::MatchAll, false);
        }
        remove_with_guard(
            &mut self.should,
            MatchAllOrNone::MatchNone,
            has_no_positive_ast_so_far,
        );
        has_no_positive_ast_so_far &= self.should.is_empty();
        remove_with_guard(
            &mut self.must_not,
            MatchAllOrNone::MatchNone,
            has_no_positive_ast_so_far,
        );

        for must_child in self.must.iter().chain(self.filter.iter()) {
            if must_child.const_predicate() == Some(MatchAllOrNone::MatchNone) {
                return TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchNone);
            }
        }
        for must_not_child in &self.must_not {
            if must_not_child.const_predicate() == Some(MatchAllOrNone::MatchAll) {
                return TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchNone);
            }
        }
        let has_positive_children =
            !(self.must.is_empty() && self.should.is_empty() && self.filter.is_empty());

        if !has_positive_children {
            if self.minimum_should_match.unwrap_or(0) > 0 {
                return MatchAllOrNone::MatchNone.into();
            }
            if self
                .must_not
                .iter()
                .all(|must_not| must_not.const_predicate() == Some(MatchAllOrNone::MatchNone))
            {
                return MatchAllOrNone::MatchAll.into();
            }
            self.must.push(TantivyQueryAst::match_all());
        } else {
            let num_children =
                self.must.len() + self.should.len() + self.must_not.len() + self.filter.len();
            if num_children == 1
                && self.minimum_should_match.is_none()
                && let Some(ast) = self.must.pop().or(self.should.pop())
            {
                return ast;
            }
            // We do not optimize a single filter clause for the moment.
            // We do need a mechanism to make sure we keep the boost of 0.
        }

        TantivyQueryAst::Bool(self)
    }
}

impl From<TantivyBoolQuery> for TantivyQueryAst {
    fn from(bool_query: TantivyBoolQuery) -> Self {
        TantivyQueryAst::Bool(bool_query)
    }
}

impl From<TantivyBoolQuery> for Box<dyn TantivyQuery> {
    fn from(bool_query: TantivyBoolQuery) -> Box<dyn TantivyQuery> {
        let mut clause: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::with_capacity(
            bool_query.must.len()
                + bool_query.must_not.len()
                + bool_query.should.len()
                + bool_query.filter.len(),
        );
        for (occur, child_asts) in [
            (Occur::Must, bool_query.must),
            (Occur::MustNot, bool_query.must_not),
            (Occur::Should, bool_query.should),
        ] {
            for child_ast in child_asts {
                let sub_query = child_ast.into();
                clause.push((occur, sub_query));
            }
        }
        for filter_child in bool_query.filter {
            let filter_query = filter_child.into();
            clause.push((
                Occur::Must,
                Box::new(TantivyConstScoreQuery::new(filter_query, 0.0f32)),
            ));
        }
        let tantivy_bool_query = if let Some(minimum_should_match) = bool_query.minimum_should_match
        {
            BooleanQuery::with_minimum_required_clauses(clause, minimum_should_match)
        } else {
            BooleanQuery::from(clause)
        };
        Box::new(tantivy_bool_query)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tantivy::query::{EmptyQuery, TermQuery};

    use super::TantivyBoolQuery;
    use crate::query_ast::tantivy_query_ast::{MatchAllOrNone, TantivyQueryAst, remove_with_guard};

    fn term(val: &str) -> TantivyQueryAst {
        use tantivy::schema::{Field, Term};
        TermQuery::new(
            Term::from_field_text(Field::from_field_id(0), val),
            Default::default(),
        )
        .into()
    }

    #[test]
    fn test_simplify_bool_query_with_no_clauses() {
        let bool_query = TantivyBoolQuery::default();
        assert_eq!(bool_query.simplify(), TantivyQueryAst::match_all());
    }

    #[test]
    fn test_remove_with_guard() {
        {
            let mut asts = Vec::new();
            // we are just checking for panics
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, true);
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, false);
        }
        {
            let mut asts = vec![
                MatchAllOrNone::MatchAll.into(),
                MatchAllOrNone::MatchAll.into(),
            ];
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, true);
            assert_eq!(asts.len(), 1);
        }
        {
            let mut asts = vec![
                MatchAllOrNone::MatchAll.into(),
                MatchAllOrNone::MatchAll.into(),
            ];
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, false);
            assert!(asts.is_empty());
        }
        {
            let mut asts = vec![
                MatchAllOrNone::MatchAll.into(),
                MatchAllOrNone::MatchNone.into(),
                MatchAllOrNone::MatchAll.into(),
            ];
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, true);
            assert_eq!(asts.len(), 1);
        }
        {
            let mut asts = vec![
                MatchAllOrNone::MatchAll.into(),
                MatchAllOrNone::MatchNone.into(),
                MatchAllOrNone::MatchAll.into(),
            ];
            remove_with_guard(&mut asts, MatchAllOrNone::MatchAll, false);
            assert_eq!(asts.len(), 1);
        }
    }

    #[test]
    fn test_simplify_bool_query_with_one_clauses() {
        {
            let tantivy_query = EmptyQuery.into();
            let bool_query = TantivyBoolQuery {
                must: vec![tantivy_query],
                ..Default::default()
            };
            assert!(bool_query.simplify().as_leaf().is_some());
        }
        {
            let tantivy_query = EmptyQuery.into();
            let bool_query = TantivyBoolQuery {
                should: vec![tantivy_query],
                ..Default::default()
            };
            assert!(bool_query.simplify().as_leaf().is_some());
        }
        {
            let tantivy_query = EmptyQuery.into();
            let bool_query = TantivyBoolQuery {
                filter: vec![tantivy_query],
                ..Default::default()
            };
            // We do not simplify filter. We somehow need a mechanism to make sure we end up with a
            // const-score.
            assert!(bool_query.simplify().as_leaf().is_none());
        }
    }

    #[test]
    fn test_bool_negative_query_add_wildcard() {
        let tantivy_query = EmptyQuery.into();
        let simplified_ast = TantivyBoolQuery {
            must_not: vec![tantivy_query],
            ..Default::default()
        }
        .simplify();
        let simplified_ast_bool = simplified_ast.as_bool_query().unwrap();
        assert_eq!(simplified_ast_bool.must_not.len(), 1);
        assert_eq!(
            simplified_ast_bool.should.len() + simplified_ast_bool.filter.len(),
            0
        );
        assert_eq!(simplified_ast_bool.must.len(), 1);
        assert_eq!(
            simplified_ast_bool.must[0].const_predicate(),
            Some(MatchAllOrNone::MatchAll)
        );
    }

    #[test]
    fn test_bool_multiple_negative_query_add_wildcard() {
        let simplified_ast = TantivyBoolQuery {
            must_not: vec![EmptyQuery.into(), EmptyQuery.into()],
            ..Default::default()
        }
        .simplify();
        let simplified_ast_bool = simplified_ast.as_bool_query().unwrap();
        assert_eq!(simplified_ast_bool.must_not.len(), 2);
        assert_eq!(
            simplified_ast_bool.should.len() + simplified_ast_bool.filter.len(),
            0
        );
        assert_eq!(simplified_ast_bool.must.len(), 1);
        assert_eq!(
            simplified_ast_bool.must[0].const_predicate(),
            Some(MatchAllOrNone::MatchAll)
        );
    }

    #[test]
    fn test_bool_multiple_negative_query_with_positive() {
        let simplified_ast = TantivyBoolQuery {
            must: vec![EmptyQuery.into()],
            must_not: vec![EmptyQuery.into(), EmptyQuery.into()],
            ..Default::default()
        }
        .simplify();
        let simplified_ast_bool = simplified_ast.as_bool_query().unwrap();
        assert_eq!(simplified_ast_bool.must_not.len(), 2);
        assert_eq!(
            simplified_ast_bool.should.len() + simplified_ast_bool.filter.len(),
            0
        );
        assert_eq!(simplified_ast_bool.must.len(), 1);
        assert!(simplified_ast_bool.must[0].const_predicate().is_none(),);
    }

    #[test]
    fn test_should_lift_simplification() {
        let test_leaf = TantivyQueryAst::Leaf(Box::new(tantivy::query::AllQuery));
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            should: vec![
                test_leaf.clone(),
                TantivyQueryAst::Bool(TantivyBoolQuery {
                    should: vec![test_leaf.clone(), test_leaf],
                    ..Default::default()
                }),
            ],
            ..Default::default()
        });
        let simplified_ast = ast.clone().simplify();
        assert_ne!(simplified_ast, ast);
        let TantivyQueryAst::Bool(bool_query) = simplified_ast else {
            panic!();
        };
        assert_eq!(bool_query.should.len(), 3);
        assert!(bool_query.must.is_empty());
        assert!(bool_query.filter.is_empty());
        assert!(bool_query.must_not.is_empty());
        assert!(bool_query.minimum_should_match.is_none());
    }

    #[test]
    fn test_minimum_should_match_prevent_lift_simplification() {
        let test_leaf = TantivyQueryAst::Leaf(Box::new(tantivy::query::AllQuery));
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            should: vec![
                test_leaf.clone(),
                TantivyQueryAst::Bool(TantivyBoolQuery {
                    should: vec![test_leaf.clone(), test_leaf],
                    ..Default::default()
                }),
            ],
            minimum_should_match: Some(2),
            ..Default::default()
        });
        let simplified_ast = ast.clone().simplify();
        assert_eq!(simplified_ast, ast);
    }

    #[test]
    fn test_simplify_bool_query_with_match_all_must_not_clauses() {
        let tantivy_query = EmptyQuery.into();
        let bool_query = TantivyBoolQuery {
            must: vec![tantivy_query],
            must_not: vec![TantivyQueryAst::match_all()],
            ..Default::default()
        };
        assert_eq!(
            bool_query.simplify().const_predicate(),
            Some(MatchAllOrNone::MatchNone)
        );
    }

    #[test]
    fn test_simplify_bool_query_with_match_must_clauses() {
        let tantivy_query = EmptyQuery.into();
        let bool_query = TantivyBoolQuery {
            must: vec![tantivy_query, TantivyQueryAst::match_all()],
            ..Default::default()
        }
        .simplify();
        assert!(bool_query.as_leaf().is_some());
    }

    #[test]
    fn test_simplify_bool_query_with_match_must_and_other_positive_clauses() {
        let bool_query = TantivyBoolQuery {
            must: vec![TantivyQueryAst::match_all()],
            filter: vec![EmptyQuery.into()],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                filter: vec![EmptyQuery.into()],
                ..Default::default()
            }
            .into()
        );
    }

    #[test]
    fn test_simplify_bool_query_with_match_none_must_clauses() {
        let tantivy_query = EmptyQuery.into();
        let bool_query = TantivyBoolQuery {
            must: vec![TantivyQueryAst::match_none()],
            should: vec![tantivy_query],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query.const_predicate(),
            Some(MatchAllOrNone::MatchNone)
        );
    }

    #[test]
    fn test_simplify_bool_query_with_match_none_no_positive_clauses() {
        let bool_query = TantivyBoolQuery {
            must_not: vec![TantivyQueryAst::match_none()],
            ..Default::default()
        }
        .simplify();
        assert_eq!(bool_query.const_predicate(), Some(MatchAllOrNone::MatchAll));
    }

    #[test]
    fn test_simplify_empty_bool_query_matches_all() {
        let empty_bool_query = TantivyBoolQuery::default().simplify();
        assert_eq!(
            empty_bool_query.const_predicate(),
            Some(MatchAllOrNone::MatchAll)
        );
    }

    #[test]
    fn test_simplify_lift_bool_bool() {
        let bool_query = TantivyBoolQuery {
            must: vec![
                TantivyBoolQuery {
                    must: vec![term("abc"), term("def")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    must: vec![term("ghi"), term("jkl")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                must: vec![term("abc"), term("def"), term("ghi"), term("jkl"),],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            should: vec![
                TantivyBoolQuery {
                    should: vec![term("abc"), term("def")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    should: vec![term("ghi"), term("jkl")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                should: vec![term("abc"), term("def"), term("ghi"), term("jkl"),],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            must: vec![
                TantivyBoolQuery {
                    must: vec![term("abc"), term("def")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    should: vec![term("ghi"), term("jkl")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                must: vec![
                    term("abc"),
                    term("def"),
                    TantivyBoolQuery {
                        should: vec![term("ghi"), term("jkl")],
                        ..Default::default()
                    }
                    .into(),
                ],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            should: vec![
                TantivyBoolQuery {
                    must: vec![term("abc")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    filter: vec![term("ghi")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                should: vec![
                    term("abc"),
                    // filter can't get optimized for scoring reasons
                    TantivyBoolQuery {
                        filter: vec![term("ghi")],
                        ..Default::default()
                    }
                    .into(),
                ],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            must: vec![
                TantivyBoolQuery {
                    should: vec![term("abc")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    should: vec![term("def")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                must: vec![term("abc"), term("def"),],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            must_not: vec![
                TantivyBoolQuery {
                    should: vec![term("abc")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    must: vec![term("def")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                must: vec![MatchAllOrNone::MatchAll.into()],
                must_not: vec![term("abc"), term("def"),],
                ..Default::default()
            }
            .into()
        );

        let bool_query = TantivyBoolQuery {
            must: vec![
                TantivyBoolQuery {
                    must_not: vec![term("abc"), term("def")],
                    ..Default::default()
                }
                .into(),
                TantivyBoolQuery {
                    must_not: vec![term("ghi")],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .simplify();
        assert_eq!(
            bool_query,
            TantivyBoolQuery {
                must: vec![MatchAllOrNone::MatchAll.into()],
                must_not: vec![term("abc"), term("def"), term("ghi"),],
                ..Default::default()
            }
            .into()
        );
    }

    #[derive(Debug, Clone)]
    struct ConstQuery(bool, u32);

    impl tantivy::query::Query for ConstQuery {
        fn weight(
            &self,
            _: tantivy::query::EnableScoring<'_>,
        ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
            unimplemented!()
        }
    }

    impl TantivyQueryAst {
        fn evaluate_test(&self) -> Option<u32> {
            match self {
                TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchNone) => None,
                TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchAll) => Some(0),
                TantivyQueryAst::Bool(bool_query) => bool_query.evaluate_test(),
                TantivyQueryAst::Leaf(query) => {
                    let const_query = query
                        .downcast_ref::<ConstQuery>()
                        .expect("query wasn't a ConstQuery");
                    const_query.0.then_some(const_query.1)
                }
            }
        }
    }

    impl TantivyBoolQuery {
        fn evaluate_test(&self) -> Option<u32> {
            if self
                .must_not
                .iter()
                .any(|sub_ast| sub_ast.evaluate_test().is_some())
            {
                return None;
            }

            let mut should_score = 0u32;
            let mut matching_should_count = 0;
            for should in &self.should {
                if let Some(score) = should.evaluate_test() {
                    should_score += score;
                    matching_should_count += 1;
                }
            }

            if let Some(minimum_should_match) = self.minimum_should_match
                && minimum_should_match > matching_should_count
            {
                return None;
            }

            if self.must.len() + self.filter.len() > 0 {
                if self
                    .must
                    .iter()
                    .all(|sub_ast| sub_ast.evaluate_test().is_some())
                    && self
                        .filter
                        .iter()
                        .all(|sub_ast| sub_ast.evaluate_test().is_some())
                {
                    Some(
                        self.must
                            .iter()
                            .map(|sub_ast| sub_ast.evaluate_test().unwrap())
                            .sum::<u32>()
                            + should_score,
                    )
                } else {
                    None
                }
            } else {
                if self.should.is_empty() {
                    // by convention, an empty query returns all match.
                    return Some(0);
                }
                self.should
                    .iter()
                    .any(|sub_ast| sub_ast.evaluate_test().is_some())
                    .then_some(should_score)
            }
        }
    }

    fn ast_strategy() -> impl Strategy<Value = TantivyQueryAst> {
        let ast_leaf = proptest::prop_oneof![
            Just(TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchNone)),
            Just(TantivyQueryAst::ConstPredicate(MatchAllOrNone::MatchAll)),
            (prop::bool::ANY, 0u32..5)
                .prop_map(|(matc, score)| TantivyQueryAst::Leaf(Box::new(ConstQuery(matc, score)))),
        ];

        ast_leaf.prop_recursive(4, 32, 16, |element| {
            let must = proptest::collection::vec(element.clone(), 0..4);
            let filter = proptest::collection::vec(element.clone(), 0..4);
            let should = proptest::collection::vec(element.clone(), 0..4);
            let must_not = proptest::collection::vec(element.clone(), 0..4);
            let minimum_should_match = (0usize..=2).prop_map(|n: usize| n.checked_sub(1));
            (must, filter, should, must_not, minimum_should_match).prop_map(
                |(must, filter, should, must_not, minimum_should_match)| {
                    TantivyQueryAst::Bool(TantivyBoolQuery {
                        must,
                        filter,
                        should,
                        must_not,
                        minimum_should_match,
                    })
                },
            )
        })
    }

    #[track_caller]
    fn test_aux_simplify_never_change_result(ast: TantivyQueryAst) {
        let simplified_ast = ast.clone().simplify();
        assert_eq!(dbg!(simplified_ast).evaluate_test(), ast.evaluate_test());
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig {
          cases: 100000, .. ProptestConfig::default()
        })]
        #[test]
        fn test_proptest_simplify_never_change_result(ast in ast_strategy()) {
            test_aux_simplify_never_change_result(ast);
        }
    }

    #[test]
    fn test_simplify_never_change_result_simple_corner_case() {
        let ast = TantivyQueryAst::Bool(TantivyBoolQuery {
            minimum_should_match: Some(1),
            ..Default::default()
        });
        test_aux_simplify_never_change_result(ast);
    }
}
