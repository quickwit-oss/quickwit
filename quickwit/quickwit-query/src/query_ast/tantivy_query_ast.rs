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

use tantivy::query::{
    AllQuery as TantivyAllQuery, ConstScoreQuery as TantivyConstScoreQuery,
    EmptyQuery as TantivyEmptyQuery,
};
use tantivy::query_grammar::Occur;

use crate::TantivyQuery;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MatchAllOrNone {
    MatchAll,
    MatchNone,
}

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

impl PartialEq for TantivyQueryAst {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Leaf(left), Self::Leaf(right)) => {
                let left_str: String = format!("{:?}", left);
                let right_str: String = format!("{:?}", right);
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

#[derive(Default, Debug, Eq, PartialEq)]
pub(crate) struct TantivyBoolQuery {
    pub must: Vec<TantivyQueryAst>,
    pub must_not: Vec<TantivyQueryAst>,
    pub should: Vec<TantivyQueryAst>,
    pub filter: Vec<TantivyQueryAst>,
}

fn simplify_asts(asts: Vec<TantivyQueryAst>) -> Vec<TantivyQueryAst> {
    asts.into_iter().map(|ast| ast.simplify()).collect()
}

impl TantivyBoolQuery {
    pub fn simplify(mut self) -> TantivyQueryAst {
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
        self.must
            .retain(|ast| ast.const_predicate() != Some(MatchAllOrNone::MatchAll));
        self.filter
            .retain(|ast| ast.const_predicate() != Some(MatchAllOrNone::MatchAll));
        self.must_not
            .retain(|child| child.const_predicate() != Some(MatchAllOrNone::MatchNone));
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
        self.should
            .retain(|child| child.const_predicate() != Some(MatchAllOrNone::MatchNone));
        let num_children =
            self.must.len() + self.should.len() + self.must_not.len() + self.filter.len();
        if num_children == 0 {
            return TantivyQueryAst::match_all();
        }
        if num_children == 1 {
            if let Some(child) = self.must.pop() {
                return child;
            }
            if let Some(child) = self.should.pop() {
                return child;
            }
            if self.must_not.len() == 1 {
                self.must.push(TantivyQueryAst::match_all());
            }
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
        Box::new(tantivy::query::BooleanQuery::from(clause))
    }
}

#[cfg(test)]
mod tests {
    use tantivy::query::EmptyQuery;

    use super::TantivyBoolQuery;
    use crate::query_ast::tantivy_query_ast::{MatchAllOrNone, TantivyQueryAst};

    #[test]
    fn test_simplify_bool_query_with_no_clauses() {
        let bool_query = TantivyBoolQuery::default();
        assert_eq!(bool_query.simplify(), TantivyQueryAst::match_all());
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
        {
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
}
