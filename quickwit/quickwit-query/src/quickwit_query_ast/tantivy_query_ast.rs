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

use tantivy::query::{AllQuery, ConstScoreQuery, EmptyQuery};
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
pub(crate) enum TantivyQueryAst {
    Bool(TantivyBoolQuery),
    Leaf(Box<dyn TantivyQuery>),
    ConstPredicate(MatchAllOrNone),
}

impl TantivyQueryAst {
    pub fn match_all() -> Self {
        Self::ConstPredicate(MatchAllOrNone::MatchAll)
    }

    pub fn match_none() -> Self {
        Self::ConstPredicate(MatchAllOrNone::MatchNone)
    }

    fn const_predicate(&self) -> Option<MatchAllOrNone> {
        if let Self::ConstPredicate(always_or_never) = self {
            Some(*always_or_never)
        } else {
            None
        }
    }

    pub fn simplify(self) -> TantivyQueryAst {
        match self {
            TantivyQueryAst::Bool(bool_query) => bool_query.simplify(),
            ast @ _ => ast,
        }
    }
}

impl<Q: TantivyQuery> From<Q> for TantivyQueryAst {
    fn from(query: Q) -> TantivyQueryAst {
        TantivyQueryAst::Leaf(Box::new(query))
    }
}

impl Into<Box<dyn TantivyQuery>> for TantivyQueryAst {
    fn into(self) -> Box<dyn TantivyQuery> {
        match self {
            TantivyQueryAst::Bool(boolean_query) => boolean_query.into(),
            TantivyQueryAst::Leaf(leaf) => leaf,
            TantivyQueryAst::ConstPredicate(always_or_never_match) => match always_or_never_match {
                MatchAllOrNone::MatchAll => Box::new(AllQuery),
                MatchAllOrNone::MatchNone => Box::new(EmptyQuery),
            },
        }
    }
}

#[derive(Default)]
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
        self.should
            .retain(|child| child.const_predicate() != Some(MatchAllOrNone::MatchNone));
        let num_children =
            self.must.len() + self.should.len() + self.must_not.len() + self.filter.len();
        if num_children == 0 {
            return TantivyQueryAst::match_none();
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

impl Into<Box<dyn TantivyQuery>> for TantivyBoolQuery {
    fn into(self) -> Box<dyn TantivyQuery> {
        let mut clause: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::with_capacity(
            self.must.len() + self.must_not.len() + self.should.len() + self.filter.len(),
        );
        for (occur, child_asts) in [
            (Occur::Must, self.must),
            (Occur::MustNot, self.must_not),
            (Occur::Should, self.should),
        ] {
            for child_ast in child_asts {
                let sub_query = child_ast.into();
                clause.push((occur, sub_query));
            }
        }
        for filter_child in self.filter {
            let filter_query = filter_child.into();
            clause.push((
                Occur::Must,
                Box::new(ConstScoreQuery::new(filter_query, 0.0f32)),
            ));
        }
        Box::new(tantivy::query::BooleanQuery::from(clause))
    }
}
