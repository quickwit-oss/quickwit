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

use crate::not_nan_f32::NotNaNf32;
use crate::quickwit_query_ast::{
    BoolQuery, PhraseQuery, QueryAst, RangeQuery, TermQuery, TermSetQuery,
};

pub trait QueryAstVisitor<'a> {
    type Err;

    fn visit(&mut self, query_ast: &'a QueryAst) -> Result<(), Self::Err> {
        match query_ast {
            QueryAst::Bool(bool_query) => self.visit_bool(bool_query),
            QueryAst::Term(term_query) => self.visit_term(term_query),
            QueryAst::TermSet(term_set_query) => self.visit_term_set(term_set_query),
            QueryAst::Phrase(phrase_query) => self.visit_phrase(phrase_query),
            QueryAst::Range(range_query) => self.visit_range(range_query),
            QueryAst::MatchAll => self.visit_match_all(),
            QueryAst::MatchNone => self.visit_match_none(),
            QueryAst::Boost { underlying, boost } => self.visit_boost(&*underlying, *boost),
        }
    }

    fn visit_bool(&mut self, bool_query: &'a BoolQuery) -> Result<(), Self::Err> {
        for ast in bool_query
            .must
            .iter()
            .chain(bool_query.should.iter())
            .chain(bool_query.must_not.iter())
            .chain(bool_query.filter.iter())
        {
            self.visit(ast)?;
        }
        Ok(())
    }

    fn visit_term(&mut self, _term_query: &'a TermQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_term_set(&mut self, _term_query: &'a TermSetQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_phrase(&mut self, _phrase_query: &'a PhraseQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_match_all(&mut self) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_match_none(&mut self) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_boost(
        &mut self,
        underlying: &'a QueryAst,
        _boost: NotNaNf32,
    ) -> Result<(), Self::Err> {
        self.visit(underlying)
    }

    fn visit_range(&mut self, _range_query: &'a RangeQuery) -> Result<(), Self::Err> {
        Ok(())
    }
}
