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

use crate::match_all::MatchAllQuery;
use crate::match_none::MatchNoneQuery;
use crate::query_string_query::QueryStringQuery;
use crate::range_query::RangeQuery;
use crate::term_query::TermQuery;
use crate::QueryDsl;

pub trait QueryDslVisitor<'a> {
    type Err;

    fn visit(&mut self, query_dsl: &'a QueryDsl) -> Result<(), Self::Err> {
        match query_dsl {
            QueryDsl::QueryString(query_string_query) => {
                self.visit_query_string(query_string_query)
            }
            QueryDsl::Bool(bool_query) => self.visit_bool_query(bool_query),
            QueryDsl::Term(term_query) => self.visit_term(term_query),
            QueryDsl::MatchAll(just_boost) => self.visit_match_all(just_boost),
            QueryDsl::MatchNone(match_none) => self.visit_match_none(match_none),
            QueryDsl::Range(range_query) => self.visit_range(range_query),
        }
    }

    fn visit_query_string(
        &mut self,
        _query_string_query: &'a QueryStringQuery,
    ) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_bool_query(&mut self, bool_query: &'a BoolQuery) -> Result<(), Self::Err> {
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

    fn visit_match_all(&mut self, _match_all: &'a MatchAllQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_match_none(&mut self, _match_none: &'a MatchNoneQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_range(&mut self, _range_query: &'a RangeQuery) -> Result<(), Self::Err> {
        Ok(())
    }
}
