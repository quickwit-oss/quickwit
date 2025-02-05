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
