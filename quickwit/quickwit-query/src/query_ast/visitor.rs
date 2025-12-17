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

use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::cache_node::CacheState;
use crate::query_ast::field_presence::FieldPresenceQuery;
use crate::query_ast::user_input_query::UserInputQuery;
use crate::query_ast::{
    BoolQuery, CacheNode, FullTextQuery, PhrasePrefixQuery, QueryAst, RangeQuery, RegexQuery,
    TermQuery, TermSetQuery, WildcardQuery,
};

/// Simple trait to implement a Visitor over the QueryAst.
pub trait QueryAstVisitor<'a> {
    type Err;

    fn visit(&mut self, query_ast: &'a QueryAst) -> Result<(), Self::Err> {
        match query_ast {
            QueryAst::Bool(bool_query) => self.visit_bool(bool_query),
            QueryAst::Term(term_query) => self.visit_term(term_query),
            QueryAst::TermSet(term_set_query) => self.visit_term_set(term_set_query),
            QueryAst::FullText(full_text_query) => self.visit_full_text(full_text_query),
            QueryAst::PhrasePrefix(phrase_prefix_query) => {
                self.visit_phrase_prefix(phrase_prefix_query)
            }
            QueryAst::Range(range_query) => self.visit_range(range_query),
            QueryAst::MatchAll => self.visit_match_all(),
            QueryAst::MatchNone => self.visit_match_none(),
            QueryAst::Boost { underlying, boost } => self.visit_boost(underlying, *boost),
            QueryAst::UserInput(user_text_query) => self.visit_user_text(user_text_query),
            QueryAst::FieldPresence(exists) => self.visit_exists(exists),
            QueryAst::Wildcard(wildcard) => self.visit_wildcard(wildcard),
            QueryAst::Regex(regex) => self.visit_regex(regex),
            QueryAst::Cache(cache_node) => self.visit_cache_node(cache_node),
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

    fn visit_full_text(&mut self, _full_text: &'a FullTextQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_phrase_prefix(
        &mut self,
        _phrase_query: &'a PhrasePrefixQuery,
    ) -> Result<(), Self::Err> {
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

    fn visit_user_text(&mut self, _user_text_query: &'a UserInputQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_exists(&mut self, _exists_query: &'a FieldPresenceQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_wildcard(&mut self, _wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_regex(&mut self, _regex_query: &'a RegexQuery) -> Result<(), Self::Err> {
        Ok(())
    }

    fn visit_cache_node(&mut self, cache_node: &'a CacheNode) -> Result<(), Self::Err> {
        // this goes a bit again how the rest of the default Visitor behave. The rational is that in
        // practice, on a cache hit, we don't want to do anything with that node.
        // On unitialized cache, any kind of data extract could make sense (extracing tags or
        // timestamp bounds) On cache miss, we still want to know what we need for warmup.
        // But on cache hit, it's too late to do optimisation based on tags and timestamps, and we
        // don't want to warmup anything.
        if !matches!(cache_node.state, CacheState::CacheHit(_)) {
            self.visit(&cache_node.inner)?
        }
        Ok(())
    }
}

/// Simple trait to implement a Visitor over the QueryAst.
pub trait QueryAstTransformer {
    type Err;

    fn transform(&mut self, query_ast: QueryAst) -> Result<Option<QueryAst>, Self::Err> {
        match query_ast {
            QueryAst::Bool(bool_query) => self.transform_bool(bool_query),
            QueryAst::Term(term_query) => self.transform_term(term_query),
            QueryAst::TermSet(term_set_query) => self.transform_term_set(term_set_query),
            QueryAst::FullText(full_text_query) => self.transform_full_text(full_text_query),
            QueryAst::PhrasePrefix(phrase_prefix_query) => {
                self.transform_phrase_prefix(phrase_prefix_query)
            }
            QueryAst::Range(range_query) => self.transform_range(range_query),
            QueryAst::MatchAll => self.transform_match_all(),
            QueryAst::MatchNone => self.transform_match_none(),
            QueryAst::Boost { underlying, boost } => self.transform_boost(*underlying, boost),
            QueryAst::UserInput(user_text_query) => self.transform_user_text(user_text_query),
            QueryAst::FieldPresence(exists) => self.transform_exists(exists),
            QueryAst::Wildcard(wildcard) => self.transform_wildcard(wildcard),
            QueryAst::Regex(regex) => self.transform_regex(regex),
            QueryAst::Cache(cache_node) => self.transform_cache_node(cache_node),
        }
    }

    fn transform_bool(&mut self, mut bool_query: BoolQuery) -> Result<Option<QueryAst>, Self::Err> {
        bool_query.must = bool_query
            .must
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        bool_query.should = bool_query
            .should
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        bool_query.must_not = bool_query
            .must_not
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        bool_query.filter = bool_query
            .filter
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(QueryAst::Bool(bool_query)))
    }

    fn transform_term(&mut self, term_query: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::Term(term_query)))
    }

    fn transform_term_set(
        &mut self,
        term_set: TermSetQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::TermSet(term_set)))
    }

    fn transform_full_text(
        &mut self,
        full_text: FullTextQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::FullText(full_text)))
    }

    fn transform_phrase_prefix(
        &mut self,
        phrase_query: PhrasePrefixQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::PhrasePrefix(phrase_query)))
    }

    fn transform_match_all(&mut self) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::MatchAll))
    }

    fn transform_match_none(&mut self) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::MatchNone))
    }

    fn transform_boost(
        &mut self,
        underlying: QueryAst,
        boost: NotNaNf32,
    ) -> Result<Option<QueryAst>, Self::Err> {
        self.transform(underlying).map(|maybe_ast| {
            maybe_ast.map(|underlying| QueryAst::Boost {
                underlying: Box::new(underlying),
                boost,
            })
        })
    }

    fn transform_range(&mut self, range_query: RangeQuery) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::Range(range_query)))
    }

    fn transform_user_text(
        &mut self,
        user_text_query: UserInputQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::UserInput(user_text_query)))
    }

    fn transform_exists(
        &mut self,
        exists_query: FieldPresenceQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::FieldPresence(exists_query)))
    }

    fn transform_wildcard(
        &mut self,
        wildcard_query: WildcardQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::Wildcard(wildcard_query)))
    }

    fn transform_regex(&mut self, regex_query: RegexQuery) -> Result<Option<QueryAst>, Self::Err> {
        Ok(Some(QueryAst::Regex(regex_query)))
    }

    fn transform_cache_node(
        &mut self,
        cache_node: CacheNode,
    ) -> Result<Option<QueryAst>, Self::Err> {
        if matches!(cache_node.state, CacheState::CacheHit(_)) {
            return Ok(Some(cache_node.into()));
        }
        self.transform(*cache_node.inner).map(|maybe_ast| {
            maybe_ast.map(|inner| {
                QueryAst::Cache(CacheNode {
                    inner: Box::new(inner),
                    state: Default::default(),
                })
            })
        })
    }
}
