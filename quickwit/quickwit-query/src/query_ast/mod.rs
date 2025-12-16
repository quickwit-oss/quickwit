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
use tantivy::query::BoostQuery as TantivyBoostQuery;
use tantivy::schema::Schema as TantivySchema;

use crate::tokenizers::TokenizerManager;

mod bool_query;
mod cache_node;
mod field_presence;
mod full_text_query;
mod phrase_prefix_query;
mod range_query;
mod regex_query;
mod tantivy_query_ast;
mod term_query;
mod term_set_query;
mod user_input_query;
pub(crate) mod utils;
mod visitor;
mod wildcard_query;

pub use bool_query::BoolQuery;
pub use cache_node::{CacheNode, HitSet, PredicateCache, PredicateCacheInjector};
pub use field_presence::FieldPresenceQuery;
pub use full_text_query::{FullTextMode, FullTextParams, FullTextQuery};
pub use phrase_prefix_query::PhrasePrefixQuery;
pub use range_query::RangeQuery;
pub use regex_query::{AutomatonQuery, JsonPathPrefix, RegexQuery};
use tantivy_query_ast::TantivyQueryAst;
pub use term_query::TermQuery;
pub use term_set_query::TermSetQuery;
pub use user_input_query::UserInputQuery;
pub use visitor::{QueryAstTransformer, QueryAstVisitor};
pub use wildcard_query::WildcardQuery;

use crate::{BooleanOperand, InvalidQuery, NotNaNf32};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum QueryAst {
    Bool(BoolQuery),
    Term(TermQuery),
    TermSet(TermSetQuery),
    FieldPresence(FieldPresenceQuery),
    FullText(FullTextQuery),
    PhrasePrefix(PhrasePrefixQuery),
    Range(RangeQuery),
    UserInput(UserInputQuery),
    Wildcard(WildcardQuery),
    Regex(RegexQuery),
    MatchAll,
    MatchNone,
    Boost {
        underlying: Box<QueryAst>,
        boost: NotNaNf32,
    },
    Cache(CacheNode),
}

impl QueryAst {
    pub fn parse_user_query(
        self: QueryAst,
        default_search_fields: &[String],
    ) -> anyhow::Result<QueryAst> {
        match self {
            QueryAst::Bool(BoolQuery {
                must,
                must_not,
                should,
                filter,
                minimum_should_match,
            }) => {
                let must = parse_user_query_in_asts(must, default_search_fields)?;
                let must_not = parse_user_query_in_asts(must_not, default_search_fields)?;
                let should = parse_user_query_in_asts(should, default_search_fields)?;
                let filter = parse_user_query_in_asts(filter, default_search_fields)?;
                Ok(BoolQuery {
                    must,
                    must_not,
                    should,
                    filter,
                    minimum_should_match,
                }
                .into())
            }
            ast @ QueryAst::Term(_)
            | ast @ QueryAst::TermSet(_)
            | ast @ QueryAst::FullText(_)
            | ast @ QueryAst::PhrasePrefix(_)
            | ast @ QueryAst::MatchAll
            | ast @ QueryAst::MatchNone
            | ast @ QueryAst::FieldPresence(_)
            | ast @ QueryAst::Range(_)
            | ast @ QueryAst::Wildcard(_)
            | ast @ QueryAst::Regex(_) => Ok(ast),
            QueryAst::UserInput(user_text_query) => {
                user_text_query.parse_user_query(default_search_fields)
            }
            QueryAst::Boost { underlying, boost } => {
                let underlying = underlying.parse_user_query(default_search_fields)?;
                Ok(QueryAst::Boost {
                    underlying: Box::new(underlying),
                    boost,
                })
            }
            QueryAst::Cache(cache_node) => {
                let inner = cache_node.inner.parse_user_query(default_search_fields)?;
                let uninitialized =
                    matches!(cache_node.state, cache_node::CacheState::Uninitialized);
                debug_assert!(
                    uninitialized,
                    "QueryAst::parse_user_query called on initialized CacheNode, this is probably \
                     a misstake"
                );
                if !uninitialized {
                    tracing::warn!(
                        "QueryAst::parse_user_query called on initialized CacheNode, cache \
                         discarded"
                    );
                }
                Ok(CacheNode {
                    inner: Box::new(inner),
                    // inner got modified, the result is supposed to be equivalent, but to be safe,
                    // lets reinitialize the cache in practice this function
                    // shouldn't ever be called after cache was resolved
                    state: cache_node::CacheState::Uninitialized,
                }
                .into())
            }
        }
    }

    pub fn boost(self, scale_boost_opt: Option<NotNaNf32>) -> Self {
        let Some(scale_boost) = scale_boost_opt else {
            return self;
        };
        match self {
            QueryAst::Boost { underlying, boost } => {
                let scale_boost_f32: f32 = scale_boost.into();
                let boost_f32: f32 = boost.into();
                let new_boost =
                    NotNaNf32::try_from(scale_boost_f32 * boost_f32).unwrap_or(NotNaNf32::ZERO);
                QueryAst::Boost {
                    underlying,
                    boost: new_boost,
                }
            }
            ast => {
                let underlying = Box::new(ast);
                QueryAst::Boost {
                    underlying,
                    boost: scale_boost,
                }
            }
        }
    }
}

/// Context used when building a tantivy ast.
pub struct BuildTantivyAstContext<'a> {
    pub schema: &'a TantivySchema,
    pub tokenizer_manager: &'a TokenizerManager,
    pub search_fields: &'a [String],
    pub with_validation: bool,
}

impl<'a> BuildTantivyAstContext<'a> {
    pub fn for_test(schema: &'a TantivySchema) -> Self {
        use once_cell::sync::Lazy;

        // we do that to have a TokenizerManager with a long enough lifetime
        static DEFAULT_TOKENIZER_MANAGER: Lazy<TokenizerManager> =
            Lazy::new(crate::create_default_quickwit_tokenizer_manager);

        BuildTantivyAstContext {
            schema,
            tokenizer_manager: &DEFAULT_TOKENIZER_MANAGER,
            search_fields: &[],
            with_validation: true,
        }
    }

    pub fn without_validation(mut self) -> Self {
        self.with_validation = false;
        self
    }
}

trait BuildTantivyAst {
    /// Transforms a query Ast node into a TantivyQueryAst.
    ///
    /// This function is supposed to return an error if it detects a problem in the schema.
    /// It can call `into_tantivy_ast_call_me` but should never call `into_tantivy_ast_impl`.
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery>;

    /// This method is meant to be called, but should never be overloaded.
    fn build_tantivy_ast_call(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let tantivy_ast_res = self.build_tantivy_ast_impl(context);
        if !context.with_validation && tantivy_ast_res.is_err() {
            return match tantivy_ast_res {
                res @ Ok(_) | res @ Err(InvalidQuery::UserQueryNotParsed) => res,
                Err(_) => Ok(TantivyQueryAst::match_none()),
            };
        }
        tantivy_ast_res
    }
}

impl BuildTantivyAst for QueryAst {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        match self {
            QueryAst::Bool(bool_query) => bool_query.build_tantivy_ast_call(context),
            QueryAst::Term(term_query) => term_query.build_tantivy_ast_call(context),
            QueryAst::Range(range_query) => range_query.build_tantivy_ast_call(context),
            QueryAst::MatchAll => Ok(TantivyQueryAst::match_all()),
            QueryAst::MatchNone => Ok(TantivyQueryAst::match_none()),
            QueryAst::Boost { boost, underlying } => {
                let underlying = underlying.build_tantivy_ast_call(context)?.simplify();
                let boost_query = TantivyBoostQuery::new(underlying.into(), (*boost).into());
                Ok(boost_query.into())
            }
            QueryAst::TermSet(term_set) => term_set.build_tantivy_ast_call(context),
            QueryAst::FullText(full_text_query) => full_text_query.build_tantivy_ast_call(context),
            QueryAst::PhrasePrefix(phrase_prefix_query) => {
                phrase_prefix_query.build_tantivy_ast_call(context)
            }
            QueryAst::UserInput(user_text_query) => user_text_query.build_tantivy_ast_call(context),
            QueryAst::FieldPresence(field_presence) => {
                field_presence.build_tantivy_ast_call(context)
            }
            QueryAst::Wildcard(wildcard) => wildcard.build_tantivy_ast_call(context),
            QueryAst::Regex(regex) => regex.build_tantivy_ast_call(context),
            QueryAst::Cache(cache_node) => cache_node.build_tantivy_ast_call(context),
        }
    }
}

impl QueryAst {
    pub fn build_tantivy_query(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<Box<dyn crate::TantivyQuery>, InvalidQuery> {
        let tantivy_query_ast = self.build_tantivy_ast_call(context)?;
        Ok(tantivy_query_ast.simplify().into())
    }
}

fn parse_user_query_in_asts(
    asts: Vec<QueryAst>,
    default_search_fields: &[String],
) -> anyhow::Result<Vec<QueryAst>> {
    asts.into_iter()
        .map(|ast| ast.parse_user_query(default_search_fields))
        .collect::<anyhow::Result<_>>()
}

/// Parses a user query and returns a JSON query AST.
///
/// The resulting query does not include `UserInputQuery` nodes.
/// The resolution assumes that there are no default search fields
/// in the doc mapper.
///
/// # Panics
///
/// Panics if the user text is invalid.
pub fn qast_json_helper(user_text: &str, default_fields: &[&'static str]) -> String {
    let ast = qast_helper(user_text, default_fields);
    serde_json::to_string(&ast).expect("The query AST should be JSON serializable.")
}

pub fn qast_helper(user_text: &str, default_fields: &[&'static str]) -> QueryAst {
    let default_fields: Vec<String> = default_fields
        .iter()
        .map(|default_field| default_field.to_string())
        .collect();
    query_ast_from_user_text(user_text, Some(default_fields))
        .parse_user_query(&[])
        .expect("The user query should be valid.")
}

/// Creates a QueryAST with a single UserInputQuery node.
///
/// Disclaimer:
/// At this point the query has not been parsed.
///
/// The actual parsing is meant to happen on a root node,
/// `default_fields` can be passed to decide which field should be search
/// if not specified specifically in the user query (e.g. hello as opposed to "body:hello").
///
/// If it is not supplied, the docmapper search fields are meant to be used.
///
/// If no boolean operator is specified, the default is `AND` (contrary to the Elasticsearch
/// default).
pub fn query_ast_from_user_text(user_text: &str, default_fields: Option<Vec<String>>) -> QueryAst {
    UserInputQuery {
        user_text: user_text.to_string(),
        default_fields,
        default_operator: BooleanOperand::And,
        lenient: false,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
    use crate::query_ast::{
        BoolQuery, BuildTantivyAst, BuildTantivyAstContext, QueryAst, UserInputQuery,
        query_ast_from_user_text,
    };
    use crate::{BooleanOperand, InvalidQuery};

    #[test]
    fn test_user_query_not_parsed() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "*".to_string(),
            default_fields: Default::default(),
            default_operator: Default::default(),
            lenient: false,
        }
        .into();
        let schema = tantivy::schema::Schema::builder().build();
        let build_tantivy_ast_err: InvalidQuery = query_ast
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap_err();
        assert!(matches!(
            build_tantivy_ast_err,
            InvalidQuery::UserQueryNotParsed
        ));
    }

    #[test]
    fn test_user_query_parsed() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "*".to_string(),
            default_fields: Default::default(),
            default_operator: Default::default(),
            lenient: false,
        }
        .into();
        let query_ast_with_parsed_user_query: QueryAst = query_ast.parse_user_query(&[]).unwrap();
        let schema = tantivy::schema::Schema::builder().build();
        let tantivy_query_ast = query_ast_with_parsed_user_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        assert_eq!(&tantivy_query_ast, &TantivyQueryAst::match_all(),);
    }

    #[test]
    fn test_user_query_parsed_query_ast() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "*".to_string(),
            default_fields: Default::default(),
            default_operator: Default::default(),
            lenient: false,
        }
        .into();
        let bool_query_ast: QueryAst = BoolQuery {
            filter: vec![query_ast],
            ..Default::default()
        }
        .into();
        let query_ast_with_parsed_user_query: QueryAst =
            bool_query_ast.parse_user_query(&[]).unwrap();
        let schema = tantivy::schema::Schema::builder().build();
        let tantivy_query_ast = query_ast_with_parsed_user_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let tantivy_query_ast_simplified = tantivy_query_ast.simplify();
        // This does not get more simplified than this, because we need the boost 0 score.
        let tantivy_bool_query = tantivy_query_ast_simplified.as_bool_query().unwrap();
        assert_eq!(tantivy_bool_query.must.len(), 0);
        assert_eq!(tantivy_bool_query.should.len(), 0);
        assert_eq!(tantivy_bool_query.must_not.len(), 0);
        assert_eq!(tantivy_bool_query.filter.len(), 1);
        assert_eq!(&tantivy_bool_query.filter[0], &TantivyQueryAst::match_all(),);
    }

    #[test]
    fn test_query_parse_default_occur_must() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "field:hello field:toto".to_string(),
            default_fields: None,
            default_operator: crate::BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::Bool(bool_query) = query_ast else {
            panic!()
        };
        assert_eq!(bool_query.must.len(), 2);
    }

    #[test]
    fn test_query_parse_default_occur_should() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "field:hello field:toto".to_string(),
            default_fields: None,
            default_operator: crate::BooleanOperand::Or,
            lenient: false,
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::Bool(bool_query) = query_ast else {
            panic!()
        };
        assert_eq!(bool_query.should.len(), 2);
    }

    #[test]
    fn test_query_ast_from_user_text_default_as_and() {
        let ast = query_ast_from_user_text("hello you", None);
        let QueryAst::UserInput(input_query) = ast else {
            panic!()
        };
        assert_eq!(input_query.default_operator, BooleanOperand::And);
    }
}
