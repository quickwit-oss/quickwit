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

use serde::{Deserialize, Serialize};
use tantivy::query::BoostQuery as TantivyBoostQuery;
use tantivy::schema::Schema as TantivySchema;

mod bool_query;
mod full_text_query;
mod phrase_prefix_query;
mod range_query;
mod tantivy_query_ast;
mod term_query;
mod term_set_query;
mod user_input_query;
pub(crate) mod utils;
mod visitor;

pub use bool_query::BoolQuery;
pub use full_text_query::{FullTextMode, FullTextParams, FullTextQuery};
pub use phrase_prefix_query::PhrasePrefixQuery;
pub use range_query::RangeQuery;
use tantivy_query_ast::TantivyQueryAst;
pub use term_query::TermQuery;
pub use term_set_query::TermSetQuery;
pub use user_input_query::UserInputQuery;
pub use visitor::QueryAstVisitor;

use crate::{InvalidQuery, NotNaNf32};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum QueryAst {
    Bool(BoolQuery),
    Term(TermQuery),
    TermSet(TermSetQuery),
    FullText(FullTextQuery),
    PhrasePrefix(PhrasePrefixQuery),
    Range(RangeQuery),
    UserInput(UserInputQuery),
    MatchAll,
    MatchNone,
    Boost {
        underlying: Box<QueryAst>,
        boost: NotNaNf32,
    },
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
                }
                .into())
            }
            ast @ QueryAst::Term(_)
            | ast @ QueryAst::TermSet(_)
            | ast @ QueryAst::FullText(_)
            | ast @ QueryAst::PhrasePrefix(_)
            | ast @ QueryAst::MatchAll
            | ast @ QueryAst::MatchNone
            | ast @ QueryAst::Range(_) => Ok(ast),
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

trait BuildTantivyAst {
    /// Transforms a query Ast node into a TantivyQueryAst.
    ///
    /// This function is supposed to return an error if it detects a problem in the schema.
    /// It can call `into_tantivy_ast_call_me` but should never call `into_tantivy_ast_impl`.
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery>;

    /// This method is meant to be called, but should never be overloaded.
    fn build_tantivy_ast_call(
        &self,
        schema: &TantivySchema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let tantivy_ast_res = self.build_tantivy_ast_impl(schema, search_fields, with_validation);
        if !with_validation && tantivy_ast_res.is_err() {
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
        schema: &TantivySchema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        match self {
            QueryAst::Bool(bool_query) => {
                bool_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::Term(term_query) => {
                term_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::Range(range_query) => {
                range_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::MatchAll => Ok(TantivyQueryAst::match_all()),
            QueryAst::MatchNone => Ok(TantivyQueryAst::match_none()),
            QueryAst::Boost { boost, underlying } => {
                let underlying =
                    underlying.build_tantivy_ast_call(schema, search_fields, with_validation)?;
                let boost_query = TantivyBoostQuery::new(underlying.into(), (*boost).into());
                Ok(boost_query.into())
            }
            QueryAst::TermSet(term_set) => {
                term_set.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::FullText(full_text_query) => {
                full_text_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::PhrasePrefix(phrase_prefix_query) => {
                phrase_prefix_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
            QueryAst::UserInput(user_text_query) => {
                user_text_query.build_tantivy_ast_call(schema, search_fields, with_validation)
            }
        }
    }
}

impl QueryAst {
    pub fn build_tantivy_query(
        &self,
        schema: &TantivySchema,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<Box<dyn crate::TantivyQuery>, InvalidQuery> {
        let tantivy_query_ast =
            self.build_tantivy_ast_call(schema, search_fields, with_validation)?;
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

#[cfg(test)]
mod tests {
    use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
    use crate::query_ast::{BoolQuery, BuildTantivyAst, QueryAst, UserInputQuery};
    use crate::InvalidQuery;

    #[test]
    fn test_user_query_not_parsed() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "*".to_string(),
            default_fields: Default::default(),
            default_operator: Default::default(),
        }
        .into();
        let schema = tantivy::schema::Schema::builder().build();
        let build_tantivy_ast_err: InvalidQuery = query_ast
            .build_tantivy_ast_call(&schema, &[], true)
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
        }
        .into();
        let query_ast_with_parsed_user_query: QueryAst = query_ast.parse_user_query(&[]).unwrap();
        let schema = tantivy::schema::Schema::builder().build();
        let tantivy_query_ast = query_ast_with_parsed_user_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        assert_eq!(&tantivy_query_ast, &TantivyQueryAst::match_all(),);
    }

    #[test]
    fn test_user_query_parsed_query_ast() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "*".to_string(),
            default_fields: Default::default(),
            default_operator: Default::default(),
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
            .build_tantivy_ast_call(&schema, &[], true)
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
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::Bool(bool_query) = query_ast else { panic!() };
        assert_eq!(bool_query.must.len(), 2);
    }

    #[test]
    fn test_query_parse_default_occur_should() {
        let query_ast: QueryAst = UserInputQuery {
            user_text: "field:hello field:toto".to_string(),
            default_fields: None,
            default_operator: crate::BooleanOperand::Or,
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::Bool(bool_query) = query_ast else { panic!() };
        assert_eq!(bool_query.should.len(), 2);
    }
}
