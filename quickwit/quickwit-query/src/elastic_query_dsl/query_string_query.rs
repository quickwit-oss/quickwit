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

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::query_grammar::{
    Occur, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::not_nan_f32::NotNaNf32;
use crate::quickwit_query_ast::{self, QueryAst};
use crate::{DefaultOperator, JsonLiteral};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub(crate) struct QueryStringQuery {
    pub query: String,
    /// Limitation. We do not support * at the moment.
    /// We do not support JSON field either.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub default_operator: DefaultOperator,
}

impl ConvertableToQueryAst for QueryStringQuery {
    fn convert_to_query_ast(
        self,
        default_search_fields: &[&str],
    ) -> anyhow::Result<crate::quickwit_query_ast::QueryAst> {
        let user_input_ast = tantivy::query_grammar::parse_query(&self.query)
            .map_err(|_| anyhow::anyhow!("Failed to parse query: `{}`.", &self.query))?;
        let default_occur = match self.default_operator {
            DefaultOperator::And => Occur::Must,
            DefaultOperator::Or => Occur::Should,
        };
        convert_user_input_ast_to_query_ast(user_input_ast, default_occur, default_search_fields)
    }
}

fn convert_user_input_literal(
    user_input_literal: UserInputLiteral,
    default_search_fields: &[&str],
) -> QueryAst {
    let UserInputLiteral {
        field_name,
        phrase,
        slop,
    } = user_input_literal;
    let field_names: Vec<String> = if let Some(field_name) = field_name {
        vec![field_name]
    } else {
        default_search_fields
            .iter()
            .map(|field_name| field_name.to_string())
            .collect()
    };
    let mut phrase_queries: Vec<QueryAst> = field_names
        .into_iter()
        .map(|field_name| {
            quickwit_query_ast::PhraseQuery {
                field: field_name,
                phrase: phrase.clone(),
                slop,
            }
            .into()
        })
        .collect();
    if phrase_queries.is_empty() {
        QueryAst::MatchNone
    } else if phrase_queries.len() == 1 {
        phrase_queries.pop().unwrap().into()
    } else {
        quickwit_query_ast::BoolQuery {
            should: phrase_queries,
            ..Default::default()
        }
        .into()
    }
}

fn convert_user_input_ast_to_query_ast(
    user_input_ast: UserInputAst,
    default_occur: Occur,
    default_search_fields: &[&str],
) -> anyhow::Result<QueryAst> {
    match user_input_ast {
        UserInputAst::Clause(clause) => {
            let mut bool_query = quickwit_query_ast::BoolQuery::default();
            for (occur_opt, sub_ast) in clause {
                let sub_ast = convert_user_input_ast_to_query_ast(
                    sub_ast,
                    default_occur,
                    default_search_fields,
                )?;
                let children_ast_for_occur: &mut Vec<QueryAst> =
                    match occur_opt.unwrap_or(default_occur) {
                        Occur::Should => &mut bool_query.should,
                        Occur::Must => &mut bool_query.must,
                        Occur::MustNot => &mut bool_query.must_not,
                    };
                children_ast_for_occur.push(sub_ast);
            }
            Ok(bool_query.into())
        }
        UserInputAst::Leaf(leaf) => match *leaf {
            UserInputLeaf::Literal(literal) => {
                Ok(convert_user_input_literal(literal, default_search_fields))
            }
            UserInputLeaf::All => Ok(QueryAst::MatchAll),
            UserInputLeaf::Range {
                field,
                lower,
                upper,
            } => {
                let field: String = field.context("Range query without field is not supported.")?;
                let convert_bound = |user_input_bound: UserInputBound| match user_input_bound {
                    UserInputBound::Inclusive(user_text) => {
                        Bound::Included(JsonLiteral::String(user_text))
                    }
                    UserInputBound::Exclusive(user_text) => {
                        Bound::Excluded(JsonLiteral::String(user_text))
                    }
                    UserInputBound::Unbounded => Bound::Unbounded,
                };
                let range_query = quickwit_query_ast::RangeQuery {
                    field,
                    lower_bound: convert_bound(lower),
                    upper_bound: convert_bound(upper),
                };
                Ok(range_query.into())
            }
            UserInputLeaf::Set { field, elements } => {
                let fields: Vec<&str> = if let Some(field) = field.as_ref() {
                    vec![field.as_str()]
                } else {
                    default_search_fields.to_vec()
                };
                let mut terms_per_field: HashMap<String, HashSet<String>> = Default::default();
                let terms: HashSet<String> = elements.into_iter().collect();
                for field in fields {
                    terms_per_field.insert(field.to_string(), terms.clone());
                }
                let term_set_query = quickwit_query_ast::TermSetQuery { terms_per_field };
                Ok(term_set_query.into())
            }
        },
        UserInputAst::Boost(underlying, boost) => {
            let query_ast = convert_user_input_ast_to_query_ast(
                *underlying,
                default_occur,
                default_search_fields,
            )?;
            let boost: NotNaNf32 = (boost as f32)
                .try_into()
                .map_err(|err_msg: &str| anyhow::anyhow!(err_msg))?;
            Ok(QueryAst::Boost {
                underlying: Box::new(query_ast),
                boost,
            })
        }
    }
}
