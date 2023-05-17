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
use tantivy::schema::Schema as TantivySchema;

use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{self, BuildTantivyAst, QueryAst};
use crate::{DefaultOperator, InvalidQuery, JsonLiteral};

/// A query expressed in the tantivy query grammar DSL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UserInputQuery {
    pub user_text: String,
    // Set of search fields to search into for text not specifically
    // targetting a field.
    //
    // If None, the default search fields, as defined in the DocMapper
    // will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_fields: Option<Vec<String>>,
    pub default_operator: DefaultOperator,
}

impl UserInputQuery {
    /// Parse the user query to generate a structured QueryAST, without any UserInputQuery node.
    ///
    /// The `UserInputQuery` have an optional search_fields property that takes precedence over
    /// the `default_search_fields`.
    ///
    /// In quickwit, the search fields in the `UserInputQuery` are usually supplied with the user
    /// request.
    /// The default_search_fields argument on the other hand, is the default search fields defined
    /// in the `DocMapper`.
    pub fn parse_user_query(&self, default_search_fields: &[String]) -> anyhow::Result<QueryAst> {
        let search_fields = self
            .default_fields
            .as_ref()
            .map(|search_fields| &search_fields[..])
            .unwrap_or(default_search_fields);
        let user_input_ast = tantivy::query_grammar::parse_query(&self.user_text)
            .map_err(|_| anyhow::anyhow!("Failed to parse query: `{}`.", &self.user_text))?;
        let default_occur = match self.default_operator {
            DefaultOperator::And => Occur::Must,
            DefaultOperator::Or => Occur::Should,
        };
        convert_user_input_ast_to_query_ast(user_input_ast, default_occur, search_fields)
    }
}

impl From<UserInputQuery> for QueryAst {
    fn from(user_text_query: UserInputQuery) -> Self {
        QueryAst::UserInput(user_text_query)
    }
}

impl BuildTantivyAst for UserInputQuery {
    fn build_tantivy_ast_impl(
        &self,
        _schema: &TantivySchema,
        _default_search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, crate::InvalidQuery> {
        Err(InvalidQuery::UserQueryNotParsed)
    }
}

fn convert_user_input_ast_to_query_ast(
    user_input_ast: UserInputAst,
    default_occur: Occur,
    default_search_fields: &[String],
) -> anyhow::Result<QueryAst> {
    match user_input_ast {
        UserInputAst::Clause(clause) => {
            let mut bool_query = query_ast::BoolQuery::default();
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
                convert_user_input_literal(literal, default_search_fields)
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
                let range_query = query_ast::RangeQuery {
                    field,
                    lower_bound: convert_bound(lower),
                    upper_bound: convert_bound(upper),
                };
                Ok(range_query.into())
            }
            UserInputLeaf::Set { field, elements } => {
                let field_names: Vec<String> = if let Some(field) = field.as_ref() {
                    vec![field.to_string()]
                } else {
                    default_search_fields.to_vec()
                };
                if field_names.is_empty() {
                    anyhow::bail!("Set query need to target a specific field.");
                }
                let mut terms_per_field: HashMap<String, HashSet<String>> = Default::default();
                let terms: HashSet<String> = elements.into_iter().collect();
                for field in field_names {
                    terms_per_field.insert(field.to_string(), terms.clone());
                }
                let term_set_query = query_ast::TermSetQuery { terms_per_field };
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

fn convert_user_input_literal(
    user_input_literal: UserInputLiteral,
    default_search_fields: &[String],
) -> anyhow::Result<QueryAst> {
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
    if field_names.is_empty() {
        anyhow::bail!("Query requires a default search field and none was supplied.");
    }
    let mut phrase_queries: Vec<QueryAst> = field_names
        .into_iter()
        .map(|field_name| {
            query_ast::PhraseQuery {
                field: field_name,
                phrase: phrase.clone(),
                slop,
            }
            .into()
        })
        .collect();
    if phrase_queries.is_empty() {
        Ok(QueryAst::MatchNone)
    } else if phrase_queries.len() == 1 {
        Ok(phrase_queries.pop().unwrap())
    } else {
        Ok(query_ast::BoolQuery {
            should: phrase_queries,
            ..Default::default()
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use crate::query_ast::{BoolQuery, BuildTantivyAst, QueryAst, UserInputQuery};
    use crate::{DefaultOperator, InvalidQuery};

    #[test]
    fn test_user_input_query_not_parsed_error() {
        let user_input_query = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: None,
            default_operator: DefaultOperator::And,
        };
        let schema = tantivy::schema::Schema::builder().build();
        {
            let invalid_query = user_input_query
                .build_tantivy_ast_call(&schema, &[], true)
                .unwrap_err();
            assert!(matches!(invalid_query, InvalidQuery::UserQueryNotParsed));
        }
        {
            let invalid_query = user_input_query
                .build_tantivy_ast_call(&schema, &[], false)
                .unwrap_err();
            assert!(matches!(invalid_query, InvalidQuery::UserQueryNotParsed));
        }
    }

    #[test]
    fn test_user_input_query_missing_fields() {
        {
            let invalid_err = UserInputQuery {
                user_text: "hello".to_string(),
                default_fields: None,
                default_operator: DefaultOperator::And,
            }
            .parse_user_query(&[])
            .unwrap_err();
            assert_eq!(
                &invalid_err.to_string(),
                "Query requires a default search field and none was supplied."
            );
        }
        {
            let invalid_err = UserInputQuery {
                user_text: "hello".to_string(),
                default_fields: Some(Vec::new()),
                default_operator: DefaultOperator::And,
            }
            .parse_user_query(&[])
            .unwrap_err();
            assert_eq!(
                &invalid_err.to_string(),
                "Query requires a default search field and none was supplied."
            );
        }
    }

    #[test]
    fn test_user_input_query_predefined_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: None,
            default_operator: DefaultOperator::And,
        }
        .parse_user_query(&["defaultfield".to_string()])
        .unwrap();
        let QueryAst::Phrase(phrase_query) = ast else { panic!() };
        assert_eq!(&phrase_query.field, "defaultfield");
        assert_eq!(&phrase_query.phrase, "hello");
        assert_eq!(phrase_query.slop, 0);
    }

    #[test]
    fn test_user_input_query_override_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: Some(vec!["defaultfield".to_string()]),
            default_operator: DefaultOperator::And,
        }
        .parse_user_query(&["defaultfieldweshouldignore".to_string()])
        .unwrap();
        let QueryAst::Phrase(phrase_query) = ast else { panic!() };
        assert_eq!(&phrase_query.field, "defaultfield");
        assert_eq!(&phrase_query.phrase, "hello");
        assert_eq!(phrase_query.slop, 0);
    }

    #[test]
    fn test_user_input_query_several_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: Some(vec!["fielda".to_string(), "fieldb".to_string()]),
            default_operator: DefaultOperator::And,
        }
        .parse_user_query(&["defaultfieldweshouldignore".to_string()])
        .unwrap();
        let QueryAst::Bool(BoolQuery { should, ..}) = ast else { panic!() };
        assert_eq!(should.len(), 2);
    }

    #[test]
    fn test_user_input_query_field_specified_in_user_input() {
        let ast = UserInputQuery {
            user_text: "myfield:hello".to_string(),
            default_fields: Some(vec!["fieldtoignore".to_string()]),
            default_operator: DefaultOperator::And,
        }
        .parse_user_query(&["fieldtoignore".to_string()])
        .unwrap();
        let QueryAst::Phrase(phrase_query) = ast else { panic!() };
        assert_eq!(&phrase_query.field, "myfield");
        assert_eq!(&phrase_query.phrase, "hello");
        assert_eq!(phrase_query.slop, 0);
    }
}
