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

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use tantivy::query_grammar::{
    Delimiter, Occur, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::{
    self, BuildTantivyAst, BuildTantivyAstContext, FieldPresenceQuery, FullTextMode,
    FullTextParams, QueryAst, TantivyQueryAst,
};
use crate::{BooleanOperand, InvalidQuery, JsonLiteral};

const DEFAULT_PHRASE_QUERY_MAX_EXPANSION: u32 = 50;

/// A query expressed in the tantivy query grammar DSL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UserInputQuery {
    pub user_text: String,
    // Set of search fields to search into for text not specifically
    // targeting a field.
    //
    // If None, the default search fields, as defined in the DocMapper
    // will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_fields: Option<Vec<String>>,
    pub default_operator: BooleanOperand,
    /// Support missing fields
    pub lenient: bool,
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
            .map_err(|_| anyhow::anyhow!("failed to parse query: `{}`", &self.user_text))?;
        let default_occur = match self.default_operator {
            BooleanOperand::And => Occur::Must,
            BooleanOperand::Or => Occur::Should,
        };
        convert_user_input_ast_to_query_ast(
            user_input_ast,
            default_occur,
            search_fields,
            self.lenient,
        )
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
        _context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, crate::InvalidQuery> {
        Err(InvalidQuery::UserQueryNotParsed)
    }
}

/// Convert the AST of a text query to a QueryAst, filling in default field and default occur when
/// they were not present.
fn convert_user_input_ast_to_query_ast(
    user_input_ast: UserInputAst,
    default_occur: Occur,
    default_search_fields: &[String],
    lenient: bool,
) -> anyhow::Result<QueryAst> {
    match user_input_ast {
        UserInputAst::Clause(clause) => {
            let mut bool_query = query_ast::BoolQuery::default();
            for (occur_opt, sub_ast) in clause {
                let sub_ast = convert_user_input_ast_to_query_ast(
                    sub_ast,
                    default_occur,
                    default_search_fields,
                    lenient,
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
                convert_user_input_literal(literal, default_search_fields, lenient)
            }
            UserInputLeaf::All => Ok(QueryAst::MatchAll),
            UserInputLeaf::Range {
                field,
                lower,
                upper,
            } => {
                let field = if let Some(field) = field {
                    field
                } else if default_search_fields.len() == 1 {
                    default_search_fields[0].clone()
                } else if default_search_fields.is_empty() {
                    bail!("range query without field is not supported");
                } else {
                    bail!("range query with multiple fields is not supported");
                };
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
                    anyhow::bail!("set query need to target a specific field");
                }
                let mut terms_per_field: HashMap<String, BTreeSet<String>> = Default::default();
                let terms: BTreeSet<String> = elements.into_iter().collect();
                for field in field_names {
                    terms_per_field.insert(field.to_string(), terms.clone());
                }
                let term_set_query = query_ast::TermSetQuery { terms_per_field };
                Ok(term_set_query.into())
            }
            UserInputLeaf::Exists { field } => Ok(FieldPresenceQuery { field }.into()),
            UserInputLeaf::Regex { field, pattern } => {
                let field = if let Some(field) = field {
                    field
                } else if default_search_fields.len() == 1 {
                    default_search_fields[0].clone()
                } else if default_search_fields.is_empty() {
                    bail!("regex query without field is not supported");
                } else {
                    bail!("regex query with multiple fields is not supported");
                };
                let regex_query = query_ast::RegexQuery {
                    field,
                    regex: pattern,
                };
                Ok(regex_query.into())
            }
        },
        UserInputAst::Boost(underlying, boost) => {
            let query_ast = convert_user_input_ast_to_query_ast(
                *underlying,
                default_occur,
                default_search_fields,
                lenient,
            )?;
            let boost: NotNaNf32 = (boost.into_inner() as f32)
                .try_into()
                .map_err(|err_msg: &str| anyhow::anyhow!(err_msg))?;
            Ok(QueryAst::Boost {
                underlying: Box::new(query_ast),
                boost,
            })
        }
    }
}

fn is_wildcard(phrase: &str) -> bool {
    use std::ops::ControlFlow;
    enum State {
        Normal,
        Escaped,
    }

    phrase
        .chars()
        .try_fold(State::Normal, |state, c| match state {
            State::Escaped => ControlFlow::Continue(State::Normal),
            State::Normal => {
                if c == '*' || c == '?' {
                    // we are in a wildcard query
                    ControlFlow::Break(())
                } else if c == '\\' {
                    ControlFlow::Continue(State::Escaped)
                } else {
                    ControlFlow::Continue(State::Normal)
                }
            }
        })
        .is_break()
}

/// Convert a leaf of a text query AST to a QueryAst.
/// This may generate more than a single leaf if there are multiple default fields.
fn convert_user_input_literal(
    user_input_literal: UserInputLiteral,
    default_search_fields: &[String],
    lenient: bool,
) -> anyhow::Result<QueryAst> {
    let UserInputLiteral {
        field_name,
        phrase,
        prefix,
        delimiter,
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
        anyhow::bail!("query requires a default search field and none was supplied");
    }
    let mode = match delimiter {
        Delimiter::None => FullTextMode::PhraseFallbackToIntersection,
        Delimiter::SingleQuotes => FullTextMode::Bool {
            operator: BooleanOperand::And,
        },
        Delimiter::DoubleQuotes => FullTextMode::Phrase { slop },
    };
    let full_text_params = FullTextParams {
        tokenizer: None,
        mode,
        zero_terms_query: crate::MatchAllOrNone::MatchNone,
    };
    let wildcard = delimiter == Delimiter::None && is_wildcard(&phrase);
    let mut phrase_queries: Vec<QueryAst> = field_names
        .into_iter()
        .map(|field_name| {
            if prefix {
                query_ast::PhrasePrefixQuery {
                    field: field_name,
                    phrase: phrase.clone(),
                    params: full_text_params.clone(),
                    max_expansions: DEFAULT_PHRASE_QUERY_MAX_EXPANSION,
                    lenient,
                }
                .into()
            } else if wildcard {
                query_ast::WildcardQuery {
                    field: field_name,
                    value: phrase.clone(),
                    lenient,
                    case_insensitive: false,
                }
                .into()
            } else {
                query_ast::FullTextQuery {
                    field: field_name,
                    text: phrase.clone(),
                    params: full_text_params.clone(),
                    lenient,
                }
                .into()
            }
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
    use crate::query_ast::{
        BoolQuery, BuildTantivyAst, BuildTantivyAstContext, FullTextMode, FullTextQuery, QueryAst,
        UserInputQuery,
    };
    use crate::{BooleanOperand, InvalidQuery};

    #[test]
    fn test_user_input_query_not_parsed_error() {
        let user_input_query = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
            lenient: false,
        };
        let schema = tantivy::schema::Schema::builder().build();
        {
            let invalid_query = user_input_query
                .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
                .unwrap_err();
            assert!(matches!(invalid_query, InvalidQuery::UserQueryNotParsed));
        }
        {
            let invalid_query = user_input_query
                .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
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
                default_operator: BooleanOperand::And,
                lenient: false,
            }
            .parse_user_query(&[])
            .unwrap_err();
            assert_eq!(
                &invalid_err.to_string(),
                "query requires a default search field and none was supplied"
            );
        }
        {
            let invalid_err = UserInputQuery {
                user_text: "hello".to_string(),
                default_fields: Some(Vec::new()),
                default_operator: BooleanOperand::And,
                lenient: false,
            }
            .parse_user_query(&[])
            .unwrap_err();
            assert_eq!(
                &invalid_err.to_string(),
                "query requires a default search field and none was supplied"
            );
        }
    }

    #[test]
    fn test_user_input_query_predefined_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&["defaultfield".to_string()])
        .unwrap();
        let QueryAst::FullText(phrase_query) = ast else {
            panic!()
        };
        assert_eq!(&phrase_query.field, "defaultfield");
        assert_eq!(&phrase_query.text, "hello");
        assert_eq!(
            phrase_query.params.mode,
            FullTextMode::PhraseFallbackToIntersection
        );
    }

    #[test]
    fn test_user_input_query_phrase_with_prefix() {
        let ast = UserInputQuery {
            user_text: "field:\"hello\"*".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::PhrasePrefix(phrase_prefix_query) = ast else {
            panic!()
        };
        assert_eq!(&phrase_prefix_query.field, "field");
        assert_eq!(&phrase_prefix_query.phrase, "hello");
        assert_eq!(phrase_prefix_query.max_expansions, 50);
        assert_eq!(
            phrase_prefix_query.params.mode,
            FullTextMode::Phrase { slop: 0 }
        );
    }

    #[test]
    fn test_user_input_query_override_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: Some(vec!["defaultfield".to_string()]),
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&["defaultfieldweshouldignore".to_string()])
        .unwrap();
        let QueryAst::FullText(phrase_query) = ast else {
            panic!()
        };
        assert_eq!(&phrase_query.field, "defaultfield");
        assert_eq!(&phrase_query.text, "hello");
        assert_eq!(
            phrase_query.params.mode,
            FullTextMode::PhraseFallbackToIntersection
        );
    }

    #[test]
    fn test_user_input_query_several_default_fields() {
        let ast = UserInputQuery {
            user_text: "hello".to_string(),
            default_fields: Some(vec!["fielda".to_string(), "fieldb".to_string()]),
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&["defaultfieldweshouldignore".to_string()])
        .unwrap();
        let QueryAst::Bool(BoolQuery { should, .. }) = ast else {
            panic!()
        };
        assert_eq!(should.len(), 2);
    }

    #[test]
    fn test_user_input_query_field_specified_in_user_input() {
        let ast = UserInputQuery {
            user_text: "myfield:hello".to_string(),
            default_fields: Some(vec!["fieldtoignore".to_string()]),
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&["fieldtoignore".to_string()])
        .unwrap();
        let QueryAst::FullText(full_text_query) = ast else {
            panic!()
        };
        assert_eq!(&full_text_query.field, "myfield");
        assert_eq!(&full_text_query.text, "hello");
        assert_eq!(
            full_text_query.params.mode,
            FullTextMode::PhraseFallbackToIntersection
        );
    }

    #[test]
    fn test_user_input_query_different_delimiter() {
        let parse_user_query_delimiter_util = |query: &str| {
            let ast = UserInputQuery {
                user_text: query.to_string(),
                default_fields: None,
                default_operator: BooleanOperand::Or,
                lenient: false,
            }
            .parse_user_query(&[])
            .unwrap();
            let QueryAst::FullText(full_text_query) = ast else {
                panic!()
            };
            full_text_query
        };
        {
            let double_quote_query: FullTextQuery =
                parse_user_query_delimiter_util("jobtitle:\"editor-in-chief\"");
            assert_eq!(&double_quote_query.field, "jobtitle");
            assert_eq!(&double_quote_query.text, "editor-in-chief");
            assert_eq!(
                double_quote_query.params.mode,
                FullTextMode::Phrase { slop: 0 }
            );
        }
        {
            let double_quote_query: FullTextQuery =
                parse_user_query_delimiter_util("jobtitle:\"editor-in-chief\"~2");
            assert_eq!(&double_quote_query.field, "jobtitle");
            assert_eq!(&double_quote_query.text, "editor-in-chief");
            assert_eq!(
                double_quote_query.params.mode,
                FullTextMode::Phrase { slop: 2 }
            );
        }
        {
            let double_quote_query: FullTextQuery =
                parse_user_query_delimiter_util("jobtitle:'editor-in-chief'");
            assert_eq!(&double_quote_query.field, "jobtitle");
            assert_eq!(&double_quote_query.text, "editor-in-chief");
            assert_eq!(
                double_quote_query.params.mode,
                FullTextMode::Bool {
                    operator: BooleanOperand::And
                }
            );
        }
        {
            let double_quote_query: FullTextQuery =
                parse_user_query_delimiter_util("jobtitle:editor-in-chief");
            assert_eq!(&double_quote_query.field, "jobtitle");
            assert_eq!(&double_quote_query.text, "editor-in-chief");
            assert_eq!(
                double_quote_query.params.mode,
                FullTextMode::PhraseFallbackToIntersection
            );
        }
    }

    #[test]
    fn test_user_input_query_regex() {
        let ast = UserInputQuery {
            user_text: "field: /.*/".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
            lenient: false,
        }
        .parse_user_query(&[])
        .unwrap();
        let QueryAst::Regex(regex_query) = ast else {
            panic!()
        };
        assert_eq!(&regex_query.field, "field");
        assert_eq!(&regex_query.regex, ".*");
    }
}
