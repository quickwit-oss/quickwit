// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashSet;

use tantivy::query::QueryParserError as TantivyQueryParserError;
use tantivy_query_grammar::{Occur, UserInputAst, UserInputLeaf, UserInputLiteral};

use crate::QueryParserError;

/// Extracts all filters related to tag fields.
/// Returns a list of `tag_field_name:tag_value` found in the query.
pub fn extract_tags_from_query(
    raw_query: &str,
    tag_field_names: &HashSet<String>,
) -> Result<Option<TagFiltersAST>, QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(raw_query)
        .map_err(|_| TantivyQueryParserError::SyntaxError)?;
    let filters_ast = collect_tag_filters(user_input_ast, tag_field_names);
    Ok(simplify_ast(filters_ast))
}

/// Intermediary AST that may contain leaf that are
/// equivalent to the "True" predicate.
#[derive(Debug, PartialEq, Eq, Clone)]
enum UnsimplifiedTagFiltersAST {
    And(Vec<UnsimplifiedTagFiltersAST>),
    Or(Vec<UnsimplifiedTagFiltersAST>),
    Tag { tag: String },
    True,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TagFiltersAST {
    And(Vec<TagFiltersAST>),
    Or(Vec<TagFiltersAST>),
    Tag { tag: String },
}

// Takes a tag AST and simplify it in such a way that the resulting tree:
// - represents a boolean expression that is equivalent to the original ast
// - it can be equal to true, or to false, but if it is isn't True and False does not appear in the
//   AST
// - it does not contain any NOT clause.
//
// Returning None here, is to be interpreted as returning `True`.
//
// The latter two conditions are enforced by the type system.
fn simplify_ast(ast: UnsimplifiedTagFiltersAST) -> Option<TagFiltersAST> {
    match ast {
        UnsimplifiedTagFiltersAST::And(conditions) => {
            let mut pruned_conditions: Vec<TagFiltersAST> =
                conditions.into_iter().filter_map(simplify_ast).collect();
            match pruned_conditions.len() {
                0 => None,
                1 => pruned_conditions.pop().unwrap().into(),
                _ => TagFiltersAST::And(pruned_conditions).into(),
            }
        }
        UnsimplifiedTagFiltersAST::Or(conditions) => {
            let mut pruned_conditions: Vec<TagFiltersAST> = Vec::new();
            for condition in conditions {
                // If we get None as part of the condition here, we return None
                // directly. (Remember None == True0.
                pruned_conditions.push(simplify_ast(condition)?);
            }
            match pruned_conditions.len() {
                0 => None,
                1 => pruned_conditions.pop().unwrap().into(),
                _ => TagFiltersAST::Or(pruned_conditions).into(),
            }
        }
        UnsimplifiedTagFiltersAST::Tag { tag } => TagFiltersAST::Tag { tag }.into(),
        UnsimplifiedTagFiltersAST::True => None,
    }
}

fn collect_tag_filters_for_clause(
    clause: Vec<(Occur, UnsimplifiedTagFiltersAST)>,
) -> UnsimplifiedTagFiltersAST {
    if clause.len() == 0 {
        return UnsimplifiedTagFiltersAST::True;
    }
    if clause.iter().any(|(occur, _)| occur == &Occur::Must) {
        let removed_should_clause: Vec<UnsimplifiedTagFiltersAST> = clause
            .into_iter()
            .filter_map(|(occur, ast)| match occur {
                Occur::Must => Some(ast),
                Occur::MustNot => Some(UnsimplifiedTagFiltersAST::True),
                Occur::Should => None,
            })
            .collect();
        // We will handle the case where removed_should_clause.len() == 1 in the simply
        // phase.
        return UnsimplifiedTagFiltersAST::And(removed_should_clause);
    }
    let converted_not_clause = clause
        .into_iter()
        .map(|(occur, ast)| match occur {
            Occur::MustNot => UnsimplifiedTagFiltersAST::True,
            Occur::Should => ast,
            Occur::Must => {
                unreachable!("This should never happen due to check above.")
            }
        })
        .collect();
    UnsimplifiedTagFiltersAST::Or(converted_not_clause)
}

/// This function takes a user input AST and extracts a boolean formula
/// using only tags fields, true, and false as literals such that the
/// query implies this boolean formula.
///
/// MustNot are transformed into `True` to keep the code simple
/// and correct.
/// TermQuery on fields that are not tag fields are transformed into the predicate `True`.
///
///
/// In other words, we are guaranteed that if we were to run the query
/// described by this predicate only, the matched documents would all
/// be in the original query too (The opposite is rarely true).
fn collect_tag_filters(
    user_input_ast: UserInputAst,
    tag_field_names: &HashSet<String>,
) -> UnsimplifiedTagFiltersAST {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            let clause_with_resolved_occur: Vec<(Occur, UnsimplifiedTagFiltersAST)> = sub_queries
                .into_iter()
                .map(|(occur_opt, ast)| {
                    (
                        occur_opt.unwrap_or(Occur::Should),
                        collect_tag_filters(ast, tag_field_names),
                    )
                })
                .collect();
            collect_tag_filters_for_clause(clause_with_resolved_occur)
        }
        UserInputAst::Boost(ast, _) => collect_tag_filters(*ast, tag_field_names),
        UserInputAst::Leaf(leaf) => match *leaf {
            UserInputLeaf::Literal(UserInputLiteral {
                field_name: Some(field_name),
                phrase,
            }) => {
                if tag_field_names.contains(&field_name) {
                    UnsimplifiedTagFiltersAST::Tag {
                        tag: format!("{}:{}", field_name, phrase),
                    }
                } else {
                    UnsimplifiedTagFiltersAST::True
                }
            }
            UserInputLeaf::Literal(UserInputLiteral {
                field_name: None, ..
            })
            | UserInputLeaf::All
            | UserInputLeaf::Range { .. } => UnsimplifiedTagFiltersAST::True,
        },
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;

    use super::{extract_tags_from_query, TagFiltersAST};

    fn hashset(tags: &[&str]) -> HashSet<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    #[test]
    fn test_extract_tags_from_query_invalid_query() -> anyhow::Result<()> {
        assert!(matches!(
            extract_tags_from_query(":>", &hashset(&["bart", "lisa"])),
            Err(..)
        ));
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_all() -> anyhow::Result<()> {
        assert_eq!(extract_tags_from_query("*", &hashset(&[]))?, None);
        assert_eq!(
            extract_tags_from_query("*", &hashset(&["user", "lang"]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_range_query() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("title:>foo lang:fr", &hashset(&["user", "lang"]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_mixed_disjunction() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("title:foo user:bart lang:fr", &hashset(&["user", "lang"]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_and_or() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query(
                "title:foo AND (user:bart OR lang:fr)",
                &hashset(&["user", "lang"])
            )?
            .unwrap(),
            TagFiltersAST::Or(vec![
                TagFiltersAST::Tag {
                    tag: String::from("user:bart")
                },
                TagFiltersAST::Tag {
                    tag: String::from("lang:fr")
                }
            ]),
        );
        assert_eq!(
            extract_tags_from_query(
                "title:foo AND (user:bart OR lang:fr)",
                &hashset(&["title", "lang"])
            )?
            .unwrap(),
            TagFiltersAST::Tag {
                tag: String::from("title:foo"),
            },
        );
        Ok(())
    }

    #[test]
    fn test_conjunction_of_tags() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(user:bart AND lang:fr)", &hashset(&["user", "lang"]))?
                .unwrap(),
            TagFiltersAST::And(vec![
                TagFiltersAST::Tag {
                    tag: "user:bart".to_string()
                },
                TagFiltersAST::Tag {
                    tag: "lang:fr".to_string()
                }
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_conjunction_of_tag_and_non_tag() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(user:bart AND lang:fr)", &hashset(&["user"]))?.unwrap(),
            TagFiltersAST::Tag {
                tag: "user:bart".to_string()
            }
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_and_non_tag() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(user:bart OR lang:fr)", &hashset(&["user"]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_disjunction_with_not_clause() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(user:bart -lang:fr)", &hashset(&["user", "lang"]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_conjunction_with_not_clause() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("user:bart AND NOT lang:fr", &hashset(&["user", "lang"]))?
                .unwrap(),
            TagFiltersAST::Tag {
                tag: "user:bart".to_string()
            }
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_must_should() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(+user:bart lang:fr)", &hashset(&["user", "lang"]))?.unwrap(),
            TagFiltersAST::Tag {
                tag: "user:bart".to_string()
            }
        );
        Ok(())
    }
}
