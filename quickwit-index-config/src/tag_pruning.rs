/// Copyright (C) 2021 Quickwit, Inc.
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
use tantivy::query::QueryParserError as TantivyQueryParserError;
use tantivy_query_grammar::{Occur, UserInputAst, UserInputLeaf, UserInputLiteral};

use crate::QueryParserError;

/// Extracts all filters related to tag fields.
/// Returns a list of `tag_field_name:tag_value` found in the query.
pub fn extract_tags_from_query(
    raw_query: &str,
    tag_field_names: &[String],
) -> Result<Option<TagFiltersAST>, QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(raw_query)
        .map_err(|_| TantivyQueryParserError::SyntaxError)?;
    let filters_ast = collect_tag_filters(&user_input_ast, tag_field_names);
    let pruned_filters = filters_ast.and_then(prune_unusable_filters);
    let simplified_filters = pruned_filters.and_then(simplify_ast);
    Ok(simplified_filters)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TagFiltersAST {
    And(Vec<TagFiltersAST>),
    Or(Vec<TagFiltersAST>),
    Tag { tag: String },
    Not(Box<TagFiltersAST>),
    NoTagFound,
}

fn wrap_with_occur(occur: &Occur, condition: TagFiltersAST) -> Option<TagFiltersAST> {
    match occur {
        Occur::Must => Some(TagFiltersAST::And(vec![condition])),
        Occur::Should => Some(TagFiltersAST::Or(vec![condition])),
        Occur::MustNot => Some(TagFiltersAST::Not(Box::new(condition))),
    }
}

fn simplify_ast(filter: TagFiltersAST) -> Option<TagFiltersAST> {
    match filter {
        TagFiltersAST::And(conditions) if conditions.len() == 1 => {
            let cond = conditions.first().unwrap();
            if cond == &TagFiltersAST::NoTagFound {
                None
            } else {
                simplify_ast(conditions.first().unwrap().clone())
            }
        }
        TagFiltersAST::And(conditions) if conditions.len() == 0 => None,
        TagFiltersAST::And(conditions) if conditions.len() > 1 => Some(TagFiltersAST::And(
            conditions
                .into_iter()
                .flat_map(|c| simplify_ast(c))
                .collect(),
        )),
        TagFiltersAST::Or(conditions) if conditions.len() == 1 => {
            simplify_ast(conditions.first().unwrap().clone())
        }
        TagFiltersAST::Or(conditions) if conditions.len() == 0 => None,
        TagFiltersAST::Or(conditions) if conditions.len() > 1 => {
            let pruned_conditions: Vec<TagFiltersAST> = conditions
                .into_iter()
                .flat_map(|c| simplify_ast(c))
                .collect();
            Some(TagFiltersAST::Or(pruned_conditions))
        }
        ast @ TagFiltersAST::Tag { tag: _ } => Some(ast),
        TagFiltersAST::Not(ast) => simplify_ast(*ast).map(|c| TagFiltersAST::Not(Box::new(c))),
        TagFiltersAST::NoTagFound => None,
        _ => None,
    }
}
fn prune_unusable_filters(filter: TagFiltersAST) -> Option<TagFiltersAST> {
    match filter {
        TagFiltersAST::And(conditions) if conditions.len() == 1 => {
            let cond = conditions.first().unwrap();
            if cond == &TagFiltersAST::NoTagFound {
                None
            } else {
                prune_unusable_filters(conditions.first().unwrap().clone())
            }
        }
        TagFiltersAST::And(conditions) if conditions.len() == 0 => None,
        TagFiltersAST::And(conditions) if conditions.len() > 1 => {
            prune_unusable_filters(TagFiltersAST::And(
                conditions
                    .into_iter()
                    .flat_map(|c| prune_unusable_filters(c))
                    .collect(),
            ))
        }
        TagFiltersAST::Or(conditions) if conditions.len() == 1 => {
            prune_unusable_filters(conditions.first().unwrap().clone())
        }
        TagFiltersAST::Or(conditions) if conditions.len() == 0 => None,
        TagFiltersAST::Or(conditions) if conditions.len() > 1 => {
            let pruned_conditions: Vec<TagFiltersAST> = conditions
                .into_iter()
                .flat_map(|c| prune_unusable_filters(c))
                .collect();
            let is_usefull = !pruned_conditions.contains(&TagFiltersAST::NoTagFound);
            let condition = TagFiltersAST::Or(pruned_conditions);
            if is_usefull {
                Some(condition)
            } else {
                None
            }
        }
        ast @ TagFiltersAST::Tag { tag: _ } => Some(ast),
        TagFiltersAST::Not(ast) => {
            prune_unusable_filters(*ast).map(|c| TagFiltersAST::Not(Box::new(c)))
        }
        TagFiltersAST::NoTagFound => Some(TagFiltersAST::NoTagFound),
        _ => None,
    }
}

fn collect_tag_filters(
    user_input_ast: &UserInputAst,
    tag_field_names: &[String],
) -> Option<TagFiltersAST> {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            let conditions: Vec<TagFiltersAST> = sub_queries
                .iter()
                .flat_map(|(occ, cond)| {
                    collect_tag_filters(cond, tag_field_names)
                        .and_then(|c| wrap_with_occur(&occ.unwrap_or(Occur::Should), c))
                })
                .collect();
            Some(TagFiltersAST::Or(conditions))
        }
        UserInputAst::Boost(ast, _) => collect_tag_filters(ast, tag_field_names),
        UserInputAst::Leaf(leaf) => match &**leaf {
            UserInputLeaf::Literal(UserInputLiteral {
                field_name: Some(field_name),
                phrase,
            }) if tag_field_names.contains(&field_name) => Some(TagFiltersAST::Tag {
                tag: format!("{}:{}", field_name, phrase),
            }),
            _ => Some(TagFiltersAST::NoTagFound),
        },
    }
}

#[cfg(test)]
mod test {

    use crate::tag_pruning::TagFiltersAST;

    use super::extract_tags_from_query;

    #[test]
    fn test_extract_tags_from_query() -> anyhow::Result<()> {
        assert!(matches!(
            extract_tags_from_query(":>", &["bart".to_string(), "lisa".to_string()]),
            Err(..)
        ));

        assert_eq!(extract_tags_from_query("*", &[])?, None);

        assert_eq!(
            extract_tags_from_query("*", &["user".to_string(), "lang".to_string()])?,
            None
        );

        assert_eq!(
            extract_tags_from_query(
                "title:>foo lang:fr",
                &["user".to_string(), "lang".to_string()]
            )?,
            None,
        );

        assert_eq!(
            extract_tags_from_query(
                "title:foo user:bart lang:fr",
                &["user".to_string(), "lang".to_string()]
            )?,
            None
        );
        assert_eq!(
            extract_tags_from_query(
                "title:foo AND (user:bart OR lang:fr)",
                &["user".to_string(), "lang".to_string()]
            )?,
            Some(TagFiltersAST::Or(vec![
                TagFiltersAST::Tag {
                    tag: String::from("user:bart")
                },
                (TagFiltersAST::Tag {
                    tag: String::from("lang:fr")
                })
            ])),
        );
        assert_eq!(
            extract_tags_from_query(
                "title:foo AND (user:bart OR lang:fr)",
                &["title".to_string(), "lang".to_string()]
            )?,
            Some(TagFiltersAST::Tag {
                tag: String::from("title:foo"),
            }),
        );
        Ok(())
    }
}
