// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::BTreeSet;
use std::fmt::Display;

use serde::{Deserialize, Serialize};
use tantivy::query::QueryParserError as TantivyQueryParserError;
use tantivy_query_grammar::{Occur, UserInputAst, UserInputLeaf, UserInputLiteral};

use crate::QueryParserError;

fn user_input_ast_to_tags_filter_ast(user_input_ast: UserInputAst) -> Option<TagFilterAst> {
    let filters_ast = collect_tag_filters(user_input_ast);
    let term_filters_ast = simplify_ast(filters_ast)?;
    Some(expand_to_tag_ast(term_filters_ast))
}

/// Returns true if and only if tag is of form `{field_name}:any_value`.
pub fn match_tag_field_name(field_name: &str, tag: &str) -> bool {
    tag.len() > field_name.len()
        && tag.as_bytes()[field_name.len()] == b':'
        && tag.starts_with(field_name)
}

/// Tags a user query and returns a TagFilterAst that
/// represents a filtering predicate over a set of tags.
///
/// If the predicate evaluates to false for a given set of tags
/// associated with a split, we are guaranteed that no documents
/// in the split matches the query.
pub fn extract_tags_from_query(user_query: &str) -> Result<Option<TagFilterAst>, QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(user_query)
        .map_err(|_| TantivyQueryParserError::SyntaxError(user_query.to_string()))?;
    Ok(user_input_ast_to_tags_filter_ast(user_input_ast))
}

/// Intermediary AST that may contain leaf that are
/// equivalent to the "Uninformative" predicate.
#[derive(Debug, PartialEq, Eq, Clone)]
enum UnsimplifiedTagFilterAst {
    And(Vec<UnsimplifiedTagFilterAst>),
    Or(Vec<UnsimplifiedTagFilterAst>),
    Tag {
        is_present: bool,
        field: String,
        value: String,
    },
    /// Uninformative represents a node which could be
    /// True or False regardless of the tag values.
    ///
    /// Any subnode of the `UserInputAST` can be
    /// replaced by `Uninformative` while still being correct.
    Uninformative,
}

/// Represents a tag filter used for split pruning.
#[derive(Debug, PartialEq, Clone)]
enum TermFilterAst {
    And(Vec<TermFilterAst>),
    Or(Vec<TermFilterAst>),
    Term {
        is_present: bool,
        field: String,
        value: String,
    },
}

/// Records terms into a set of tags.
///
/// A special tag `{field_name}!` is always added to the tag set.
/// It indicates that `{field_name}` is in the list of the
/// `DocMapper` attribute `tag_fields`.
///
/// See `SplitMetadata` in `quickwit_metastore` for more detail.
pub fn append_to_tag_set(field_name: &str, values: &[String], tag_set: &mut BTreeSet<String>) {
    tag_set.insert(field_tag(field_name));
    for value in values {
        tag_set.insert(term_tag(field_name, value));
    }
}

/// Represents a predicate over the set of tags associated with a given split.
#[allow(missing_docs)]
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum TagFilterAst {
    And(Vec<TagFilterAst>),
    Or(Vec<TagFilterAst>),
    Tag {
        /// If set to false, the predicate tests for the absence of the tag.
        is_present: bool,
        tag: String,
    },
}

impl Display for TagFilterAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (is_or, children) = match self {
            TagFilterAst::And(children) => (false, children),
            TagFilterAst::Or(children) => (true, children),
            TagFilterAst::Tag { is_present, tag } => {
                if !is_present {
                    write!(f, "¬")?;
                }
                write!(f, "{}", tag)?;
                return Ok(());
            }
        };
        if children.is_empty() {
            return Ok(());
        }
        if children.len() == 1 {
            write!(f, "{}", children[0])?;
            return Ok(());
        }
        if is_or {
            write!(f, "(")?;
        }
        let mut children_it = children.iter();
        write!(f, "{}", children_it.next().unwrap())?;
        for child in children_it {
            if is_or {
                write!(f, " ∨ {}", child)?;
            } else {
                write!(f, " ∧ {}", child)?;
            }
        }
        if is_or {
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl TagFilterAst {
    /// Evaluates the tag filter predicate over a set of tags.
    pub fn evaluate(&self, tag_set: &BTreeSet<String>) -> bool {
        match self {
            TagFilterAst::And(children) => {
                children.iter().all(|child_ast| child_ast.evaluate(tag_set))
            }
            TagFilterAst::Or(children) => {
                children.iter().any(|child_ast| child_ast.evaluate(tag_set))
            }
            TagFilterAst::Tag { is_present, tag } => tag_set.contains(tag) == *is_present,
        }
    }
}

// Takes a tag AST and simplify it.
//
// The resulting AST does not contain any uninformative leaves.
//
// Returning None here, is to be interpreted as returning `True`.
fn simplify_ast(ast: UnsimplifiedTagFilterAst) -> Option<TermFilterAst> {
    match ast {
        UnsimplifiedTagFilterAst::And(conditions) => {
            let mut pruned_conditions: Vec<TermFilterAst> =
                conditions.into_iter().filter_map(simplify_ast).collect();
            match pruned_conditions.len() {
                0 => None,
                1 => pruned_conditions.pop().unwrap().into(),
                _ => TermFilterAst::And(pruned_conditions).into(),
            }
        }
        UnsimplifiedTagFilterAst::Or(conditions) => {
            let mut pruned_conditions: Vec<TermFilterAst> = Vec::new();
            for condition in conditions {
                // If we get None as part of the condition here, we return None
                // directly. (Remember None means True).
                pruned_conditions.push(simplify_ast(condition)?);
            }
            match pruned_conditions.len() {
                0 => None,
                1 => pruned_conditions.pop().unwrap().into(),
                _ => TermFilterAst::Or(pruned_conditions).into(),
            }
        }
        UnsimplifiedTagFilterAst::Tag {
            is_present,
            field,
            value,
        } => TermFilterAst::Term {
            is_present,
            field,
            value,
        }
        .into(),
        UnsimplifiedTagFilterAst::Uninformative => None,
    }
}

/// Special tag to indicate that a field is listed in the
/// `DocMapper` `tag_fields` attribute.
pub fn field_tag(field_name: &str) -> String {
    format!("{}!", field_name)
}

fn term_tag(field: &str, value: &str) -> String {
    format!("{}:{}", field, value)
}

fn expand_to_tag_ast(terms_filter_ast: TermFilterAst) -> TagFilterAst {
    match terms_filter_ast {
        TermFilterAst::And(children) => {
            TagFilterAst::And(children.into_iter().map(expand_to_tag_ast).collect())
        }
        TermFilterAst::Or(children) => {
            TagFilterAst::Or(children.into_iter().map(expand_to_tag_ast).collect())
        }
        TermFilterAst::Term {
            is_present,
            field,
            value,
        } => {
            let field_is_tag = TagFilterAst::Tag {
                is_present: false,
                tag: field_tag(&field),
            };
            let term_tag = TagFilterAst::Tag {
                is_present,
                tag: term_tag(&field, &value),
            };
            TagFilterAst::Or(vec![field_is_tag, term_tag])
        }
    }
}

fn collect_tag_filters_for_clause(
    clause: Vec<(Occur, UnsimplifiedTagFilterAst)>,
) -> UnsimplifiedTagFilterAst {
    if clause.is_empty() {
        return UnsimplifiedTagFilterAst::Uninformative;
    }
    if clause.iter().any(|(occur, _)| occur == &Occur::Must) {
        let removed_should_clause: Vec<UnsimplifiedTagFilterAst> = clause
            .into_iter()
            .filter_map(|(occur, ast)| match occur {
                Occur::Must => Some(ast),
                Occur::MustNot => Some(negate_ast(ast)),
                Occur::Should => None,
            })
            .collect();
        // We will handle the case where removed_should_clause.len() == 1 in the simplify
        // phase.
        return UnsimplifiedTagFilterAst::And(removed_should_clause);
    }
    let converted_not_clause = clause
        .into_iter()
        .map(|(occur, ast)| match occur {
            Occur::MustNot => negate_ast(ast),
            Occur::Should => ast,
            Occur::Must => {
                unreachable!("This should never happen due to check above.")
            }
        })
        .collect();
    UnsimplifiedTagFilterAst::Or(converted_not_clause)
}

/// Negate the unsimplified ast, pushing the negation to the leaf
/// using De Morgan's law
/// - NOT( A AND B )=> NOT(A) OR NOT(B)
/// - NOT( A OR B )=> NOT(A) AND NOT(B)
/// - NOT( Tag ) => NotTag
/// - NOT( NotTag ) => Tag
/// - NOT( Uninformative ) => Uninformative.
fn negate_ast(clause: UnsimplifiedTagFilterAst) -> UnsimplifiedTagFilterAst {
    match clause {
        UnsimplifiedTagFilterAst::And(leaves) => {
            UnsimplifiedTagFilterAst::Or(leaves.into_iter().map(negate_ast).collect())
        }
        UnsimplifiedTagFilterAst::Or(leaves) => {
            UnsimplifiedTagFilterAst::And(leaves.into_iter().map(negate_ast).collect())
        }
        UnsimplifiedTagFilterAst::Tag {
            is_present,
            field,
            value,
        } => UnsimplifiedTagFilterAst::Tag {
            is_present: !is_present,
            field,
            value,
        },
        UnsimplifiedTagFilterAst::Uninformative => UnsimplifiedTagFilterAst::Uninformative,
    }
}

/// query implies this boolean formula.

/// TermQuery on fields that are not tag fields are transformed into the predicate `Uninformative`.
///
///
/// In other words, we are guaranteed that if we were to run the query
/// described by this predicate only, the matched documents would all
/// be in the original query too (The opposite is rarely true).
fn collect_tag_filters(user_input_ast: UserInputAst) -> UnsimplifiedTagFilterAst {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            let clause_with_resolved_occur: Vec<(Occur, UnsimplifiedTagFilterAst)> = sub_queries
                .into_iter()
                .map(|(occur_opt, ast)| {
                    (occur_opt.unwrap_or(Occur::Should), collect_tag_filters(ast))
                })
                .collect();
            collect_tag_filters_for_clause(clause_with_resolved_occur)
        }
        UserInputAst::Boost(ast, _) => collect_tag_filters(*ast),
        UserInputAst::Leaf(leaf) => match *leaf {
            UserInputLeaf::Literal(UserInputLiteral {
                field_name: Some(field_name),
                phrase,
                slop: _,
            }) => UnsimplifiedTagFilterAst::Tag {
                is_present: true,
                field: field_name,
                value: phrase,
            },
            UserInputLeaf::Literal(UserInputLiteral {
                field_name: None, ..
            })
            | UserInputLeaf::All
            | UserInputLeaf::Range { .. } => UnsimplifiedTagFilterAst::Uninformative,
        },
    }
}

/// Helper to build a TagFilterAst checking for the presence of a tag.
pub fn tag(tag: impl ToString) -> TagFilterAst {
    TagFilterAst::Tag {
        is_present: true,
        tag: tag.to_string(),
    }
}

/// Helper to build a TagFilterAst checking for the absence of a tag.
pub fn no_tag(tag: impl ToString) -> TagFilterAst {
    TagFilterAst::Tag {
        is_present: false,
        tag: tag.to_string(),
    }
}
#[cfg(test)]
mod test {
    use super::extract_tags_from_query;

    #[test]
    fn test_extract_tags_from_query_invalid_query() -> anyhow::Result<()> {
        assert!(matches!(extract_tags_from_query(":>"), Err(..)));
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_all() -> anyhow::Result<()> {
        assert_eq!(extract_tags_from_query("*")?, None);
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_range_query() -> anyhow::Result<()> {
        assert_eq!(extract_tags_from_query("title:>foo lang:fr")?, None);
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_range_query_conjunction() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("title:>foo AND lang:fr")?
                .unwrap()
                .to_string(),
            "(¬lang! ∨ lang:fr)"
        );
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_mixed_disjunction() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("title:foo user:bart lang:fr")?
                .unwrap()
                .to_string(),
            "((¬title! ∨ title:foo) ∨ (¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        Ok(())
    }

    #[test]
    fn test_extract_tags_from_query_and_or() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("title:foo AND (user:bart OR lang:fr)")?
                .unwrap()
                .to_string(),
            "(¬title! ∨ title:foo) ∧ ((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        Ok(())
    }

    #[test]
    fn test_conjunction_of_tags() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("(user:bart AND lang:fr)")?
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart) ∧ (¬lang! ∨ lang:fr)"
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tags() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("(user:bart OR lang:fr)")?
                .unwrap()
                .to_string(),
            "((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_disjunction_with_not_clause() -> anyhow::Result<()> {
        assert_eq!(
            extract_tags_from_query("(user:bart -lang:fr)")?
                .unwrap()
                .to_string(),
            "((¬user! ∨ user:bart) ∨ (¬lang! ∨ ¬lang:fr))"
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_conjunction_with_not_clause() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("user:bart AND NOT lang:fr")?
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart) ∧ (¬lang! ∨ ¬lang:fr)"
        );
        Ok(())
    }

    #[test]
    fn test_disjunction_of_tag_must_should() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query("(+user:bart lang:fr)")?
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        Ok(())
    }

    #[test]
    fn test_match_tag_field_name() {
        assert!(super::match_tag_field_name("tagfield", "tagfield:val"));
        assert!(super::match_tag_field_name("tagfield", "tagfield:"));
        assert!(!super::match_tag_field_name("tagfield", "tagfield"));
        assert!(!super::match_tag_field_name("tagfield", "tag:val"));
        assert!(!super::match_tag_field_name("tagfield", "tagfiele:val"));
        assert!(!super::match_tag_field_name("tagfield", "t:val"));
    }
}
