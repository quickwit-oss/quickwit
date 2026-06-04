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

use std::collections::BTreeSet;
use std::fmt::Display;

use quickwit_query::query_ast::QueryAst;
use serde::{Deserialize, Serialize};
use tantivy::query_grammar::Occur;

/// Returns true if and only if tag is of form `{field_name}:any_value`.
pub fn match_tag_field_name(field_name: &str, tag: &str) -> bool {
    tag.len() > field_name.len()
        && tag.as_bytes()[field_name.len()] == b':'
        && tag.starts_with(field_name)
}

/// Tags a user query and returns a TagFilterAst that represents a filtering
/// predicate over a set of tags.
///
/// If the predicate evaluates to false for a given set of tags associated with
/// a split, we are guaranteed that no documents in the split matches the query.
///
/// Setting `tag_fields` to `None` will create an AST with all possible tag
/// filters from the query. This ensures that all pruning opportunities are
/// considered, but it can also lead to very large tag filter ASTs. This can put
/// a lot of pressure on the metastore.
pub fn extract_tags_from_query(
    query_ast: QueryAst,
    tag_fields: Option<&BTreeSet<String>>,
) -> Option<TagFilterAst> {
    if let Some(tag_fields) = tag_fields
        && tag_fields.is_empty()
    {
        return None;
    }
    let mut unsimplified = extract_unsimplified_tags_filter_ast(query_ast);
    if let Some(tag_fields) = tag_fields {
        unsimplified = prune_unsimplified_tag_filter_ast(unsimplified, tag_fields);
    }
    let term_filters_ast = simplify_ast(unsimplified)?;
    Some(expand_to_tag_ast(term_filters_ast))
}

/// Replaces every `Tag` node whose field is not in `tag_fields` with
/// `Uninformative`, leaving the rest of the tree intact.
fn prune_unsimplified_tag_filter_ast(
    ast: UnsimplifiedTagFilterAst,
    tag_fields: &BTreeSet<String>,
) -> UnsimplifiedTagFilterAst {
    match ast {
        UnsimplifiedTagFilterAst::And(children) => UnsimplifiedTagFilterAst::And(
            children
                .into_iter()
                .map(|child| prune_unsimplified_tag_filter_ast(child, tag_fields))
                .collect(),
        ),
        UnsimplifiedTagFilterAst::Or(children) => UnsimplifiedTagFilterAst::Or(
            children
                .into_iter()
                .map(|child| prune_unsimplified_tag_filter_ast(child, tag_fields))
                .collect(),
        ),
        UnsimplifiedTagFilterAst::Tag { ref field, .. } => {
            if tag_fields.contains(field) {
                ast
            } else {
                UnsimplifiedTagFilterAst::Uninformative
            }
        }
        UnsimplifiedTagFilterAst::Uninformative => UnsimplifiedTagFilterAst::Uninformative,
    }
}

fn extract_unsimplified_tags_filter_ast(query_ast: QueryAst) -> UnsimplifiedTagFilterAst {
    match query_ast {
        QueryAst::Bool(bool_query) => {
            let mut clause_with_resolved_occur: Vec<(Occur, UnsimplifiedTagFilterAst)> = Vec::new();
            for (occur, children) in [
                (Occur::Must, bool_query.must),
                (Occur::Must, bool_query.filter),
                (Occur::Should, bool_query.should),
                (Occur::MustNot, bool_query.must_not),
            ] {
                for child_ast in children {
                    let child_unsimplified_tag_ast =
                        extract_unsimplified_tags_filter_ast(child_ast);
                    clause_with_resolved_occur.push((occur, child_unsimplified_tag_ast));
                }
            }
            collect_tag_filters_for_clause(clause_with_resolved_occur)
        }
        QueryAst::Term(term_query) => UnsimplifiedTagFilterAst::Tag {
            is_present: true,
            field: term_query.field,
            value: term_query.value,
        },
        QueryAst::MatchAll | QueryAst::MatchNone => UnsimplifiedTagFilterAst::Uninformative,
        QueryAst::Range(_) => {
            // We could technically add support for range over some quantitive tag value (like we do
            // for timestamps). This is not supported at this point.
            UnsimplifiedTagFilterAst::Uninformative
        }
        QueryAst::TermSet(term_set) => {
            let children: Vec<UnsimplifiedTagFilterAst> = term_set
                .terms_per_field
                .into_iter()
                .flat_map(|(field, terms)| {
                    terms
                        .into_iter()
                        .map(move |term| UnsimplifiedTagFilterAst::Tag {
                            is_present: true,
                            field: field.clone(),
                            value: term,
                        })
                })
                .collect();
            UnsimplifiedTagFilterAst::Or(children)
        }
        QueryAst::FullText(full_text_query) => {
            // TODO This is a bug in a sense.
            // A phrase is supposed to go through the tokenizer.
            UnsimplifiedTagFilterAst::Tag {
                is_present: true,
                field: full_text_query.field,
                value: full_text_query.text,
            }
        }
        QueryAst::PhrasePrefix(phrase_prefix_query) => {
            // TODO same as FullText above.
            UnsimplifiedTagFilterAst::Tag {
                is_present: true,
                field: phrase_prefix_query.field,
                value: phrase_prefix_query.phrase,
            }
        }
        QueryAst::Wildcard(wildcard_query) => {
            // TODO same as FullText above.
            UnsimplifiedTagFilterAst::Tag {
                is_present: true,
                field: wildcard_query.field,
                value: wildcard_query.value,
            }
        }
        QueryAst::Boost { underlying, .. } => extract_unsimplified_tags_filter_ast(*underlying),
        QueryAst::UserInput(_user_text_query) => {
            panic!("Extract unsimplified should only be called on AST without UserInputQuery.");
        }
        QueryAst::FieldPresence(_) => UnsimplifiedTagFilterAst::Uninformative,
        QueryAst::Regex(_) => UnsimplifiedTagFilterAst::Uninformative,
        QueryAst::Cache(cache_node) => extract_unsimplified_tags_filter_ast(*cache_node.inner),
    }
}

/// Intermediary AST that may contain leaf that are
/// equivalent to the "Uninformative" predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    Term { field: String, value: String },
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
                write!(f, "{tag}")?;
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
                write!(f, " ∨ {child}")?;
            } else {
                write!(f, " ∧ {child}")?;
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
        } => {
            if is_present {
                Some(TermFilterAst::Term { field, value })
            } else {
                // we can't do tag pruning on negative filters. If `field` can be one of 1 or 2,
                // and we search for not(1), we don't want to remove a split where
                // tags=[1,2] (which is_present: false does). It's even more problematic if some
                // documents have `field` unset, because we don't record that at all, so can't
                // even reject a split based on it having tags=[1].
                None
            }
        }
        UnsimplifiedTagFilterAst::Uninformative => None,
    }
}

/// Special tag to indicate that a field is listed in the
/// `DocMapper` `tag_fields` attribute.
pub fn field_tag(field_name: &str) -> String {
    format!("{field_name}!")
}

fn term_tag(field: &str, value: &str) -> String {
    format!("{field}:{value}")
}

fn expand_to_tag_ast(terms_filter_ast: TermFilterAst) -> TagFilterAst {
    match terms_filter_ast {
        TermFilterAst::And(children) => {
            TagFilterAst::And(children.into_iter().map(expand_to_tag_ast).collect())
        }
        TermFilterAst::Or(children) => {
            TagFilterAst::Or(children.into_iter().map(expand_to_tag_ast).collect())
        }
        TermFilterAst::Term { field, value } => {
            // TODO: this is wasteful when the field is targeted many times, e.g
            // in TermSetQuery queries
            // - (¬user! ∨ user:bart) ∨ (¬user! ∨ user:homer) ∨ (¬user! ∨ user:lisa)
            // - (¬user! ∨ user:bart ∨ user:homer ∨ user:lisa)
            let field_is_tag = TagFilterAst::Tag {
                is_present: false,
                tag: field_tag(&field),
            };
            let term_tag = TagFilterAst::Tag {
                is_present: true,
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
    use std::collections::BTreeSet;

    use quickwit_query::BooleanOperand;
    use quickwit_query::query_ast::{QueryAst, UserInputQuery};

    use super::extract_tags_from_query;
    use crate::tag_pruning::TagFilterAst;

    fn extract_tags_from_query_helper(
        user_query: &str,
        tag_fields: Option<&[&str]>,
    ) -> Option<TagFilterAst> {
        let query_ast: QueryAst = UserInputQuery {
            user_text: user_query.to_string(),
            default_fields: None,
            default_operator: BooleanOperand::Or,
            lenient: false,
        }
        .into();
        let parsed_query_ast = query_ast.parse_user_query(&[]).unwrap();
        let tag_fields_set: Option<BTreeSet<String>> =
            tag_fields.map(|fields| fields.iter().map(|s| s.to_string()).collect());
        extract_tags_from_query(parsed_query_ast, tag_fields_set.as_ref())
    }

    #[test]
    fn test_extract_tags_from_query_all() {
        assert_eq!(extract_tags_from_query_helper("*", None), None);
        assert_eq!(extract_tags_from_query_helper("*", Some(&["title"])), None);
    }

    #[test]
    fn test_extract_tags_from_query_range_query() {
        assert_eq!(
            extract_tags_from_query_helper("title:>foo lang:fr", None),
            None
        );
        assert_eq!(
            extract_tags_from_query_helper("title:>foo lang:fr", Some(&["title"])),
            None
        );
        assert_eq!(
            extract_tags_from_query_helper("title:>foo lang:fr", Some(&[])),
            None
        );
    }

    #[test]
    fn test_extract_tags_from_query_range_query_conjunction() {
        assert_eq!(
            &extract_tags_from_query_helper("title:>foo AND lang:fr", None)
                .unwrap()
                .to_string(),
            "(¬lang! ∨ lang:fr)"
        );
        assert_eq!(
            extract_tags_from_query_helper("title:>foo AND lang:fr", Some(&["title"])),
            None
        );
        assert_eq!(
            extract_tags_from_query_helper("title:>foo AND lang:fr", Some(&["lang"]))
                .unwrap()
                .to_string(),
            "(¬lang! ∨ lang:fr)"
        );
        assert_eq!(
            extract_tags_from_query_helper("title:>foo AND lang:fr", Some(&[])),
            None
        );
    }

    #[test]
    fn test_extract_tags_from_query_and_or() -> anyhow::Result<()> {
        assert_eq!(
            &extract_tags_from_query_helper("title:foo AND (user:bart OR lang:fr)", None)
                .unwrap()
                .to_string(),
            "(¬title! ∨ title:foo) ∧ ((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        // Non-tag fields in the OR branch make it uninformative; it is then dropped from the AND
        assert_eq!(
            &extract_tags_from_query_helper(
                "title:foo AND (user:bart OR lang:fr)",
                Some(&["title", "user"])
            )
            .unwrap()
            .to_string(),
            "(¬title! ∨ title:foo)"
        );
        assert_eq!(
            &extract_tags_from_query_helper(
                "title:foo AND (user:bart OR lang:fr)",
                Some(&["title", "user", "lang"])
            )
            .unwrap()
            .to_string(),
            "(¬title! ∨ title:foo) ∧ ((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        Ok(())
    }

    #[test]
    fn test_conjunction_of_tags() {
        assert_eq!(
            &extract_tags_from_query_helper("(user:bart AND lang:fr)", None)
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart) ∧ (¬lang! ∨ lang:fr)"
        );
        // Non-tag field is dropped from AND, leaving only the tag field
        assert_eq!(
            &extract_tags_from_query_helper("(user:bart AND lang:fr)", Some(&["user"]))
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        assert_eq!(
            &extract_tags_from_query_helper("(user:bart AND lang:fr)", Some(&["user", "lang"]))
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart) ∧ (¬lang! ∨ lang:fr)"
        );
    }

    #[test]
    fn test_disjunction_of_tags() {
        assert_eq!(
            &extract_tags_from_query_helper("(user:bart OR lang:fr)", None)
                .unwrap()
                .to_string(),
            "((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
        // A non-tag field in an OR branch makes the whole disjunction uninformative
        assert_eq!(
            extract_tags_from_query_helper("(user:bart OR lang:fr)", Some(&["user"])),
            None
        );
        assert_eq!(
            &extract_tags_from_query_helper("(user:bart OR lang:fr)", Some(&["user", "lang"]))
                .unwrap()
                .to_string(),
            "((¬user! ∨ user:bart) ∨ (¬lang! ∨ lang:fr))"
        );
    }

    #[test]
    fn test_disjunction_of_tag_disjunction_with_not_clause() {
        // ORed negative tags make the result inconclusive. See simplify_ast() for details
        assert!(extract_tags_from_query_helper("(user:bart OR -lang:fr)", None).is_none());
        assert!(
            extract_tags_from_query_helper("(user:bart OR -lang:fr)", Some(&["user"])).is_none()
        );
    }

    #[test]
    fn test_disjunction_of_tag_conjunction_with_not_clause() {
        // negative tags are removed from AND clauses. See simplify_ast() for details
        assert_eq!(
            &extract_tags_from_query_helper("user:bart AND NOT lang:fr", None)
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        // user is a tag field: NOT lang was already dropped, result is the same
        assert_eq!(
            &extract_tags_from_query_helper("user:bart AND NOT lang:fr", Some(&["user"]))
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        // only lang is a tag field: user becomes uninformative, NOT lang is also dropped
        assert_eq!(
            extract_tags_from_query_helper("user:bart AND NOT lang:fr", Some(&["lang"])),
            None
        );
    }

    #[test]
    fn test_disjunction_of_tag_must_should() {
        assert_eq!(
            &extract_tags_from_query_helper("(+user:bart lang:fr)", None)
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        // Should clauses are dropped when a Must is present; user is a tag field: same result
        assert_eq!(
            &extract_tags_from_query_helper("(+user:bart lang:fr)", Some(&["user"]))
                .unwrap()
                .to_string(),
            "(¬user! ∨ user:bart)"
        );
        // user is not a tag field: the Must clause becomes uninformative
        assert_eq!(
            extract_tags_from_query_helper("(+user:bart lang:fr)", Some(&["lang"])),
            None
        );
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
