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

use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use sea_query::{Cond, Expr, all};

/// Takes a tag filter AST and returns a SQL condition using MySQL's `MEMBER OF()`
/// for JSON array membership testing. Tag values are always bound as parameters
/// via `Expr::cust_with_values` to prevent SQL injection.
pub(super) fn generate_sql_condition(tag_ast: &TagFilterAst) -> Cond {
    match tag_ast {
        TagFilterAst::And(child_asts) => {
            if child_asts.is_empty() {
                return all![Expr::cust("TRUE")];
            }
            child_asts
                .iter()
                .map(generate_sql_condition)
                .fold(Cond::all(), |cond, child_cond| cond.add(child_cond))
        }
        TagFilterAst::Or(child_asts) => {
            if child_asts.is_empty() {
                return all![Expr::cust("TRUE")];
            }
            child_asts
                .iter()
                .map(generate_sql_condition)
                .fold(Cond::any(), |cond, child_cond| cond.add(child_cond))
        }
        TagFilterAst::Tag { tag, is_present } => {
            let expr = Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(tag.to_string())))],
            );
            if *is_present {
                all![expr]
            } else {
                all![expr.not()]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag};
    use sea_query::any;

    use super::*;

    fn test_tags_filter_expression_helper(tags_ast: TagFilterAst, expected: Cond) {
        assert_eq!(generate_sql_condition(&tags_ast), expected);
    }

    #[test]
    fn test_tags_filter_expression_single_tag() {
        let tags_ast = tag("my_field:titi");

        let expected = all![Expr::cust_with_values(
            "? MEMBER OF(tags)",
            [sea_query::Value::String(Some(Box::new(
                "my_field:titi".to_string()
            )))],
        )];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_filter_expression_not_tag() {
        let expected = all![
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "my_field:titi".to_string()
                )))],
            )
            .not()
        ];

        test_tags_filter_expression_helper(no_tag("my_field:titi"), expected);
    }

    #[test]
    fn test_tags_filter_expression_ands() {
        let tags_ast = TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2"), tag("tag:val3")]);

        let expected = all![
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "tag:val1".to_string()
                )))],
            ),
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "tag:val2".to_string()
                )))],
            ),
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "tag:val3".to_string()
                )))],
            ),
        ];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_filter_expression_and_or() {
        let tags_ast = TagFilterAst::Or(vec![
            TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);

        let expected = any![
            all![
                Expr::cust_with_values(
                    "? MEMBER OF(tags)",
                    [sea_query::Value::String(Some(Box::new(
                        "tag:val1".to_string()
                    )))],
                ),
                Expr::cust_with_values(
                    "? MEMBER OF(tags)",
                    [sea_query::Value::String(Some(Box::new(
                        "tag:val2".to_string()
                    )))],
                ),
            ],
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "tag:val3".to_string()
                )))],
            ),
        ];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_filter_expression_and_or_correct_parenthesis() {
        let tags_ast = TagFilterAst::And(vec![
            TagFilterAst::Or(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);

        let expected = all![
            any![
                Expr::cust_with_values(
                    "? MEMBER OF(tags)",
                    [sea_query::Value::String(Some(Box::new(
                        "tag:val1".to_string()
                    )))],
                ),
                Expr::cust_with_values(
                    "? MEMBER OF(tags)",
                    [sea_query::Value::String(Some(Box::new(
                        "tag:val2".to_string()
                    )))],
                ),
            ],
            Expr::cust_with_values(
                "? MEMBER OF(tags)",
                [sea_query::Value::String(Some(Box::new(
                    "tag:val3".to_string()
                )))],
            ),
        ];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_special_characters() {
        // Verify that special characters are handled safely via parameter binding
        let tags_ast = tag("tag:value'with\"quotes$and\\backslash");

        let expected = all![Expr::cust_with_values(
            "? MEMBER OF(tags)",
            [sea_query::Value::String(Some(Box::new(
                "tag:value'with\"quotes$and\\backslash".to_string()
            )))],
        )];

        test_tags_filter_expression_helper(tags_ast, expected);
    }
}
