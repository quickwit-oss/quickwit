// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use sea_query::{all, Cond, Expr};

// We use dollar-quoted strings in PostgreSQL.
//
// In order to ensure that we do not risk SQL injection,
// we need to generate a string that does not appear in
// the literal we want to dollar quote.
fn generate_dollar_guard(tag: &str) -> String {
    if !tag.contains('$') {
        // That's our happy path here.
        return String::new();
    }
    let mut dollar_guard = String::new();
    loop {
        dollar_guard.push_str("QuickwitGuard");
        // This terminates because `dollar_guard`
        // will eventually be longer than `tag`.
        if !tag.contains(&dollar_guard) {
            return dollar_guard;
        }
    }
}

/// Takes a tag filter AST and returns a SQL expression that can be used as
/// a filter.
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
            let dollar_guard = generate_dollar_guard(tag);
            let expr_str = format!("${dollar_guard}${tag}${dollar_guard}$ = ANY(tags)");
            let expr = if *is_present {
                Expr::cust(expr_str)
            } else {
                Expr::cust(expr_str).not()
            };
            all![expr]
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

        let expected = all![Expr::cust("$$my_field:titi$$ = ANY(tags)")];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_filter_expression_not_tag() {
        let expected = all![Expr::cust("$$my_field:titi$$ = ANY(tags)").not()];

        test_tags_filter_expression_helper(no_tag("my_field:titi"), expected);
    }

    #[test]
    fn test_tags_filter_expression_ands() {
        let tags_ast = TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2"), tag("tag:val3")]);

        let expected = all![
            Expr::cust("$$tag:val1$$ = ANY(tags)"),
            Expr::cust("$$tag:val2$$ = ANY(tags)"),
            Expr::cust("$$tag:val3$$ = ANY(tags)"),
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
                Expr::cust("$$tag:val1$$ = ANY(tags)"),
                Expr::cust("$$tag:val2$$ = ANY(tags)"),
            ],
            Expr::cust("$$tag:val3$$ = ANY(tags)"),
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
                Expr::cust("$$tag:val1$$ = ANY(tags)"),
                Expr::cust("$$tag:val2$$ = ANY(tags)"),
            ],
            Expr::cust("$$tag:val3$$ = ANY(tags)"),
        ];

        test_tags_filter_expression_helper(tags_ast, expected);
    }

    #[test]
    fn test_tags_sql_injection_attempt() {
        let tags_ast = tag("tag:$$;DELETE FROM something_evil");

        let expected = all![Expr::cust(
            "$QuickwitGuard$tag:$$;DELETE FROM something_evil$QuickwitGuard$ = ANY(tags)"
        ),];

        test_tags_filter_expression_helper(tags_ast, expected);

        let tags_ast = tag("tag:$QuickwitGuard$;DELETE FROM something_evil");

        let expected = all![Expr::cust(
            "$QuickwitGuardQuickwitGuard$tag:$QuickwitGuard$;DELETE FROM \
             something_evil$QuickwitGuardQuickwitGuard$ = ANY(tags)"
        )];

        test_tags_filter_expression_helper(tags_ast, expected);
    }
}
