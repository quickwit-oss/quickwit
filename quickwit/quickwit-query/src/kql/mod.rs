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

//! KQL (Kibana Query Language) parser and translation to `QueryAst`.
//!
//! # Disambiguation
//!
//! "KQL" is overloaded. Throughout this codebase it refers to the
//! **Kibana** Query Language — a single-expression predicate grammar
//! like `level:error and status:>=500`, documented at
//! <https://www.elastic.co/docs/explore-analyze/query-filter/languages/kql>.
//!
//! It is **not** Microsoft's Kusto Query Language, which is a pipeline
//! language (`Table | where ... | summarize ... | top ...`) used by
//! Azure Data Explorer, Log Analytics, and Sentinel. Kusto is not
//! implemented here; future support for it should land under a
//! non-colliding name (e.g. `kusto`).
//!
//! # Architecture
//!
//! Implemented as a thin translation layer rather than a new query
//! variant: KQL inputs are parsed here and lowered to a `QueryAst` built
//! out of the existing variants (`BoolQuery`, `FullTextQuery`, ...). The
//! core enum, the visitor traits, tag pruning, and root-search are all
//! untouched.
//!
//! Entry point: [`kql_to_query_ast`].

mod ast;
mod kibana_conformance;
mod lexer;
mod lower;
mod metrics;
mod parser;

use std::time::Instant;

use lower::lower_kql_ast;
use parser::parse_kql;

use crate::kql::metrics::{KQL_PARSE_DURATION_SECONDS, KQL_PARSE_FAILURES_TOTAL, KQL_PARSE_TOTAL};
use crate::query_ast::QueryAst;

/// Translate a KQL string into a `QueryAst`.
///
/// `default_fields` carries the user-supplied search-field override (the
/// `?search_field=` REST parameter or `fields` JSON DSL key). When `None`,
/// bare values defer resolution to the search root via `UserInputQuery`,
/// which expands them against each index's `default_search_fields`.
///
/// Wraps the parse + lower pass in the `quickwit_kql_*` Prometheus
/// counters/histogram so SRE can distinguish KQL traffic from the
/// Tantivy-grammar path.
pub fn kql_to_query_ast(
    input: &str,
    default_fields: Option<&[String]>,
    lenient: bool,
) -> anyhow::Result<QueryAst> {
    KQL_PARSE_TOTAL.inc();
    let started_at = Instant::now();
    let result = kql_to_query_ast_inner(input, default_fields, lenient);
    KQL_PARSE_DURATION_SECONDS.observe(started_at.elapsed().as_secs_f64());
    if result.is_err() {
        KQL_PARSE_FAILURES_TOTAL.inc();
    }
    result
}

fn kql_to_query_ast_inner(
    input: &str,
    default_fields: Option<&[String]>,
    lenient: bool,
) -> anyhow::Result<QueryAst> {
    let kql_ast = parse_kql(input)?;
    lower_kql_ast(kql_ast, default_fields, lenient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_ast::{BoolQuery, FullTextQuery, UserInputQuery};

    #[test]
    fn test_kql_to_query_ast_field_value() {
        let ast = kql_to_query_ast("level:error", None, false).unwrap();
        let QueryAst::FullText(FullTextQuery { field, text, .. }) = ast else {
            panic!()
        };
        assert_eq!(field, "level");
        assert_eq!(text, "error");
    }

    #[test]
    fn test_kql_to_query_ast_bare_value_defers_via_user_input() {
        let ast = kql_to_query_ast("error", None, false).unwrap();
        let QueryAst::UserInput(uiq) = ast else {
            panic!()
        };
        assert_eq!(uiq.user_text, "error");
    }

    #[test]
    fn test_kql_to_query_ast_explicit_fields_propagate() {
        let fields = vec!["body".to_string(), "summary".to_string()];
        let ast = kql_to_query_ast("error", Some(&fields), false).unwrap();
        let QueryAst::UserInput(UserInputQuery { default_fields, .. }) = ast else {
            panic!()
        };
        assert_eq!(default_fields, Some(fields));
    }

    #[test]
    fn test_kql_to_query_ast_compound_boolean() {
        let ast = kql_to_query_ast("level:error and service:api", None, false).unwrap();
        let QueryAst::Bool(BoolQuery { must, .. }) = ast else {
            panic!()
        };
        assert_eq!(must.len(), 2);
    }

    #[test]
    fn test_kql_to_query_ast_invalid_input_errors_and_increments_failure_counter() {
        // Just confirm error propagation. Counter assertions live in the
        // metrics integration tests; here we check the surface only.
        assert!(kql_to_query_ast("level:", None, false).is_err());
    }

    #[test]
    fn test_kql_to_query_ast_empty_input_errors() {
        assert!(kql_to_query_ast("", None, false).is_err());
    }

    #[test]
    fn test_kql_to_query_ast_bare_star_match_all() {
        assert_eq!(
            kql_to_query_ast("*", None, false).unwrap(),
            QueryAst::MatchAll
        );
    }
}
