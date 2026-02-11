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

use quickwit_proto::metastore::IndexRoutingRule;

use super::filter_expr::{FilterExpr, Key, StringPattern};

// TODO:
// some ideas of optimization
// - re-order the and / or so the least costly pattern are evaluated first
// - use binary tree for or / and
// - use regexp for complexe pattern matching
// - given an ordered list of filters, build a decision tree or a state machine automata

/// LogRouter routes logs to indexes based on filter rules.
///
/// Rules are evaluated in order; the first matching rule determines the target index.
#[derive(Debug, Clone)]
pub struct LogRouter {
    rules: Vec<Rule>,
}

#[derive(Debug, Clone)]
struct Rule {
    filter_exp: FilterExpr,
    index_id: String,
}

impl LogRouter {
    /// Creates a new LogRouter from a list of routing rules.
    pub fn create_from_rules(rules: Vec<IndexRoutingRule>) -> anyhow::Result<Self> {
        let rules = rules
            .into_iter()
            .map(|r| {
                Ok(Rule {
                    filter_exp: r.filter.parse()?,
                    index_id: r.index_id,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self { rules })
    }

    /// Resolves which index a log should be routed to based on the first matching rule.
    ///
    /// `get_tag` is a closure that retrieves tag values from the document.
    /// `get_custom_field` is a closure that retrieves custom field values using a path.
    /// Returns the index_id of the first matching rule, or `None` if no rule matches.
    pub fn resolve_index<'a, 'b, FT, FA>(
        &'b self,
        get_tag: &FT,
        get_custom_field: &FA,
    ) -> Option<&'b str>
    where
        FT: Fn(&str) -> Option<&'a str>,
        FA: Fn(&[String]) -> Option<&'a str>,
    {
        self.rules
            .iter()
            .find(|rule| matches_expr(&rule.filter_exp, get_tag, get_custom_field))
            .map(|rule| rule.index_id.as_str())
    }
}

fn matches_expr<'a, FT, FA>(expr: &FilterExpr, get_tag: &FT, get_custom_field: &FA) -> bool
where
    FT: Fn(&str) -> Option<&'a str>,
    FA: Fn(&[String]) -> Option<&'a str>,
{
    match expr {
        FilterExpr::All => true,
        FilterExpr::Never => false,
        FilterExpr::Match { key, pattern } => {
            let value = match key {
                Key::Tag(tag) => get_tag(tag),
                Key::CustomField(path) => get_custom_field(path),
            };
            match value {
                Some(v) => matches_pattern(pattern, v),
                None => false,
            }
        }
        FilterExpr::And(exprs) => exprs
            .iter()
            .all(|e| matches_expr(e, get_tag, get_custom_field)),
        FilterExpr::Or(exprs) => exprs
            .iter()
            .any(|e| matches_expr(e, get_tag, get_custom_field)),
        FilterExpr::Not(inner) => !matches_expr(inner, get_tag, get_custom_field),
    }
}

fn matches_pattern(pattern: &StringPattern, value: &str) -> bool {
    match pattern {
        StringPattern::Any => true,
        StringPattern::Exact(s) => value == s,
        StringPattern::Prefix(prefix) => value.starts_with(prefix),
        StringPattern::Suffix(suffix) => value.ends_with(suffix),
        StringPattern::Contains(s) => value.contains(s),
        StringPattern::PrefixAndSuffix { prefix, suffix } => {
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len()
        }
        StringPattern::Or(patterns) => patterns.iter().any(|p| matches_pattern(p, value)),
        StringPattern::And(patterns) => patterns.iter().all(|p| matches_pattern(p, value)),
        StringPattern::Not(inner) => !matches_pattern(inner, value),
    }
}
