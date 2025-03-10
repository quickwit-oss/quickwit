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

use std::str::FromStr;

use serde::Deserialize;
use vrl::datadog_filter::{build_matcher, regex, Filter, Matcher, Resolver, Run};
use vrl::datadog_search_syntax::{Comparison, ComparisonValue, Field, QueryNode};

use crate::error::PipelineError;
use crate::path_access::get_nested;
use crate::{ProcessedLog, StringOrVec};

/// Parses a query using the VRL parser.
///
/// The VRL parser implements the Datadog search syntax. It is used to match
/// logs based on their attributes and tags.
pub fn build_vrl_matcher(query: &str) -> Result<Box<dyn Matcher<ProcessedLog>>, PipelineError> {
    let node = QueryNode::from_str(query).map_err(|e| PipelineError::QueryParse {
        message: e.to_string(),
    })?;

    Ok(build_matcher(&node, &FilterResolver)?)
}

#[derive(Debug, Clone, Deserialize)]
struct FilterResolver;

const DEFAULT_FIELD: &str = "_default_";

/// Uses the default `Resolver`, to build a `Vec<Field>`.
///
/// Resolves the field name to the corresponding `Field` enum variant.
///
/// Field is from vrl crate, which is a custom enum that represents the different types of fields.
/// We don't need Field::Default, as we can manually expand it instead.
impl Resolver for FilterResolver {
    fn build_fields(&self, attr: &str) -> Vec<Field> {
        // If no field is specified, it will be expanded to DEFAULT_FIELD by VRL.
        // TODO: Check if this is the correct behavior. Normally this would do a tokenized field
        // search e.g. on message. We don't do that currently.
        if attr == DEFAULT_FIELD {
            return vec![
                Field::Reserved("message".to_string()),
                Field::Attribute("error.message".to_string()),
                Field::Attribute("error.stack".to_string()),
                Field::Attribute("title".to_string()),
            ];
        }

        /// Attributes that represent special fields in Datadog.
        static RESERVED_ATTRIBUTES: &[&str] = &[
            "host",
            "source",
            "status",
            "service",
            "trace_id",
            "message",
            "timestamp",
            "tags", // TODO: Check if this should be handled differently
        ];

        // Attributes start with '@' and are custom fields in `ProcessedLog::custom`.
        // If a field is not a Field::Reserved, it's a Field::Tag
        let field = match attr {
            v if RESERVED_ATTRIBUTES.contains(&v) => Field::Reserved(v.to_string()),
            v if v.starts_with('@') => Field::Attribute(v[1..].to_string()),
            v => Field::Tag(v.to_string()),
        };

        vec![field]
    }
}

/// TODO: All reserved fields are of type String except for `timestamp` and `tags`, which are
/// unhandled below currently
/// Implementation of `Filter` for `FilterResolver`.
///
/// Note: Our resolver will never return a `Field::Default` variant, so we can ignore it.
impl Filter<ProcessedLog> for FilterResolver {
    /// Check if a field exists in the log.
    fn exists(
        &self,
        field: Field,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        match field {
            Field::Default(_) => {
                unreachable!();
            }
            // Core attributes like "message", "status", "host", etc.
            Field::Reserved(attr) => {
                // All attr exists except trace_id is optional
                match attr.as_str() {
                    "trace_id" => Ok(Run::boxed(|log: &ProcessedLog| log.trace_id.is_some())),
                    _ => Ok(Box::new(true)),
                }
            }

            Field::Attribute(custom_path) => Ok(Run::boxed(move |log: &ProcessedLog| {
                get_nested(&log.custom, custom_path.split('.')).is_some()
            })),

            // For tags (like `env:` or `region:`), we look up in `log.tag`.
            Field::Tag(tag_str) => Ok(Run::boxed(move |log: &ProcessedLog| {
                log.tag.contains_key(&tag_str)
            })),
        }
    }

    /// For exact matches like `foo`.
    fn equals(
        &self,
        field: Field,
        to_match: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let to_match = to_match.to_string();
        Ok(match_on_string(field, move |s: &str, contains: bool| {
            if contains {
                // Replace with a case-insensitive comparison
                s.to_lowercase().contains(&to_match.to_lowercase())
            } else {
                s == to_match
            }
        }))
    }

    fn prefix(
        &self,
        field: Field,
        prefix: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let prefix = prefix.to_string();
        Ok(match_on_string(field, move |s: &str, contains: bool| {
            if contains {
                s.contains(&prefix)
            } else {
                s.starts_with(&prefix)
            }
        }))
    }

    /// For wildcard queries like `foo*bar`.
    fn wildcard(
        &self,
        field: Field,
        wildcard: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let re = regex::wildcard_regex(wildcard);
        Ok(match_on_string(field, move |s: &str, _contains: bool| {
            re.is_match(s)
        }))
    }

    /// For range queries like `> 5`, `< 10`, etc.
    fn compare(
        &self,
        field: Field,
        comparator: Comparison,
        comparison_value: ComparisonValue,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        // TODO: Only f64 comparisons are supported here.
        // Strings and integers are not handled.
        Ok(Run::boxed(move |log: &ProcessedLog| {
            // Convert the log's value to an f64 if possible
            let log_val = match &field {
                Field::Default(_) => unreachable!(),
                Field::Reserved(attr) => {
                    if let Some(s) = log.get_core_string_field_by_name(attr) {
                        s.parse::<f64>().ok()
                    } else {
                        None
                    }
                }

                Field::Attribute(custom_path) => {
                    if let Some(v) = get_nested(&log.custom, custom_path.split('.')) {
                        match v {
                            serde_json::Value::Number(num) => num.as_f64(),
                            serde_json::Value::String(s) => s.parse::<f64>().ok(),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }

                Field::Tag(tag_str) => {
                    // Optionally parse the first string if we have a single string, ignoring Vec.
                    if let Some(v) = log.tag.get(tag_str) {
                        match v {
                            StringOrVec::String(s) => s.parse::<f64>().ok(),
                            StringOrVec::Vec(_) => None,
                        }
                    } else {
                        None
                    }
                }
            };

            // Convert the `comparison_value` to f64 as well
            let rhs = match &comparison_value {
                ComparisonValue::Float(num) => Some(*num),
                ComparisonValue::String(s) => s.parse::<f64>().ok(),
                ComparisonValue::Integer(s) => Some(*s as f64),
                ComparisonValue::Unbounded => None,
            };

            match (log_val, rhs) {
                (Some(left), Some(right)) => match comparator {
                    Comparison::Gt => left > right,
                    Comparison::Gte => left >= right,
                    Comparison::Lt => left < right,
                    Comparison::Lte => left <= right,
                },
                _ => false,
            }
        }))
    }
}

/// A helper function that creates a matcher using a predicate on &str type fields.
///
/// Search on default fields like message behaves like contains, so this method returns a bool flag
/// if the search should be done on the default field.
fn match_on_string<F>(field: Field, pred: F) -> Box<dyn Matcher<ProcessedLog>>
where F: Fn(&str, bool) -> bool + Clone + 'static + Send + Sync {
    match field {
        Field::Default(_) => unreachable!(),
        Field::Reserved(attr) => Run::boxed(move |log: &ProcessedLog| {
            log.get_core_string_field_by_name(&attr)
                .map(|text| pred(text, attr == "message"))
                .unwrap_or(false)
        }),
        Field::Attribute(custom_path) => Run::boxed(move |log: &ProcessedLog| {
            get_nested(&log.custom, custom_path.split('.'))
                .and_then(|value| {
                    if let serde_json::Value::String(json_string) = value {
                        Some(json_string)
                    } else {
                        None
                    }
                })
                .map(|text| {
                    pred(
                        text,
                        custom_path == "error.message"
                            || custom_path == "error.stack"
                            || custom_path == "title",
                    )
                })
                .unwrap_or(false)
        }),
        Field::Tag(tag_str) => Run::boxed(move |log: &ProcessedLog| {
            log.tag
                .get(&tag_str)
                .map(|val| match val {
                    StringOrVec::String(tags) => pred(tags, false),
                    StringOrVec::Vec(tags) => tags.iter().any(|tag| pred(tag, false)),
                })
                .unwrap_or(false)
        }),
    }
}

#[cfg(test)]
mod vrl_matcher_tests {
    use std::collections::HashMap;

    use serde_json::json;
    use time::OffsetDateTime;

    use super::{build_vrl_matcher, ProcessedLog};
    use crate::StringOrVec; // if that’s your local enum for tags

    /// Helper to build a sample ProcessedLog
    fn make_log() -> ProcessedLog {
        let mut custom_map = serde_json::Map::new();
        custom_map.insert("user_id".to_string(), json!("1234"));
        custom_map.insert("float_val".to_string(), json!(3.10f64));
        custom_map.insert("nested".to_string(), json!({"level": "over9000"}));

        let mut tag_map = HashMap::new();
        // "env" => "dev"
        tag_map.insert("env".to_string(), StringOrVec::String("dev".to_string()));
        // "region" => ["us-east", "east"]
        tag_map.insert(
            "region".to_string(),
            StringOrVec::Vec(vec!["us-east".to_string(), "east".to_string()]),
        );

        ProcessedLog {
            message: "[2025-03-10T10:58:38.384+0000][31740.749s][511][info][gc,cpu   ] GC(18381) \
                      User=0.15s Sys=0.00s Real=0.02s hello"
                .to_string(),
            status: "info".to_string(),
            timestamp: OffsetDateTime::now_utc(),
            host: "myhost".to_string(),
            service: "myservice".to_string(),
            source: "mysource".to_string(),
            tags: vec!["env:dev".to_string(), "region:us-east".to_string()],
            tag: tag_map,
            trace_id: None,
            span_id: None,
            custom: custom_map,
            id: "abcd1234".to_string(),
            discovery_timestamp: 0,
            ingest_size_in_bytes: 42,
        }
    }

    /// Basic test: match the `service` core field
    #[test]
    fn test_search_on_default() {
        let log = make_log();
        let matcher = build_vrl_matcher("hello").expect("failed to parse query");
        assert!(matcher.run(&log), "Should find 'hello' in message");
    }

    /// Basic test: match the `service` core field
    #[test]
    fn test_match_core_service() {
        let log = make_log();
        let matcher = build_vrl_matcher("service:myservice").expect("failed to parse query");
        assert!(matcher.run(&log), "Should match service == \"myservice\"");

        // Negative check
        let matcher = build_vrl_matcher("service:another-service").expect("failed to parse query");
        assert!(!matcher.run(&log), "Should not match different service");
    }

    /// Match a tag with the Datadog syntax: "region:us-east"
    #[test]
    fn test_match_tag() {
        let log = make_log();

        // "region:us-east" should match since region is a list containing "us-east"
        let matcher = build_vrl_matcher("region:us-east").expect("failed to parse query");
        assert!(matcher.run(&log));

        // "region:west" should fail, no "west" in region
        let matcher = build_vrl_matcher("region:west").expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Combine filters with AND logic (Datadog syntax: "service: X AND host: Y")
    #[test]
    fn test_and_logic() {
        let log = make_log();

        // Matches both
        let matcher =
            build_vrl_matcher("service:myservice AND host:myhost").expect("failed to parse query");
        assert!(matcher.run(&log), "Should match both conditions");

        // Fails second condition
        let matcher = build_vrl_matcher("service:myservice AND host:anotherhost")
            .expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Show OR logic: "host:myhost OR region:us-east"
    #[test]
    fn test_or_logic() {
        let log = make_log();

        // Should match, because `host:myhost` is true
        // (Datadog syntax typically is: host:myhost OR region:west)
        let matcher =
            build_vrl_matcher("host:myhost OR region:west").expect("failed to parse query");
        assert!(matcher.run(&log));

        // Should fail, neither side matches
        let matcher =
            build_vrl_matcher("host:unknown OR region:west").expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Matches custom JSON attributes in `log.custom`:
    #[test]
    fn test_match_custom_attr() {
        let log = make_log();

        // "user_id:1234" => user_id == "1234"
        let matcher = build_vrl_matcher("@user_id:1234").expect("failed to parse query");
        assert!(matcher.run(&log));

        // "float_val:3.10" might or might not match, depending on your equals logic
        let matcher = build_vrl_matcher("@float_val:3.10").expect("failed to parse query");
        assert!(
            !matcher.run(&log),
            "We store 3.10 as a JSON number, might differ from '3.10' text"
        );
    }

    /// Example of nested JSON comparison: "nested.level:over9000"
    #[test]
    fn test_match_nested() {
        let log = make_log();

        // "nested.level:over9000" should match
        let matcher = build_vrl_matcher("@nested.level:over9000").expect("failed to parse query");
        assert!(matcher.run(&log));

        // "nested.level:over9001" should fail
        let matcher = build_vrl_matcher("@nested.level:over9001").expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Example of numeric comparison if your Filter supports it: "float_val > 3"
    #[test]
    fn test_compare_float_val() {
        let log = make_log();

        let matcher = build_vrl_matcher("@float_val:>3").expect("failed to parse query");
        assert!(matcher.run(&log));

        let matcher = build_vrl_matcher("@float_val:>4").expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Example of wildcard or prefix if your Filter supports it: "service:my*" (wildcard syntax)
    #[test]
    fn test_match_wildcard() {
        let log = make_log();

        // e.g., "service:my*" means "service" starts with "my"
        let matcher = build_vrl_matcher("service:my*").expect("failed to parse query");
        assert!(matcher.run(&log));

        // negative example
        let matcher = build_vrl_matcher("service:not*").expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    /// Demonstrate a negation: "NOT region:east"
    #[test]
    fn test_match_negation() {
        // region:east
        let log = make_log();

        // "region:east" is true, so "NOT region:east" is false
        let matcher = build_vrl_matcher("NOT region:east").expect("failed to parse query");
        assert!(!matcher.run(&log));

        // "NOT region:west" => since region:west is false, negation is true
        let matcher = build_vrl_matcher("NOT region:west").expect("failed to parse query");
        assert!(matcher.run(&log));
    }

    #[test]
    fn test_or_on_source() {
        let log = make_log();
        let query = "source:(mysource OR othersource OR datadog-agent)";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
        // Alternate syntax
        let query = "source:(mysource || othersource || datadog-agent)";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
        let query = "source:(othersource OR datadog-agent)";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    #[test]
    fn test_tag_exists() {
        let log = make_log();
        let query = "env:*";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
        let query = "envv:*";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(!matcher.run(&log));

        let query = "env:* AND region:east";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
    }

    #[test]
    fn test_mix_tag_and_core_attr() {
        let log = make_log();
        // implicit AND
        let query = "env:dev service:myservice";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
        let query = "env:dev service:otherservice";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    #[test]
    fn test_mix_tag_and_negated_core_attr() {
        let log = make_log();
        let query = "env:dev AND NOT service:myservice";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    #[test]
    fn test_two_tags() {
        let log = make_log();
        let query = "env:dev region:us-east";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
        let query = "env:dev region:west";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(!matcher.run(&log));
    }

    #[test]
    fn test_default_field() {
        let log = make_log();
        let query = "\"[gc,cpu   ]\"";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));

        // Case insensitive
        let query = "GC";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));

        // TODO: This should match "[gc,cpu   ]" although the spaces are not the same
        //let query = "\"[gc,cpu ]\"";
        //let matcher = build_vrl_matcher(query).expect("failed to parse query");
        //assert!(matcher.run(&log));
    }

    #[test]
    fn test_match_everything() {
        let log = make_log();
        let query = "*";
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        assert!(matcher.run(&log));
    }
}
