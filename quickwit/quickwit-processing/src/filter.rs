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
use vrl::datadog_filter::{Filter, Matcher, Resolver, Run, build_matcher, regex as vrl_regex};
use vrl::datadog_search_syntax::{Comparison, ComparisonValue, Field as VRLField, QueryNode};

use crate::ProcessedLog;
use crate::default_field_search::matches;
use crate::error::PipelineError;
use crate::path_access::get_nested;
use crate::string_or_vec::StringOrVec;

/// Parses a query using the VRL parser.
///
/// The VRL parser implements the Datadog search syntax. It is used to match
/// logs based on their attributes and tags.
pub fn build_vrl_matcher(query: &str) -> Result<Box<dyn Matcher<ProcessedLog>>, PipelineError> {
    let query_node = QueryNode::from_str(query).map_err(|e| PipelineError::QueryParse {
        message: e.to_string(),
    })?;
    Ok(build_matcher(&query_node, &FilterResolver)?)
}

#[derive(Debug, Clone, Deserialize)]
struct FilterResolver;

/// If no field is specified, it will be expanded to "_default_" by VRL.
/// TODO: This should be an export from the vrl crate.
const VRL_DEFAULT_FIELD: &str = "_default_";

/// Uses the default `Resolver`, to build a `Vec<VRLField>`.
///
/// Resolves the field name to the corresponding `VRLField` enum variant.
///
/// VRLField is from vrl crate, which is a custom enum that represents the different types of
/// fields. We don't need VRLField::Default, as we can manually expand it instead.
impl Resolver for FilterResolver {
    fn build_fields(&self, attr: &str) -> Vec<VRLField> {
        // If no field is specified, it will be expanded to "_default_" by VRL.
        // TODO: Check if this is the correct behavior. Normally this would do a tokenized field
        // search e.g. on message. We don't do that currently.
        if attr == VRL_DEFAULT_FIELD {
            return vec![
                VRLField::Reserved("message".to_string()),
                VRLField::Attribute("error.message".to_string()),
                VRLField::Attribute("error.stack".to_string()),
                VRLField::Attribute("title".to_string()),
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
        // If a field is not a VRLField::Reserved, it's a VRLField::Tag
        let field = match attr {
            v if RESERVED_ATTRIBUTES.contains(&v) => VRLField::Reserved(v.to_string()),
            v if v.starts_with('@') => VRLField::Attribute(v[1..].to_string()),
            v => VRLField::Tag(v.to_string()),
        };

        vec![field]
    }
}

/// TODO: All reserved fields are of type String except for `timestamp` and `tags`, which are
/// unhandled below currently
/// Implementation of `Filter` for `FilterResolver`.
///
/// Note: Our resolver will never return a `VRLField::Default` variant, so we can ignore it.
impl Filter<ProcessedLog> for FilterResolver {
    /// Check if a field exists in the log.
    fn exists(
        &self,
        field: VRLField,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        match field {
            VRLField::Default(_) => {
                unreachable!();
            }
            // Core attributes like "message", "status", "host", etc.
            VRLField::Reserved(attr) => {
                // All attr exists except trace_id is optional
                match attr.as_str() {
                    "trace_id" => Ok(Run::boxed(|log: &ProcessedLog| log.trace_id.is_some())),
                    _ => Ok(Box::new(true)),
                }
            }

            VRLField::Attribute(custom_path) => Ok(Run::boxed(move |log: &ProcessedLog| {
                get_nested(&log.custom, &custom_path).is_some()
            })),

            // For tags (like `env:` or `region:`), we look up in `log.tag`.
            VRLField::Tag(tag_str) => Ok(Run::boxed(move |log: &ProcessedLog| {
                log.tag.contains_key(&tag_str)
            })),
        }
    }

    /// For exact matches like `foo`.
    fn equals(
        &self,
        field: VRLField,
        to_match: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let to_match = to_match.to_string();
        Ok(match_on_string(
            field,
            move |field_content: &str, is_default_field: bool| {
                if is_default_field {
                    // TODO: matches should be already case insensitive
                    matches(
                        to_match.to_lowercase().as_str(),
                        field_content.to_lowercase().as_str(),
                    )
                } else {
                    field_content == to_match
                }
            },
        ))
    }

    fn prefix(
        &self,
        field: VRLField,
        prefix: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let prefix = prefix.to_string();
        Ok(match_on_string(
            field,
            move |field_content: &str, is_default_field: bool| {
                if is_default_field {
                    // VRL removes the wildcard from the prefix, so we need to add it back for the
                    // matches function.
                    let prefix = prefix.to_lowercase() + "*";
                    matches(prefix.as_str(), field_content.to_lowercase().as_str())
                } else {
                    field_content.starts_with(&prefix)
                }
            },
        ))
    }

    /// For wildcard queries like `foo*bar`.
    fn wildcard(
        &self,
        field: VRLField,
        pattern_with_wildcard: &str,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        let pattern_with_wildcard = pattern_with_wildcard.to_string();
        Ok(match_on_string(
            field,
            move |field_content: &str, is_default_field: bool| {
                if is_default_field {
                    matches(
                        pattern_with_wildcard.to_lowercase().as_str(),
                        field_content.to_lowercase().as_str(),
                    )
                } else {
                    let re = vrl_regex::wildcard_regex(&pattern_with_wildcard);
                    re.is_match(field_content)
                }
            },
        ))
    }

    /// For range queries like `> 5`, `< 10`, etc.
    fn compare(
        &self,
        field: VRLField,
        comparator: Comparison,
        comparison_value: ComparisonValue,
    ) -> Result<Box<dyn Matcher<ProcessedLog>>, vrl::path::PathParseError> {
        // TODO: Only f64 comparisons are supported here.
        // Strings and integers are not handled.
        Ok(Run::boxed(move |log: &ProcessedLog| {
            // Convert the log's value to an f64 if possible
            let log_val = match &field {
                VRLField::Default(_) => unreachable!(),
                VRLField::Reserved(attr) => {
                    if let Some(s) = log.get_core_string_field_by_name(attr) {
                        s.parse::<f64>().ok()
                    } else {
                        None
                    }
                }

                VRLField::Attribute(custom_path) => {
                    if let Some(v) = get_nested(&log.custom, &custom_path) {
                        match v {
                            serde_json::Value::Number(num) => num.as_f64(),
                            serde_json::Value::String(s) => s.parse::<f64>().ok(),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }

                VRLField::Tag(tag_str) => {
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
/// The predicate callback is called with 2 parameters: (field content, flag if it's a default
/// field) Match on default fields (`message`, `title`, `error.message` or `error.stack`) behave
/// different, more like a fulltext search.
fn match_on_string<F>(field: VRLField, pred: F) -> Box<dyn Matcher<ProcessedLog>>
where
    F: Fn(&str, bool) -> bool + Clone + 'static + Send + Sync,
{
    match field {
        VRLField::Default(_) => unreachable!(),
        VRLField::Reserved(attr) => Run::boxed(move |log: &ProcessedLog| {
            log.get_core_string_field_by_name(&attr)
                .map(|text| pred(text, attr == "message"))
                .unwrap_or(false)
        }),
        VRLField::Attribute(custom_path) => Run::boxed(move |log: &ProcessedLog| {
            get_nested(&log.custom, &custom_path)
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
        VRLField::Tag(tag_str) => Run::boxed(move |log: &ProcessedLog| {
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

    use serde_json::json;
    use time::OffsetDateTime;

    use super::{ProcessedLog, build_vrl_matcher};
    use crate::processed_log::tests::make_datadog_log_msg;

    /// Helper to build a sample ProcessedLog
    fn make_log() -> ProcessedLog {
        let mut msg = make_datadog_log_msg();
        msg.status = Some("info".to_string());
        msg.message = "[2025-03-10T10:58:38.384+0000][31740.749s][511][info][gc,cpu   ] GC(18381) \
                       User=0.15s Sys=0.00s Real=0.02s hello"
            .to_string()
            .into();
        msg.timestamp = Some(OffsetDateTime::now_utc());
        msg.hostname = Some("myhost".to_string());
        msg.service = Some("myservice".to_string());
        msg.ddsource = Some("mysource".to_string());

        msg.ddtags.push("env:dev".to_string());
        msg.ddtags.push("region:us-east".to_string());
        msg.ddtags.push("region:east".to_string());

        let mut processed_log = ProcessedLog::from_datadog_log_msg(msg);
        let mut custom_map = serde_json::Map::new();
        custom_map.insert("user_id".to_string(), json!("1234"));
        custom_map.insert("float_val".to_string(), json!(3.10f64));
        custom_map.insert("nested".to_string(), json!({"level": "over9000"}));
        processed_log.custom = custom_map.clone();

        processed_log
    }

    fn check_query(query: &str, log: &ProcessedLog) -> bool {
        let matcher = build_vrl_matcher(query).expect("failed to parse query");
        matcher.run(log)
    }

    /// Basic test: match the `service` core field
    #[test]
    fn test_search_on_default() {
        let log = make_log();
        assert!(check_query("hello", &log), "Should find 'hello' in message");
    }

    /// Basic test: match the `service` core field
    #[test]
    fn test_match_core_service() {
        let log = make_log();
        assert!(
            check_query("service:myservice", &log),
            "Should match service == \"myservice\""
        );
        // Negative check
        assert!(
            !check_query("service:another-service", &log),
            "Should not match different service"
        );
    }

    /// Match a tag with the Datadog syntax: "region:us-east"
    #[test]
    fn test_match_tag() {
        let log = make_log();
        // "region:us-east" should match since region is a list containing "us-east"
        assert!(check_query("region:us-east", &log));
        // "region:west" should fail, no "west" in region
        assert!(!check_query("region:west", &log));
    }

    /// Combine filters with AND logic (Datadog syntax: "service: X AND host: Y")
    #[test]
    fn test_and_logic() {
        let log = make_log();
        // Matches both
        assert!(
            check_query("service:myservice AND host:myhost", &log),
            "Should match both conditions"
        );
        // Fails second condition
        assert!(!check_query("service:myservice AND host:anotherhost", &log));
    }

    /// Show OR logic: "host:myhost OR region:us-east"
    #[test]
    fn test_or_logic() {
        let log = make_log();
        // Should match, because `host:myhost` is true (Datadog syntax typically is: host:myhost OR
        // region:west)
        assert!(check_query("host:myhost OR region:west", &log));
        // Should fail, neither side matches
        assert!(!check_query("host:unknown OR region:west", &log));
    }

    /// Matches custom JSON attributes in `log.custom`:
    #[test]
    fn test_match_custom_attr() {
        let log = make_log();
        // "user_id:1234" => user_id == "1234"
        assert!(check_query("@user_id:1234", &log));
        // "float_val:3.10" might or might not match, depending on your equals logic
        assert!(
            !check_query("@float_val:3.10", &log),
            "We store 3.10 as a JSON number, might differ from '3.10' text"
        );
    }

    /// Example of nested JSON comparison: "nested.level:over9000"
    #[test]
    fn test_match_nested() {
        let log = make_log();
        // "nested.level:over9000" should match
        assert!(check_query("@nested.level:over9000", &log));
        // "nested.level:over9001" should fail
        assert!(!check_query("@nested.level:over9001", &log));
    }

    /// Example of numeric comparison if your Filter supports it: "float_val > 3"
    #[test]
    fn test_compare_float_val() {
        let log = make_log();
        assert!(check_query("@float_val:>3", &log));
        assert!(!check_query("@float_val:>4", &log));
    }

    /// Example of wildcard or prefix if your Filter supports it: "service:my*" (wildcard syntax)
    #[test]
    fn test_match_wildcard() {
        let log = make_log();
        // e.g., "service:my*" means "service" starts with "my"
        assert!(check_query("service:my*", &log));
        // negative example
        assert!(!check_query("service:not*", &log));
    }

    /// Demonstrate a negation: "NOT region:east"
    #[test]
    fn test_match_negation() {
        let log = make_log();
        // "region:east" is true, so "NOT region:east" is false
        assert!(!check_query("NOT region:east", &log));
        // "NOT region:west" => since region:west is false, negation is true
        assert!(check_query("NOT region:west", &log));
    }

    #[test]
    fn test_or_on_source() {
        let log = make_log();
        assert!(check_query(
            "source:(mysource OR othersource OR datadog-agent)",
            &log
        ));
        // Alternate syntax
        assert!(check_query(
            "source:(mysource || othersource || datadog-agent)",
            &log
        ));
        assert!(!check_query("source:(othersource OR datadog-agent)", &log));
    }

    #[test]
    fn test_source_simple() {
        let log = make_log();
        assert!(check_query("source:mysource", &log));
    }

    #[test]
    fn test_tag_exists() {
        let log = make_log();
        assert!(check_query("env:*", &log));
        assert!(!check_query("envv:*", &log));
        assert!(check_query("env:* AND region:east", &log));
    }

    #[test]
    fn test_mix_tag_and_core_attr() {
        let log = make_log();
        // implicit AND
        assert!(check_query("env:dev service:myservice", &log));
        assert!(!check_query("env:dev service:otherservice", &log));
    }

    #[test]
    fn test_mix_tag_and_negated_core_attr() {
        let log = make_log();
        assert!(!check_query("env:dev AND NOT service:myservice", &log));
    }

    #[test]
    fn test_two_tags() {
        let log = make_log();
        assert!(check_query("env:dev region:us-east", &log));
        assert!(!check_query("env:dev region:west", &log));
    }

    #[test]
    fn test_default_field_bsh() {
        let mut log = make_log();
        log.message = "[2025-03-10T10:58:38.384+0000][31740.749s][511][info][gc,cpu   ] GC(18381) \
                       User=0.15s Sys=0.00s Real=0.02s hello"
            .to_string();

        assert!(check_query("\"[gc,cpu   ]\"", &log));
        // Case insensitive
        assert!(check_query("GC", &log));
        // TODO: This should match "[gc,cpu   ]" although the spaces are not the same
        assert!(check_query("\"[gc,cpu ]\"", &log));
    }

    #[test]
    fn test_match_everything() {
        let log = make_log();
        assert!(check_query("*", &log));
    }

    #[ignore]
    #[test]
    // TODO: Enable after https://github.com/vectordotdev/vrl/pull/1334 is merged, which passes the
    // information on if it's a phrase search or not.
    // Use cases based on experimental tests at https://ddstaging.datadoghq.com/logs/pipelines/pipeline/add
    fn test_default_field_phrase_matching() {
        let mut log = make_log();
        log.message = "Setting Handles in set_event_mentions for event_id:8008795072438008673, \
                       fetching."
            .to_string();

        assert!(check_query(
            "\"Setting Handles in set_event_mentions\"",
            &log
        ));
        // casing is ignored
        assert!(check_query(
            "\"Setting handles in set_event_mentions\"",
            &log
        ));
        // special chars are ignored
        assert!(check_query(
            "\"Setting [] handles .. in set_event_mentions\"",
            &log
        ));
        // Wildcard in quotes does not work
        assert!(!check_query(
            "\"Setting handle* in set_event_mentions\"",
            &log
        ));
    }

    #[test]
    // Use cases based on experimental tests at https://ddstaging.datadoghq.com/logs/pipelines/pipeline/add
    fn test_default_field_matching_tokens() {
        let mut log = make_log();
        // Use cases based on experimental tests
        log.message = "Setting Handles in set_event_mentions for event_id:8008795072438008673, \
                       fetching."
            .to_string();

        assert!(!check_query("Setting Handle", &log));
        // Wildcards in tokens are allowed
        assert!(check_query("Setting Hand*", &log));

        // some tokens are filtered and ignored, but some are not
        // Some have more complex behavior like `.`
        // _ is not filtered => No hit
        assert!(!check_query("Setting Handles ___", &log));
        // These are filtered => Hit
        let filtered_chars = vec!["@", "^", "%", ":"];
        for c in filtered_chars {
            assert!(
                check_query(&format!("\"Setting Handles {c}\""), &log),
                "{}",
                c
            );
        }
        // Wildcards in tokens are allowed
        assert!(check_query("Setting Hand*", &log));

        // Weird matching behavior
        // TODO: VRL tokenizes this into:
        // AttributeTerm { attr: "_default_", value: "Setting" }
        // AttributeWildcard { attr: "_default_", wildcard: "Hand*..." }
        assert!(check_query("Setting Hand*...", &log));

        // Since ":' is a ignored token, it can be replaced with '.' and still matches
        // This is a little weird, because the opposite is not the case (I think)
        assert!(check_query("\"event_id:8008795072438008673\"", &log));
        // TODO: This is not handled correctly yet
        //assert!(check_query("\"event_id.8008795072438008673\"", &log));
    }

    #[test]
    // Use cases based on experimental tests at https://ddstaging.datadoghq.com/logs/pipelines/pipeline/add
    fn test_default_field_matching_ip_addr() {
        let mut log = make_log();
        // Use cases based on experimental tests
        // '.' is a special case, it can be replaced with any char if encapsulated in alphanumeric
        // chars (so it seems)
        log.message = "127.0.0.1".to_string();

        assert!(check_query("127.0.0.1", &log));
        assert!(!check_query("127.0.0..1", &log));
        // ':' is a token separator for this query
        assert!(!check_query("\"127:0:0:1\"", &log));
    }

    #[ignore]
    #[test]
    // Use cases based on experimental tests at https://ddstaging.datadoghq.com/logs/pipelines/pipeline/add
    // TODO: Understand the weird behavior and decide if we want to follow it
    fn test_default_field_weird() {
        let mut log = make_log();
        log.message = "[CommitProcessor:105:o.a.z.s.q.LearnerSessionTracker@116]".to_string();
        assert!(check_query(
            "[CommitProcessor:105:o.a.z.s.q.LearnerSessionTracker@116]",
            &log
        ));

        // Hits, but it shouldn't since LearnerSessionTrac is not the full token
        // Buggy behavior?
        assert!(check_query(
            "[CommitProcessor:105:o.a.z.s.q.LearnerSessionTrac",
            &log
        ));
        assert!(check_query("CommitProcessor:105:o.a.z.s.q.L", &log));
        // No hit
        assert!(!check_query("CommitProcessor:105:o.a.z.s.q.", &log));
    }
}
