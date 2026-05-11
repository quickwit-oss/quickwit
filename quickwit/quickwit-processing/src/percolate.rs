use std::borrow::Cow;

pub use event_percolation::Percolator;
use event_percolation::{Document, PercolatorConfig, ResolvedColumn, Value as PercValue};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;

use crate::ProcessedLog;
use crate::path_access::get_nested_segments;
use crate::string_or_vec::StringOrVec;

const DEFAULT_COLUMNS: &[&str] = &[
    "message",
    "custom.error.message",
    "custom.error.stack",
    "custom.title",
];
const TOP_LEVEL_COLUMNS: &[&str] = &[
    "message",
    "source",
    "status",
    "service",
    "host",
    "trace_id",
    "span_id",
    "timestamp",
];

pub fn default_percolator_config() -> PercolatorConfig {
    PercolatorConfig::new()
        .with_default_columns(DEFAULT_COLUMNS.iter().copied())
        .with_tokenized_columns(DEFAULT_COLUMNS.iter().copied())
        .with_top_level_columns(TOP_LEVEL_COLUMNS.iter().copied())
}

fn to_percolation_value(value: Option<&Value>) -> PercValue<'_> {
    match value {
        Some(Value::String(s)) => PercValue::Utf8(Cow::Borrowed(s), None),
        Some(Value::Number(n)) => n
            .as_f64()
            .map(PercValue::Number)
            .unwrap_or(PercValue::Missing),
        Some(Value::Bool(b)) => PercValue::Bool(*b),
        Some(Value::Array(arr)) => {
            // TODO: Allocating here is not ideal, but the percolator API requires it. We should
            // consider changing the API to allow for non-allocating array values.

            if arr.is_empty() {
                return PercValue::Missing;
            }
            if arr.len() == 1 {
                return to_percolation_value(arr.first());
            }

            let values: Vec<Cow<'_, str>> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(Cow::Borrowed(s.as_str())),
                    Value::Number(n) => Some(Cow::Owned(n.to_string())),
                    Value::Bool(b) => Some(Cow::Owned(b.to_string())),
                    _ => None,
                })
                .collect();
            PercValue::Utf8Array(values.into())
        }
        _ => PercValue::Missing,
    }
}

fn string_or_vec_to_percolation_value(value: &StringOrVec) -> PercValue<'_> {
    match value {
        StringOrVec::String(s) => PercValue::Utf8(Cow::Borrowed(s.as_str()), None),
        StringOrVec::Vec(values) => PercValue::Utf8Array(
            values
                .iter()
                .map(|value| Cow::Borrowed(value.as_str()))
                .collect::<Vec<_>>()
                .into(),
        ),
    }
}

fn tag_value<'a>(log: &'a ProcessedLog, key: &str) -> PercValue<'a> {
    log.tag
        .get(key)
        .map(string_or_vec_to_percolation_value)
        .unwrap_or(PercValue::Missing)
}

fn direct_value<'a>(log: &'a ProcessedLog, column: &str) -> PercValue<'a> {
    match column {
        "message" | "status" | "host" | "service" | "source" | "trace_id" => log
            .get_core_string_field_by_name(column)
            .map(|value| PercValue::Utf8(Cow::Borrowed(value), None))
            .unwrap_or(PercValue::Missing),
        "timestamp" => PercValue::Utf8(
            Cow::Owned(
                log.timestamp
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| log.timestamp.to_string()),
            ),
            None,
        ),
        _ => PercValue::Missing,
    }
}

fn custom_value<'a, S>(log: &'a ProcessedLog, segments: &[S]) -> PercValue<'a>
where
    S: AsRef<str>,
{
    to_percolation_value(get_nested_segments(&log.custom, segments))
}

impl Document for ProcessedLog {
    fn get(&mut self, column: &ResolvedColumn) -> PercValue<'_> {
        match column {
            ResolvedColumn::Tag(tag_key) => tag_value(self, tag_key),
            ResolvedColumn::Path(segments) => {
                if segments.is_empty() {
                    return PercValue::Missing;
                }

                // Single-segment top-level column
                if let [col] = segments.as_slice()
                    && TOP_LEVEL_COLUMNS.contains(&col.as_str())
                {
                    return direct_value(self, col);
                }

                // Strip `custom.` prefix added by the percolator for @-attribute paths
                let custom_segments = if segments[0] == "custom" {
                    &segments[1..]
                } else {
                    segments.as_slice()
                };

                // Custom attribute lookup
                if !custom_segments.is_empty() {
                    let custom = custom_value(self, custom_segments);
                    if !matches!(custom, PercValue::Missing) {
                        return custom;
                    }
                }

                PercValue::Missing
            }
        }
    }
}

#[cfg(test)]
fn compiled_filter(query: &str) -> Result<Percolator, String> {
    Percolator::new(vec![query.trim()], &default_percolator_config())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_processed_log;

    #[test]
    fn test_custom_field_matching() {
        let filter = compiled_filter("@status:error AND @service:salesforce").unwrap();
        let mut log = make_processed_log();
        log.custom.insert("status".to_string(), json!("error"));
        log.custom
            .insert("service".to_string(), json!("salesforce"));

        assert!(filter.any_match(&mut log).unwrap());
    }

    #[test]
    fn test_array_matching() {
        let filter = compiled_filter("@monitor.groups:\"*\"").unwrap();
        let mut log = make_processed_log();
        log.custom
            .insert("monitor".to_string(), json!({"groups": ["*"]}));

        assert!(filter.any_match(&mut log).unwrap());
    }

    #[test]
    fn test_top_level_direct_field() {
        let filter = compiled_filter("service:web-server").unwrap();
        let mut log = make_processed_log();
        log.service = Some("web-server".to_string());

        assert!(filter.any_match(&mut log).unwrap());
    }

    #[test]
    fn test_custom_shadows_direct_field() {
        // @service:X and service:X both resolve to Path(["service"]).
        // the core attribute should take precedence over the custom field
        let filter = compiled_filter("service:from-custom").unwrap();
        let mut log = make_processed_log();
        log.service = Some("from-direct".to_string());
        log.custom
            .insert("service".to_string(), json!("from-custom"));

        assert!(!filter.any_match(&mut log).unwrap());
    }
}
