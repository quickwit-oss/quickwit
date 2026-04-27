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

use std::cmp::min;

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, KeyString, LogEvent, Metric, ObjectMap, TraceEvent, Value};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

/// Maximum tag length in bytes. Mirrors `model.MaxTagLength` in `dd-go`.
const MAX_TAG_LENGTH: usize = 200;

/// Maximum hostname length in Unicode code points. Mirrors the upstream VRL
/// `length()` semantics — `length()` on a string returns code points, so this
/// is the faithful port even though strict DNS bounds at 253 ASCII bytes.
const MAX_HOSTNAME_CHARS: usize = 253;

/// Normalizes hostnames and tag key/value pairs across all three signal types.
///
/// Hostname normalization mirrors the VRL `normalize_*_host` transforms in
/// `dd-source/domains/quickhouse/apps/byoc-pipeline/vector.yaml`: control
/// characters are stripped, `<`/`>` are replaced with `-`, and overly long or
/// NUL-bearing hostnames are wiped to an empty string.
///
/// Tag normalization ports `model.NormalizeTag` from `dd-go/model/tags.go`
/// using the default `MaxTagLength = 200`. Keys and values are normalized
/// independently — empty-key tags are dropped, empty-value tags are kept.
///
/// The order of operations is **hostname first, then tags**. The hostname
/// field is excluded from tag normalization to avoid the (already-normalized)
/// hostname being rewritten a second time under different rules.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NameNormalizerConfig;

impl vector_lib::configurable::NamedComponent for NameNormalizerConfig {
    fn get_component_name(&self) -> &'static str {
        "name_normalizer"
    }
}

impl GenerateConfig for NameNormalizerConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "name_normalizer")]
impl TransformConfig for NameNormalizerConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(NameNormalizer))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::all_bits(),
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct NameNormalizer;

impl FunctionTransform for NameNormalizer {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        match &mut event {
            Event::Log(log) => normalize_log(log),
            Event::Metric(metric) => normalize_metric(metric),
            Event::Trace(trace) => normalize_trace(trace),
        }
        output.push(event);
    }
}

// ---------------------------------------------------------------------------
// Per-signal handlers
// ---------------------------------------------------------------------------

fn normalize_log(log: &mut LogEvent) {
    if let Some(host) = log.get("hostname").and_then(|v| v.as_str())
        && let Some(new_host) = normalize_hostname(&host)
    {
        log.insert("hostname", new_host);
    }

    let Some(tags_value) = log.get_mut("tags") else {
        return;
    };
    normalize_object_map(tags_value, |_key| false);
}

fn normalize_metric(metric: &mut Metric) {
    if let Some(host) = metric.tag_value("host")
        && let Some(new_host) = normalize_hostname(&host)
    {
        metric.replace_tag("host".to_string(), new_host);
    }

    let Some(tags_mut) = metric.tags_mut() else {
        return;
    };
    let original = std::mem::take(tags_mut);
    for (key, value) in original.into_iter_single() {
        if key == "host" {
            // Hostname normalization already covered this tag; leave it alone
            // to avoid re-running tag rules over a hostname-shaped string.
            metric.replace_tag(key, value);
            continue;
        }
        let new_key = normalize_tag(&key);
        if new_key.is_empty() {
            continue;
        }
        let new_value = normalize_tag_value(&value);
        metric.replace_tag(new_key, new_value);
    }
}

fn normalize_trace(trace: &mut TraceEvent) {
    if let Some(host) = trace.get("meta.host").and_then(|v| v.as_str())
        && let Some(new_host) = normalize_hostname(&host)
    {
        trace.insert("meta.host", new_host);
    }

    let Some(meta_value) = trace.get_mut("meta") else {
        return;
    };
    normalize_object_map(meta_value, |key| {
        // Skip the hostname (already normalized above) and the Datadog-internal
        // `_dd` namespace. Vector parses dots in target paths as separators, so
        // wire-format keys like `_dd.p.tid` and `_dd.tags.container` end up as
        // a single nested object under the `_dd` top-level key.
        key == "host" || key == "_dd"
    });
}

/// In-place tag normalization for an `ObjectMap`-shaped value. The `skip`
/// predicate identifies keys that should pass through unmodified — either
/// because they are reserved/internal (e.g. `_dd.*` on traces) or because
/// they have already been normalized by the hostname pass.
fn normalize_object_map(value: &mut Value, skip: impl Fn(&str) -> bool) {
    let Value::Object(map) = value else {
        return;
    };
    let original: ObjectMap = std::mem::take(map);
    for (key, val) in original {
        if skip(key.as_str()) {
            map.insert(key, val);
            continue;
        }
        let new_key = normalize_tag(key.as_str());
        if new_key.is_empty() {
            continue;
        }
        let new_val = match val.as_str() {
            Some(s) => Value::from(normalize_tag_value(&s)),
            None => val,
        };
        map.insert(KeyString::from(new_key), new_val);
    }
}

// ---------------------------------------------------------------------------
// Hostname normalization
// ---------------------------------------------------------------------------

/// Returns `Some(normalized)` when the hostname differs from the input, or
/// `None` when no rewrite is needed (input already valid). Input rewrites to
/// `""` when it exceeds the length limit or contains NUL.
fn normalize_hostname(host: &str) -> Option<String> {
    if host.is_empty() {
        return None;
    }
    if host.chars().count() > MAX_HOSTNAME_CHARS || host.contains('\x00') {
        return Some(String::new());
    }
    let normalized: String = host
        .chars()
        .filter(|c| !matches!(c, '\n' | '\r' | '\t'))
        .map(|c| match c {
            '<' | '>' => '-',
            other => other,
        })
        .collect();
    if normalized == host {
        None
    } else {
        Some(normalized)
    }
}

// ---------------------------------------------------------------------------
// Tag normalization (port of `dd-go/model/tags.go::NormalizeTag`)
// ---------------------------------------------------------------------------

/// Normalizes a tag key. Returns the empty string when the input cannot be
/// salvaged (e.g. all garbage, all leading non-alpha).
///
/// Rules (matching dd-go):
/// 1. Convert to lowercase Unicode.
/// 2. Convert bad characters to `_`.
/// 3. Dedupe contiguous underscores.
/// 4. Strip leading non-alpha characters except `:`.
/// 5. Truncate to 200 bytes.
/// 6. Strip a single trailing underscore.
fn normalize_tag(input: &str) -> String {
    normalize_tag_with_max(input, MAX_TAG_LENGTH)
}

/// Normalizes a tag value. Loosens the leading-character rule of
/// [`normalize_tag`] to allow values starting with digits / `.` / `/` / `-`,
/// matching `dd-go/model/tags.go::NormalizeTagValue`.
fn normalize_tag_value(input: &str) -> String {
    normalize_tag_value_with_max(input, MAX_TAG_LENGTH)
}

fn normalize_tag_with_max(input: &str, max_tag_length: usize) -> String {
    let buf_capacity = min(input.len(), max_tag_length).saturating_add(3);
    let mut buf: Vec<u8> = Vec::with_capacity(buf_capacity);
    let mut last_was_underscore = false;

    for (byte_idx, ch) in input.char_indices() {
        if buf.len() >= max_tag_length {
            break;
        }
        // Bound processing of pathological inputs ("test🍣🍣[...]🍣") — once
        // the byte cursor is past `2 * max_tag_length`, discard the rest.
        if byte_idx > 2 * max_tag_length {
            break;
        }

        match ch {
            'a'..='z' => {
                buf.push(ch as u8);
                last_was_underscore = false;
            }
            'A'..='Z' => {
                buf.push((ch as u8) + (b'a' - b'A'));
                last_was_underscore = false;
            }
            ':' => {
                buf.push(b':');
                last_was_underscore = false;
            }
            c if c.is_alphabetic() => {
                for lc in c.to_lowercase() {
                    let mut s = [0u8; 4];
                    let encoded = lc.encode_utf8(&mut s);
                    buf.extend_from_slice(encoded.as_bytes());
                }
                last_was_underscore = false;
            }
            // Strip leading non-alpha (except `:`, handled above).
            _ if buf.is_empty() => {}
            '.' | '/' | '-' => {
                buf.push(ch as u8);
                last_was_underscore = false;
            }
            c if c.is_numeric() => {
                let mut s = [0u8; 4];
                let encoded = c.encode_utf8(&mut s);
                buf.extend_from_slice(encoded.as_bytes());
                last_was_underscore = false;
            }
            _ if !last_was_underscore => {
                buf.push(b'_');
                last_was_underscore = true;
            }
            _ => {
                // Swallow contiguous underscore-equivalents.
            }
        }
    }

    if last_was_underscore {
        buf.pop();
    }
    // Buffer is valid UTF-8 by construction (ASCII pushes plus encode_utf8).
    String::from_utf8(buf).unwrap_or_default()
}

fn normalize_tag_value_with_max(input: &str, max_tag_length: usize) -> String {
    if input.is_empty() {
        return String::new();
    }
    let first_byte = input.as_bytes()[0];
    let valid_ascii_start = matches!(first_byte, b'a'..=b'z' | b':');
    if valid_ascii_start {
        return normalize_tag_with_max(input, max_tag_length);
    }
    // Multi-byte first char or uppercase ASCII: defer to the unicode-letter check.
    if let Some(first_char) = input.chars().next()
        && first_char.is_alphabetic()
    {
        return normalize_tag_with_max(input, max_tag_length);
    }
    // First char would be rejected by `normalize_tag` (leading digit, `.`, `-`,
    // `/`, garbage, ...). Prepend `a:`, normalize, then slice the prefix off.
    // dd-go uses `2 + maxTagLength` so the trimmed value can still occupy the
    // full budget.
    let mut prefixed = String::with_capacity(input.len() + 2);
    prefixed.push_str("a:");
    prefixed.push_str(input);
    let normalized = normalize_tag_with_max(&prefixed, 2 + max_tag_length);
    if let Some(stripped) = normalized.strip_prefix("a:") {
        stripped.to_string()
    } else {
        // The prepended prefix was eaten — input collapsed to nothing.
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use vector::event::{
        Event, LogEvent, Metric, MetricKind, MetricTags, MetricValue, TraceEvent, Value,
    };

    use super::*;

    fn run(event: Event) -> Vec<Event> {
        let mut transform = NameNormalizer;
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    // -----------------------------------------------------------------------
    // normalize_tag — unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_empty_input_is_empty() {
        assert_eq!(normalize_tag(""), "");
    }

    #[test]
    fn tag_already_normalized_is_unchanged() {
        assert_eq!(normalize_tag("env:prod"), "env:prod");
        assert_eq!(normalize_tag("region:us-east-1"), "region:us-east-1");
    }

    #[test]
    fn tag_uppercase_is_lowercased() {
        assert_eq!(normalize_tag("ENV:PROD"), "env:prod");
    }

    #[test]
    fn tag_spaces_and_punctuation_become_underscore() {
        assert_eq!(normalize_tag("hello world"), "hello_world");
        assert_eq!(normalize_tag("foo!bar"), "foo_bar");
    }

    #[test]
    fn tag_contiguous_garbage_collapses() {
        assert_eq!(normalize_tag("foo!!@@bar"), "foo_bar");
        assert_eq!(normalize_tag("foo   bar"), "foo_bar");
    }

    #[test]
    fn tag_leading_digit_is_dropped() {
        // `normalize_tag` strips all leading non-alpha (except `:`).
        assert_eq!(normalize_tag("123foo"), "foo");
    }

    #[test]
    fn tag_value_leading_digit_is_retained() {
        // `normalize_tag_value` preserves leading digits via the `a:` trick.
        assert_eq!(normalize_tag_value("123"), "123");
        assert_eq!(normalize_tag_value("1.2.3"), "1.2.3");
        assert_eq!(normalize_tag_value("-foo"), "-foo");
    }

    #[test]
    fn tag_truncated_to_200_bytes() {
        let input: String = "a".repeat(250);
        let out = normalize_tag(&input);
        assert_eq!(out.len(), 200);
    }

    #[test]
    fn tag_trailing_underscore_stripped() {
        assert_eq!(normalize_tag("foo!"), "foo");
        assert_eq!(normalize_tag("foo___"), "foo");
    }

    #[test]
    fn tag_garbage_past_2x_max_discarded() {
        // 401 bytes of `!` followed by one `a`. The `!` chars all coalesce to
        // a single leading underscore (which is then stripped because buf is
        // empty), and the `a` past byte 400 is discarded by the 2*max guard.
        let mut input = "!".repeat(401);
        input.push('a');
        assert_eq!(normalize_tag(&input), "");
    }

    #[test]
    fn tag_unicode_letter_lowercased_and_preserved() {
        assert_eq!(normalize_tag("Ω"), "ω");
        assert_eq!(normalize_tag("Ωmega"), "ωmega");
    }

    #[test]
    fn tag_only_garbage_is_empty() {
        assert_eq!(normalize_tag("!!!"), "");
        assert_eq!(normalize_tag("123"), "");
    }

    // -----------------------------------------------------------------------
    // normalize_hostname — unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn host_empty_is_noop() {
        assert_eq!(normalize_hostname(""), None);
    }

    #[test]
    fn host_already_clean_is_noop() {
        assert_eq!(normalize_hostname("web-01.example.com"), None);
    }

    #[test]
    fn host_length_253_preserved() {
        let host: String = "a".repeat(253);
        assert_eq!(normalize_hostname(&host), None);
    }

    #[test]
    fn host_length_254_wiped() {
        let host: String = "a".repeat(254);
        assert_eq!(normalize_hostname(&host), Some(String::new()));
    }

    #[test]
    fn host_with_nul_wiped() {
        assert_eq!(normalize_hostname("evil\x00host"), Some(String::new()),);
    }

    #[test]
    fn host_strips_control_chars() {
        assert_eq!(
            normalize_hostname("hello\nworld"),
            Some("helloworld".to_string()),
        );
        assert_eq!(normalize_hostname("a\rb\tc"), Some("abc".to_string()),);
    }

    #[test]
    fn host_replaces_angle_brackets() {
        assert_eq!(
            normalize_hostname("<weird>host"),
            Some("-weird-host".to_string()),
        );
    }

    #[test]
    fn host_combined_case() {
        assert_eq!(
            normalize_hostname("<host\nname>"),
            Some("-hostname-".to_string()),
        );
    }

    // -----------------------------------------------------------------------
    // Per-signal end-to-end tests
    // -----------------------------------------------------------------------

    #[test]
    fn log_hostname_and_tags_rewritten() {
        let mut log = LogEvent::default();
        log.insert("hostname", "<weird>host");
        log.insert("tags.Service", "My Service");
        log.insert("tags.ENV", "prod");
        // Tag with key that normalizes to empty must be dropped.
        log.insert("tags.!!!", "ignored");

        let events = run(Event::Log(log));
        let log = events[0].as_log();
        assert_eq!(log.get("hostname"), Some(&Value::from("-weird-host")));
        // Keys lowercased; values lowercased + spaces → `_`.
        assert_eq!(log.get("tags.service"), Some(&Value::from("my_service")));
        assert_eq!(log.get("tags.env"), Some(&Value::from("prod")));
        // Original mixed-case keys are gone.
        assert!(log.get("tags.Service").is_none());
        assert!(log.get("tags.ENV").is_none());
        // Empty-key tag dropped entirely.
        assert!(log.get("tags.!!!").is_none());
    }

    #[test]
    fn metric_host_rewritten_and_other_tags_normalized() {
        let mut metric_tags = MetricTags::default();
        metric_tags.insert("host".to_string(), "<weird>host".to_string());
        metric_tags.insert("Service".to_string(), "My Service".to_string());
        metric_tags.insert("ENV".to_string(), "Prod".to_string());
        metric_tags.insert("!!!".to_string(), "dropped".to_string());

        let metric = Metric::new(
            "cpu.usage",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        )
        .with_tags(Some(metric_tags));

        let events = run(Event::Metric(metric));
        let m = events[0].as_metric();

        // Host tag rewritten by hostname rules — `<` and `>` → `-`, NOT
        // collapsed to `_` by tag rules.
        assert_eq!(m.tag_value("host").as_deref(), Some("-weird-host"));
        // Other tags rewritten by tag rules.
        assert_eq!(m.tag_value("service").as_deref(), Some("my_service"));
        assert_eq!(m.tag_value("env").as_deref(), Some("prod"));
        // Old keys gone.
        assert!(m.tag_value("Service").is_none());
        assert!(m.tag_value("ENV").is_none());
        // Empty-key tag dropped.
        assert!(m.tag_value("!!!").is_none());
    }

    #[test]
    fn trace_meta_host_and_tags_rewritten_dd_keys_skipped() {
        // Note on `meta._dd.*` shape: Vector parses dots in target paths as
        // separators, so the wire-format flat key `_dd.p.tid` is stored as
        // `meta -> _dd -> p -> tid` (nested objects). The skip rule for the
        // top-level `_dd` key keeps the entire Datadog-internal subtree
        // untouched, regardless of what shape it takes.
        let mut trace = TraceEvent::default();
        trace.insert("meta.host", "<weird>host");
        trace.insert("meta.Service", "My Service");
        trace.insert("meta.ENV", "Prod");
        trace.insert("meta._dd.p.tid", "deadbeef");
        trace.insert("meta._dd.tags.container", "container_id_42");

        let events = run(Event::Trace(trace));
        let trace = events[0].as_trace();

        // Host rewritten by hostname rules.
        assert_eq!(trace.get("meta.host"), Some(&Value::from("-weird-host")));
        // User keys rewritten by tag rules.
        assert_eq!(trace.get("meta.service"), Some(&Value::from("my_service")),);
        assert_eq!(trace.get("meta.env"), Some(&Value::from("prod")));
        // The entire `_dd` subtree is preserved verbatim — both the leading
        // underscore and the original casing/values.
        assert_eq!(trace.get("meta._dd.p.tid"), Some(&Value::from("deadbeef")),);
        assert_eq!(
            trace.get("meta._dd.tags.container"),
            Some(&Value::from("container_id_42")),
        );
    }
}
