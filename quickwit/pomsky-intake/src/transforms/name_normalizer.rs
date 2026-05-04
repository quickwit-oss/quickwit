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
use vector::event::metric::TagValue;
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
    // TODO(name-normalization): hostname normalization rewrites the lookup
    // key used by `add_host_tags` against `HostTagsStore`. Lookups assume the
    // metadata service returns hostnames in the same normalized form.
    // Awaiting confirmation from the metadata-service team before relying on
    // this in production.
    if let Some(host) = log.get("hostname").and_then(|v| v.as_str())
        && let Some(new_host) = normalize_hostname(&host)
    {
        log.insert("hostname", new_host);
    }

    normalize_log_ddtags(log);

    // The `tags.*` object isn't populated for Datadog Agent logs at this
    // stage (those tags ride in `ddtags`, handled above) but may exist for
    // other sources or after future pipeline rewires. Normalize it
    // defensively so the contract — "after this transform, all tag keys and
    // values are normalized" — holds regardless of source shape.
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
    // Iterate over every value of every tag (multi-valued tags emit one
    // entry per value) and re-insert each into the now-empty tag set. This
    // preserves multi-valued tags through the normalization pipeline.
    let original = std::mem::take(tags_mut);
    for (key, value) in original.into_iter_all() {
        if key == "host" {
            // Hostname normalization already covered this tag; leave it alone
            // to avoid re-running tag rules over a hostname-shaped string.
            tags_mut.insert(key, value);
            continue;
        }
        let new_key = if is_normalized_tag_key(&key) {
            key
        } else {
            let n = normalize_tag(&key);
            if n.is_empty() {
                continue;
            }
            n
        };
        let new_value = match value {
            TagValue::Bare => TagValue::Bare,
            TagValue::Value(s) => {
                if is_normalized_tag_value(&s) {
                    TagValue::Value(s)
                } else {
                    TagValue::Value(normalize_tag_value(&s))
                }
            }
        };
        tags_mut.insert(new_key, new_value);
    }
}

fn normalize_trace(trace: &mut TraceEvent) {
    // Hostname lives at top-level `host` by the time spans reach this
    // transform — `preprocess_span::remap` promotes `meta._dd.hostname`
    // to `host` (and OTLP traces land there too). Normalizing `meta.host`
    // would be a no-op against the actual hostname.
    if let Some(host) = trace.get("host").and_then(|v| v.as_str())
        && let Some(new_host) = normalize_hostname(&host)
    {
        trace.insert("host", new_host);
    }

    let Some(meta_value) = trace.get_mut("meta") else {
        return;
    };
    // TODO(name-normalization): `_dd` is the only top-level reserved
    // namespace we currently treat as pass-through. Other potentially
    // reserved or protocol-significant keys under `meta` (e.g. `env`,
    // `version`, `service`, span-link metadata) are still normalized,
    // which may rewrite them. Awaiting reviewer feedback on which keys
    // should be added to the skip-list.
    normalize_object_map(meta_value, |key| {
        // Skip the Datadog-internal `_dd` namespace. Vector parses dots in
        // target paths as separators, so wire-format keys like `_dd.p.tid`
        // and `_dd.tags.container` end up as a single nested object under
        // the `_dd` top-level key.
        key == "_dd"
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
        // Fast path: if the key is already normalized, reuse the existing
        // KeyString and skip the slow-path allocation. Falls through only
        // when normalization would mutate the key (or the input contains
        // non-ASCII characters that require Unicode lowercasing).
        let new_key = if is_normalized_tag_key(key.as_str()) {
            key
        } else {
            let n = normalize_tag(key.as_str());
            if n.is_empty() {
                continue;
            }
            KeyString::from(n)
        };
        // Fast path on the value mirrors the key path: if the existing
        // string is already normalized (or the value isn't a string at all),
        // reuse the original `Value` without constructing a new one. This
        // matters for high-cardinality telemetry where most tag values pass
        // through unchanged.
        let value_change: Option<String> = match val.as_str() {
            Some(s) if !is_normalized_tag_value(&s) => Some(normalize_tag_value(&s)),
            _ => None,
        };
        let new_val = match value_change {
            Some(new) => Value::from(new),
            None => val,
        };
        map.insert(new_key, new_val);
    }
}

/// Normalizes the `ddtags` field on a Datadog Agent log. By default the
/// agent emits a comma-separated string ("env:prod,team:foo"); when the
/// upstream `datadog_agent` source has `parse_ddtags = true`, the field
/// arrives as a `Value::Array` of strings instead. We handle both shapes.
fn normalize_log_ddtags(log: &mut LogEvent) {
    // CSV path: read once, write only on change. Borrow ends at the
    // `and_then` boundary because `normalize_ddtags_csv` returns an owned
    // `Option<String>`, freeing `log` for the subsequent `insert`.
    let csv_replacement = log
        .get("ddtags")
        .and_then(|v| v.as_str())
        .and_then(|s| normalize_ddtags_csv(&s));
    if let Some(new) = csv_replacement {
        log.insert("ddtags", new);
        return;
    }
    // Array path: mutate in place. Only reached when `ddtags` is not a
    // string (so the CSV branch was a no-op).
    if let Some(Value::Array(arr)) = log.get_mut("ddtags") {
        normalize_ddtags_array(arr);
    }
}

/// Normalizes a comma-separated `ddtags` CSV string. Returns `Some(new)`
/// when at least one tag was rewritten or dropped, or `None` when every
/// entry was already in normalized form (no allocation in the common case).
fn normalize_ddtags_csv(input: &str) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    let mut output = String::new();
    let mut any_change = false;
    let mut wrote_first = false;
    for tag in input.split(',') {
        if is_normalized_tag_key(tag) {
            if !wrote_first {
                output.reserve(input.len());
            }
            if wrote_first {
                output.push(',');
            }
            output.push_str(tag);
            wrote_first = true;
            continue;
        }
        any_change = true;
        let normalized = normalize_tag(tag);
        if normalized.is_empty() {
            continue;
        }
        if !wrote_first {
            output.reserve(input.len());
        }
        if wrote_first {
            output.push(',');
        }
        output.push_str(&normalized);
        wrote_first = true;
    }
    if any_change { Some(output) } else { None }
}

/// In-place normalization of a `parse_ddtags = true`-shaped array of tag
/// strings. Non-string entries pass through; entries that are already
/// normalized are reused without allocation.
fn normalize_ddtags_array(arr: &mut Vec<Value>) {
    let original = std::mem::take(arr);
    arr.reserve(original.len());
    for v in original {
        // Decide what to do without holding a borrow into `v`, so we can
        // freely move it into `arr` afterwards.
        let replacement: Option<String> = match v.as_str() {
            Some(s) if !is_normalized_tag_key(&s) => Some(normalize_tag(&s)),
            _ => None,
        };
        match replacement {
            Some(n) if n.is_empty() => {} // drop
            Some(n) => arr.push(Value::from(n)),
            None => arr.push(v),
        }
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

/// Fast path: returns `true` iff `normalize_tag(input) == input`. Limited to
/// pure-ASCII inputs because non-ASCII characters require Unicode-aware
/// lowercasing (handled by the slow path). When this returns `true` the
/// caller can reuse the existing `String`/`KeyString` without allocating.
fn is_normalized_tag_key(input: &str) -> bool {
    if input.is_empty() || input.len() > MAX_TAG_LENGTH || !input.is_ascii() {
        return false;
    }
    let bytes = input.as_bytes();
    if !matches!(bytes[0], b'a'..=b'z' | b':') {
        return false;
    }
    let mut last_was_underscore = false;
    for &b in bytes {
        match b {
            b'a'..=b'z' | b'0'..=b'9' | b':' | b'.' | b'/' | b'-' => {
                last_was_underscore = false;
            }
            b'_' => {
                if last_was_underscore {
                    return false;
                }
                last_was_underscore = true;
            }
            _ => return false,
        }
    }
    !last_was_underscore
}

/// Fast path mirror of [`is_normalized_tag_key`] for tag *values*. Differs
/// from the key check by also accepting leading digits / `.` / `/` / `-`,
/// matching `normalize_tag_value`'s relaxed leading-character rule. Empty
/// input is considered already normalized (empty values are valid).
fn is_normalized_tag_value(input: &str) -> bool {
    if input.is_empty() {
        return true;
    }
    if input.len() > MAX_TAG_LENGTH || !input.is_ascii() {
        return false;
    }
    let bytes = input.as_bytes();
    if !matches!(bytes[0], b'a'..=b'z' | b':' | b'0'..=b'9' | b'.' | b'/' | b'-') {
        return false;
    }
    let mut last_was_underscore = false;
    for &b in bytes {
        match b {
            b'a'..=b'z' | b'0'..=b'9' | b':' | b'.' | b'/' | b'-' => {
                last_was_underscore = false;
            }
            b'_' => {
                if last_was_underscore {
                    return false;
                }
                last_was_underscore = true;
            }
            _ => return false,
        }
    }
    !last_was_underscore
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
    fn metric_multi_valued_tag_values_preserved() {
        // A multi-valued tag like `key:val1,val2` must survive normalization
        // with both values intact (only the key/values are normalized in
        // place, none of the values are dropped).
        let mut metric_tags = MetricTags::default();
        metric_tags.insert("Team".to_string(), "Alpha".to_string());
        metric_tags.insert("Team".to_string(), "Beta Squad".to_string());

        let metric = Metric::new(
            "cpu.usage",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        )
        .with_tags(Some(metric_tags));

        let events = run(Event::Metric(metric));
        let m = events[0].as_metric();
        let tags = m.tags().expect("tags present");
        let values: Vec<&str> = tags
            .iter_all()
            .filter_map(|(k, v)| if k == "team" { v } else { None })
            .collect();
        assert_eq!(values, vec!["alpha", "beta_squad"]);
        assert!(tags.iter_all().all(|(k, _)| k != "Team"));
    }

    // -----------------------------------------------------------------------
    // Fast-path helper tests
    // -----------------------------------------------------------------------

    /// Property-style sanity check: when `is_normalized_tag_key` returns true,
    /// the slow path must produce the input verbatim. When it returns false,
    /// the slow path may or may not differ (Unicode-only inputs always return
    /// false even when already lowercase).
    #[test]
    fn fast_path_key_is_consistent_with_slow_path() {
        let already_normalized = [
            "env:prod",
            "region:us-east-1",
            "foo",
            "foo:bar:baz",
            "service",
            "k8s.io/cluster",
        ];
        for input in already_normalized {
            assert!(
                is_normalized_tag_key(input),
                "expected fast path to accept {input:?}",
            );
            assert_eq!(
                normalize_tag(input),
                input,
                "slow path should be a no-op for {input:?}",
            );
        }

        let needs_change = [
            "",         // empty (treated as not-normalized so caller can drop)
            "ENV:PROD", // uppercase
            "foo bar",  // space
            "foo!",     // trailing non-alnum
            "foo__bar", // contiguous underscore
            "foo_",     // trailing underscore
            "123foo",   // leading digit
            "Ωmega",    // non-ASCII
        ];
        for input in needs_change {
            assert!(
                !is_normalized_tag_key(input),
                "expected fast path to reject {input:?}",
            );
        }
    }

    #[test]
    fn fast_path_value_is_consistent_with_slow_path() {
        let already_normalized = [
            "", // empty value is allowed
            "prod",
            "1.2.3",
            "-foo",
            "/var/log",
            "us-east-1",
        ];
        for input in already_normalized {
            assert!(
                is_normalized_tag_value(input),
                "expected fast path to accept {input:?}",
            );
            assert_eq!(
                normalize_tag_value(input),
                input,
                "slow path should be a no-op for {input:?}",
            );
        }

        let needs_change = [
            "Prod",          // uppercase
            "my service",    // space
            "value!",        // trailing non-alnum
            "trail_",        // trailing underscore
            "double__under", // contiguous underscore
        ];
        for input in needs_change {
            assert!(
                !is_normalized_tag_value(input),
                "expected fast path to reject {input:?}",
            );
        }
    }

    // -----------------------------------------------------------------------
    // ddtags — unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn ddtags_csv_already_normalized_is_noop() {
        assert_eq!(normalize_ddtags_csv("env:prod,team:foo"), None);
        assert_eq!(normalize_ddtags_csv(""), None);
    }

    #[test]
    fn ddtags_csv_rewrites_uppercase_and_spaces() {
        assert_eq!(
            normalize_ddtags_csv("ENV:PROD,Service:My Service").as_deref(),
            Some("env:prod,service:my_service"),
        );
    }

    #[test]
    fn ddtags_csv_drops_empty_normalized_entries() {
        // `123` and `!!!` both normalize to "" and are dropped; surviving
        // entries are joined with commas (no leading/trailing comma).
        assert_eq!(
            normalize_ddtags_csv("123,env:prod,!!!").as_deref(),
            Some("env:prod"),
        );
    }

    #[test]
    fn ddtags_csv_dropping_only_entry_yields_empty_string() {
        assert_eq!(normalize_ddtags_csv("123").as_deref(), Some(""));
    }

    #[test]
    fn log_ddtags_csv_string_is_normalized() {
        let mut log = LogEvent::default();
        log.insert("hostname", "web-01");
        log.insert("ddtags", "ENV:PROD,Service:My Service");

        let events = run(Event::Log(log));
        let log = events[0].as_log();
        assert_eq!(
            log.get("ddtags"),
            Some(&Value::from("env:prod,service:my_service")),
        );
    }

    #[test]
    fn log_ddtags_csv_string_unchanged_is_not_rewritten() {
        let mut log = LogEvent::default();
        log.insert("hostname", "web-01");
        log.insert("ddtags", "env:prod,team:foo");

        let events = run(Event::Log(log));
        let log = events[0].as_log();
        assert_eq!(log.get("ddtags"), Some(&Value::from("env:prod,team:foo")));
    }

    #[test]
    fn log_ddtags_array_is_normalized_in_place() {
        // Simulates the shape produced when the `datadog_agent` source has
        // `parse_ddtags = true`: ddtags arrives as `Value::Array`.
        let mut log = LogEvent::default();
        log.insert("hostname", "web-01");
        log.insert(
            "ddtags",
            Value::Array(vec![
                Value::from("ENV:PROD"),
                Value::from("Service:My Service"),
                Value::from("123"), // normalizes to empty → dropped
                Value::from("env:already_clean"),
            ]),
        );

        let events = run(Event::Log(log));
        let log = events[0].as_log();
        let arr = match log.get("ddtags") {
            Some(Value::Array(a)) => a,
            other => panic!("expected ddtags array, got {other:?}"),
        };
        let strings: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(|c| c.into_owned()))
            .collect();
        assert_eq!(
            strings,
            vec!["env:prod", "service:my_service", "env:already_clean"],
        );
    }

    #[test]
    fn trace_host_and_tags_rewritten_dd_keys_skipped() {
        // Note on `meta._dd.*` shape: Vector parses dots in target paths as
        // separators, so the wire-format flat key `_dd.p.tid` is stored as
        // `meta -> _dd -> p -> tid` (nested objects). The skip rule for the
        // top-level `_dd` key keeps the entire Datadog-internal subtree
        // untouched, regardless of what shape it takes.
        let mut trace = TraceEvent::default();
        trace.insert("host", "<weird>host");
        trace.insert("meta.Service", "My Service");
        trace.insert("meta.ENV", "Prod");
        trace.insert("meta._dd.p.tid", "deadbeef");
        trace.insert("meta._dd.tags.container", "container_id_42");

        let events = run(Event::Trace(trace));
        let trace = events[0].as_trace();

        // Host rewritten by hostname rules.
        assert_eq!(trace.get("host"), Some(&Value::from("-weird-host")));
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
