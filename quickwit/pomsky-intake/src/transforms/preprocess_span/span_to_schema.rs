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

//! Maps the apm-processing-aligned canonical Datadog span to the shape the
//! `datadog-spans` index expects. This is the tail of the `preprocess_span`
//! pipeline — it consumes the canonical shape produced by
//! `preprocess_dd_trace` (i64 IDs, i64-ns `start`, decoded `meta_struct`) and
//! emits the SaaS-style document.

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::LazyLock;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use rand::RngExt as _;
use vector::event::{ObjectMap, TraceEvent, Value};

static STATUS_OK: LazyLock<Value> = LazyLock::new(|| Value::Bytes(Bytes::from_static(b"ok")));
static STATUS_ERROR: LazyLock<Value> = LazyLock::new(|| Value::Bytes(Bytes::from_static(b"error")));

/// Internal-namespace prefixes whose tags are kept at indexation time even
/// though they start with `_`. Mirrors `keepNamespaces` in dd-go
/// `trace/intake/events/mapper.go`.
const KEEP_NAMESPACES: &[&str] = &[
    "_dd.appsec.",
    "_dd.iast.",
    "_dd.error_tracking.",
    "_dd.issue.",
    "_dd.ci.",
    "_dd.di.",
    "_dd.ld.",
    "_dd.code_origin.",
    "_dd.debug.",
    "_dd.recommendation.",
    "_dd.test.",
    "_dd.library_capabilities.",
    "_dd.ai_guard.",
];

/// Internal tag keys that are kept at indexation time even though they
/// start with `_`. Mirrors `keepAttributes` in dd-go
/// `trace/intake/events/mapper.go`.
static KEEP_ATTRIBUTES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "_dd.hostname",
        "_dd.agent_hostname",
        "_dd.tracer_version",
        "_dd.agent_version",
        "_dd.application.id",
        "_dd.resource.id",
        "_dd.session.id",
        "_dd.view.id",
        "_dd.action.id",
        "_dd.error_tracking",
        "_top_level",
        "_trace_root",
        "_dd.base_service",
        "_dd.sds.default",
        "_dd.stack",
        "_dd.peer.service.resolved_from",
        "_dd.flattened.peer.tags",
        "_dd.rule_psr",
        "_dd.origin",
        "_dd.filter.id",
        "_dd.source",
        "_dd.git.repository.source",
        "_dd.git.commit.source",
        "_dd.renaming.rule_id",
        "_dd.query_signature",
        "_dd.djm_serverless_processed",
        "_dd.svc_src",
    ]
    .into_iter()
    .collect()
});

/// Mirrors dd-go's `events.isValidTag` in `trace/intake/events/mapper.go`:
/// underscore-prefixed and `ddtags` keys are stripped unless they're in
/// the `KEEP_ATTRIBUTES` allowlist or under a `KEEP_NAMESPACES` prefix.
fn is_valid_tag(key: &str) -> bool {
    if KEEP_ATTRIBUTES.contains(key) {
        return true;
    }
    if KEEP_NAMESPACES.iter().any(|ns| key.starts_with(ns)) {
        return true;
    }
    !key.is_empty() && !key.starts_with('_') && key != "ddtags"
}

/// Remaps a canonical Datadog span event to the schema's field shape:
/// - stringify `span_id`/`parent_id` as unsigned decimals and derive `trace_id_low` as the unsigned
///   decimal of the canonical `trace_id`; format `trace_id` as 32-char hex when `meta._dd.p.tid` is
///   present and valid (exactly 16 lowercase hex chars), else as the same decimal as `trace_id_low`
/// - derive `start_time` (i64 unix ns) from canonical `start`, `timestamp` (rfc3339 ms) from `start
///   + duration`, and `discovery_timestamp` (i64 unix ms) from ingest now
/// - rename `name` → `operation_name`, `resource` → `resource_name`
/// - derive `status` from the wire `error` flag (0 → "ok", else "error")
/// - lift `meta.error.type` to top-level `error.type` (matches SaaS shape; the rest of `error.*`
///   stays only under `custom.error.*`)
/// - promote `meta._dd.hostname` and `meta.env` to top-level `host`/`env`
/// - compute `resource_hash` (lower 64 bits of murmur3_x64_128, hex)
/// - hardcode `single_span` and `analytics_enabled` to `false`
/// - emit a random positive `tiebreaker`
/// - drop the leftover canonical `start` (already extracted into `start_time`)
/// - fold `meta`, `metrics`, `meta_struct`, `duration`, `span_links`, and `span_events` into the
///   catch-all `custom` JSON field. Keys from `meta`/`metrics`/`meta_struct` are filtered through
///   `is_valid_tag` (mirroring dd-go's `events.isValidTag`): underscore-prefixed and `ddtags` keys
///   are stripped unless allowlisted. Schema's `custom` declares `expand_dots: true`, so dotted
///   keys like `_dd.agent_version` get nested at indexing time.
pub(super) fn span_to_schema(trace: &mut TraceEvent) {
    derive_timestamps(trace);
    stringify_ids(trace);
    promote_host_and_env(trace);
    derive_status_and_error_type(trace);

    // Hardcode the SaaS-side flags we have no signal for; populated as
    // `false` by convention so the columnar fast-field is dense.
    trace.insert("single_span", false);
    trace.insert("analytics_enabled", false);

    // Random positive 32-bit number — matches the magnitude SaaS produces.
    let tiebreaker = rand::rng().random_range(0i64..=i64::from(u32::MAX));
    trace.insert("tiebreaker", tiebreaker);

    derive_resource_fields(trace);
    backfill_agent_hostname(trace);
    trace.remove("start");
    fold_into_custom(trace);
}

/// If `meta._dd.agent_hostname` is absent or empty, copies `meta._dd.hostname`
/// into it so that trace-query's `flattenCustom` can populate
/// `span.Meta["_dd.agent_hostname"]` for the waterfall UI — which is what
/// SaaS spans carry but BYOC spans were missing.
fn backfill_agent_hostname(trace: &mut TraceEvent) {
    let Some(Value::Object(meta)) = trace.get_mut("meta") else {
        return;
    };
    let already_set = matches!(
        meta.get("_dd.agent_hostname").and_then(Value::as_str),
        Some(s) if !s.is_empty(),
    );
    if already_set {
        return;
    }
    let Some(hostname) = meta.get("_dd.hostname").cloned() else {
        return;
    };
    meta.insert("_dd.agent_hostname".into(), hostname);
}

/// Promotes `meta._dd.hostname` → `host` and `meta.env` → `env`. Keys in
/// `meta` are stored with literal dots (e.g. `"_dd.hostname"`), so we read
/// them directly off the inner `ObjectMap` rather than through Vector's
/// path-traversal API which would split on the dot.
fn promote_host_and_env(trace: &mut TraceEvent) {
    let host_opt;
    let env_opt;
    {
        let empty = ObjectMap::new();
        let meta = match trace.get("meta") {
            Some(Value::Object(m)) => m,
            _ => &empty,
        };
        host_opt = meta.get("_dd.hostname").cloned();
        env_opt = meta.get("env").cloned();
    }
    if let Some(host) = host_opt {
        trace.insert("host", host);
    }
    if let Some(env) = env_opt {
        trace.insert("env", env);
    }
}

/// Derives `status` from the wire `error` flag (0 → `"ok"`, else `"error"`)
/// and lifts `meta.error.type` to a top-level `error` object carrying only
/// `type`. The rest of `error.*` stays under `custom.error.*` via the
/// meta fold — matches SaaS shape; narrow lifting keeps cross-product
/// query semantics consistent.
fn derive_status_and_error_type(trace: &mut TraceEvent) {
    if let Some(Value::Integer(raw)) = trace.get("error") {
        let status = if *raw == 0 {
            STATUS_OK.clone()
        } else {
            STATUS_ERROR.clone()
        };
        trace.insert("status", status);
    }
    trace.remove("error");

    if let Some(error_type) = trace
        .get("meta")
        .and_then(|v| v.as_object())
        .and_then(|m| m.get("error.type"))
        .cloned()
    {
        let mut error_obj = ObjectMap::new();
        error_obj.insert("type".into(), error_type);
        trace.insert("error", Value::Object(error_obj));
    }
}

/// Renames `name` → `operation_name`, `resource` → `resource_name`, and
/// emits `resource_hash` from the original resource string.
fn derive_resource_fields(trace: &mut TraceEvent) {
    if let Some(name) = trace.remove("name") {
        trace.insert("operation_name", name);
    }
    if let Some(resource_value) = trace.remove("resource") {
        if let Some(resource_str) = resource_value.as_str() {
            trace.insert("resource_hash", resource_hash(resource_str.as_bytes()));
        }
        trace.insert("resource_name", resource_value);
    }
}

/// Folds `meta`, `metrics`, `meta_struct`, `duration`, `span_links`, and
/// `span_events` into the catch-all `custom` JSON field. Keys from the
/// tag maps are filtered through `is_valid_tag` (mirroring dd-go's
/// `events.isValidTag`): underscore-prefixed and `ddtags` keys are
/// stripped unless allowlisted.
fn fold_into_custom(trace: &mut TraceEvent) {
    let mut custom = ObjectMap::new();
    fold_filtered_map(trace, "meta", &mut custom);
    fold_filtered_map(trace, "metrics", &mut custom);
    fold_filtered_map(trace, "meta_struct", &mut custom);
    if let Some(duration) = trace.remove("duration") {
        custom.insert("duration".into(), duration);
    }
    if let Some(span_links) = trace.remove("span_links") {
        custom.insert("span_links".into(), span_links);
    }
    if let Some(span_events) = trace.remove("span_events") {
        custom.insert("span_events".into(), span_events);
    }
    if !custom.is_empty() {
        trace.insert("custom", Value::Object(custom));
    }
}

/// Removes `key` from `trace` (expected to be a flat `ObjectMap`) and
/// inserts each entry that passes `is_valid_tag` into `custom`.
fn fold_filtered_map(trace: &mut TraceEvent, key: &str, custom: &mut ObjectMap) {
    let Some(Value::Object(map)) = trace.remove(key) else {
        return;
    };
    for (k, v) in map {
        if is_valid_tag(k.as_str()) {
            custom.insert(k, v);
        }
    }
}

/// Derives the three timestamp fields the `datadog-spans` index expects from
/// the canonical `start` (i64 unix ns) and `duration` (i64 ns):
/// - `start_time` — copy of `start`, kept for explicit naming in the doc
/// - `timestamp` — rfc3339 ms (`Z`-suffixed) of `floor((start + duration) / 1e6)`; this is the
///   index's `timestamp_field`, so the doc is dropped without it
/// - `discovery_timestamp` — when intake observed the span (i64 unix ms)
fn derive_timestamps(trace: &mut TraceEvent) {
    let start_ns_opt = match trace.get("start") {
        Some(Value::Integer(ns)) => Some(*ns),
        _ => None,
    };
    let duration_ns = match trace.get("duration") {
        Some(Value::Integer(d)) => *d,
        _ => 0,
    };

    if let Some(start_ns) = start_ns_opt {
        trace.insert("start_time", start_ns);
        let end_ns = start_ns.saturating_add(duration_ns);
        let end_ms = end_ns.div_euclid(1_000_000);
        if let Some(end_dt) = DateTime::<Utc>::from_timestamp_millis(end_ms) {
            trace.insert(
                "timestamp",
                end_dt.to_rfc3339_opts(SecondsFormat::Millis, true),
            );
        }
    }

    trace.insert("discovery_timestamp", Utc::now().timestamp_millis());
}

/// Stringifies canonical i64 IDs (which match Java `SpanMap`'s `long`) into
/// the form the SaaS spans index uses:
/// - `span_id`, `parent_id` are always unsigned decimal strings
/// - `trace_id_low` is the unsigned decimal of the canonical `trace_id` (the lower 64 bits)
/// - `trace_id` is the 32-char hex `{upper_64_hex}{lower_64_hex}` when `meta._dd.p.tid` is present
///   and valid (exactly 16 lowercase hex chars, matching apm-processing's validation); falls back
///   to the same decimal as `trace_id_low` when absent or invalid; `trace_id` is kept for schema
fn stringify_ids(trace: &mut TraceEvent) {
    let lower_opt = match trace.get("trace_id") {
        Some(Value::Integer(raw)) => Some(*raw as u64),
        _ => None,
    };
    let upper_hex_opt = trace
        .get("meta")
        .and_then(|v| v.as_object())
        .and_then(|m| m.get("_dd.p.tid"))
        .and_then(|v| v.as_str())
        .filter(|s| is_valid_trace_id_high(s))
        .map(|s| s.to_string());

    if let Some(lower) = lower_opt {
        let decimal_low = lower.to_string();
        let trace_id_str = match &upper_hex_opt {
            Some(upper) => format!("{upper}{lower:016x}"),
            None => decimal_low.clone(),
        };
        trace.insert("trace_id", trace_id_str);
        trace.insert("trace_id_low", decimal_low);
    }

    stringify_id(trace, "span_id");
    stringify_id(trace, "parent_id");
}

/// Reinterprets the i64-encoded canonical ID as u64 and formats it as
/// an unsigned decimal string.
fn stringify_id(trace: &mut TraceEvent, key: &str) {
    let Some(Value::Integer(raw)) = trace.get(key) else {
        return;
    };
    let decimal = (*raw as u64).to_string();
    trace.insert(key, decimal);
}

/// Returns `true` if `s` is exactly 16 lowercase hex characters — the valid
/// format for `_dd.p.tid` (the upper 64 bits of a 128-bit trace ID).
/// Mirrors apm-processing's `getValidatedHigher64BitsTraceId` validation.
fn is_valid_trace_id_high(s: &str) -> bool {
    s.len() == 16
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Computes the resource hash exactly as logs-backend's
/// `Resources.resourceHash`: lower 64 bits of MurmurHash3 x64 128 with
/// seed 0, formatted as minimum-length lowercase hex (no zero-padding —
/// `Long.toHexString` strips leading zeros).
fn resource_hash(resource: &[u8]) -> String {
    let h = murmur3::murmur3_x64_128(&mut Cursor::new(resource), 0)
        .expect("murmur3 over an in-memory slice cannot fail");
    format!("{:x}", h as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dd_trace(fields: impl FnOnce(&mut TraceEvent)) -> TraceEvent {
        let mut trace = TraceEvent::default();
        fields(&mut trace);
        trace
    }

    fn run(fields: impl FnOnce(&mut TraceEvent)) -> TraceEvent {
        let mut trace = dd_trace(fields);
        span_to_schema(&mut trace);
        trace
    }

    #[test]
    fn test_resource_hash_matches_logs_backend() {
        // Cross-check value taken from a real Datadog SaaS span:
        // resource_name="POST /api/ui/frontend_telemetry/metrics"
        //  → resource_hash="8a8a34f089631534"
        assert_eq!(
            resource_hash(b"POST /api/ui/frontend_telemetry/metrics"),
            "8a8a34f089631534",
        );
    }

    #[test]
    fn test_derives_start_time_and_timestamp() {
        // start = 2024-08-19T09:35:43.000Z, duration = 5 ms.
        let trace = run(|t| {
            t.insert("start", 1_724_060_143_000_000_000i64);
            t.insert("duration", 5_000_000i64);
        });
        assert_eq!(
            trace.get("start_time"),
            Some(&Value::Integer(1_724_060_143_000_000_000)),
        );
        assert_eq!(
            trace.get("timestamp"),
            Some(&Value::from("2024-08-19T09:35:43.005Z")),
        );
        // `discovery_timestamp` is set to "now" — just check it's present.
        assert!(matches!(
            trace.get("discovery_timestamp"),
            Some(Value::Integer(_)),
        ));
        // Canonical `start` is dropped after derivation.
        assert!(trace.get("start").is_none());
    }

    #[test]
    fn test_discovery_timestamp_set_without_start() {
        let trace = run(|_| {});
        assert!(trace.get("start_time").is_none());
        assert!(trace.get("timestamp").is_none());
        assert!(matches!(
            trace.get("discovery_timestamp"),
            Some(Value::Integer(_)),
        ));
    }

    #[test]
    fn test_stringifies_ids_as_unsigned_decimal() {
        // Canonical IDs are i64 (Java SpanMap's `long`); the schema doc has
        // them as unsigned decimal strings, and trace_id_low mirrors trace_id.
        let trace = run(|t| {
            t.insert("trace_id", 12345678i64);
            t.insert("span_id", 87654321i64);
            t.insert("parent_id", 0i64);
        });
        assert_eq!(trace.get("trace_id"), Some(&Value::from("12345678")));
        assert_eq!(trace.get("trace_id_low"), Some(&Value::from("12345678")));
        assert_eq!(trace.get("span_id"), Some(&Value::from("87654321")));
        assert_eq!(trace.get("parent_id"), Some(&Value::from("0")));
    }

    #[test]
    fn test_128bit_trace_id_assembles_hex_string() {
        // When meta._dd.p.tid is a valid 16-char lowercase hex string (the
        // upper 64 bits of a 128-bit trace ID), trace_id is the 32-char hex
        // {upper}{lower:016x} and trace_id_low stays as the decimal of the lower
        // 64 bits for backward compatibility with 64-bit consumers.
        let mut meta = ObjectMap::new();
        meta.insert("_dd.p.tid".into(), Value::from("69d64a1900000000"));
        let trace = run(|t| {
            t.insert("trace_id", 0x2c76445808c4c2d6i64);
            t.insert("meta", Value::Object(meta));
        });
        assert_eq!(
            trace.get("trace_id"),
            Some(&Value::from("69d64a19000000002c76445808c4c2d6")),
        );
        assert_eq!(
            trace.get("trace_id_low"),
            Some(&Value::from("3203823329815610070")),
        );
        // _dd.p.tid is not in KEEP_ATTRIBUTES, so is_valid_tag strips it from custom.
        if let Some(Value::Object(custom)) = trace.get("custom") {
            assert!(custom.get("_dd.p.tid").is_none());
        }
    }

    #[test]
    fn test_128bit_trace_id_invalid_high_falls_back_to_decimal() {
        // When _dd.p.tid fails validation (wrong length, uppercase, or non-hex),
        // stringify_ids falls back to the unsigned-decimal form of the lower 64
        // bits — same as if _dd.p.tid were absent.
        for invalid in [
            "69D64A1900000000",
            "69d64a190000000",
            "69d64a19000000000",
            "",
        ] {
            let mut meta = ObjectMap::new();
            meta.insert("_dd.p.tid".into(), Value::from(invalid));
            let trace = run(|t| {
                t.insert("trace_id", 0x2c76445808c4c2d6i64);
                t.insert("meta", Value::Object(meta));
            });
            assert_eq!(
                trace.get("trace_id"),
                Some(&Value::from("3203823329815610070")),
                "expected decimal fallback for invalid _dd.p.tid={invalid:?}",
            );
            assert_eq!(
                trace.get("trace_id_low"),
                Some(&Value::from("3203823329815610070")),
                "trace_id_low should always be the decimal lower-64",
            );
        }
    }

    #[test]
    fn test_stringifies_large_ids_as_unsigned() {
        // u64::MAX is reinterpreted by Vector as -1i64; the schema doc must
        // present it as the unsigned decimal 18446744073709551615.
        let trace = run(|t| {
            t.insert("trace_id", -1i64);
            t.insert("span_id", -1i64);
        });
        assert_eq!(
            trace.get("trace_id"),
            Some(&Value::from("18446744073709551615")),
        );
        assert_eq!(
            trace.get("trace_id_low"),
            Some(&Value::from("18446744073709551615")),
        );
        assert_eq!(
            trace.get("span_id"),
            Some(&Value::from("18446744073709551615")),
        );
    }

    #[test]
    fn test_renames_name_and_resource() {
        let trace = run(|t| {
            t.insert("name", "http.request");
            t.insert("resource", "POST /api/foo");
        });
        assert_eq!(
            trace.get("operation_name"),
            Some(&Value::from("http.request")),
        );
        assert_eq!(
            trace.get("resource_name"),
            Some(&Value::from("POST /api/foo")),
        );
        assert!(trace.get("name").is_none());
        assert!(trace.get("resource").is_none());
    }

    #[test]
    fn test_emits_resource_hash() {
        let trace = run(|t| {
            t.insert("resource", "POST /api/ui/frontend_telemetry/metrics");
        });
        assert_eq!(
            trace.get("resource_hash"),
            Some(&Value::from("8a8a34f089631534")),
        );
    }

    #[test]
    fn test_error_flag_to_status() {
        let trace = run(|t| {
            t.insert("error", 0i64);
        });
        assert_eq!(trace.get("status"), Some(&Value::from("ok")));
        assert!(trace.get("error").is_none());

        let trace = run(|t| {
            t.insert("error", 1i64);
        });
        assert_eq!(trace.get("status"), Some(&Value::from("error")));
    }

    #[test]
    fn test_extracts_error_type_from_meta() {
        let mut meta = ObjectMap::new();
        meta.insert("error.type".into(), Value::from("*fmt.wrapError"));
        meta.insert("error.message".into(), Value::from("boom"));
        meta.insert("error.stack".into(), Value::from("traceback…"));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
            t.insert("error", 1i64);
        });
        // Top-level `error` only carries `type` — matches SaaS shape.
        let Some(Value::Object(error_obj)) = trace.get("error") else {
            panic!(
                "error should be a structured object, got {:?}",
                trace.get("error"),
            );
        };
        assert_eq!(error_obj.get("type"), Some(&Value::from("*fmt.wrapError")));
        assert_eq!(error_obj.get("message"), None);
        assert_eq!(error_obj.get("stack"), None);
        // Everything (including type) still appears under custom.error.* via
        // the meta fold; expand_dots nests them at indexing time.
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(
            custom.get("error.type"),
            Some(&Value::from("*fmt.wrapError")),
        );
        assert_eq!(custom.get("error.message"), Some(&Value::from("boom")));
        assert_eq!(custom.get("error.stack"), Some(&Value::from("traceback…")));
        // The wire `error` flag is still reflected in `status`.
        assert_eq!(trace.get("status"), Some(&Value::from("error")));
    }

    #[test]
    fn test_no_error_when_meta_lacks_error_type() {
        // status="error" (from wire flag) but no error.type in meta — match
        // event 26 in spans.json: top-level `error` should be absent.
        let trace = run(|t| {
            t.insert("error", 1i64);
        });
        assert!(trace.get("error").is_none());
        assert_eq!(trace.get("status"), Some(&Value::from("error")));
    }

    #[test]
    fn test_promotes_host_and_env_from_meta() {
        let mut meta = ObjectMap::new();
        meta.insert("_dd.hostname".into(), Value::from("host-1"));
        meta.insert("env".into(), Value::from("prod"));
        meta.insert("custom_tag".into(), Value::from("kept-under-custom"));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
        });
        assert_eq!(trace.get("host"), Some(&Value::from("host-1")));
        assert_eq!(trace.get("env"), Some(&Value::from("prod")));
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object, got {:?}", trace.get("custom"),);
        };
        // `_dd.hostname` is allowlisted so it survives the tag filter and
        // appears in custom (as well as being promoted to top-level `host`).
        assert_eq!(custom.get("_dd.hostname"), Some(&Value::from("host-1")));
        assert_eq!(custom.get("env"), Some(&Value::from("prod")));
        assert_eq!(
            custom.get("custom_tag"),
            Some(&Value::from("kept-under-custom")),
        );
    }

    #[test]
    fn test_folds_meta_metrics_duration_under_custom() {
        let mut meta = ObjectMap::new();
        meta.insert("user.id".into(), Value::from("42"));
        // `_dd.tracer_version` is allowlisted; the underscore-prefixed
        // `_sampling_priority_v1` is not, so the latter is stripped.
        meta.insert("_dd.tracer_version".into(), Value::from("1.2.3"));
        let mut metrics = ObjectMap::new();
        metrics.insert("_sampling_priority_v1".into(), Value::from(1.0));
        metrics.insert("process_id".into(), Value::from(4242.0));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
            t.insert("metrics", Value::Object(metrics));
            t.insert("duration", 3_764_100i64);
        });
        assert!(trace.get("meta").is_none());
        assert!(trace.get("metrics").is_none());
        assert!(trace.get("duration").is_none());
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(custom.get("user.id"), Some(&Value::from("42")));
        assert_eq!(
            custom.get("_dd.tracer_version"),
            Some(&Value::from("1.2.3")),
        );
        assert!(custom.get("_sampling_priority_v1").is_none());
        assert_eq!(custom.get("process_id"), Some(&Value::from(4242.0)));
        assert_eq!(custom.get("duration"), Some(&Value::from(3_764_100i64)));
    }

    #[test]
    fn test_keep_namespace_prefix_allowed() {
        // `_dd.appsec.` is a kept namespace, so `_dd.appsec.findings` survives
        // even though it starts with an underscore.
        let mut meta = ObjectMap::new();
        meta.insert("_dd.appsec.findings".into(), Value::from("triggered"));
        meta.insert("_dd.other.x".into(), Value::from("dropped"));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
        });
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(
            custom.get("_dd.appsec.findings"),
            Some(&Value::from("triggered")),
        );
        assert!(custom.get("_dd.other.x").is_none());
    }

    #[test]
    fn test_folds_decoded_meta_struct_under_custom() {
        // meta_struct leaves are already decoded by preprocess_dd_trace; this
        // transform just folds them through to `custom` (subject to the tag
        // filter — `_dd.appsec.` is a kept namespace).
        let mut appsec = ObjectMap::new();
        appsec.insert("enabled".into(), Value::Integer(1));
        appsec.insert(
            "rules".into(),
            Value::Array(vec![Value::from("a"), Value::from("b")]),
        );
        let mut meta_struct = ObjectMap::new();
        meta_struct.insert("_dd.appsec.json".into(), Value::Object(appsec));
        let trace = run(|t| {
            t.insert("meta_struct", Value::Object(meta_struct));
        });
        assert!(trace.get("meta_struct").is_none());
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        let Some(Value::Object(appsec_out)) = custom.get("_dd.appsec.json") else {
            panic!("custom['_dd.appsec.json'] should be an object");
        };
        assert_eq!(appsec_out.get("enabled"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_backfill_agent_hostname_from_dd_hostname() {
        // When _dd.agent_hostname is absent, _dd.hostname is copied into it.
        let mut meta = ObjectMap::new();
        meta.insert("_dd.hostname".into(), Value::from("i-0701afde620240d89"));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
        });
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(
            custom.get("_dd.agent_hostname"),
            Some(&Value::from("i-0701afde620240d89")),
            "_dd.agent_hostname should be backfilled from _dd.hostname",
        );
    }

    #[test]
    fn test_backfill_agent_hostname_not_overwritten_when_set() {
        // When _dd.agent_hostname is already set, it is preserved as-is.
        let mut meta = ObjectMap::new();
        meta.insert("_dd.hostname".into(), Value::from("node-hostname"));
        meta.insert("_dd.agent_hostname".into(), Value::from("agent-hostname"));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
        });
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(
            custom.get("_dd.agent_hostname"),
            Some(&Value::from("agent-hostname")),
            "_dd.agent_hostname should not be overwritten when already set",
        );
    }

    #[test]
    fn test_backfill_agent_hostname_empty_string_is_overwritten() {
        // An empty _dd.agent_hostname is treated as absent and backfilled.
        let mut meta = ObjectMap::new();
        meta.insert("_dd.hostname".into(), Value::from("i-0abc123"));
        meta.insert("_dd.agent_hostname".into(), Value::from(""));
        let trace = run(|t| {
            t.insert("meta", Value::Object(meta));
        });
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        assert_eq!(
            custom.get("_dd.agent_hostname"),
            Some(&Value::from("i-0abc123")),
            "empty _dd.agent_hostname should be backfilled from _dd.hostname",
        );
    }

    #[test]
    fn test_static_flags_and_tiebreaker() {
        let trace = run(|_| {});
        assert_eq!(trace.get("single_span"), Some(&Value::Boolean(false)));
        assert_eq!(trace.get("analytics_enabled"), Some(&Value::Boolean(false)));
        let Some(Value::Integer(tb)) = trace.get("tiebreaker") else {
            panic!(
                "tiebreaker should be an integer, got {:?}",
                trace.get("tiebreaker"),
            );
        };
        assert!(*tb >= 0, "tiebreaker should be positive");
        assert!(*tb <= i64::from(u32::MAX), "tiebreaker should fit in u32");
    }
}
