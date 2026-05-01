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

//! Maps a normalized Datadog span event to the shape the `datadog-spans`
//! index expects. Models the Global Pipeline step in logs-backend's
//! `SpansRemapper` — runs after structural normalization and is responsible
//! for renames, derivations, and folding of catch-all maps.

use std::io::Cursor;
use std::sync::LazyLock;

use bytes::Bytes;
use rand::RngExt as _;
use vector::event::{ObjectMap, TraceEvent, Value};

static STATUS_OK: LazyLock<Value> =
    LazyLock::new(|| Value::Bytes(Bytes::from_static(b"ok")));
static STATUS_ERROR: LazyLock<Value> =
    LazyLock::new(|| Value::Bytes(Bytes::from_static(b"error")));

/// Remaps a normalized Datadog span event to the schema's field shape:
/// - rename `name` → `operation_name`, `resource` → `resource_name`
/// - derive `status` from the wire `error` flag (0 → "ok", else "error")
/// - lift `meta.error.type` to top-level `error.type` (matches SaaS shape; the rest of `error.*`
///   stays only under `custom.error.*`)
/// - promote `meta._dd.hostname` and `meta.env` to top-level `host`/`env`
/// - compute `resource_hash` (lower 64 bits of murmur3_x64_128, hex)
/// - hardcode `single_span` and `analytics_enabled` to `false`
/// - emit a random positive `tiebreaker`
/// - drop the leftover `start` Timestamp (already extracted into `start_time`)
/// - fold `meta`, `metrics`, `meta_struct` (msgpack-decoded), `duration`, `span_links`, and
///   `span_events` into the catch-all `custom` JSON field. Schema's `custom` declares `expand_dots:
///   true`, so dotted keys like `_dd.agent_version` get nested at indexing time.
pub(super) fn remap_dd_span_to_schema(trace: &mut TraceEvent) {
    // Promote host/env from `meta` to top-level fields. Keys in `meta` are
    // stored with literal dots (e.g. "_dd.hostname"), so we read them
    // directly off the inner ObjectMap rather than through Vector's
    // path-traversal API which would split on the dot.
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

    if let Some(Value::Integer(raw)) = trace.get("error") {
        let status = if *raw == 0 { STATUS_OK.clone() } else { STATUS_ERROR.clone() };
        trace.insert("status", status);
    }
    trace.remove("error");

    // SaaS docs surface only `error.type` at the top level under `error.type`
    // and leave everything else (`error.message`, `error.stack`, …) under
    // `custom.error.*` via the meta fold. Mirror that — narrow lifting keeps
    // cross-product query semantics consistent.
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

    // Hardcode the SaaS-side flags we have no signal for; populated as
    // `false` by convention so the columnar fast-field is dense.
    trace.insert("single_span", false);
    trace.insert("analytics_enabled", false);

    // Random positive 32-bit number — matches the magnitude SaaS produces.
    let tiebreaker = rand::rng().random_range(0i64..=i64::from(u32::MAX));
    trace.insert("tiebreaker", tiebreaker);

    if let Some(name) = trace.remove("name") {
        trace.insert("operation_name", name);
    }
    if let Some(resource_value) = trace.remove("resource") {
        if let Some(resource_str) = resource_value.as_str() {
            trace.insert("resource_hash", resource_hash(resource_str.as_bytes()));
        }
        trace.insert("resource_name", resource_value);
    }
    trace.remove("start");

    let mut custom = ObjectMap::new();
    if let Some(Value::Object(meta_map)) = trace.remove("meta") {
        for (k, v) in meta_map {
            custom.insert(k, v);
        }
    }
    if let Some(Value::Object(metrics_map)) = trace.remove("metrics") {
        for (k, v) in metrics_map {
            custom.insert(k, v);
        }
    }
    if let Some(Value::Object(meta_struct_map)) = trace.remove("meta_struct") {
        for (k, v) in meta_struct_map {
            // meta_struct values are msgpack-encoded byte blobs. Decode each
            // leaf into a JSON value so the structured content (e.g. AppSec
            // findings) can be queried via `custom._dd.appsec.*`.
            if let Some(decoded) = decode_meta_struct_leaf(&v) {
                custom.insert(k, decoded);
            }
        }
    }
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

/// Decodes a `meta_struct` leaf (`Value::Bytes`) as msgpack into a Vector
/// `Value`. Mirrors logs-backend's `SpansProtobufPayloadParser.decodeMetaStructValue`.
/// Returns None on a non-bytes input or a decode error — we'd rather drop a
/// malformed leaf than reject the whole span.
fn decode_meta_struct_leaf(value: &Value) -> Option<Value> {
    let bytes = match value {
        Value::Bytes(b) => b,
        _ => return None,
    };
    rmp_serde::from_slice(bytes).ok()
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
    use chrono::{TimeZone, Utc};

    use super::*;

    fn dd_trace(fields: impl FnOnce(&mut TraceEvent)) -> TraceEvent {
        let mut trace = TraceEvent::default();
        fields(&mut trace);
        trace
    }

    fn run(fields: impl FnOnce(&mut TraceEvent)) -> TraceEvent {
        let mut trace = dd_trace(fields);
        remap_dd_span_to_schema(&mut trace);
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
        let mut metrics = ObjectMap::new();
        metrics.insert("_sampling_priority_v1".into(), Value::from(1.0));
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
        assert_eq!(custom.get("_sampling_priority_v1"), Some(&Value::from(1.0)),);
        assert_eq!(custom.get("duration"), Some(&Value::from(3_764_100i64)));
    }

    #[test]
    fn test_decodes_meta_struct_into_custom() {
        // msgpack-encode a small object {"enabled": 1, "rules": ["a", "b"]}
        // and stash it under meta_struct["_dd.appsec"], mirroring what an
        // AppSec-instrumented tracer sends on the wire.
        let payload = serde_json::json!({
            "enabled": 1,
            "rules": ["a", "b"],
        });
        let bytes = rmp_serde::to_vec(&payload).expect("encode msgpack");
        let mut meta_struct = ObjectMap::new();
        meta_struct.insert("_dd.appsec".into(), Value::Bytes(bytes.into()));
        let trace = run(|t| {
            t.insert("meta_struct", Value::Object(meta_struct));
        });
        assert!(trace.get("meta_struct").is_none());
        let Some(Value::Object(custom)) = trace.get("custom") else {
            panic!("custom should be an object");
        };
        let Some(Value::Object(appsec)) = custom.get("_dd.appsec") else {
            panic!("custom['_dd.appsec'] should be a decoded object");
        };
        assert_eq!(appsec.get("enabled"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_drops_malformed_meta_struct_leaf() {
        // 0xC1 is the "never-used" msgpack format byte — guaranteed to fail.
        let mut meta_struct = ObjectMap::new();
        meta_struct.insert("_dd.bad".into(), Value::Bytes(vec![0xC1u8].into()));
        let trace = run(|t| {
            t.insert("meta_struct", Value::Object(meta_struct));
        });
        if let Some(Value::Object(custom)) = trace.get("custom") {
            assert!(custom.get("_dd.bad").is_none());
        }
    }

    #[test]
    fn test_drops_start_timestamp_field() {
        // `start` (Vector DateTime) is already extracted into `start_time`
        // by the timestamp normalizer; the leftover should not pollute the
        // indexed doc.
        let trace = run(|t| {
            t.insert("start", Utc.timestamp_nanos(1_724_060_143_000_000_000));
        });
        assert!(trace.get("start").is_none());
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
