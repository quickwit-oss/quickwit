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

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, ObjectMap, TraceEvent, Value};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

/// Preprocesses Datadog agent trace chunks before they are exploded into
/// individual spans. Propagates chunk-level fields onto each span so they
/// survive `explode_trace_spans`, and canonicalizes each span into the
/// apm-processing-aligned shape (single i64-ns `start`, msgpack-decoded
/// `meta_struct` leaves) so downstream span-level processors see the same
/// paths the Java pipeline operates on.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreprocessDdTraceConfig;

impl vector_lib::configurable::NamedComponent for PreprocessDdTraceConfig {
    fn get_component_name(&self) -> &'static str {
        "preprocess_dd_trace"
    }
}

impl GenerateConfig for PreprocessDdTraceConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "preprocess_dd_trace")]
impl TransformConfig for PreprocessDdTraceConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(PreprocessDdTrace))
    }

    fn input(&self) -> Input {
        Input::trace()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Trace,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PreprocessDdTrace;

impl FunctionTransform for PreprocessDdTrace {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if let Event::Trace(trace) = &mut event {
            propagate_chunk_meta(trace);
            canonicalize_spans(trace);
        }
        output.push(event);
    }
}

/// Mirrors `SpansV07Parser.populateSpanTags` in logs-backend: copies the
/// chunk-event's `host` and `env` into each span's `meta` map under
/// `_dd.hostname` and `env` respectively. Existing keys are preserved so
/// per-span overrides win. Without this, both fall off when
/// `explode_trace_spans` strips chunk-level fields.
fn propagate_chunk_meta(trace: &mut TraceEvent) {
    // Cheap pre-check so we don't clone host/env when there are no spans.
    if !matches!(trace.get("spans"), Some(Value::Array(_))) {
        return;
    }
    let host_opt = trace.get("host").cloned();
    let env_opt = trace.get("env").cloned();
    let Some(Value::Array(spans)) = trace.get_mut("spans") else {
        return;
    };
    for span in spans {
        let Value::Object(span_fields) = span else {
            continue;
        };
        ensure_meta(span_fields, "_dd.hostname", host_opt.as_ref());
        ensure_meta(span_fields, "env", env_opt.as_ref());
    }
}

fn ensure_meta(span_fields: &mut ObjectMap, key: &str, value_opt: Option<&Value>) {
    let Some(value) = value_opt else {
        return;
    };
    let meta_value = span_fields
        .entry("meta".into())
        .or_insert_with(|| Value::Object(ObjectMap::new()));
    let Value::Object(meta) = meta_value else {
        return;
    };
    if !meta.contains_key(key) {
        meta.insert(key.into(), value.clone());
    }
}

/// Canonicalizes each span in `.spans` to match the shape of the Java
/// apm-processing `SpanMap`: IDs as raw i64 (Java `long`), `start` as a
/// single i64 unix-ns value, `meta_struct` leaves msgpack-decoded.
/// `meta`/`metrics` are intentionally left flat with literal dotted keys
/// (legacy — does not match Java's nested layout).
fn canonicalize_spans(trace: &mut TraceEvent) {
    let Some(Value::Array(spans)) = trace.get_mut("spans") else {
        return;
    };
    for span in spans {
        let Value::Object(span_fields) = span else {
            continue;
        };
        canonicalize_start(span_fields);
        canonicalize_meta_struct(span_fields);
    }
}

/// Replaces `start` (Vector `DateTime` from the agent wire) with a single
/// i64 unix-ns value, matching the Java parser's `long ns` representation.
fn canonicalize_start(span_fields: &mut ObjectMap) {
    let Some(Value::Timestamp(dt)) = span_fields.get("start") else {
        return;
    };
    let Some(ns) = dt.timestamp_nanos_opt() else {
        return;
    };
    span_fields.insert("start".into(), Value::Integer(ns));
}

/// Decodes each `meta_struct` leaf (msgpack `Value::Bytes`) into a structured
/// Vector `Value`. Mirrors `SpansProtobufPayloadParser.decodeMetaStructValue`
/// in logs-backend: a malformed leaf is dropped, not failed. Non-`Bytes`
/// leaves are passed through unchanged.
fn canonicalize_meta_struct(span_fields: &mut ObjectMap) {
    let Some(Value::Object(meta_struct)) = span_fields.get_mut("meta_struct") else {
        return;
    };
    meta_struct.retain(|_k, v| match v {
        Value::Bytes(bytes) => match rmp_serde::from_slice::<Value>(bytes) {
            Ok(decoded) => {
                *v = decoded;
                true
            }
            Err(_) => false,
        },
        _ => true,
    });
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn run(span: ObjectMap) -> ObjectMap {
        let mut trace = TraceEvent::default();
        trace.insert("spans", Value::Array(vec![Value::Object(span)]));
        canonicalize_spans(&mut trace);
        let Some(Value::Array(mut spans)) = trace.remove("spans") else {
            panic!("spans missing");
        };
        let Some(Value::Object(span)) = spans.pop() else {
            panic!("span missing");
        };
        span
    }

    #[test]
    fn test_leaves_ids_as_i64() {
        // Canonical IDs match Java SpanMap's `long`; stringification happens
        // in span_to_schema, not here.
        let mut span = ObjectMap::new();
        span.insert("trace_id".into(), Value::Integer(0xbc614e));
        span.insert("span_id".into(), Value::Integer(0xdeadbeef));
        span.insert("parent_id".into(), Value::Integer(-1));
        let span = run(span);
        assert_eq!(span.get("trace_id"), Some(&Value::Integer(0xbc614e)));
        assert_eq!(span.get("span_id"), Some(&Value::Integer(0xdeadbeef)));
        assert_eq!(span.get("parent_id"), Some(&Value::Integer(-1)));
    }

    #[test]
    fn test_canonicalizes_start_to_unix_ns() {
        let mut span = ObjectMap::new();
        span.insert(
            "start".into(),
            Value::Timestamp(Utc.timestamp_nanos(1_724_060_143_000_000_000)),
        );
        let span = run(span);
        assert_eq!(
            span.get("start"),
            Some(&Value::Integer(1_724_060_143_000_000_000)),
        );
    }

    #[test]
    fn test_decodes_meta_struct_leaves() {
        let payload = serde_json::json!({"enabled": 1, "rules": ["a", "b"]});
        let bytes = rmp_serde::to_vec(&payload).expect("encode msgpack");
        let mut meta_struct = ObjectMap::new();
        meta_struct.insert("_dd.appsec".into(), Value::Bytes(bytes.into()));
        let mut span = ObjectMap::new();
        span.insert("meta_struct".into(), Value::Object(meta_struct));
        let span = run(span);
        let Some(Value::Object(meta_struct)) = span.get("meta_struct") else {
            panic!("meta_struct missing");
        };
        let Some(Value::Object(appsec)) = meta_struct.get("_dd.appsec") else {
            panic!("_dd.appsec should be decoded into an object");
        };
        assert_eq!(appsec.get("enabled"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_drops_malformed_meta_struct_leaf() {
        // 0xC1 is the "never-used" msgpack format byte — guaranteed to fail.
        let mut meta_struct = ObjectMap::new();
        meta_struct.insert("_dd.bad".into(), Value::Bytes(vec![0xC1u8].into()));
        let mut span = ObjectMap::new();
        span.insert("meta_struct".into(), Value::Object(meta_struct));
        let span = run(span);
        let Some(Value::Object(meta_struct)) = span.get("meta_struct") else {
            panic!("meta_struct missing");
        };
        assert!(meta_struct.get("_dd.bad").is_none());
    }

    #[test]
    fn test_propagates_host_and_env_into_span_meta() {
        let span = ObjectMap::new();
        let mut trace = TraceEvent::default();
        trace.insert("host", "host-1");
        trace.insert("env", "prod");
        trace.insert("spans", Value::Array(vec![Value::Object(span)]));
        let mut t = PreprocessDdTrace;
        let mut out = OutputBuffer::with_capacity(1);
        t.transform(&mut out, Event::Trace(trace));
        let events: Vec<_> = out.into_events().collect();
        let trace = events[0].as_trace();
        let Some(Value::Array(spans)) = trace.get("spans") else {
            panic!("spans missing");
        };
        let Value::Object(span) = &spans[0] else {
            panic!("span missing");
        };
        let Value::Object(meta) = span.get("meta").expect("meta") else {
            panic!("meta should be object");
        };
        assert_eq!(meta.get("_dd.hostname"), Some(&Value::from("host-1")));
        assert_eq!(meta.get("env"), Some(&Value::from("prod")));
    }
}
