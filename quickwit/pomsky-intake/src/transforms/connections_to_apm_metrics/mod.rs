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

//! Extract APM metrics (USM-style) from agent CollectorConnections payloads.
//!
//! Receives Log events carrying the decoded CollectorConnections protobuf
//! bytes from the `connections` source, extracts per-connection protocol
//! stats (HTTP, HTTP/2, gRPC, Kafka, Postgres, Redis), resolves service
//! names, and emits `universal.*` distribution + count metrics plus the
//! `trace.services_by_operation` family for service list discovery.
//!
//! Reference implementation: dd-source `domains/quickhouse/apps/byoc-usm-stats/`
//! (Go, branch `eyal.brami/byoc-usm-stats`).

mod aggregator;
mod emit;
mod parser;
mod resolver;
mod sketch;
mod types;

use chrono::{DateTime, Utc};
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, LogEvent, Value};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

use crate::protos::process::CollectorConnections;
use crate::sources::connections::{CONNECTIONS_PROTO_FIELD, CONNECTIONS_TIMESTAMP_FIELD};

/// Safety cap on connections per single CollectorConnections payload.
///
/// Each connection can spawn tens of `ProtoStat`s (one per endpoint per
/// status code per protocol) and each ProtoStat carries owned strings +
/// optional sketch bytes. A payload with millions of connections would OOM
/// the intake before we ever hit the aggregator. A misbehaving or malicious
/// agent shouldn't be able to take down pomsky-intake with one POST.
///
/// The bound is generous: a healthy agent reports ~500-5000 connections
/// every 30 s. One million is ~200× the 99th-percentile real payload.
const MAX_CONNECTIONS_PER_PAYLOAD: usize = 1_000_000;

/// Processes agent CollectorConnections payloads and emits APM metrics.
///
/// Input: Log events carrying decoded CollectorConnections protobuf bytes
/// (from the `connections` source).
///
/// Output: Metric events — `universal.<proto>.<dir>.hits` (counts),
/// `universal.<proto>.<dir>` (distribution sketches), and the
/// `trace.services_by_operation` metric family.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsToApmMetricsConfig;

impl vector_lib::configurable::NamedComponent for ConnectionsToApmMetricsConfig {
    fn get_component_name(&self) -> &'static str {
        "connections_to_apm_metrics"
    }
}

impl GenerateConfig for ConnectionsToApmMetricsConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "connections_to_apm_metrics")]
impl TransformConfig for ConnectionsToApmMetricsConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(ConnectionsToApmMetrics))
    }

    fn input(&self) -> Input {
        // Log events carrying CollectorConnections protobuf bytes
        // (envelope/zstd already handled by the `connections` source).
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        // Metric events (universal.* counts + distribution sketches,
        // plus the trace.services_by_operation family).
        vec![TransformOutput::new(
            DataType::Metric,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct ConnectionsToApmMetrics;

impl FunctionTransform for ConnectionsToApmMetrics {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let Event::Log(log) = &event else {
            // Vector's config validator routes only Log events here (see
            // `input() -> Input::log()` above), so anything else is a bug.
            debug_assert!(false, "non-log event reached connections_to_apm_metrics");
            warn!("dropping non-log event; input validator should have filtered it");
            return;
        };

        let bytes = match log.get(CONNECTIONS_PROTO_FIELD) {
            Some(Value::Bytes(b)) => b.clone(),
            _ => {
                warn!("log event missing connections_proto field; dropping");
                return;
            }
        };

        let ts = read_timestamp(log);

        let mut cc = match CollectorConnections::decode(bytes.as_ref()) {
            Ok(cc) => cc,
            Err(err) => {
                warn!(%err, "failed to decode CollectorConnections; dropping");
                return;
            }
        };

        if cc.connections.len() > MAX_CONNECTIONS_PER_PAYLOAD {
            warn!(
                host = %cc.host_name,
                connections = cc.connections.len(),
                cap = MAX_CONNECTIONS_PER_PAYLOAD,
                "connections payload exceeds cap, truncating"
            );
            cc.connections.truncate(MAX_CONNECTIONS_PER_PAYLOAD);
        }

        debug!(
            host = %cc.host_name,
            connections = cc.connections.len(),
            "decoded connections payload"
        );

        let usm_stats = parser::extract_usm_stats(&mut cc);
        let buckets = aggregator::aggregate(&usm_stats);
        emit::emit_all(&cc.host_name, ts, &buckets, output);
    }
}

/// Reads the agent-supplied timestamp from the log event. Returns `None`
/// when the `connections` source did not find a timestamp on the envelope.
fn read_timestamp(log: &LogEvent) -> Option<DateTime<Utc>> {
    let secs = match log.get(CONNECTIONS_TIMESTAMP_FIELD) {
        Some(Value::Integer(n)) => *n,
        _ => return None,
    };
    DateTime::<Utc>::from_timestamp(secs, 0)
}

#[cfg(test)]
mod tests {
    use vector::event::{LogEvent, MetricValue};

    use super::*;

    /// Smoke test: feed the entire `dump_1775477179` production capture through
    /// the transform and report aggregate stats. Marked `#[ignore]` so the
    /// regular test suite stays fast and self-contained; opt in with:
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1775477179 cargo test \
    ///         -p pomsky-intake --release --lib dump_smoke -- \
    ///         --ignored --nocapture
    ///
    /// The test fails only on hard errors (panic, file missing). Per-payload
    /// decode failures are counted and reported but do NOT fail the test —
    /// the dump may legitimately contain non-CollectorConnections messages
    /// (matches the Go sidecar's `decoder.IterateDumpFile` behaviour).
    #[test]
    #[ignore]
    fn dump_smoke_runs_transform_across_full_dump() {
        use std::collections::HashMap;
        use std::path::PathBuf;

        use prost::Message as ProstMessage;
        use vector::transforms::OutputBuffer;

        use crate::sources::connections::{
            CONNECTIONS_PROTO_FIELD, CONNECTIONS_TIMESTAMP_FIELD, decode_envelope,
        };

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        let dump_bytes = bytes::Bytes::from(
            std::fs::read(&dump_path)
                .unwrap_or_else(|err| panic!("read dump {}: {err}", dump_path.display())),
        );
        eprintln!(
            "loaded dump {} ({} bytes)",
            dump_path.display(),
            dump_bytes.len()
        );

        let mut transform = ConnectionsToApmMetrics;
        let mut output = OutputBuffer::default();

        let mut total_messages = 0u64;
        let mut envelope_decode_errs = 0u64;
        let mut cc_decode_errs = 0u64;
        let mut cc_decoded = 0u64;
        let mut empty_payloads = 0u64;
        let mut total_connections = 0u64;
        let mut payloads_with_aggregations = 0u64;
        let mut emitted_total = 0u64;
        let mut by_metric_name: HashMap<String, u64> = HashMap::new();

        let mut offset = 0usize;
        while offset + 8 <= dump_bytes.len() {
            let len_bytes: [u8; 8] = dump_bytes[offset..offset + 8]
                .try_into()
                .expect("8-byte slice");
            let msg_len = u64::from_le_bytes(len_bytes) as usize;
            offset += 8;
            if msg_len == 0 || offset + msg_len > dump_bytes.len() {
                eprintln!(
                    "dump iterator: invalid msg_len={msg_len} at offset {} — aborting",
                    offset - 8
                );
                break;
            }
            let msg = dump_bytes.slice(offset..offset + msg_len);
            offset += msg_len;
            total_messages += 1;

            let (proto_bytes, ts) = match decode_envelope(&msg) {
                Ok(t) => t,
                Err(_) => {
                    envelope_decode_errs += 1;
                    continue;
                }
            };

            // Sanity-check the bytes parse as CollectorConnections before
            // handing to the transform (transforms log+drop on parse failure
            // anyway, but we want to count them as a separate bucket).
            let cc = match CollectorConnections::decode(proto_bytes.as_ref()) {
                Ok(cc) => cc,
                Err(_) => {
                    cc_decode_errs += 1;
                    continue;
                }
            };
            cc_decoded += 1;
            if cc.connections.is_empty() {
                empty_payloads += 1;
            }
            total_connections += cc.connections.len() as u64;
            let has_any_aggs = cc.connections.iter().any(|c| {
                !c.http_aggregations.is_empty()
                    || !c.http2_aggregations.is_empty()
                    || !c.data_streams_aggregations.is_empty()
                    || !c.database_aggregations.is_empty()
            });
            if has_any_aggs {
                payloads_with_aggregations += 1;
            }

            let mut log = LogEvent::default();
            log.insert(CONNECTIONS_PROTO_FIELD, proto_bytes);
            if let Some(secs) = ts {
                log.insert(CONNECTIONS_TIMESTAMP_FIELD, secs);
            }

            transform.transform(&mut output, Event::Log(log));

            for event in output.drain() {
                if let Event::Metric(m) = event {
                    emitted_total += 1;
                    *by_metric_name.entry(m.name().to_string()).or_default() += 1;
                }
            }

            if total_messages.is_multiple_of(1000) {
                eprintln!("  progress: {total_messages} messages | {emitted_total} metric events");
            }
        }

        eprintln!("=== dump smoke summary ===");
        eprintln!("total messages         : {total_messages}");
        eprintln!("envelope decode errors : {envelope_decode_errs}");
        eprintln!("cc decode errors       : {cc_decode_errs}");
        eprintln!("cc decoded ok          : {cc_decoded}");
        eprintln!("  empty (no conns)     : {empty_payloads}");
        eprintln!("  with USM aggregations: {payloads_with_aggregations}");
        eprintln!("  total connections    : {total_connections}");
        eprintln!("emitted metric events  : {emitted_total}");
        eprintln!("--- by metric name ---");
        let mut by_name: Vec<_> = by_metric_name.iter().collect();
        by_name.sort_by_key(|(name, _)| name.to_string());
        for (name, count) in by_name {
            eprintln!("  {count:>8}  {name}");
        }
    }

    /// Writes the full Rust transform output for the pinned dump to
    /// `USM_RUST_NDJSON` (default `/tmp/rust-reference.ndjson`) in the same
    /// NDJSON schema as the Go sidecar's `EMIT_MODE=jsonall`. Opt in with:
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1775477179 cargo test \
    ///         -p pomsky-intake --release --lib dump_equivalence_write_ndjson \
    ///         -- --ignored --nocapture
    ///
    /// The output is then diffable against `/tmp/go-reference.ndjson`
    /// produced by `EMIT_MODE=jsonall go run -tags=dynamic .`.
    #[test]
    #[ignore]
    fn dump_equivalence_write_ndjson() {
        use std::io::{BufWriter, Write};
        use std::path::PathBuf;

        use prost::Message as ProstMessage;
        use serde_json::{Map, Value as JsonValue, json};
        use vector::transforms::OutputBuffer;
        use vector_lib::event::metric::MetricSketch;

        use crate::sources::connections::{
            CONNECTIONS_PROTO_FIELD, CONNECTIONS_TIMESTAMP_FIELD, decode_envelope,
        };

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        let out_path: PathBuf = std::env::var_os("USM_RUST_NDJSON")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/rust-reference.ndjson"));

        let dump_bytes = bytes::Bytes::from(
            std::fs::read(&dump_path)
                .unwrap_or_else(|err| panic!("read dump {}: {err}", dump_path.display())),
        );
        let out_file = std::fs::File::create(&out_path)
            .unwrap_or_else(|err| panic!("create {}: {err}", out_path.display()));
        let mut writer = BufWriter::new(out_file);

        eprintln!(
            "rust ndjson: dump={} ({} bytes) → out={}",
            dump_path.display(),
            dump_bytes.len(),
            out_path.display()
        );

        let mut transform = ConnectionsToApmMetrics;
        let mut output = OutputBuffer::default();
        let mut lines_written = 0u64;
        let mut total_messages = 0u64;
        let mut offset = 0usize;

        while offset + 8 <= dump_bytes.len() {
            let len_bytes: [u8; 8] = dump_bytes[offset..offset + 8]
                .try_into()
                .expect("8-byte slice");
            let msg_len = u64::from_le_bytes(len_bytes) as usize;
            offset += 8;
            if msg_len == 0 || offset + msg_len > dump_bytes.len() {
                break;
            }
            let msg = dump_bytes.slice(offset..offset + msg_len);
            offset += msg_len;
            total_messages += 1;

            let (proto_bytes, ts) = match decode_envelope(&msg) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if CollectorConnections::decode(proto_bytes.as_ref()).is_err() {
                continue;
            }

            let mut log = LogEvent::default();
            log.insert(CONNECTIONS_PROTO_FIELD, proto_bytes);
            if let Some(secs) = ts {
                log.insert(CONNECTIONS_TIMESTAMP_FIELD, secs);
            }

            transform.transform(&mut output, Event::Log(log));

            for event in output.drain() {
                let Event::Metric(metric) = event else {
                    continue;
                };
                let ts = metric
                    .timestamp()
                    .map(|t| t.timestamp())
                    .unwrap_or_default();
                let (host, tags) = extract_host_and_tags(metric.tags());
                let record = match metric.value() {
                    MetricValue::Counter { value } => json!({
                        "metric": metric.name(),
                        "type": "counter",
                        "timestamp": ts,
                        "host": host,
                        "tags": tags,
                        "value": *value,
                    }),
                    MetricValue::Sketch {
                        sketch: MetricSketch::AgentDDSketch(sk),
                    } => {
                        let bin_map = sk.bin_map();
                        let keys: Vec<JsonValue> = bin_map
                            .keys
                            .iter()
                            .map(|k| JsonValue::from(i32::from(*k)))
                            .collect();
                        let counts: Vec<JsonValue> = bin_map
                            .counts
                            .iter()
                            .map(|n| JsonValue::from(u32::from(*n)))
                            .collect();
                        let mut sketch_obj = Map::new();
                        sketch_obj.insert("count".into(), JsonValue::from(sk.count()));
                        sketch_obj.insert("sum".into(), JsonValue::from(sk.sum().unwrap_or(0.0)));
                        sketch_obj.insert("min".into(), JsonValue::from(sk.min().unwrap_or(0.0)));
                        sketch_obj.insert("max".into(), JsonValue::from(sk.max().unwrap_or(0.0)));
                        sketch_obj.insert("avg".into(), JsonValue::from(sk.avg().unwrap_or(0.0)));
                        sketch_obj.insert("keys".into(), JsonValue::Array(keys));
                        sketch_obj.insert("counts".into(), JsonValue::Array(counts));
                        json!({
                            "metric": metric.name(),
                            "type": "sketch",
                            "timestamp": ts,
                            "host": host,
                            "tags": tags,
                            "sketch": JsonValue::Object(sketch_obj),
                        })
                    }
                    _ => continue,
                };
                serde_json::to_writer(&mut writer, &record).expect("json write");
                writer.write_all(b"\n").expect("newline");
                lines_written += 1;
            }

            if total_messages.is_multiple_of(1000) {
                eprintln!("  progress: {total_messages} messages | {lines_written} lines");
            }
        }
        writer.flush().expect("flush");

        eprintln!(
            "rust ndjson done: {lines_written} lines ({} messages processed)",
            total_messages
        );
    }

    fn extract_host_and_tags(tags: Option<&vector::event::MetricTags>) -> (String, Vec<String>) {
        let Some(tags) = tags else {
            return (String::new(), Vec::new());
        };
        let mut host = String::new();
        let mut out: Vec<String> = Vec::new();
        for (k, v) in tags.iter_single() {
            if k == "host" {
                host = v.to_string();
            } else {
                out.push(format!("{k}:{v}"));
            }
        }
        out.sort();
        (host, out)
    }
}
