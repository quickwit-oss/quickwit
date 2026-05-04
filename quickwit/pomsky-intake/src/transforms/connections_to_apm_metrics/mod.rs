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

    /// Prototype container-tag enrichment measurement. Answers: if we built a
    /// CSV of `(container_id → best service name ever observed)` from every
    /// container the agent ever tagged with a real service anywhere in the
    /// dump, how many `service:container:<id>` or `service:<hostname>`
    /// fallbacks could the CSV cure?
    ///
    /// This is the upper bound of what Alan's CSV-enrichment pattern could
    /// resolve on this dump using only the dump's own data as reference.
    /// In production the CSV would be populated by a K8s-watching sidecar.
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1775477179 cargo test \
    ///         -p pomsky-intake --release --lib dump_container_enrichment_probe \
    ///         -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_container_enrichment_probe() {
        use std::collections::HashMap;
        use std::path::PathBuf;

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        eprintln!("reading {}...", dump_path.display());
        let dump_bytes = bytes::Bytes::from(std::fs::read(&dump_path).expect("read dump"));
        eprintln!("read {} bytes", dump_bytes.len());

        // Two streaming passes — keeps memory O(unique containers + unique hosts)
        // regardless of dump size, so the big (multi-GB) dumps work.

        // Pass 1: walk the dump, collect:
        //   * container_id prefix → first real service name seen for it
        //   * host → { service → count } (to derive unambiguous host mapping)
        let mut container_real_service: HashMap<String, String> = HashMap::new();
        let mut host_service_counts: HashMap<String, HashMap<String, u32>> = HashMap::new();
        let mut pass1_payloads = 0u64;
        iterate_dump(&dump_bytes, |cc| {
            pass1_payloads += 1;
            if pass1_payloads.is_multiple_of(10_000) {
                eprintln!("  pass 1 progress: {pass1_payloads} payloads");
            }
            for conn in &cc.connections {
                let svc = super::resolver::resolve_service(cc, conn);
                let is_fallback = svc.starts_with("container:") || svc == cc.host_name;
                if is_fallback {
                    continue;
                }
                if let Some(prefix) = conn
                    .laddr
                    .as_ref()
                    .map(|a| a.container_id.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| &s[..s.len().min(12)])
                {
                    container_real_service
                        .entry(prefix.to_string())
                        .or_insert_with(|| svc.clone());
                }
                *host_service_counts
                    .entry(cc.host_name.clone())
                    .or_default()
                    .entry(svc)
                    .or_default() += 1;
            }
        });

        let unambiguous_host_service: HashMap<String, String> = host_service_counts
            .into_iter()
            .filter_map(|(h, mut svcs)| match svcs.len() {
                1 => Some((h, svcs.drain().next().unwrap().0)),
                _ => None,
            })
            .collect();

        eprintln!(
            "pass 1 done: {} container entries, {} unambiguous-host entries",
            container_real_service.len(),
            unambiguous_host_service.len()
        );

        // Pass 2: stream again, classify each connection directly into counters.
        let mut total = 0u64;
        let mut real_service = 0u64;
        let mut fallback_container = 0u64;
        let mut fallback_hostname = 0u64;
        let mut container_fb_cured_by_cid = 0u64;
        let mut container_fb_cured_by_host_fallback = 0u64;
        let mut container_fb_genuinely_unresolvable = 0u64;
        let mut host_fb_cured = 0u64;
        let mut host_fb_genuinely_unresolvable = 0u64;
        let mut pass2_payloads = 0u64;

        iterate_dump(&dump_bytes, |cc| {
            pass2_payloads += 1;
            if pass2_payloads.is_multiple_of(10_000) {
                eprintln!("  pass 2 progress: {pass2_payloads} payloads");
            }
            for conn in &cc.connections {
                total += 1;
                let cid_prefix = conn
                    .laddr
                    .as_ref()
                    .map(|a| a.container_id.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s[..s.len().min(12)].to_string());

                let svc = super::resolver::resolve_service(cc, conn);

                if let Some(suffix) = svc.strip_prefix("container:") {
                    fallback_container += 1;
                    if container_real_service.contains_key(suffix) {
                        container_fb_cured_by_cid += 1;
                    } else if unambiguous_host_service.contains_key(&cc.host_name) {
                        container_fb_cured_by_host_fallback += 1;
                    } else {
                        container_fb_genuinely_unresolvable += 1;
                    }
                } else if svc == cc.host_name {
                    fallback_hostname += 1;
                    let cured_by_cid = cid_prefix
                        .as_ref()
                        .map(|p| container_real_service.contains_key(p))
                        .unwrap_or(false);
                    let cured_by_host = unambiguous_host_service.contains_key(&cc.host_name);
                    if cured_by_cid || cured_by_host {
                        host_fb_cured += 1;
                    } else {
                        host_fb_genuinely_unresolvable += 1;
                    }
                } else {
                    real_service += 1;
                }
            }
        });

        let total_fallback = fallback_container + fallback_hostname;
        let total_fb_cured =
            container_fb_cured_by_cid + container_fb_cured_by_host_fallback + host_fb_cured;

        eprintln!("=== container→service enrichment probe ===");
        eprintln!("observed connections          : {total}");
        eprintln!(
            "  real service tag            : {real_service} ({:.1}%)",
            pct(real_service, total)
        );
        eprintln!(
            "  fallback `service:container`: {fallback_container} ({:.1}%)",
            pct(fallback_container, total)
        );
        eprintln!(
            "  fallback `service:<host>`   : {fallback_hostname} ({:.1}%)",
            pct(fallback_hostname, total)
        );
        eprintln!();
        eprintln!(
            "container-id CSV entries     : {}",
            container_real_service.len()
        );
        eprintln!(
            "unambiguous-host CSV entries : {}",
            unambiguous_host_service.len()
        );
        eprintln!();
        if fallback_container > 0 {
            eprintln!("`service:container:` fallbacks:");
            eprintln!(
                "  cured by container-id CSV   : {container_fb_cured_by_cid} ({:.1}%)",
                pct(container_fb_cured_by_cid, fallback_container)
            );
            eprintln!(
                "  cured by unambiguous host   : {container_fb_cured_by_host_fallback} ({:.1}%)",
                pct(container_fb_cured_by_host_fallback, fallback_container)
            );
            eprintln!(
                "  genuinely unresolvable      : {container_fb_genuinely_unresolvable} ({:.1}%)",
                pct(container_fb_genuinely_unresolvable, fallback_container)
            );
        }
        if fallback_hostname > 0 {
            eprintln!("`service:<host>` fallbacks:");
            eprintln!(
                "  cured by some CSV           : {host_fb_cured} ({:.1}%)",
                pct(host_fb_cured, fallback_hostname)
            );
            eprintln!(
                "  genuinely unresolvable      : {host_fb_genuinely_unresolvable} ({:.1}%)",
                pct(host_fb_genuinely_unresolvable, fallback_hostname)
            );
        }
        eprintln!();
        eprintln!(
            "upper bound: CSV resolves {:.1}% of all fallbacks ({} of {})",
            pct(total_fb_cured, total_fallback),
            total_fb_cured,
            total_fallback,
        );
        eprintln!(
            "            leaving {:.1}% of connections still unresolved",
            pct(total_fallback - total_fb_cured, total)
        );
    }

    fn pct(num: u64, denom: u64) -> f64 {
        if denom == 0 {
            0.0
        } else {
            num as f64 * 100.0 / denom as f64
        }
    }

    /// For each USM-active connection where the resolver matches a priority
    /// tag, report WHICH (source, tag_name) matched. Tells us what fraction
    /// of the "resolved 20%" comes from process `service:` (DD_SERVICE) vs
    /// container tags that actually made it into the payload vs host tags.
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1776751591 cargo test \
    ///         -p pomsky-intake --release --lib dump_resolved_source_diag \
    ///         -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_resolved_source_diag() {
        use std::collections::HashMap;
        use std::path::PathBuf;

        use super::resolver::iterate_tags;

        #[derive(Clone, Copy, Debug)]
        enum Src {
            Process,
            Container,
            Host,
        }
        // Duplicated here intentionally — mirrors the priority list in
        // resolver.rs::SERVICE_CANDIDATES. Kept local so this probe can
        // evolve without touching the resolver.
        const CANDIDATES: &[(Src, &str)] = &[
            (Src::Process, "service"),
            (Src::Container, "service"),
            (Src::Process, "http.iis.subsite"),
            (Src::Process, "http.iis.app_pool"),
            (Src::Container, "app"),
            (Src::Container, "short_image"),
            (Src::Container, "kube_container_name"),
            (Src::Container, "container_name"),
            (Src::Container, "kube_deployment"),
            (Src::Container, "kube_service"),
            (Src::Host, "service"),
            (Src::Host, "app"),
            (Src::Process, "process_context"),
        ];

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        eprintln!("reading {}...", dump_path.display());
        let dump_bytes = bytes::Bytes::from(std::fs::read(&dump_path).expect("read dump"));

        let mut total_usm_active = 0u64;
        let mut resolved = 0u64;
        // (source_label, tag_name) -> count
        let mut wins: HashMap<(&'static str, &'static str), u64> = HashMap::new();
        let mut payloads = 0u64;

        iterate_dump(&dump_bytes, |cc| {
            payloads += 1;
            if payloads.is_multiple_of(50_000) {
                eprintln!("  {payloads} payloads");
            }
            for conn in &cc.connections {
                let has_usm = !conn.http_aggregations.is_empty()
                    || !conn.http2_aggregations.is_empty()
                    || !conn.data_streams_aggregations.is_empty()
                    || !conn.database_aggregations.is_empty();
                if !has_usm {
                    continue;
                }
                total_usm_active += 1;

                // Walk each source, note every tag that matches the priority
                // list, keep the lowest-priority-index winner.
                let mut best: Option<usize> = None;
                let mut check = |src: Src, buffer: &[u8], idx: i32| {
                    if idx < 0 {
                        return;
                    }
                    iterate_tags(buffer, idx, |tag| {
                        if let Some(colon) = tag.iter().position(|&b| b == b':') {
                            let name = &tag[..colon];
                            let value = &tag[colon + 1..];
                            if value.is_empty() {
                                return true;
                            }
                            for (i, (csrc, cname)) in CANDIDATES.iter().enumerate() {
                                if !matches!(
                                    (csrc, src),
                                    (Src::Process, Src::Process)
                                        | (Src::Container, Src::Container)
                                        | (Src::Host, Src::Host)
                                ) {
                                    continue;
                                }
                                if name == cname.as_bytes() {
                                    match best {
                                        Some(b) if b <= i => {}
                                        _ => best = Some(i),
                                    }
                                    break;
                                }
                            }
                            !matches!(best, Some(0))
                        } else {
                            true
                        }
                    });
                };
                if conn.tags_idx > 0 {
                    check(Src::Process, &cc.encoded_connections_tags, conn.tags_idx);
                }
                if conn.local_container_tags_index >= 0 {
                    check(
                        Src::Container,
                        &cc.encoded_tags,
                        conn.local_container_tags_index,
                    );
                }
                if cc.host_tags_index > 0 {
                    check(Src::Host, &cc.encoded_tags, cc.host_tags_index);
                }

                if let Some(idx) = best {
                    resolved += 1;
                    let (src, name) = CANDIDATES[idx];
                    let src_label = match src {
                        Src::Process => "process",
                        Src::Container => "container",
                        Src::Host => "host",
                    };
                    *wins.entry((src_label, name)).or_default() += 1;
                }
            }
        });

        eprintln!();
        eprintln!("=== resolved-by-source breakdown ===");
        eprintln!("total USM-active : {total_usm_active}");
        eprintln!(
            "resolved         : {resolved} ({:.1}%)",
            pct(resolved, total_usm_active)
        );
        eprintln!();
        eprintln!("winner (source:tag)                    count     % of resolved");
        let mut ranked: Vec<_> = wins.iter().collect();
        ranked.sort_by_key(|(_, c)| std::cmp::Reverse(**c));
        for ((src, tag), count) in ranked {
            eprintln!(
                "  {src:<10} {tag:<25}  {count:>10}   {:>5.1}%",
                pct(*count, resolved)
            );
        }
    }

    /// Streams the dump's `[u64 LE msg_len][msg]` frames, decodes envelope +
    /// `CollectorConnections`, invokes the callback per payload. Skips
    /// envelope / proto decode errors silently — matches the Go iterator.
    fn iterate_dump(dump_bytes: &bytes::Bytes, mut cb: impl FnMut(&CollectorConnections)) {
        use prost::Message as ProstMessage;

        use crate::sources::connections::decode_envelope;

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
            let Ok((proto_bytes, _ts)) = decode_envelope(&msg) else {
                continue;
            };
            let Ok(cc) = CollectorConnections::decode(proto_bytes.as_ref()) else {
                continue;
            };
            cb(&cc);
        }
    }

    /// Diagnostic probe: for every connection that HAD USM aggregations but
    /// where our resolver produced a fallback service (`container:<id>` or
    /// `<hostname>`), dump the *actual* tags present in each source buffer
    /// so we can see empirically why no candidate in the priority list matched.
    ///
    /// Rate-limited: up to `USM_DIAG_SAMPLES` examples (default 30) are
    /// logged in full; the rest are just counted by "pattern" (the
    /// concatenated sorted tag-key set at container+host scope) so we see
    /// which shapes are most common.
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1775477179 cargo test \
    ///         -p pomsky-intake --release --lib dump_fallback_tag_diag \
    ///         -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_fallback_tag_diag() {
        use std::collections::HashMap;
        use std::path::PathBuf;

        use prost::Message as ProstMessage;

        use super::resolver::iterate_tags;
        use crate::sources::connections::decode_envelope;

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        let sample_limit: usize = std::env::var("USM_DIAG_SAMPLES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let dump_bytes = bytes::Bytes::from(std::fs::read(&dump_path).expect("read dump"));

        // Histogram over the "container+host tag-key shape" so we see which
        // tag sets the agent is emitting (if any) when we fall back.
        let mut shape_counts: HashMap<String, u64> = HashMap::new();
        let mut samples_dumped = 0usize;
        let mut total_usm_active = 0u64;
        let mut total_resolved = 0u64;
        let mut total_fallback_with_usm = 0u64;
        let mut total_no_tags = 0u64;
        let mut no_tags_with_container_id = 0u64;
        let mut no_tags_no_container_id = 0u64;
        let mut no_tags_no_cid_but_pid_map = 0u64;

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

            let Ok((proto_bytes, _ts)) = decode_envelope(&msg) else {
                continue;
            };
            let Ok(cc) = CollectorConnections::decode(proto_bytes.as_ref()) else {
                continue;
            };

            for conn in &cc.connections {
                let has_usm = !conn.http_aggregations.is_empty()
                    || !conn.http2_aggregations.is_empty()
                    || !conn.data_streams_aggregations.is_empty()
                    || !conn.database_aggregations.is_empty();
                if !has_usm {
                    continue;
                }

                total_usm_active += 1;

                let svc = super::resolver::resolve_service(&cc, conn);
                let is_fallback = svc.starts_with("container:") || svc == cc.host_name;
                if !is_fallback {
                    total_resolved += 1;
                    continue;
                }
                total_fallback_with_usm += 1;

                // Harvest the ACTUAL tags present at each source.
                let mut process_tags: Vec<String> = Vec::new();
                if conn.tags_idx > 0 {
                    iterate_tags(&cc.encoded_connections_tags, conn.tags_idx, |t| {
                        if let Ok(s) = std::str::from_utf8(t) {
                            process_tags.push(s.to_string());
                        }
                        true
                    });
                }
                let mut container_tags: Vec<String> = Vec::new();
                if conn.local_container_tags_index >= 0 {
                    iterate_tags(&cc.encoded_tags, conn.local_container_tags_index, |t| {
                        if let Ok(s) = std::str::from_utf8(t) {
                            container_tags.push(s.to_string());
                        }
                        true
                    });
                }
                let mut host_tags: Vec<String> = Vec::new();
                if cc.host_tags_index > 0 {
                    iterate_tags(&cc.encoded_tags, cc.host_tags_index, |t| {
                        if let Ok(s) = std::str::from_utf8(t) {
                            host_tags.push(s.to_string());
                        }
                        true
                    });
                }

                if process_tags.is_empty() && container_tags.is_empty() && host_tags.is_empty() {
                    total_no_tags += 1;
                    let laddr_cid = conn
                        .laddr
                        .as_ref()
                        .map(|a| a.container_id.as_str())
                        .unwrap_or("");
                    let has_laddr_cid = !laddr_cid.is_empty();
                    let has_pid_cid = cc.container_for_pid.contains_key(&conn.pid);
                    if has_laddr_cid {
                        no_tags_with_container_id += 1;
                    } else if has_pid_cid {
                        no_tags_no_cid_but_pid_map += 1;
                    } else {
                        no_tags_no_container_id += 1;
                    }
                }

                // Build a "shape" signature from the TAG KEYS we saw at
                // container+host scope — irrespective of values. This tells
                // us "which combinations of keys the agent shipped."
                let mut keys: Vec<String> = container_tags
                    .iter()
                    .chain(host_tags.iter())
                    .filter_map(|t| t.split_once(':').map(|(k, _)| k.to_string()))
                    .collect();
                keys.sort();
                keys.dedup();
                let shape = if keys.is_empty() {
                    "<no container/host tags>".to_string()
                } else {
                    keys.join(",")
                };
                *shape_counts.entry(shape).or_default() += 1;

                if samples_dumped < sample_limit {
                    samples_dumped += 1;
                    eprintln!("--- fallback sample #{} ---", samples_dumped);
                    eprintln!("  resolved: {svc}");
                    eprintln!("  host: {}", cc.host_name);
                    eprintln!(
                        "  container_id: {}",
                        conn.laddr
                            .as_ref()
                            .map(|a| a.container_id.as_str())
                            .unwrap_or("")
                    );
                    eprintln!(
                        "  process tags ({}): {:?}",
                        process_tags.len(),
                        process_tags
                    );
                    eprintln!(
                        "  container tags ({}): {:?}",
                        container_tags.len(),
                        container_tags
                    );
                    eprintln!("  host tags ({}): {:?}", host_tags.len(), host_tags);
                }
            }
        }

        let tags_but_no_match = total_fallback_with_usm - total_no_tags;

        eprintln!();
        eprintln!("=== USM-active connections breakdown ===");
        eprintln!("total USM-active               : {total_usm_active}");
        eprintln!(
            "  resolved (real service tag)  : {} ({:.1}%)",
            total_resolved,
            pct(total_resolved, total_usm_active)
        );
        eprintln!(
            "  fallback                     : {} ({:.1}%)",
            total_fallback_with_usm,
            pct(total_fallback_with_usm, total_usm_active)
        );
        eprintln!();
        eprintln!("=== breakdown of fallback (by root cause) ===");
        eprintln!("(percentages against TOTAL USM-active, not against fallback subset)");
        eprintln!();
        eprintln!(
            "container w/ cid, 0 tags shipped : {} ({:.1}%)",
            no_tags_with_container_id,
            pct(no_tags_with_container_id, total_usm_active)
        );
        eprintln!(
            "host process, no container_id    : {} ({:.1}%)",
            no_tags_no_container_id,
            pct(no_tags_no_container_id, total_usm_active)
        );
        eprintln!(
            "no cid, pid-map hit              : {} ({:.1}%)",
            no_tags_no_cid_but_pid_map,
            pct(no_tags_no_cid_but_pid_map, total_usm_active)
        );
        eprintln!(
            "tags present, none match list    : {} ({:.1}%)",
            tags_but_no_match,
            pct(tags_but_no_match, total_usm_active)
        );
        eprintln!();
        eprintln!("--- top 20 tag-key shapes ---");
        let mut ranked: Vec<_> = shape_counts.iter().collect();
        ranked.sort_by_key(|(_, c)| std::cmp::Reverse(**c));
        for (shape, count) in ranked.iter().take(20) {
            eprintln!(
                "  {count:>8}  {:.1}%  {shape}",
                pct(**count, total_fallback_with_usm)
            );
        }
    }

    /// Measures per-payload compressed body size, decompressed CollectorConnections
    /// size, and decode_envelope wall-clock latency. Used to decide whether the
    /// source's sync zstd path should stay inline or move to `spawn_blocking`.
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1775477179 cargo test \
    ///         -p pomsky-intake --release --lib dump_payload_size_probe \
    ///         -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_payload_size_probe() {
        use std::path::PathBuf;
        use std::time::Instant;

        use crate::sources::connections::decode_envelope;

        let dump_path: PathBuf = std::env::var_os("USM_DUMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").expect("HOME unset");
                PathBuf::from(home).join("Downloads/dump_1775477179")
            });
        let dump_bytes = bytes::Bytes::from(std::fs::read(&dump_path).expect("read dump"));

        let mut compressed_sizes: Vec<usize> = Vec::new();
        let mut decompressed_sizes: Vec<usize> = Vec::new();
        let mut decode_micros: Vec<u64> = Vec::new();

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

            // Compressed-body size (what we'd pass to zstd). Env version +
            // header length = first few bytes; the rest is the compressed body.
            // We compute it cheaply by calling decode_envelope and trusting
            // the input slice arithmetic approximately.
            let compressed_est = msg.len().saturating_sub(3); // V8 prefix; slightly off for V3-V7 but dominant path is V8

            let t0 = Instant::now();
            let Ok((proto_bytes, _ts)) = decode_envelope(&msg) else {
                continue;
            };
            let elapsed_us = t0.elapsed().as_micros() as u64;

            compressed_sizes.push(compressed_est);
            decompressed_sizes.push(proto_bytes.len());
            decode_micros.push(elapsed_us);
        }

        fn stats(name: &str, v: &mut [u64]) {
            if v.is_empty() {
                eprintln!("{name}: no data");
                return;
            }
            v.sort_unstable();
            let n = v.len();
            let p = |q: f64| -> u64 { v[((n as f64 - 1.0) * q) as usize] };
            let sum: u64 = v.iter().sum();
            let avg = sum / n as u64;
            eprintln!(
                "{name:<16}: n={n} min={} p50={} p90={} p99={} max={} avg={avg}",
                v[0],
                p(0.50),
                p(0.90),
                p(0.99),
                v[n - 1]
            );
        }

        let mut comp: Vec<u64> = compressed_sizes.iter().map(|x| *x as u64).collect();
        let mut decomp: Vec<u64> = decompressed_sizes.iter().map(|x| *x as u64).collect();
        eprintln!("=== dump payload size probe ===");
        stats("compressed_B", &mut comp);
        stats("decompressed_B", &mut decomp);
        stats("decode_us", &mut decode_micros);
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

    /// Estimates the wire-payload cost of `expected_tags_duration` being on:
    /// for each `CollectorConnections` message in the dump, encode the proto
    /// twice — once as-is (knob ON, container+host tags packed) and once with
    /// just those tag fields stripped (knob OFF). Compress both with zstd
    /// (level 3, the default the agent uses) and report the deltas.
    ///
    /// Run with a "knob ON" dump (e.g. captured from a host where
    /// `system_probe_config.expected_tags_duration` is set high). The output
    /// answers the BYOC bandwidth question: "how much extra does packing
    /// container+host tags inline cost on the wire?".
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1777294025 cargo test \
    ///         -p pomsky-intake --release --lib dump_tag_packing_size_probe -- \
    ///         --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_tag_packing_size_probe() {
        use std::io::Read;
        use std::path::PathBuf;

        use prost::Message as _;

        use crate::protos::process::CollectorConnections;
        use crate::sources::connections::decode_envelope;

        let path = std::env::var("USM_DUMP_PATH")
            .map(PathBuf::from)
            .expect("set USM_DUMP_PATH");

        // zstd compression level matching the agent default.
        const ZSTD_LEVEL: i32 = 3;

        let mut file = std::fs::File::open(&path).expect("open dump");

        let mut messages: u64 = 0;
        let mut connections: u64 = 0;
        let mut had_packed_tags: u64 = 0;
        let mut total_uncompressed_with: u64 = 0;
        let mut total_uncompressed_without: u64 = 0;
        let mut total_compressed_with: u64 = 0;
        let mut total_compressed_without: u64 = 0;
        let mut per_msg_compressed_delta: Vec<i64> = Vec::new();

        loop {
            let mut len_buf = [0u8; 8];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let msg_len = u64::from_le_bytes(len_buf) as usize;
            let mut frame = vec![0u8; msg_len];
            if file.read_exact(&mut frame).is_err() {
                break;
            }
            // Strip the envelope (V3-V8) and decompress the body to get raw
            // CollectorConnections bytes.
            let frame_bytes = bytes::Bytes::from(frame);
            let Ok((proto_bytes, _ts)) = decode_envelope(&frame_bytes) else {
                continue;
            };
            let Ok(cc) = CollectorConnections::decode(proto_bytes.as_ref()) else {
                continue;
            };

            messages += 1;
            connections += cc.connections.len() as u64;

            // Re-encode the proto as-is (knob ON state, the dump's actual content).
            let with_bytes = cc.encode_to_vec();
            let with_compressed =
                zstd::encode_all(with_bytes.as_slice(), ZSTD_LEVEL).expect("zstd with-tags");

            // Strip the fields the knob controls and re-encode (knob OFF state).
            let mut stripped = cc.clone();
            let had_any = !stripped.encoded_tags.is_empty()
                || stripped.host_tags_index != 0
                || stripped
                    .connections
                    .iter()
                    .any(|c| c.local_container_tags_index != 0);
            if had_any {
                had_packed_tags += 1;
            }
            stripped.encoded_tags.clear();
            stripped.host_tags_index = 0;
            for c in &mut stripped.connections {
                c.local_container_tags_index = 0;
            }

            let without_bytes = stripped.encode_to_vec();
            let without_compressed =
                zstd::encode_all(without_bytes.as_slice(), ZSTD_LEVEL).expect("zstd without-tags");

            total_uncompressed_with += with_bytes.len() as u64;
            total_uncompressed_without += without_bytes.len() as u64;
            total_compressed_with += with_compressed.len() as u64;
            total_compressed_without += without_compressed.len() as u64;
            per_msg_compressed_delta
                .push(with_compressed.len() as i64 - without_compressed.len() as i64);
        }

        if messages == 0 {
            println!("no decodable messages found in {path:?}");
            return;
        }

        per_msg_compressed_delta.sort();
        let median_delta = per_msg_compressed_delta[per_msg_compressed_delta.len() / 2];
        let p95_idx = (per_msg_compressed_delta.len() as f64 * 0.95) as usize;
        let p95_delta = per_msg_compressed_delta[p95_idx.min(per_msg_compressed_delta.len() - 1)];
        let max_delta = *per_msg_compressed_delta.last().unwrap();

        fn pct(saved: i64, full: u64) -> f64 {
            if full == 0 {
                return 0.0;
            }
            saved as f64 * 100.0 / full as f64
        }

        let unc_saved = total_uncompressed_with as i64 - total_uncompressed_without as i64;
        let cmp_saved = total_compressed_with as i64 - total_compressed_without as i64;

        println!();
        println!("=== payload size: with vs without packed container+host tags ===");
        println!("dump                 : {}", path.display());
        println!("messages             : {messages}");
        println!("connections          : {connections}");
        println!(
            "messages w/ packed tags : {had_packed_tags} ({:.1}%)",
            had_packed_tags as f64 * 100.0 / messages as f64
        );
        println!();
        println!("  -- uncompressed --");
        println!(
            "  with knob   : {} ({} bytes/msg, {} bytes/conn)",
            total_uncompressed_with,
            total_uncompressed_with / messages,
            total_uncompressed_with / connections.max(1)
        );
        println!(
            "  without     : {} ({} bytes/msg, {} bytes/conn)",
            total_uncompressed_without,
            total_uncompressed_without / messages,
            total_uncompressed_without / connections.max(1)
        );
        println!(
            "  knob cost   : +{} bytes total ({:+.1}%)",
            unc_saved,
            pct(unc_saved, total_uncompressed_without)
        );
        println!();
        println!("  -- zstd level {ZSTD_LEVEL} --");
        println!(
            "  with knob   : {} ({} bytes/msg, {} bytes/conn)",
            total_compressed_with,
            total_compressed_with / messages,
            total_compressed_with / connections.max(1)
        );
        println!(
            "  without     : {} ({} bytes/msg, {} bytes/conn)",
            total_compressed_without,
            total_compressed_without / messages,
            total_compressed_without / connections.max(1)
        );
        println!(
            "  knob cost   : +{} bytes total ({:+.1}%)",
            cmp_saved,
            pct(cmp_saved, total_compressed_without)
        );
        println!();
        println!(
            "  per-message compressed delta: median={median_delta}  p95={p95_delta}  \
             max={max_delta}",
        );
    }

    /// Estimates the customer-side bandwidth cost of splitting the agent's
    /// CollectorConnections payload into two separate streams:
    ///
    /// - Stream A (existing connections stream, slim): the same CollectorConnections, but with USM
    ///   aggregation bytes stripped from each connection.
    /// - Stream B (new USM-only stream): a CollectorConnections-shaped payload carrying only
    ///   USM-active connections, with NPM-only fields (byte/packet counters, retransmits, rtt, DNS
    ///   stats, conntrack, routes, …) cleared and only the USM context preserved (5-tuple,
    ///   container_id, direction, namespace, tag indices, USM aggregations, encoded_tags,
    ///   encoded_connections_tags).
    ///
    /// Cost = (Stream A + Stream B) − Original. Stream B duplicates the
    /// envelope-level metadata + per-USM-conn 5-tuple/container context,
    /// which is what the customer would pay for.
    ///
    ///     USM_DUMP_PATH=~/Downloads/dump_1777294025 cargo test \
    ///         -p pomsky-intake --release --lib dump_split_streams_cost_probe \
    ///         -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_split_streams_cost_probe() {
        use std::io::Read;
        use std::path::PathBuf;

        use prost::Message as _;

        use crate::protos::process::CollectorConnections;
        use crate::sources::connections::decode_envelope;

        let path = std::env::var("USM_DUMP_PATH")
            .map(PathBuf::from)
            .expect("set USM_DUMP_PATH");

        const ZSTD_LEVEL: i32 = 3;

        let mut file = std::fs::File::open(&path).expect("open dump");

        let mut messages: u64 = 0;
        let mut connections: u64 = 0;
        let mut usm_active_connections: u64 = 0;
        let mut total_orig_unc: u64 = 0;
        let mut total_orig_cmp: u64 = 0;
        let mut total_a_unc: u64 = 0;
        let mut total_a_cmp: u64 = 0;
        let mut total_b_unc: u64 = 0;
        let mut total_b_cmp: u64 = 0;
        let mut per_msg_cmp_delta: Vec<i64> = Vec::new();

        loop {
            let mut len_buf = [0u8; 8];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let msg_len = u64::from_le_bytes(len_buf) as usize;
            let mut frame = vec![0u8; msg_len];
            if file.read_exact(&mut frame).is_err() {
                break;
            }
            let frame_bytes = bytes::Bytes::from(frame);
            let Ok((proto_bytes, _ts)) = decode_envelope(&frame_bytes) else {
                continue;
            };
            let Ok(cc) = CollectorConnections::decode(proto_bytes.as_ref()) else {
                continue;
            };

            messages += 1;
            connections += cc.connections.len() as u64;
            let usm_active = cc
                .connections
                .iter()
                .filter(|c| {
                    !c.http_aggregations.is_empty()
                        || !c.http2_aggregations.is_empty()
                        || !c.data_streams_aggregations.is_empty()
                        || !c.database_aggregations.is_empty()
                })
                .count() as u64;
            usm_active_connections += usm_active;

            // Original payload size — re-encoded to keep apples-to-apples
            // with the alternative encodings below.
            let orig_bytes = cc.encode_to_vec();
            let orig_cmp = zstd::encode_all(orig_bytes.as_slice(), ZSTD_LEVEL).unwrap();

            // Stream A: same connections, USM aggregation bytes stripped.
            let mut stream_a = cc.clone();
            for c in &mut stream_a.connections {
                c.http_aggregations.clear();
                c.http2_aggregations.clear();
                c.data_streams_aggregations.clear();
                c.database_aggregations.clear();
            }
            let a_bytes = stream_a.encode_to_vec();
            let a_cmp = zstd::encode_all(a_bytes.as_slice(), ZSTD_LEVEL).unwrap();

            // Stream B: only USM-active connections, NPM-only fields cleared.
            let mut stream_b = cc.clone();
            stream_b.connections.retain(|c| {
                !c.http_aggregations.is_empty()
                    || !c.http2_aggregations.is_empty()
                    || !c.data_streams_aggregations.is_empty()
                    || !c.database_aggregations.is_empty()
            });
            for c in &mut stream_b.connections {
                // Drop NPM-only counters and conntrack/route/DNS context.
                // Keep: pid, laddr, raddr, family, type, direction, net_ns,
                // is_local_port_ephemeral, protocol, tags_idx, tags_checksum,
                // local_container_tags_index, *_aggregations, encoded refs.
                c.last_bytes_sent = 0;
                c.last_bytes_received = 0;
                c.last_retransmits = 0;
                c.last_packets_sent = 0;
                c.last_packets_received = 0;
                c.last_tcp_established = 0;
                c.last_tcp_closed = 0;
                c.rtt = 0;
                c.rtt_var = 0;
                c.intra_host = false;
                c.ip_translation = None;
                c.dns_successful_responses = 0;
                c.dns_failed_responses = 0;
                c.dns_timeouts = 0;
                c.dns_success_latency_sum = 0;
                c.dns_failure_latency_sum = 0;
                c.dns_count_by_rcode.clear();
                c.dns_stats_by_domain.clear();
                c.dns_stats_by_domain_by_query_type.clear();
                c.dns_stats_by_domain_offset_by_query_type.clear();
                c.route_idx = 0;
                c.route_target_idx = 0;
                c.tcp_failures_by_err_code.clear();
                c.remote_network_id = String::new();
                c.remote_ecs_task = String::new();
                c.tags = Vec::new();
                c.resolv_conf_idx = 0;
                c.system_probe_conn = false;
            }
            // Drop CC-level fields that only NPM cares about.
            stream_b.routes.clear();
            stream_b.route_metadata.clear();
            stream_b.encoded_dns.clear();
            stream_b.encoded_dns_lookups.clear();
            stream_b.encoded_domain_database.clear();
            stream_b.domains.clear();
            stream_b.resolv_confs.clear();
            stream_b.conn_telemetry = None;
            stream_b.conn_telemetry_map.clear();
            let b_bytes = stream_b.encode_to_vec();
            let b_cmp = zstd::encode_all(b_bytes.as_slice(), ZSTD_LEVEL).unwrap();

            total_orig_unc += orig_bytes.len() as u64;
            total_orig_cmp += orig_cmp.len() as u64;
            total_a_unc += a_bytes.len() as u64;
            total_a_cmp += a_cmp.len() as u64;
            total_b_unc += b_bytes.len() as u64;
            total_b_cmp += b_cmp.len() as u64;
            per_msg_cmp_delta
                .push((a_cmp.len() as i64 + b_cmp.len() as i64) - orig_cmp.len() as i64);
        }

        if messages == 0 {
            println!("no decodable messages found in {path:?}");
            return;
        }

        per_msg_cmp_delta.sort();
        let median = per_msg_cmp_delta[per_msg_cmp_delta.len() / 2];
        let p95_idx = (per_msg_cmp_delta.len() as f64 * 0.95) as usize;
        let p95 = per_msg_cmp_delta[p95_idx.min(per_msg_cmp_delta.len() - 1)];
        let max = *per_msg_cmp_delta.last().unwrap();

        let total_split_unc = total_a_unc + total_b_unc;
        let total_split_cmp = total_a_cmp + total_b_cmp;

        fn ratio(num: i64, den: u64) -> f64 {
            if den == 0 {
                return 0.0;
            }
            num as f64 * 100.0 / den as f64
        }

        println!();
        println!("=== split-stream cost: original vs (Stream A + Stream B) ===");
        println!("dump                : {}", path.display());
        println!("messages            : {messages}");
        println!("connections         : {connections} (usm-active: {usm_active_connections})");
        println!();
        println!("  -- uncompressed --");
        println!(
            "  original           : {} ({} B/msg)",
            total_orig_unc,
            total_orig_unc / messages
        );
        println!(
            "  Stream A           : {} ({} B/msg, {:+.1}% of orig)",
            total_a_unc,
            total_a_unc / messages,
            ratio(total_a_unc as i64 - total_orig_unc as i64, total_orig_unc)
        );
        println!(
            "  Stream B           : {} ({} B/msg, {:+.1}% of orig)",
            total_b_unc,
            total_b_unc / messages,
            ratio(total_b_unc as i64, total_orig_unc)
        );
        println!(
            "  A + B  total       : {} ({} B/msg)",
            total_split_unc,
            total_split_unc / messages
        );
        println!(
            "  customer cost      : {:+} bytes total ({:+.1}%)",
            total_split_unc as i64 - total_orig_unc as i64,
            ratio(
                total_split_unc as i64 - total_orig_unc as i64,
                total_orig_unc
            )
        );
        println!();
        println!("  -- zstd level {ZSTD_LEVEL} --");
        println!(
            "  original           : {} ({} B/msg)",
            total_orig_cmp,
            total_orig_cmp / messages
        );
        println!(
            "  Stream A           : {} ({} B/msg, {:+.1}% of orig)",
            total_a_cmp,
            total_a_cmp / messages,
            ratio(total_a_cmp as i64 - total_orig_cmp as i64, total_orig_cmp)
        );
        println!(
            "  Stream B           : {} ({} B/msg, {:+.1}% of orig)",
            total_b_cmp,
            total_b_cmp / messages,
            ratio(total_b_cmp as i64, total_orig_cmp)
        );
        println!(
            "  A + B  total       : {} ({} B/msg)",
            total_split_cmp,
            total_split_cmp / messages
        );
        println!(
            "  customer cost      : {:+} bytes total ({:+.1}%)",
            total_split_cmp as i64 - total_orig_cmp as i64,
            ratio(
                total_split_cmp as i64 - total_orig_cmp as i64,
                total_orig_cmp
            )
        );
        println!();
        println!("  per-message compressed delta: median={median}  p95={p95}  max={max}",);
    }
}
