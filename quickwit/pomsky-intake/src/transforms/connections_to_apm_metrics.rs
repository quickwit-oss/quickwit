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

use serde::{Deserialize, Serialize};
use tracing::warn;
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::Event;
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

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
    // TODO(eyal.brami): remove once the real body uses both parameters.
    #[allow(unused_variables)]
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let Event::Log(log) = &event else {
            // Vector's config validator routes only Log events here (see
            // `input() -> Input::log()` above), so anything else is a bug.
            debug_assert!(false, "non-log event reached connections_to_apm_metrics");
            warn!("dropping non-log event; input validator should have filtered it");
            return;
        };

        // TODO(eyal.brami): Implement the full processing pipeline:
        //
        // The upstream `connections` source has already stripped the envelope
        // and zstd-decompressed the body — this transform only needs to parse
        // the CollectorConnections protobuf and extract metrics.
        //
        // 1. Read the proto bytes from the log event's CONNECTIONS_PROTO_FIELD (see
        //    `sources::connections::CONNECTIONS_PROTO_FIELD`).
        // 2. Decode CollectorConnections via `prost::Message::decode` using the types from
        //    `crate::protos::process`.
        // 3. For each connection: a. Direction fixup (listening port inference, DNS
        //    reclassification). b. Resolve service name from EncodedTags (process → container →
        //    host tag precedence). c. Resolve env from tags. d. For each protocol aggregation
        //    (Http, Http2, Kafka, Database):
        //       - Second-level protobuf decode of the opaque `bytes` field.
        //       - Extract per-endpoint/resource stats (hits, errors, latencies).
        //       - Emit universal.<proto>.<dir>.hits count metric.
        //       - Emit universal.<proto>.<dir> distribution sketch metric.
        //       - Emit trace.services_by_operation family metrics.
        // 4. Push all emitted Metric events to `output`.
        //
        // Reference: Go implementation at
        //   dd-source/domains/quickhouse/apps/byoc-usm-stats/.
        // Additional Rust deps to add (follow-up PR):
        //   - A DDSketch crate for sketch decoding (sketches-ddsketch or datasketches-rust).
    }
}
