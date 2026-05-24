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

//! Shared types used across the transform's sub-modules.
//!
//! The types here mirror the Go sidecar's `parser.USMStat` and the
//! `aggregator.BucketKey` / `aggregator.ServiceIndexKey` tuples; the
//! struct names follow the Rust idiom but the fields are a 1:1 port.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::protos::process::ConnectionDirection;

/// Per-connection per-endpoint record produced by a protocol parser.
///
/// Parsers construct `ProtoStat` (no service/env/direction — those are
/// connection-level). The orchestrator resolves per-connection
/// service/env/direction and lifts each `ProtoStat` into a `UsmStat` by
/// stamping those fields on. This keeps parsers from inventing placeholder
/// service names to satisfy non-optional fields.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ProtoStat {
    pub(super) operation: Operation,
    pub(super) resource: String,
    pub(super) status: i32,
    pub(super) hits: u32,
    pub(super) errors: u32,
    pub(super) latencies: Option<Bytes>,
    pub(super) first_latency_sample: Option<f64>,
}

/// Per-connection enrichment resolved before the protocol parsers run.
///
/// Grouped into one struct to keep `UsmStat::from_proto_stat`'s signature
/// from drifting open-ended as we add tag families (env, version, tls.library,
/// iis.*, …). Mirrors NSX's `USMInfo` shape minus the SaaS-only `PrimaryTags`
/// (those need per-org configuration we don't have in BYOC).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub(super) struct ConnectionTags {
    pub(super) env: Option<String>,
    pub(super) version: Option<String>,
    /// TLS library name (e.g. `openssl`, `gotls`). Process-source only.
    /// Matches `dd-go/trace/apps/network-stats-extractor/converter/inventory.
    /// go::processTagForHTTPInfo`.
    pub(super) tls_library: Option<String>,
    /// IIS-related per-process tags. Process-source only. Keys are restricted
    /// to `http.iis.{app_pool,site,sitename,subsite}` — matches NSX's `iisTags`
    /// allowlist. Empty for non-Windows workloads.
    pub(super) iis_tags: BTreeMap<String, String>,
}

/// Per-connection per-endpoint record with service/direction + connection tags resolved.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct UsmStat {
    pub(super) service: String,
    pub(super) tags: ConnectionTags,
    pub(super) direction: Direction,
    pub(super) operation: Operation,
    pub(super) resource: String,
    pub(super) status: i32,
    pub(super) hits: u32,
    pub(super) errors: u32,
    pub(super) latencies: Option<Bytes>,
    pub(super) first_latency_sample: Option<f64>,
}

impl UsmStat {
    pub(super) fn from_proto_stat(
        ps: ProtoStat,
        service: String,
        tags: ConnectionTags,
        direction: Direction,
    ) -> Self {
        Self {
            service,
            tags,
            direction,
            operation: ps.operation,
            resource: ps.resource,
            status: ps.status,
            hits: ps.hits,
            errors: ps.errors,
            latencies: ps.latencies,
            first_latency_sample: ps.first_latency_sample,
        }
    }
}

/// Flow direction of a connection after the direction-fixup pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Direction {
    Client,
    Server,
}

impl Direction {
    /// Maps the agent's `ConnectionDirection` proto enum to our internal
    /// representation. Mirrors the Go sidecar's rule: `Incoming` → server,
    /// everything else (including `Unspecified`, `Local`, `None`, and
    /// unknown int values) → client.
    pub(super) fn from_agent(d: i32) -> Self {
        if d == ConnectionDirection::Incoming as i32 {
            Self::Server
        } else {
            Self::Client
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
    }
}

/// Application-layer protocol for a stat record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Operation {
    Http,
    Http2,
    Grpc,
    Kafka,
    Postgres,
    Redis,
}

impl Operation {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Http2 => "http2",
            Self::Grpc => "grpc",
            Self::Kafka => "kafka",
            Self::Postgres => "postgres",
            Self::Redis => "redis",
        }
    }
}

/// Fine-grained bucket key for `universal.*` metric emission.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct BucketKey {
    pub(super) service: String,
    pub(super) tags: ConnectionTags,
    pub(super) operation: String,
    pub(super) resource: String,
    pub(super) status_class: Option<StatusClass>,
    pub(super) is_error: bool,
}

/// Coarser-grained bucket key for the `trace.services_by_operation` family.
/// Matches TSS's SaaS output tag set (no `resource`, no `status_class`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ServiceIndexKey {
    pub(super) service: String,
    pub(super) tags: ConnectionTags,
    pub(super) operation: String,
}

/// HTTP status class as a tag value. `None` means the status code is not
/// in the 2xx..=5xx range (including 0 from legacy paths, 1xx, and any
/// unknown value) and should not carry an `http.status_class` tag.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum StatusClass {
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

pub(super) fn status_class_from_code(code: i32) -> Option<StatusClass> {
    match code {
        200..=299 => Some(StatusClass::TwoXx),
        300..=399 => Some(StatusClass::ThreeXx),
        400..=499 => Some(StatusClass::FourXx),
        500..=599 => Some(StatusClass::FiveXx),
        _ => None,
    }
}

impl StatusClass {
    pub(super) fn as_tag_value(self) -> &'static str {
        match self {
            Self::TwoXx => "2xx",
            Self::ThreeXx => "3xx",
            Self::FourXx => "4xx",
            Self::FiveXx => "5xx",
        }
    }
}

/// Full operation string the emitter writes as the metric-name prefix and
/// the `operation_name` tag value: `universal.<proto>.<dir>`.
pub(super) fn full_operation(op: Operation, dir: Direction) -> String {
    format!("universal.{}.{}", op.as_str(), dir.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_class_boundaries() {
        assert_eq!(status_class_from_code(199), None);
        assert_eq!(status_class_from_code(200), Some(StatusClass::TwoXx));
        assert_eq!(status_class_from_code(299), Some(StatusClass::TwoXx));
        assert_eq!(status_class_from_code(300), Some(StatusClass::ThreeXx));
        assert_eq!(status_class_from_code(399), Some(StatusClass::ThreeXx));
        assert_eq!(status_class_from_code(400), Some(StatusClass::FourXx));
        assert_eq!(status_class_from_code(499), Some(StatusClass::FourXx));
        assert_eq!(status_class_from_code(500), Some(StatusClass::FiveXx));
        assert_eq!(status_class_from_code(599), Some(StatusClass::FiveXx));
        assert_eq!(status_class_from_code(600), None);
        assert_eq!(status_class_from_code(0), None);
    }

    #[test]
    fn direction_from_agent_matches_go_rule() {
        // Incoming is the only value that maps to Server. Everything else
        // (Unspecified, Outgoing, Local, None, unknown int) maps to Client —
        // matches dd-source byoc-usm-stats `resolver.Direction`.
        assert_eq!(
            Direction::from_agent(ConnectionDirection::Incoming as i32),
            Direction::Server
        );
        assert_eq!(
            Direction::from_agent(ConnectionDirection::Unspecified as i32),
            Direction::Client
        );
        assert_eq!(
            Direction::from_agent(ConnectionDirection::Outgoing as i32),
            Direction::Client
        );
        assert_eq!(
            Direction::from_agent(ConnectionDirection::Local as i32),
            Direction::Client
        );
        assert_eq!(
            Direction::from_agent(ConnectionDirection::None as i32),
            Direction::Client
        );
        // Unknown int values (e.g., a newer agent enum variant) also map to
        // Client, mirroring Go's fall-through.
        assert_eq!(Direction::from_agent(99), Direction::Client);
    }

    #[test]
    fn full_operation_names() {
        assert_eq!(
            full_operation(Operation::Http, Direction::Server),
            "universal.http.server"
        );
        assert_eq!(
            full_operation(Operation::Grpc, Direction::Client),
            "universal.grpc.client"
        );
    }
}
