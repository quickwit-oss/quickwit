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

//! Connections source — receives raw DD Agent CollectorConnections payloads,
//! strips the V8 envelope, decompresses the zstd body, and emits one Log
//! event per payload carrying the inner CollectorConnections protobuf bytes
//! for downstream transforms to parse.
//!
//! Pipeline:
//!   Agent POST /api/v1/connections → [this source] → connections_to_apm_metrics → metrics_out

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use vector::config::{
    DataType, GenerateConfig, Resource, SourceConfig, SourceContext, SourceOutput,
};
use vector::event::{Event, LogEvent};
use vector::schema::Definition;
use vector_lib::configurable::NamedComponent;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

/// Receives raw agent CollectorConnections payloads over HTTP.
///
/// Handles V8/V3-V7 envelope stripping and zstd decompression at the source,
/// then emits one Log event per payload carrying the inner CollectorConnections
/// protobuf bytes for downstream transforms to parse. Keeping envelope
/// handling here (and leaving proto parsing to the transform) follows
/// Vector's separation of concerns: sources own the wire format, transforms
/// own protocol semantics.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsSourceConfig {
    /// Listen address (e.g. "0.0.0.0:8585").
    #[serde(default = "default_address")]
    pub address: SocketAddr,
}

// Default listen address: all interfaces, port 8585.
const DEFAULT_PORT: u16 = 8585;

fn default_address() -> SocketAddr {
    // Build from typed parts — no parsing, no possible panic.
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PORT)
}

impl NamedComponent for ConnectionsSourceConfig {
    fn get_component_name(&self) -> &'static str {
        "connections"
    }
}

impl GenerateConfig for ConnectionsSourceConfig {
    fn generate_config() -> toml::Value {
        let mut table = toml::value::Table::new();
        table.insert(
            "address".to_string(),
            toml::Value::String(default_address().to_string()),
        );
        toml::Value::Table(table)
    }
}

/// Key on the emitted Log event under which the source places the inner
/// CollectorConnections protobuf bytes (envelope stripped, body decompressed;
/// the proto itself is *not* parsed here). Downstream transforms read from
/// this key.
pub const CONNECTIONS_PROTO_FIELD: &str = "connections_proto";

#[async_trait::async_trait]
#[typetag::serde(name = "connections")]
impl SourceConfig for ConnectionsSourceConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<vector::sources::Source> {
        let address = self.address;
        let out = cx.out;
        let shutdown = cx.shutdown;

        let routes = build_routes(out);

        Ok(Box::pin(async move {
            info!(%address, "connections source listening");
            warp::serve(routes)
                .bind(address)
                .await
                .graceful(async move {
                    let _ = shutdown.await;
                    info!("connections source shutting down");
                })
                .run()
                .await;
            Ok(())
        }))
    }

    fn outputs(&self, _global_log_namespace: vector::config::LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            Definition::default_legacy_namespace(),
        )]
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

/// Builds the warp filter tree for the source's HTTP routes.
///
/// - `POST /api/v1/connections` → decode + emit event
/// - `GET  /healthz`            → 200 OK
fn build_routes(
    out: vector_lib::source_sender::SourceSender,
) -> warp::filters::BoxedFilter<(Response,)> {
    // `SourceSender` is `Clone` (internally an mpsc sender); each request gets
    // its own clone, so send_event has owned `&mut self` without any mutex.
    let connections = warp::post()
        .and(warp::path("api"))
        .and(warp::path("v1"))
        .and(warp::path("connections"))
        .and(warp::path::end())
        .and(warp::body::bytes())
        .and(warp::any().map(move || out.clone()))
        .and_then(handle_connections);

    let healthz = warp::get()
        .and(warp::path("healthz"))
        .and(warp::path::end())
        .map(|| warp::reply::with_status("ok", StatusCode::OK).into_response());

    connections.or(healthz).unify().boxed()
}

/// Handles a single POST /api/v1/connections request: decodes the envelope,
/// decompresses the body, and emits a Log event carrying the raw
/// CollectorConnections protobuf bytes.
///
/// Proto parsing is intentionally left to the downstream transform to avoid
/// decoding the payload twice. This keeps the source focused on wire format
/// (envelope + compression) and the transform focused on protocol semantics.
async fn handle_connections(
    body: Bytes,
    mut out: vector_lib::source_sender::SourceSender,
) -> Result<Response, Rejection> {
    let body_len = body.len();
    // Zstd decompression can take 100s of µs to ~1 ms on the largest agent
    // payloads; offload from the tokio worker thread to the blocking pool.
    let decoded = tokio::task::spawn_blocking(move || decode_envelope(&body)).await;
    let proto_bytes = match decoded {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => {
            warn!(%err, body_len, "failed to decode connections envelope");
            return Ok(warp::reply::with_status(
                format!("envelope decode error: {err}"),
                StatusCode::BAD_REQUEST,
            )
            .into_response());
        }
        Err(join_err) => {
            error!(%join_err, body_len, "envelope decode task panicked");
            return Ok(warp::reply::with_status(
                "internal error",
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response());
        }
    };

    debug!(bytes = proto_bytes.len(), "received connections payload");

    let mut log = LogEvent::default();
    log.insert(CONNECTIONS_PROTO_FIELD, proto_bytes);

    if let Err(err) = out.send_event(Event::Log(log)).await {
        error!(%err, "failed to forward connections event");
        return Ok(
            warp::reply::with_status("internal error", StatusCode::INTERNAL_SERVER_ERROR)
                .into_response(),
        );
    }

    Ok(warp::reply::with_status("accepted", StatusCode::ACCEPTED).into_response())
}

// DD Agent message envelope versions. V3-V7 use fixed-size headers; V8 uses
// a variable-length protobuf-encoded header. Sizes mirror dd-go's
// `process/conn/message.go` and use the same `sum of field widths` style so
// the contents of each version are self-documenting.
//
// V3 fields: version(1) + encoding(1) + type(1) + subscriptionID(1)
//          + orgID(4) + timestamp(8)
const HEADER_V3_LEN: usize = 1 + 1 + 1 + 1 + 4 + 8;
// V4 adds agent version (2 bytes).
const HEADER_V4_LEN: usize = HEADER_V3_LEN + 2;
// V5 adds partitionID (2) + eventID (8).
const HEADER_V5_LEN: usize = HEADER_V4_LEN + 2 + 8;
// V6 adds "has containers" flag (1).
const HEADER_V6_LEN: usize = HEADER_V5_LEN + 1;
// V7 adds agent timestamp (8).
const HEADER_V7_LEN: usize = HEADER_V6_LEN + 8;
// V8 prefix: version(1) + uint16 LE header length(2). The protobuf header
// itself follows and is skipped by this source.
const V8_VERSION_BYTE: u8 = 0x08;
const V8_PREFIX_LEN: usize = 1 + 2;

/// Strips the DD Agent message envelope and decompresses the zstd body.
///
/// Supports V3-V7 (fixed-size headers) and V8 (protobuf-encoded variable
/// header). On success returns the decompressed body bytes — for
/// CollectorConnections payloads this is a serialized protobuf the
/// downstream transform will parse.
///
/// Takes `Bytes` (cheap refcount clone) and returns `Bytes` so the
/// raw-passthrough branch can zero-copy-slice the input rather than
/// allocating.
///
/// Reference: dd-go `process/conn/message.go` `ReadHeader` / `readHeaderV8`.
fn decode_envelope(data: &Bytes) -> Result<Bytes, String> {
    if data.len() < V8_PREFIX_LEN {
        return Err("payload too short".into());
    }

    let version = data[0];
    let body_start = if version == V8_VERSION_BYTE {
        let header_len = u16::from_le_bytes([data[1], data[2]]) as usize;
        let start = V8_PREFIX_LEN + header_len;
        if start > data.len() {
            return Err(format!("V8 header length {header_len} exceeds payload"));
        }
        start
    } else {
        match version {
            3 => HEADER_V3_LEN,
            4 => HEADER_V4_LEN,
            5 => HEADER_V5_LEN,
            6 => HEADER_V6_LEN,
            7 => HEADER_V7_LEN,
            _ => return Err(format!("unsupported message version {version}")),
        }
    };

    if body_start > data.len() {
        return Err("payload too short for header".into());
    }

    let compressed = data.slice(body_start..);
    match zstd::decode_all(compressed.as_ref()) {
        Ok(decompressed) => Ok(Bytes::from(decompressed)),
        Err(_) => {
            // Some agent versions send raw uncompressed protobuf — pass
            // through the original slice zero-copy.
            Ok(compressed)
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::protos::process::CollectorConnections;

    fn make_v8_envelope(body: &[u8]) -> Bytes {
        let compressed = zstd::encode_all(body, 3).unwrap();
        // V8 prefix: version byte + 2-byte LE header length (0 here).
        let mut envelope = vec![V8_VERSION_BYTE, 0x00, 0x00];
        envelope.extend_from_slice(&compressed);
        Bytes::from(envelope)
    }

    #[test]
    fn test_decode_envelope_too_short() {
        assert!(decode_envelope(&Bytes::from_static(b"ab")).is_err());
    }

    #[test]
    fn test_decode_envelope_unknown_version() {
        assert!(decode_envelope(&Bytes::from_static(&[0x09, 0x00, 0x00])).is_err());
    }

    #[test]
    fn test_decode_envelope_v8_roundtrip() {
        let payload = b"hello world";
        let envelope = make_v8_envelope(payload);
        let decoded = decode_envelope(&envelope).unwrap();
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn test_decode_envelope_v8_empty_body() {
        let envelope = make_v8_envelope(b"");
        let decoded = decode_envelope(&envelope).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_full_payload_roundtrip() {
        let cc = CollectorConnections {
            host_name: "test-host".to_string(),
            ..Default::default()
        };
        let proto_bytes = cc.encode_to_vec();
        let envelope = make_v8_envelope(&proto_bytes);

        let decoded_bytes = decode_envelope(&envelope).unwrap();
        let decoded = CollectorConnections::decode(decoded_bytes.as_ref()).unwrap();
        assert_eq!(decoded.host_name, "test-host");
    }

    // Note: tests for `handle_connections` require a `SourceSender` test
    // helper which is gated behind vector-core's `test` feature. Integration
    // tests exercising the full HTTP path belong in a separate tests/ file
    // once that feature is wired into pomsky-intake's dev-deps. The sync
    // envelope/proto tests above cover the source's non-plumbing logic.
}
