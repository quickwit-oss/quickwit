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
//! strips the V3-V8 envelope, decompresses the body, and emits one Log
//! event per payload carrying the inner CollectorConnections protobuf bytes
//! for downstream transforms to parse.
//!
//! Pipeline:
//!   Agent POST /api/v1/connections → [this source] → connections_to_apm_metrics → metrics_out

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use http::StatusCode;
use prost::Message;
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

use crate::protos::conn::{MessageEncoding, MessageHeader, MessageType};
use crate::protos::process::{CollectorStatus, ResCollector, res_collector};

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

/// Max accepted size of a compressed `POST /api/v1/connections` body.
///
/// The agent-side cap on a single CollectorConnections payload is tens of
/// MB post-compression. 64 MiB gives us ~8-16× headroom over the largest
/// real payload while bounding memory pressure from a misbehaving or
/// malicious agent. Requests over this return 413 without consuming the
/// body. Note: this is the compressed on-wire size; zstd expansion is
/// bounded separately by the envelope decoder's inherent cost.
const MAX_REQUEST_BODY_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

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

/// Key on the emitted Log event under which the source places the agent
/// timestamp (unix seconds as `i64`) recovered from the envelope header,
/// or the intake timestamp as a fallback. Absent when neither is present.
pub const CONNECTIONS_TIMESTAMP_FIELD: &str = "timestamp";

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
        .and(warp::body::content_length_limit(MAX_REQUEST_BODY_BYTES))
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
/// CollectorConnections protobuf bytes plus the recovered agent timestamp.
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
    let (proto_bytes, timestamp_opt) = match decoded {
        Ok(Ok(pair)) => pair,
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

    // The DD Agent populates neither `MessageHeader.Timestamp` nor
    // `MessageHeader.AgentTimestamp` in the proto body — it only sends
    // `X-DD-Agent-Timestamp` as an HTTP header. The intake stamps these
    // fields at receive time. See dd-go
    // `process/apps/process-intake/intake.go:187,283-298` ("This must be
    // calculated as time.Now() at intake, the first time the message is
    // seen.").
    //
    // Without a receive-time fallback, every metric the transform emits
    // downstream lands at epoch 0 in parquet and becomes invisible to any
    // time-windowed UI query. Mirror process-intake by defaulting to
    // `now()` when both envelope fields are zero.
    let ts = timestamp_opt.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let mut log = LogEvent::default();
    log.insert(CONNECTIONS_PROTO_FIELD, proto_bytes);
    log.insert(CONNECTIONS_TIMESTAMP_FIELD, ts);

    if let Err(err) = out.send_event(Event::Log(log)).await {
        error!(%err, "failed to forward connections event");
        return Ok(
            warp::reply::with_status("internal error", StatusCode::INTERNAL_SERVER_ERROR)
                .into_response(),
        );
    }

    // The DD Agent's `readResponseStatuses` (dd-go `pkg/process/runner/runner.go`)
    // expects the response body to be a V8-wrapped `ResCollector` protobuf with
    // a CollectorStatus carrying the next-check `interval`. Returning plain
    // text — which the warp helper would do — makes the agent log
    // `invalid message version: <ascii byte>` per submission and treat the
    // submission as failed. Mirror what dd-source's byoc-usm-stats returned.
    let body = encode_response_envelope();
    let response = http::Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/x-protobuf")
        .body(body.into())
        .expect("response builder produces a valid response");
    Ok(response)
}

/// Default connections-check interval to echo back to the agent (seconds).
/// Matches the upstream process-agent default. The agent reads this from
/// `CollectorStatus.interval` to schedule the next submission.
const RESPONSE_INTERVAL_SECS: i32 = 30;

/// Builds the V8-encoded `ResCollector` response body the agent expects after
/// a successful `POST /api/v1/connections`. Uses `Protobuf` encoding (no zstd)
/// because the body is tiny and the agent decoder accepts raw protobuf.
fn encode_response_envelope() -> Bytes {
    let res = ResCollector {
        header: Some(res_collector::Header { r#type: 0 }),
        message: String::new(),
        status: Some(CollectorStatus {
            active_clients: 0,
            interval: RESPONSE_INTERVAL_SECS,
        }),
    };
    let body = res.encode_to_vec();

    let header = MessageHeader {
        r#type: MessageType::TypeResCollector as i32,
        encoding: MessageEncoding::Protobuf as i32,
        ..Default::default()
    };
    let header_bytes = header.encode_to_vec();
    // V8 header length is a u16; ResCollector's header is well under 64 KiB.
    let header_len = u16::try_from(header_bytes.len()).expect("V8 header fits in u16");

    let mut envelope = Vec::with_capacity(V8_PREFIX_LEN + header_bytes.len() + body.len());
    envelope.push(V8_VERSION_BYTE);
    envelope.extend_from_slice(&header_len.to_le_bytes());
    envelope.extend_from_slice(&header_bytes);
    envelope.extend_from_slice(&body);
    Bytes::from(envelope)
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
// itself follows and is parsed via `MessageHeader::decode`.
const V8_VERSION_BYTE: u8 = 0x08;
const V8_PREFIX_LEN: usize = 1 + 2;

/// Strips the DD Agent message envelope and decompresses the body, returning
/// the inner protobuf bytes plus the best-available agent-side timestamp.
///
/// Supports V3-V7 (fixed-size headers) and V8 (protobuf-encoded variable
/// header). The `MessageEncoding` field of the parsed header drives
/// decompression so we don't have to guess. Unknown encoding values fall
/// back to "try zstd, return raw on failure" for resilience against newer
/// agents — matches dd-source byoc-usm-stats's `decompress` default branch.
///
/// Takes `Bytes` (cheap refcount clone) and returns `Bytes` so the
/// raw-passthrough branch can zero-copy-slice the input rather than
/// allocating.
///
/// Reference: dd-go `process/conn/message.go` `ReadHeader` (V3-V8 layouts).
pub(crate) fn decode_envelope(data: &Bytes) -> Result<(Bytes, Option<i64>), String> {
    let (header, body_start) = read_header(data)?;
    if body_start > data.len() {
        return Err("payload too short for header".to_string());
    }
    let body = decompress(header.encoding, data.slice(body_start..));
    let timestamp_opt = preferred_timestamp(&header);
    Ok((body, timestamp_opt))
}

/// Picks the best agent-side timestamp out of the parsed header: prefer the
/// agent-collection timestamp (V8 field 10 / V7 fixed-offset), fall back to
/// the intake timestamp (field 5 / V3+ fixed offset). Returns `None` if
/// neither is set (proto3 default of 0).
fn preferred_timestamp(h: &MessageHeader) -> Option<i64> {
    if h.agent_timestamp != 0 {
        Some(h.agent_timestamp)
    } else if h.timestamp != 0 {
        Some(h.timestamp)
    } else {
        None
    }
}

fn read_header(data: &Bytes) -> Result<(MessageHeader, usize), String> {
    if data.is_empty() {
        return Err("payload empty".to_string());
    }
    let version = data[0];
    if version == V8_VERSION_BYTE {
        return read_header_v8(data);
    }
    read_header_legacy(data, version)
}

fn read_header_v8(data: &Bytes) -> Result<(MessageHeader, usize), String> {
    if data.len() < V8_PREFIX_LEN {
        return Err("V8 prefix truncated".into());
    }
    let header_len = u16::from_le_bytes([data[1], data[2]]) as usize;
    let body_start = V8_PREFIX_LEN + header_len;
    if body_start > data.len() {
        return Err(format!("V8 header length {header_len} exceeds payload"));
    }
    let header = MessageHeader::decode(&data[V8_PREFIX_LEN..body_start])
        .map_err(|err| format!("V8 MessageHeader decode failed: {err}"))?;
    Ok((header, body_start))
}

fn read_header_legacy(data: &Bytes, version: u8) -> Result<(MessageHeader, usize), String> {
    let header_len = match version {
        3 => HEADER_V3_LEN,
        4 => HEADER_V4_LEN,
        5 => HEADER_V5_LEN,
        6 => HEADER_V6_LEN,
        7 => HEADER_V7_LEN,
        _ => return Err(format!("unsupported message version {version}")),
    };
    if data.len() < header_len {
        return Err("payload too short for legacy header".into());
    }
    // V3-V7 share the same first 16 bytes: version(1), encoding(1), type(1),
    // sub_id(1), org_id(4 LE), timestamp(8 LE). V7 also carries the agent
    // timestamp at offset 29 (immediately after V6's 29-byte sum).
    let agent_timestamp = if version >= 7 {
        i64::from_le_bytes(data[29..37].try_into().expect("checked length above"))
    } else {
        0
    };
    let header = MessageHeader {
        encoding: i32::from(data[1]),
        timestamp: i64::from_le_bytes(data[8..16].try_into().expect("checked length above")),
        agent_timestamp,
        ..Default::default()
    };
    Ok((header, header_len))
}

fn decompress(encoding: i32, body: Bytes) -> Bytes {
    match MessageEncoding::try_from(encoding) {
        Ok(MessageEncoding::Protobuf) => body,
        Ok(MessageEncoding::ZstdPb)
        | Ok(MessageEncoding::Zstd1xPb)
        | Ok(MessageEncoding::ZstdPBxNoCgo) => match zstd::decode_all(body.as_ref()) {
            Ok(decompressed) => Bytes::from(decompressed),
            Err(err) => {
                warn!(%err, "zstd-decoded body failed; passing raw bytes through");
                body
            }
        },
        Ok(MessageEncoding::Json) => {
            warn!("JSON-encoded payload not supported; passing raw bytes through");
            body
        }
        Ok(MessageEncoding::ZlibPb) => {
            warn!("zlib-encoded payload not supported; passing raw bytes through");
            body
        }
        Err(_) => {
            // Unknown encoding (newer agent or corrupt header) — best-effort
            // try zstd v1, fall back to raw. Mirrors dd-source byoc-usm-stats
            // `internal/decoder/decoder.go::decompress` default branch.
            zstd::decode_all(body.as_ref())
                .map(Bytes::from)
                .unwrap_or(body)
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::protos::process::CollectorConnections;

    /// Builds a V8 envelope from a `MessageHeader` and a body. The body is
    /// zstd-compressed iff the header's encoding selects a zstd variant.
    fn make_v8_envelope(header: &MessageHeader, body: &[u8]) -> Bytes {
        let header_bytes = header.encode_to_vec();
        let header_len = u16::try_from(header_bytes.len()).unwrap();
        let body_bytes = match MessageEncoding::try_from(header.encoding) {
            Ok(MessageEncoding::ZstdPb)
            | Ok(MessageEncoding::Zstd1xPb)
            | Ok(MessageEncoding::ZstdPBxNoCgo) => zstd::encode_all(body, 3).unwrap(),
            _ => body.to_vec(),
        };
        let mut envelope =
            Vec::with_capacity(V8_PREFIX_LEN + header_bytes.len() + body_bytes.len());
        envelope.push(V8_VERSION_BYTE);
        envelope.extend_from_slice(&header_len.to_le_bytes());
        envelope.extend_from_slice(&header_bytes);
        envelope.extend_from_slice(&body_bytes);
        Bytes::from(envelope)
    }

    fn raw_header() -> MessageHeader {
        MessageHeader {
            encoding: MessageEncoding::Protobuf as i32,
            ..Default::default()
        }
    }

    fn zstd_header() -> MessageHeader {
        MessageHeader {
            encoding: MessageEncoding::Zstd1xPb as i32,
            ..Default::default()
        }
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
    fn test_decode_envelope_v8_protobuf_passthrough() {
        let payload = b"hello world";
        let envelope = make_v8_envelope(&raw_header(), payload);
        let (decoded, ts) = decode_envelope(&envelope).unwrap();
        assert_eq!(decoded.as_ref(), payload);
        assert_eq!(ts, None);
    }

    #[test]
    fn test_decode_envelope_v8_zstd_decompresses() {
        let payload = b"compressed payload";
        let envelope = make_v8_envelope(&zstd_header(), payload);
        let (decoded, _ts) = decode_envelope(&envelope).unwrap();
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn test_decode_envelope_v8_empty_body() {
        let envelope = make_v8_envelope(&raw_header(), b"");
        let (decoded, ts) = decode_envelope(&envelope).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(ts, None);
    }

    #[test]
    fn envelope_carries_agent_timestamp() {
        let header = MessageHeader {
            encoding: MessageEncoding::Protobuf as i32,
            agent_timestamp: 1_700_000_000,
            ..Default::default()
        };
        let envelope = make_v8_envelope(&header, b"");
        let (body, ts) = decode_envelope(&envelope).unwrap();
        assert!(body.is_empty());
        assert_eq!(ts, Some(1_700_000_000));
    }

    #[test]
    fn envelope_falls_back_to_intake_timestamp() {
        let header = MessageHeader {
            encoding: MessageEncoding::Protobuf as i32,
            timestamp: 1_650_000_000,
            ..Default::default()
        };
        let envelope = make_v8_envelope(&header, b"");
        let (_body, ts) = decode_envelope(&envelope).unwrap();
        assert_eq!(ts, Some(1_650_000_000));
    }

    #[test]
    fn envelope_prefers_agent_over_intake_timestamp() {
        let header = MessageHeader {
            encoding: MessageEncoding::Protobuf as i32,
            timestamp: 1_650_000_000,
            agent_timestamp: 1_700_000_000,
            ..Default::default()
        };
        let envelope = make_v8_envelope(&header, b"");
        let (_body, ts) = decode_envelope(&envelope).unwrap();
        assert_eq!(ts, Some(1_700_000_000));
    }

    #[test]
    fn test_full_payload_roundtrip() {
        let cc = CollectorConnections {
            host_name: "test-host".to_string(),
            ..Default::default()
        };
        let proto_bytes = cc.encode_to_vec();
        let envelope = make_v8_envelope(&zstd_header(), &proto_bytes);

        let (decoded_bytes, _ts) = decode_envelope(&envelope).unwrap();
        let decoded = CollectorConnections::decode(decoded_bytes.as_ref()).unwrap();
        assert_eq!(decoded.host_name, "test-host");
    }

    // Note: tests for `handle_connections` require a `SourceSender` test
    // helper which is gated behind vector-core's `test` feature. Integration
    // tests exercising the full HTTP path belong in a separate tests/ file
    // once that feature is wired into pomsky-intake's dev-deps. The sync
    // envelope/proto tests above cover the source's non-plumbing logic.

    #[test]
    fn response_envelope_round_trips_through_decoder() {
        // The agent decodes the response through the same V8 envelope path it
        // uses for requests: `readResponseStatuses` in dd-go
        // `pkg/process/runner/runner.go`. Roundtripping through our own
        // `decode_envelope` is a faithful proxy: it ensures the first byte is
        // a valid version marker (not ASCII 'a'/97 like the old "accepted"
        // body), the header parses, and the body is the `ResCollector`
        // proto the agent expects.
        let envelope = encode_response_envelope();
        let (body, _ts) = decode_envelope(&envelope).expect("envelope decodes");
        let res = ResCollector::decode(body.as_ref()).expect("body is a ResCollector");
        let status = res.status.expect("status set");
        assert_eq!(status.interval, RESPONSE_INTERVAL_SECS);
    }

    #[test]
    fn response_envelope_uses_protobuf_encoding() {
        // We use raw protobuf (not zstd) for the response — the body is
        // small, and the agent's response decoder doesn't require
        // compression. If this ever changes, both sides must move together.
        let envelope = encode_response_envelope();
        // V8 prefix is [0x08][u16 LE header_len], header starts at offset 3.
        assert_eq!(envelope[0], V8_VERSION_BYTE);
        let header_len = u16::from_le_bytes([envelope[1], envelope[2]]) as usize;
        let header = MessageHeader::decode(&envelope[V8_PREFIX_LEN..V8_PREFIX_LEN + header_len])
            .expect("header decodes");
        assert_eq!(header.encoding, MessageEncoding::Protobuf as i32);
        assert_eq!(header.r#type, MessageType::TypeResCollector as i32);
    }
}
