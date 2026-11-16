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

use std::io::Read;
use std::sync::LazyLock;

use bytes::Bytes;
use bytesize::ByteSize;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use quickwit_common::metrics::IN_FLIGHT_REST_SERVER;
use quickwit_common::rate_limited_warn;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_metrics::GaugeGuard;
use thiserror::Error;
use warp::Filter;
use warp::reject::Reject;

use crate::body_limit::{ReadDecodedError, bounded_body_bytes, read_decoded};
use crate::load_shield::{LoadShield, LoadShieldPermit};

fn get_ingest_load_shield() -> &'static LoadShield {
    static LOAD_SHIELD: LazyLock<LoadShield> = LazyLock::new(|| LoadShield::new("ingest"));
    &LOAD_SHIELD
}

/// Decodes `reader`, rejecting with [`DecompressedPayloadTooLarge`] if the output exceeds `limit`.
/// Decoding stops after `limit + 1` bytes, so a decompression bomb is never fully inflated.
fn decode_to_limit<R: Read>(
    reader: R,
    limit: ByteSize,
    encoding: &str,
    compressed_len: usize,
    endpoint: &'static str,
) -> Result<Bytes, warp::Rejection> {
    match read_decoded(reader, limit) {
        Ok(decompressed) => Ok(Bytes::from(decompressed)),
        Err(ReadDecodedError::LimitExceeded) => {
            rate_limited_warn!(
                limit_per_min = 6,
                endpoint,
                encoding,
                compressed_len,
                max_decompressed_body_size = limit.as_u64(),
                "rejecting ingest request: decompressed body exceeds the maximum allowed size"
            );
            Err(warp::reject::custom(DecompressedPayloadTooLarge(limit)))
        }
        Err(ReadDecodedError::Io(error)) => {
            rate_limited_warn!(
                limit_per_min = 6,
                endpoint,
                encoding,
                compressed_len,
                %error,
                "rejecting ingest request: failed to decompress body"
            );
            Err(warp::reject::custom(CorruptedData))
        }
    }
}

/// Window log of zstd level 19. Streaming encoders declare their level's window regardless of the
/// payload size, so we accept up to this even under smaller limits.
const ZSTD_MIN_ACCEPTED_WINDOW_LOG: u32 = 23;

/// `ZSTD_WINDOWLOG_LIMIT_DEFAULT` (128 MiB).
const ZSTD_MAX_ACCEPTED_WINDOW_LOG: u32 = 27;

/// Max zstd window log for `limit`: `ceil(log2(limit))`, clamped. The decoder allocates the window
/// from the frame header before producing output, so `decode_to_limit` does not bound it.
fn zstd_window_log_max(limit: ByteSize) -> u32 {
    let ceil_log2_limit = match limit.as_u64().checked_next_power_of_two() {
        Some(power_of_two) => power_of_two.trailing_zeros(),
        None => u64::BITS,
    };
    ceil_log2_limit.clamp(ZSTD_MIN_ACCEPTED_WINDOW_LOG, ZSTD_MAX_ACCEPTED_WINDOW_LOG)
}

/// There are two ways to decompress the body:
/// - Stream the body through an async decompressor
/// - Fetch the body and then decompress the bytes
///
/// The first approach lowers the latency, while the second approach is more CPU efficient.
/// Ingesting data is usually CPU bound and there is considerable latency until the data is
/// searchable, so the second approach is more suitable for this use case.
async fn decompress_body(
    encoding: Option<String>,
    body: Bytes,
    max_decompressed_body_size: ByteSize,
    endpoint: &'static str,
) -> Result<Bytes, warp::Rejection> {
    match encoding.as_deref() {
        // Already bounded by `max_body_size`.
        Some("identity") => Ok(body),
        Some("gzip" | "x-gzip") => run_cpu_intensive(move || {
            let compressed_len = body.len();
            decode_to_limit(
                MultiGzDecoder::new(body.as_ref()),
                max_decompressed_body_size,
                "gzip",
                compressed_len,
                endpoint,
            )
        })
        .await
        .map_err(|_| warp::reject::custom(CorruptedData))?,
        Some("zstd") => run_cpu_intensive(move || {
            let compressed_len = body.len();
            let mut decoder = zstd::Decoder::new(body.as_ref())
                .map_err(|_| warp::reject::custom(CorruptedData))?;
            // Frames with a larger window fail on first read.
            decoder
                .window_log_max(zstd_window_log_max(max_decompressed_body_size))
                .map_err(|_| warp::reject::custom(CorruptedData))?;
            decode_to_limit(
                decoder,
                max_decompressed_body_size,
                "zstd",
                compressed_len,
                endpoint,
            )
        })
        .await
        .map_err(|_| warp::reject::custom(CorruptedData))?,
        Some("deflate" | "x-deflate") => run_cpu_intensive(move || {
            let compressed_len = body.len();
            decode_to_limit(
                ZlibDecoder::new(body.as_ref()),
                max_decompressed_body_size,
                "deflate",
                compressed_len,
                endpoint,
            )
        })
        .await
        .map_err(|_| warp::reject::custom(CorruptedData))?,
        Some(encoding) => Err(warp::reject::custom(UnsupportedEncoding(
            encoding.to_string(),
        ))),
        _ => Ok(body),
    }
}

#[derive(Debug, Error)]
#[error("Error while decompressing the data")]
pub(crate) struct CorruptedData;

impl Reject for CorruptedData {}

#[derive(Debug, Error)]
#[error("Unsupported Content-Encoding {}. Supported encodings are 'gzip' and 'zstd'", self.0)]
pub(crate) struct UnsupportedEncoding(String);

impl Reject for UnsupportedEncoding {}

#[derive(Debug, Error)]
#[error(
    "decompressed request body exceeds the maximum allowed size of {}",
    self.0.display().iec()
)]
pub(crate) struct DecompressedPayloadTooLarge(ByteSize);

impl Reject for DecompressedPayloadTooLarge {}

/// Reads and optionally decompresses the body, bounded by `max_body_size` on the wire and by
/// `max_decompressed_body_size` after decompression.
pub(crate) fn get_body_bytes(
    max_body_size: ByteSize,
    max_decompressed_body_size: ByteSize,
    endpoint: &'static str,
) -> impl Filter<Extract = (Body,), Error = warp::Rejection> + Clone {
    warp::header::optional("content-encoding")
        .and(bounded_body_bytes(max_body_size))
        .and_then(move |encoding: Option<String>, body: Bytes| async move {
            let permit = get_ingest_load_shield().acquire_permit().await?;
            decompress_body(encoding, body, max_decompressed_body_size, endpoint)
                .await
                .map(|content| Body::new(content, permit))
        })
}

pub(crate) struct Body {
    pub content: Bytes,
    _gauge_guard: GaugeGuard,
    _permit: LoadShieldPermit,
}

impl Body {
    pub fn new(content: Bytes, load_shield_permit: LoadShieldPermit) -> Body {
        let gauge_guard = GaugeGuard::new(&IN_FLIGHT_REST_SERVER, content.len() as f64);
        Body {
            content,
            _gauge_guard: gauge_guard,
            _permit: load_shield_permit,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::{GzEncoder, ZlibEncoder};

    use super::*;

    const MAX_DECOMPRESSED: ByteSize = ByteSize::mib(256);

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn deflate(data: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 0).unwrap()
    }

    #[tokio::test]
    async fn test_decompress_body_roundtrip_under_limit() {
        let payload = b"hello world, this is a small ingest payload".repeat(100);
        for (encoding, compressed) in [
            ("gzip", gzip(&payload)),
            ("zstd", zstd_compress(&payload)),
            ("deflate", deflate(&payload)),
        ] {
            let decompressed = decompress_body(
                Some(encoding.to_string()),
                Bytes::from(compressed),
                MAX_DECOMPRESSED,
                "test",
            )
            .await
            .unwrap_or_else(|_| panic!("`{encoding}` body should decompress"));
            assert_eq!(
                decompressed.as_ref(),
                payload.as_slice(),
                "encoding={encoding}"
            );
        }
    }

    #[tokio::test]
    async fn test_decompress_body_identity_and_none_passthrough() {
        let body = Bytes::from_static(b"raw uncompressed body");
        let out = decompress_body(
            Some("identity".to_string()),
            body.clone(),
            MAX_DECOMPRESSED,
            "test",
        )
        .await
        .unwrap();
        assert_eq!(out, body);
        let out = decompress_body(None, body.clone(), MAX_DECOMPRESSED, "test")
            .await
            .unwrap();
        assert_eq!(out, body);
    }

    #[tokio::test]
    async fn test_decompress_body_unsupported_encoding() {
        let error = decompress_body(
            Some("br".to_string()),
            Bytes::from_static(b"x"),
            MAX_DECOMPRESSED,
            "test",
        )
        .await
        .unwrap_err();
        assert!(error.find::<UnsupportedEncoding>().is_some());
    }

    #[tokio::test]
    async fn test_decompress_body_rejects_over_limit() {
        let payload = vec![0u8; 16 * 1024];
        for (encoding, compressed) in [
            ("gzip", gzip(&payload)),
            ("zstd", zstd_compress(&payload)),
            ("deflate", deflate(&payload)),
        ] {
            let error = decompress_body(
                Some(encoding.to_string()),
                Bytes::from(compressed),
                ByteSize::kib(1),
                "test",
            )
            .await
            .unwrap_err();
            assert!(
                error.find::<DecompressedPayloadTooLarge>().is_some(),
                "encoding={encoding}"
            );
        }
    }

    #[test]
    fn test_zstd_window_log_max() {
        // Clamped up to the minimum.
        assert_eq!(zstd_window_log_max(ByteSize::b(0)), 23);
        assert_eq!(zstd_window_log_max(ByteSize::kib(1)), 23);
        // Lower edge: exactly 2^23, then just above it.
        assert_eq!(zstd_window_log_max(ByteSize::mib(8)), 23);
        assert_eq!(zstd_window_log_max(ByteSize::mib(8) + ByteSize::b(1)), 24);
        // Unclamped range.
        assert_eq!(zstd_window_log_max(ByteSize::mib(16)), 24);
        assert_eq!(zstd_window_log_max(ByteSize::mib(64) + ByteSize::b(1)), 27);
        // Upper edge: exactly 2^27.
        assert_eq!(zstd_window_log_max(ByteSize::mib(128)), 27);
        // Clamped down to the maximum.
        assert_eq!(zstd_window_log_max(ByteSize::mib(128) + ByteSize::b(1)), 27);
        assert_eq!(zstd_window_log_max(ByteSize::mib(256)), 27);
        assert_eq!(zstd_window_log_max(ByteSize::b(u64::MAX)), 27);
    }

    /// Compresses like a streaming client: no content size in the frame header.
    fn zstd_streaming_compress(payload: &[u8], level: i32, window_log: Option<u32>) -> Bytes {
        let mut encoder = zstd::Encoder::new(Vec::new(), level).unwrap();
        if let Some(window_log) = window_log {
            encoder.window_log(window_log).unwrap();
        }
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(payload).unwrap();
        Bytes::from(encoder.finish().unwrap())
    }

    #[tokio::test]
    async fn test_decompress_body_zstd_accepts_streaming_window_under_small_limit() {
        // Level 19 declares an 8 MiB window, even for a tiny payload.
        let payload = b"tiny payload";
        let compressed = zstd_streaming_compress(payload, 19, None);
        let decompressed = decompress_body(
            Some("zstd".to_string()),
            compressed,
            ByteSize::kib(64),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(decompressed.as_ref(), payload);
    }

    #[tokio::test]
    async fn test_decompress_body_zstd_rejects_window_over_limit() {
        let payload = b"tiny payload";
        let compressed = zstd_streaming_compress(payload, 0, Some(25));

        let error = decompress_body(
            Some("zstd".to_string()),
            compressed.clone(),
            ByteSize::mib(16),
            "test",
        )
        .await
        .unwrap_err();
        assert!(error.find::<CorruptedData>().is_some());

        let decompressed = decompress_body(
            Some("zstd".to_string()),
            compressed,
            ByteSize::mib(32),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(decompressed.as_ref(), payload);
    }

    #[test]
    fn test_decode_to_limit_accepts_up_to_limit() {
        let data = vec![0u8; 100];
        let out = decode_to_limit(
            data.as_slice(),
            ByteSize::b(100),
            "identity",
            data.len(),
            "test",
        )
        .unwrap();
        assert_eq!(out.len(), 100);
    }

    #[test]
    fn test_decode_to_limit_rejects_overflow() {
        let data = vec![0u8; 1024];
        let error = decode_to_limit(
            data.as_slice(),
            ByteSize::b(100),
            "identity",
            data.len(),
            "test",
        )
        .unwrap_err();
        assert!(error.find::<DecompressedPayloadTooLarge>().is_some());
    }
}
