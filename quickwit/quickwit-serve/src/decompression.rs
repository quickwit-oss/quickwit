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
use quickwit_metrics::{GaugeGuard, counter};
use thiserror::Error;
use warp::Filter;
use warp::reject::Reject;

use crate::load_shield::{LoadShield, LoadShieldPermit};
use crate::metrics::DECOMPRESSION_TOO_LARGE_TOTAL;

fn get_ingest_load_shield() -> &'static LoadShield {
    static LOAD_SHIELD: LazyLock<LoadShield> = LazyLock::new(|| LoadShield::new("ingest"));
    &LOAD_SHIELD
}

/// Decodes `reader` into memory while enforcing `limit` on the decompressed size.
///
/// We read at most `limit + 1` bytes: that is enough to detect an overflow while guaranteeing we
/// never inflate (and therefore never allocate or spend CPU on) more than the limit, even when the
/// compressed input would expand to many gigabytes. This is what prevents decompression-bomb OOMs:
/// the on-the-wire content length limit only caps the *compressed* size, which says nothing about
/// how much a body expands.
fn decode_to_limit<R: Read>(
    reader: R,
    limit: ByteSize,
    encoding: &str,
    compressed_len: usize,
    endpoint: &'static str,
) -> Result<Bytes, warp::Rejection> {
    let mut decompressed = Vec::new();
    reader
        .take(limit.as_u64() + 1)
        .read_to_end(&mut decompressed)
        .map_err(|_| warp::reject::custom(CorruptedData))?;
    if decompressed.len() as u64 > limit.as_u64() {
        counter!(parent: DECOMPRESSION_TOO_LARGE_TOTAL, "endpoint" => endpoint).inc();
        rate_limited_warn!(
            limit_per_min = 6,
            endpoint,
            encoding,
            compressed_len,
            max_decompressed_body_size = limit.as_u64(),
            "rejecting ingest request: decompressed body exceeds the maximum allowed size"
        );
        return Err(warp::reject::custom(DecompressedPayloadTooLarge(limit)));
    }
    Ok(Bytes::from(decompressed))
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
        // Uncompressed bodies cannot amplify, and are already bounded by the (compressed)
        // content length limit filter applied upstream of this one, so no extra check is needed.
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
            let decoder = zstd::Decoder::new(body.as_ref())
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
    self.0.display().si()
)]
pub(crate) struct DecompressedPayloadTooLarge(ByteSize);

impl Reject for DecompressedPayloadTooLarge {}

/// Custom filter for optional decompression.
///
/// `max_decompressed_body_size` bounds the size of the body *after* decompression, protecting
/// against decompression bombs (see [`decode_to_limit`]).
pub(crate) fn get_body_bytes(
    max_decompressed_body_size: ByteSize,
    endpoint: &'static str,
) -> impl Filter<Extract = (Body,), Error = warp::Rejection> + Clone {
    warp::header::optional("content-encoding")
        .and(warp::body::bytes())
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

    // A generous limit for tests that exercise the happy path.
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
        // ~16 KiB decompressed against a 1 KiB limit: every encoding must be rejected, exercising
        // the full `decompress_body` path (and the `take()` bound) without inflating anything
        // large.
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
