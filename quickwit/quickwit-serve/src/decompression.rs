// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io::Read;

use bytes::{Buf, Bytes};
use flate2::read::GzDecoder;
use futures_util::{Stream, StreamExt};
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use warp::reject::Reject;
use warp::Filter;

/// Semaphore used to unsure we do not decompress too many bodies at the same time.
/// (the spawn blocking thread pool is usually meant for io and does not really limit
/// the number of threads.)
static DECOMPRESSION_PERMITS: Semaphore = Semaphore::const_new(3);

#[derive(Debug, Error)]
#[error("Error while decompressing the data")]
pub(crate) struct CorruptedData;

impl Reject for CorruptedData {}

#[derive(Debug, Error)]
#[error("Error while receiving the body")]
pub(crate) struct BodyTransferError;

impl Reject for BodyTransferError {}

#[derive(Debug, Error)]
#[error("Unsupported Content-Encoding {}. Supported encodings are 'gzip' and 'zstd'", self.0)]
pub(crate) struct UnsupportedEncoding(String);

impl Reject for UnsupportedEncoding {}

enum Compression {
    Gzip,
    Zstd,
}

impl TryFrom<String> for Compression {
    type Error = warp::Rejection;

    fn try_from(unknown_encoding: String) -> Result<Self, Self::Error> {
        match unknown_encoding.as_str() {
            "gzip" | "x-gzip" => Ok(Compression::Gzip),
            "zstd" => Ok(Compression::Zstd),
            _ => Err(warp::reject::custom(UnsupportedEncoding(unknown_encoding))),
        }
    }
}

fn append_buf_to_vec(mut buf: impl Buf, output: &mut Vec<u8>) {
    output.reserve(buf.remaining());
    while buf.has_remaining() {
        let chunk = buf.chunk();
        output.extend_from_slice(chunk);
        buf.advance(chunk.len());
    }
}

/// There are two ways to decompress the body:
/// - Stream the body through an async decompressor
/// - Fetch the body and then decompress the bytes
///
/// The first approach lowers the latency, while the second approach is more CPU efficient.
/// Ingesting data is usually CPU bound and there is considerable latency until the data is
/// searchable, so the second approach is more suitable for this use case.
fn decompress_body(
    encoding: String,
    compressed_body: &[u8],
    mut gauge_guard: GaugeGuard,
) -> Result<Body, warp::Rejection> {
    let compression: Compression = Compression::try_from(encoding)?;
    let compressed_body_len = compressed_body.len();
    let decompressed_body_len = compressed_body_len * 10;
    gauge_guard.add(decompressed_body_len as i64);
    let mut decompressed_body = Vec::with_capacity(decompressed_body_len);
    match compression {
        Compression::Gzip => GzDecoder::new(compressed_body).read_to_end(&mut decompressed_body),
        Compression::Zstd => zstd::Decoder::with_buffer(compressed_body)
            .and_then(|mut decoder| decoder.read_to_end(&mut decompressed_body)),
    }
    .map_err(|_| warp::reject::custom(CorruptedData))?;
    Ok(Body::new(decompressed_body, gauge_guard))
}

/// Gets the body from the stream and decompresses it if necessary.
async fn get_decompressed_body(
    content_length_opt: Option<u64>,
    encoding: Option<String>,
    mut body_stream: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + Send + Sync,
) -> Result<Body, warp::Rejection> {
    let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.rest_server);
    let mut buffer = Vec::new();
    if let Some(content_length) = content_length_opt {
        gauge_guard.add(content_length as i64);
        buffer.reserve_exact(content_length as usize);
    }
    while let Some(body_chunk_res) = body_stream.next().await {
        let Ok(body_chunk) = body_chunk_res else {
            return Err(warp::reject::custom(BodyTransferError));
        };
        append_buf_to_vec(body_chunk, &mut buffer);
        gauge_guard.set_delta(buffer.capacity() as i64);
    }
    // At this point we have the entire buffer.
    // We may still need to decompress it.
    if let Some(encoding) = encoding {
        let _decompression_permit: SemaphorePermit = DECOMPRESSION_PERMITS.acquire().await.unwrap();
        tokio::task::spawn_blocking(move || decompress_body(encoding, &buffer[..], gauge_guard))
            .await
            .map_err(|_| warp::reject::custom(CorruptedData))?
    } else {
        Ok(Body::new(buffer, gauge_guard))
    }
}

/// Custom filter for optional decompression
pub(crate) fn get_body_bytes() -> impl Filter<Extract = (Body,), Error = warp::Rejection> + Clone {
    warp::header::optional::<u64>("content-length")
        .and(warp::header::optional("content-encoding"))
        .and(warp::body::stream())
        .and_then(get_decompressed_body)
}

pub(crate) struct Body {
    pub content: Bytes,
    _gauge_guard: GaugeGuard,
}

impl Body {
    pub fn new(mut content: Vec<u8>, mut gauge_guard: GaugeGuard) -> Body {
        content.shrink_to_fit();
        gauge_guard.set_delta(content.capacity() as i64);
        Body {
            content: Bytes::from(content),
            _gauge_guard: gauge_guard,
        }
    }
}
