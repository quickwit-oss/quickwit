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

use std::io::{self, Read};

use bytes::{Buf, Bytes};
use bytesize::ByteSize;
use futures::{Stream, StreamExt};
use thiserror::Error;
use warp::Filter;
use warp::reject::Reject;

#[derive(Debug, Error)]
#[error(
    "request body exceeds the maximum allowed size of {}",
    self.0.display().iec()
)]
pub(crate) struct PayloadTooLarge(pub ByteSize);

impl Reject for PayloadTooLarge {}

#[derive(Debug, Error)]
#[error("failed to read request body: {0}")]
pub(crate) struct BodyReadError(String);

impl Reject for BodyReadError {}

/// Buffers the request body, rejecting with [`PayloadTooLarge`] once it exceeds `limit`. Unlike
/// `warp::body::content_length_limit`, does not require a `Content-Length` header.
pub(crate) fn bounded_body_bytes(
    limit: ByteSize,
) -> impl Filter<Extract = (Bytes,), Error = warp::Rejection> + Clone {
    warp::header::optional::<u64>("content-length")
        .and(warp::body::stream())
        .and_then(
            move |content_length_opt: Option<u64>, body_stream| async move {
                read_body_stream(body_stream, content_length_opt, limit)
                    .await
                    .map(Bytes::from)
            },
        )
}

async fn read_body_stream<S, B>(
    body_stream: S,
    content_length_opt: Option<u64>,
    limit: ByteSize,
) -> Result<Vec<u8>, warp::Rejection>
where
    S: Stream<Item = Result<B, warp::Error>>,
    B: Buf,
{
    // Reject early when the header announces an oversized body. Not used to presize the buffer:
    // a client could then reserve `limit` bytes per connection without sending anything.
    if let Some(content_length) = content_length_opt
        && content_length > limit.as_u64()
    {
        return Err(warp::reject::custom(PayloadTooLarge(limit)));
    }
    let max_len = limit.as_u64() as usize;
    let mut buffer: Vec<u8> = Vec::new();
    let mut body_stream = std::pin::pin!(body_stream);

    while let Some(chunk_res) = body_stream.next().await {
        let mut chunk =
            chunk_res.map_err(|error| warp::reject::custom(BodyReadError(error.to_string())))?;
        if buffer.len() + chunk.remaining() > max_len {
            return Err(warp::reject::custom(PayloadTooLarge(limit)));
        }
        reserve_capped(&mut buffer, chunk.remaining(), max_len);
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            buffer.extend_from_slice(bytes);
            let num_bytes = bytes.len();
            chunk.advance(num_bytes);
        }
    }
    Ok(buffer)
}

/// Reserves room for `additional` bytes, doubling the capacity but never past `max_capacity`.
///
/// Precondition: `buffer.len() + additional <= max_capacity`.
fn reserve_capped(buffer: &mut Vec<u8>, additional: usize, max_capacity: usize) {
    let required_len = buffer.len() + additional;
    if required_len <= buffer.capacity() {
        return;
    }
    let target_capacity = (buffer.capacity() * 2).max(required_len).min(max_capacity);
    buffer.reserve_exact(target_capacity - buffer.len());
}

const READ_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Error)]
pub(crate) enum ReadDecodedError {
    #[error("decoded body exceeds the limit")]
    LimitExceeded,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Reads `reader` to the end, failing with [`ReadDecodedError::LimitExceeded`] once the output
/// exceeds `limit`. Unlike `Read::read_to_end`, never reserves more than `limit`.
///
/// Precondition: `limit` is bounded by the config (at most 4 GiB).
pub(crate) fn read_decoded<R: Read>(
    reader: R,
    limit: ByteSize,
) -> Result<Vec<u8>, ReadDecodedError> {
    let max_len = limit.as_u64() as usize;
    // Reading one extra byte is enough to detect an overflow.
    let mut reader = reader.take(limit.as_u64() + 1);
    let mut chunk = vec![0u8; READ_CHUNK_SIZE];
    let mut buffer: Vec<u8> = Vec::new();

    loop {
        let num_bytes = match reader.read(&mut chunk) {
            Ok(0) => return Ok(buffer),
            Ok(num_bytes) => num_bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        };
        if buffer.len() + num_bytes > max_len {
            return Err(ReadDecodedError::LimitExceeded);
        }
        reserve_capped(&mut buffer, num_bytes, max_len);
        buffer.extend_from_slice(&chunk[..num_bytes]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_decoded() {
        // Exactly at the limit.
        let limit = ByteSize::b(3 * READ_CHUNK_SIZE as u64);
        let data = vec![1u8; limit.as_u64() as usize];
        let buffer = read_decoded(data.as_slice(), limit).unwrap();
        assert_eq!(buffer, data);
        assert_eq!(buffer.capacity() as u64, limit.as_u64());

        // One byte over the limit.
        let data = vec![1u8; limit.as_u64() as usize + 1];
        let error = read_decoded(data.as_slice(), limit).unwrap_err();
        assert!(matches!(error, ReadDecodedError::LimitExceeded));

        let buffer = read_decoded(&[][..], limit).unwrap();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_read_decoded_retries_on_interrupted() {
        struct InterruptOnce<'a> {
            data: &'a [u8],
            interrupted: bool,
        }
        impl Read for InterruptOnce<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                self.data.read(buf)
            }
        }
        let reader = InterruptOnce {
            data: b"hello",
            interrupted: false,
        };
        let buffer = read_decoded(reader, ByteSize::kib(1)).unwrap();
        assert_eq!(buffer, b"hello");
    }

    fn chunked_stream(
        chunks: &[&'static [u8]],
    ) -> impl Stream<Item = Result<Bytes, warp::Error>> + use<> {
        let chunks: Vec<Result<Bytes, warp::Error>> = chunks
            .iter()
            .map(|chunk| Ok(Bytes::from_static(chunk)))
            .collect();
        futures::stream::iter(chunks)
    }

    #[tokio::test]
    async fn test_read_body_stream_accepts_chunked_body_without_content_length() {
        let body = read_body_stream(
            chunked_stream(&[b"hello ", b"world"]),
            None,
            ByteSize::b(11),
        )
        .await
        .unwrap();
        assert_eq!(body, b"hello world");
    }

    #[tokio::test]
    async fn test_read_body_stream_rejects_chunked_body_over_limit() {
        let error = read_body_stream(
            chunked_stream(&[b"hello ", b"world"]),
            None,
            ByteSize::b(10),
        )
        .await
        .unwrap_err();
        assert!(error.find::<PayloadTooLarge>().is_some());
    }

    #[tokio::test]
    async fn test_read_body_stream_ignores_content_length_for_capacity() {
        let body = read_body_stream(chunked_stream(&[b"hi"]), Some(1_000_000), ByteSize::mib(1))
            .await
            .unwrap();
        assert_eq!(body, b"hi");
        assert!(body.capacity() < 1_000_000);
    }

    #[tokio::test]
    async fn test_read_body_stream_never_reserves_past_limit() {
        // Two 6-byte chunks against a 12-byte limit: doubling after the first would reserve more.
        let body = read_body_stream(
            chunked_stream(&[b"abcdef", b"ghijkl"]),
            None,
            ByteSize::b(12),
        )
        .await
        .unwrap();
        assert_eq!(body, b"abcdefghijkl");
        assert_eq!(body.capacity(), 12);
    }

    #[test]
    fn test_reserve_capped() {
        let mut buffer: Vec<u8> = Vec::new();
        reserve_capped(&mut buffer, 10, 100);
        assert!(buffer.capacity() >= 10 && buffer.capacity() <= 100);

        buffer.resize(buffer.capacity(), 0);
        reserve_capped(&mut buffer, 1, 15);
        assert_eq!(buffer.capacity(), 15);
    }

    #[tokio::test]
    async fn test_read_body_stream_rejects_early_on_content_length() {
        let error = read_body_stream(chunked_stream(&[]), Some(11), ByteSize::b(10))
            .await
            .unwrap_err();
        assert!(error.find::<PayloadTooLarge>().is_some());
    }

    #[tokio::test]
    async fn test_bounded_body_bytes_filter() {
        let filter = bounded_body_bytes(ByteSize::b(5));

        let body = warp::test::request()
            .body("hello")
            .filter(&filter)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"hello");

        let error = warp::test::request()
            .body("hello!")
            .filter(&filter)
            .await
            .unwrap_err();
        assert!(error.find::<PayloadTooLarge>().is_some());
    }
}
