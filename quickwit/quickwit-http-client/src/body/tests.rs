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

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};
use std::time::Duration;

use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

use super::decoder::{MAX_CHUNK_SIZE_LINE_SIZE, MAX_TRAILER_SECTION_SIZE};
use super::*;

struct FragmentedReader {
    inner: Cursor<Vec<u8>>,
    max_read: usize,
}

impl FragmentedReader {
    fn new(bytes: impl Into<Vec<u8>>, max_read: usize) -> Self {
        Self {
            inner: Cursor::new(bytes.into()),
            max_read,
        }
    }
}

impl AsyncRead for FragmentedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        read_buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let position = self.inner.position() as usize;
        let available = &self.inner.get_ref()[position..];
        let read_len = available.len().min(self.max_read).min(read_buf.remaining());
        read_buf.put_slice(&available[..read_len]);
        self.inner.set_position((position + read_len) as u64);
        Poll::Ready(Ok(()))
    }
}

async fn collect_body<R: AsyncRead + Unpin>(
    body: &mut ResponseBody<R>,
) -> Result<Vec<Bytes>, HttpError> {
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            frames.push(data);
        }
    }
    Ok(frames)
}

fn concat(frames: &[Bytes]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes
}

fn body<R: AsyncRead + Unpin>(
    reader: R,
    strategy: BodyStrategy,
    leftover: impl Into<Bytes>,
    target: usize,
) -> ResponseBody<R> {
    ResponseBody::new(
        reader,
        strategy,
        leftover.into(),
        BufferHint { target },
        Duration::from_secs(5),
        None,
        false,
    )
}

#[tokio::test]
async fn known_length_single_frame_from_fragmented_reads() {
    let expected = vec![b'x'; 20 * 1024];
    let reader = FragmentedReader::new(expected.clone(), 317);
    let mut body = body(
        reader,
        BodyStrategy::Known(expected.len()),
        Bytes::new(),
        32 * 1024,
    );

    assert_eq!(body.size_hint().exact(), Some(expected.len() as u64));
    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(concat(&frames), expected);
    assert_eq!(body.consumed(), expected.len());
    assert!(body.is_end_stream());
}

#[tokio::test]
async fn known_length_coalesces_to_at_least_target_sized_frames() {
    let target = 8 * 1024;
    let expected = vec![b'x'; 20 * 1024];
    let reader = FragmentedReader::new(expected.clone(), 509);
    let mut body = body(
        reader,
        BodyStrategy::Known(expected.len()),
        Bytes::new(),
        target,
    );

    let frames = collect_body(&mut body).await.unwrap();
    assert!(
        frames[..frames.len() - 1]
            .iter()
            .all(|frame| frame.len() >= target)
    );
    assert_eq!(concat(&frames), expected);
}

#[tokio::test]
async fn chunked_coalesces_across_wire_chunks_and_fragmented_reads() {
    let mut encoded = Vec::new();
    let mut expected = Vec::new();
    for byte in 0u8..16 {
        let chunk = vec![b'a' + byte; 1024];
        encoded.extend_from_slice(b"400\r\n");
        encoded.extend_from_slice(&chunk);
        encoded.extend_from_slice(b"\r\n");
        expected.extend_from_slice(&chunk);
    }
    encoded.extend_from_slice(b"0\r\n\r\n");
    let reader = FragmentedReader::new(encoded, 113);
    let mut body = body(reader, BodyStrategy::Chunked, Bytes::new(), 8 * 1024);

    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(
        frames.iter().map(Bytes::len).collect::<Vec<_>>(),
        [8 * 1024, 8 * 1024]
    );
    assert_eq!(concat(&frames), expected);
}

#[tokio::test]
async fn chunk_extensions_and_trailers_can_span_reads() {
    let encoded = b"b;extension-name=extension-value\r\nhello world\r\n\
                    0\r\nx-checksum: abcdefghijklmnopqrstuvwxyz\r\n\r\n";
    let reader = FragmentedReader::new(encoded, 3);
    let mut body = body(reader, BodyStrategy::Chunked, Bytes::new(), 8 * 1024);

    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(&concat(&frames), b"hello world");
}

#[tokio::test]
async fn chunk_size_line_is_limited() {
    let mut encoded = b"1;".to_vec();
    encoded.extend(std::iter::repeat_n(b'x', MAX_CHUNK_SIZE_LINE_SIZE));
    encoded.extend_from_slice(b"\r\na\r\n0\r\n\r\n");
    let reader = FragmentedReader::new(encoded, 257);
    let mut body = body(reader, BodyStrategy::Chunked, Bytes::new(), 8 * 1024);

    let error = body.frame().await.unwrap().unwrap_err();
    let HttpError::InvalidLength(message) = error else {
        panic!("expected InvalidLength, got {error:?}");
    };
    assert!(message.contains("chunk-size line exceeded"));
}

#[tokio::test]
async fn aggregate_trailer_section_is_limited() {
    let mut encoded = b"0\r\n".to_vec();
    while encoded.len() <= MAX_TRAILER_SECTION_SIZE {
        encoded.extend_from_slice(b"x: ");
        encoded.extend(std::iter::repeat_n(b'x', 1024));
        encoded.extend_from_slice(b"\r\n");
    }
    encoded.extend_from_slice(b"\r\n");
    let reader = FragmentedReader::new(encoded, 257);
    let mut body = body(reader, BodyStrategy::Chunked, Bytes::new(), 8 * 1024);

    let error = body.frame().await.unwrap().unwrap_err();
    let HttpError::InvalidLength(message) = error else {
        panic!("expected InvalidLength, got {error:?}");
    };
    assert!(message.contains("trailer section exceeded"));
}

#[tokio::test]
async fn until_close_coalesces_fragmented_reads() {
    let expected = vec![b'z'; 20 * 1024];
    let reader = FragmentedReader::new(expected.clone(), 251);
    let mut body = body(reader, BodyStrategy::UntilClose, Bytes::new(), 8 * 1024);

    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(
        frames.iter().map(Bytes::len).collect::<Vec<_>>(),
        [8 * 1024, 8 * 1024, 4 * 1024]
    );
    assert_eq!(concat(&frames), expected);
}

#[tokio::test]
async fn truncated_known_body_errors_once_and_then_ends() {
    let reader = FragmentedReader::new(b"abc", 1);
    let mut body = body(reader, BodyStrategy::Known(5), Bytes::new(), 8 * 1024);

    let error = body.frame().await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        HttpError::UnexpectedEof {
            read: 3,
            expected: 5
        }
    ));
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn malformed_chunked_body_errors_once_and_then_ends() {
    let reader = FragmentedReader::new(b"3\r\nabcXX", 1);
    let mut body = body(reader, BodyStrategy::Chunked, Bytes::new(), 8 * 1024);

    let error = body.frame().await.unwrap().unwrap_err();
    assert!(matches!(error, HttpError::InvalidLength(_)));
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn leftover_longer_than_content_length_is_an_error() {
    let reader = Cursor::new(Vec::new());
    let mut body = body(reader, BodyStrategy::Known(5), &b"helloXY"[..], 8 * 1024);

    let error = body.frame().await.unwrap().unwrap_err();
    assert!(matches!(error, HttpError::InvalidLength(_)));
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn per_read_timeout_fires() {
    let (client, mut server) = tokio::io::duplex(1024);
    let mut body = ResponseBody::new(
        client,
        BodyStrategy::Known(5),
        Bytes::new(),
        BufferHint { target: 8 * 1024 },
        Duration::from_millis(20),
        None,
        false,
    );

    let error = body.frame().await.unwrap().unwrap_err();
    assert!(error.is_timeout(), "expected timeout, got {error:?}");
    assert!(body.frame().await.is_none());
    server.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn read_timeout_resets_when_bytes_keep_arriving() {
    let (reader, mut writer) = tokio::io::duplex(1024);
    let writer_task = tokio::spawn(async move {
        for byte in b"abc" {
            writer.write_all(&[*byte]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(4)).await;
        }
    });
    let mut body = ResponseBody::new(
        reader,
        BodyStrategy::Known(3),
        Bytes::new(),
        BufferHint { target: 8 * 1024 },
        Duration::from_secs(5),
        None,
        false,
    );

    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(&concat(&frames), b"abc");
    writer_task.await.unwrap();
}

#[tokio::test]
async fn empty_body_is_already_complete() {
    let reader = Cursor::new(Vec::new());
    let mut body = body(reader, BodyStrategy::Empty, Bytes::new(), 8 * 1024);

    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn until_close_clean_eos_is_not_pooled() {
    let reader = Cursor::new(b"abc".to_vec());
    let pooled = Arc::new(AtomicUsize::new(0));
    let pooled_for_hook = pooled.clone();
    let pool_hook: Option<Box<dyn FnOnce(_) + Send + 'static>> = Some(Box::new(move |_| {
        pooled_for_hook.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    let mut body = ResponseBody::new(
        reader,
        BodyStrategy::UntilClose,
        Bytes::new(),
        BufferHint { target: 8 * 1024 },
        Duration::from_secs(5),
        pool_hook,
        true, // keep-alive held in the head, but UntilClose is still not poolable
    );

    let frames = collect_body(&mut body).await.unwrap();
    assert_eq!(&concat(&frames), b"abc");
    assert!(body.is_end_stream(), "reached a clean EOS");
    drop(body);
    assert_eq!(
        pooled.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "UntilClose EOS is a TCP close; the pool hook must not fire"
    );
}
