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

use std::io::{Cursor, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use futures::Stream;
use http_body::{Body, Frame, SizeHint};
use tokio::io::{AsyncRead, AsyncReadExt, Chain, ReadBuf};
use tokio::time::Sleep;
use tokio_util::codec::{Decoder, FramedRead};

use crate::error::HttpError;
use crate::response::BodyStrategy;

/// Frame coalescing target. Read-ahead can make frames larger than `target`,
/// while a known body no larger than `target` is yielded in one frame.
#[derive(Clone, Copy, Debug)]
pub struct BufferHint {
    pub target: usize,
}

impl BufferHint {
    /// Default block size balancing TTFB and allocation overhead.
    pub const DEFAULT: BufferHint = BufferHint { target: 256 * 1024 };
}

/// Default per-read idle timeout.
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);

const MIN_TARGET: usize = 8 * 1024;
const MAX_CHUNK_SIZE_LINE_SIZE: usize = 8 * 1024;
const MAX_TRAILER_SECTION_SIZE: usize = 64 * 1024;

type PrefixedReader<R> = Chain<Cursor<Bytes>, R>;
type BodyFramedReader<R> = FramedRead<IdleTimeoutReader<PrefixedReader<R>>, HttpBodyDecoder>;

/// Adds a progress-based idle timeout to an [`AsyncRead`].
struct IdleTimeoutReader<R> {
    inner: R,
    timeout: Duration,
    sleep: Pin<Box<Sleep>>,
    sleep_armed: bool,
}

impl<R> IdleTimeoutReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            sleep_armed: false,
        }
    }

    fn reset_timeout(&mut self) {
        self.sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + self.timeout);
        self.sleep_armed = true;
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for IdleTimeoutReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        read_buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.sleep_armed {
            this.reset_timeout();
        }

        let filled_before = read_buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, read_buf) {
            Poll::Ready(Ok(())) => {
                if read_buf.filled().len() > filled_before {
                    this.reset_timeout();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => {
                if this.sleep.as_mut().poll(cx).is_ready() {
                    this.sleep_armed = false;
                    Poll::Ready(Err(std::io::Error::new(
                        ErrorKind::TimedOut,
                        "response read timed out",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

#[derive(Debug)]
enum DecodeState {
    Known { expected: usize, remaining: usize },
    Chunked(ChunkedState),
    UntilClose,
    Done,
    Invalid(Option<HttpError>),
}

#[derive(Debug)]
enum ChunkedState {
    Size,
    Data { remaining: usize },
    AfterCrlf,
    Trailers,
}

enum DecodedItem {
    Data { bytes: Bytes, end_stream: bool },
    End,
}

#[derive(Debug)]
struct HttpBodyDecoder {
    state: DecodeState,
    target: usize,
    chunk_buf: BytesMut,
    chunk_decoded: usize,
    trailer_bytes: usize,
}

impl HttpBodyDecoder {
    fn new(strategy: BodyStrategy, target: usize, leftover_len: usize) -> Self {
        let state = match strategy {
            BodyStrategy::Known(expected) if leftover_len > expected => DecodeState::Invalid(Some(
                HttpError::InvalidLength("body longer than Content-Length".to_string()),
            )),
            BodyStrategy::Known(expected) => DecodeState::Known {
                expected,
                remaining: expected,
            },
            BodyStrategy::Chunked => DecodeState::Chunked(ChunkedState::Size),
            BodyStrategy::UntilClose => DecodeState::UntilClose,
            BodyStrategy::Empty => DecodeState::Done,
        };
        Self {
            state,
            target,
            chunk_buf: BytesMut::new(),
            chunk_decoded: 0,
            trailer_bytes: 0,
        }
    }

    fn initial_capacity(&self) -> usize {
        match self.state {
            DecodeState::Known { remaining, .. } => remaining.min(self.target),
            DecodeState::UntilClose => self.target,
            DecodeState::Chunked(_) => MAX_CHUNK_SIZE_LINE_SIZE,
            DecodeState::Done | DecodeState::Invalid(_) => 1,
        }
    }

    fn known_remaining(&self) -> Option<usize> {
        match self.state {
            DecodeState::Known { remaining, .. } => Some(remaining),
            DecodeState::Done => Some(0),
            _ => None,
        }
    }

    fn decode_known(&mut self, src: &mut BytesMut) -> Result<Option<DecodedItem>, HttpError> {
        let DecodeState::Known { remaining, .. } = &mut self.state else {
            unreachable!();
        };
        if src.len() > *remaining {
            return Err(HttpError::InvalidLength(
                "body longer than Content-Length".to_string(),
            ));
        }
        let minimum_frame_len = (*remaining).min(self.target);
        if src.len() < minimum_frame_len {
            src.reserve(minimum_frame_len - src.len());
            return Ok(None);
        }

        // Transfer read-ahead too, leaving no partial frame to copy on reserve.
        let bytes = std::mem::take(src).freeze();
        *remaining -= bytes.len();
        let end_stream = *remaining == 0;
        if end_stream {
            self.state = DecodeState::Done;
        }
        Ok(Some(DecodedItem::Data { bytes, end_stream }))
    }

    fn decode_until_close(&mut self, src: &mut BytesMut) -> Option<DecodedItem> {
        if src.len() < self.target {
            src.reserve(self.target - src.len());
            return None;
        }
        Some(DecodedItem::Data {
            bytes: src.split_to(self.target).freeze(),
            end_stream: false,
        })
    }

    fn take_chunk_frame(&mut self, end_stream: bool) -> DecodedItem {
        let bytes = self.chunk_buf.split().freeze();
        self.chunk_decoded += bytes.len();
        DecodedItem::Data { bytes, end_stream }
    }

    fn decode_chunked(&mut self, src: &mut BytesMut) -> Result<Option<DecodedItem>, HttpError> {
        loop {
            match &mut self.state {
                DecodeState::Chunked(ChunkedState::Size) => match httparse::parse_chunk_size(src) {
                    Ok(httparse::Status::Complete((line_len, chunk_size))) => {
                        if line_len > MAX_CHUNK_SIZE_LINE_SIZE {
                            return Err(chunk_size_line_too_large());
                        }
                        let chunk_size = usize::try_from(chunk_size).map_err(|_| {
                            HttpError::InvalidLength("chunk size does not fit in usize".to_string())
                        })?;
                        src.advance(line_len);
                        self.state = if chunk_size == 0 {
                            DecodeState::Chunked(ChunkedState::Trailers)
                        } else {
                            DecodeState::Chunked(ChunkedState::Data {
                                remaining: chunk_size,
                            })
                        };
                    }
                    Ok(httparse::Status::Partial) => {
                        if src.len() > MAX_CHUNK_SIZE_LINE_SIZE {
                            return Err(chunk_size_line_too_large());
                        }
                        return Ok(None);
                    }
                    Err(error) => {
                        return Err(HttpError::InvalidLength(format!(
                            "invalid chunk size: {error}"
                        )));
                    }
                },
                DecodeState::Chunked(ChunkedState::Data { remaining }) => {
                    let output_space = self.target - self.chunk_buf.len();
                    let take = src.len().min(*remaining).min(output_space);
                    // TODO we could try to avoid this copy, it's not on the main path though
                    self.chunk_buf.extend_from_slice(&src[..take]);
                    src.advance(take);
                    *remaining -= take;

                    if self.chunk_buf.len() == self.target {
                        return Ok(Some(self.take_chunk_frame(false)));
                    }
                    if *remaining == 0 {
                        self.state = DecodeState::Chunked(ChunkedState::AfterCrlf);
                        continue;
                    }
                    return Ok(None);
                }
                DecodeState::Chunked(ChunkedState::AfterCrlf) => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    if &src[..2] != b"\r\n" {
                        return Err(HttpError::InvalidLength(format!(
                            "expected CRLF after chunk, got {:?}",
                            &src[..2]
                        )));
                    }
                    src.advance(2);
                    self.state = DecodeState::Chunked(ChunkedState::Size);
                }
                DecodeState::Chunked(ChunkedState::Trailers) => {
                    let Some(line_len) = find_crlf(src) else {
                        if src.len() > MAX_TRAILER_SECTION_SIZE - self.trailer_bytes {
                            return Err(trailer_section_too_large());
                        }
                        return Ok(None);
                    };
                    let encoded_line_len = line_len + 2;
                    if encoded_line_len > MAX_TRAILER_SECTION_SIZE - self.trailer_bytes {
                        return Err(trailer_section_too_large());
                    }
                    self.trailer_bytes += encoded_line_len;
                    src.advance(encoded_line_len);
                    if line_len != 0 {
                        continue;
                    }
                    self.state = DecodeState::Done;
                    return if self.chunk_buf.is_empty() {
                        Ok(Some(DecodedItem::End))
                    } else {
                        Ok(Some(self.take_chunk_frame(true)))
                    };
                }
                _ => unreachable!(),
            }
        }
    }

    fn unexpected_chunked_eof(&self) -> HttpError {
        let buffered = self.chunk_decoded + self.chunk_buf.len();
        let expected = match self.state {
            DecodeState::Chunked(ChunkedState::Data { remaining }) => buffered + remaining,
            _ => 0,
        };
        HttpError::UnexpectedEof {
            read: buffered,
            expected,
        }
    }
}

impl Decoder for HttpBodyDecoder {
    type Item = DecodedItem;
    type Error = HttpError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            DecodeState::Known { .. } => self.decode_known(src),
            DecodeState::Chunked(_) => self.decode_chunked(src),
            DecodeState::UntilClose => Ok(self.decode_until_close(src)),
            DecodeState::Done => Ok(Some(DecodedItem::End)),
            DecodeState::Invalid(ref mut error) => Err(error
                .take()
                .unwrap_or_else(|| HttpError::InvalidLength("invalid body state".to_string()))),
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(item) = self.decode(src)? {
            return Ok(Some(item));
        }
        match self.state {
            DecodeState::Known {
                expected,
                remaining,
            } => Err(HttpError::UnexpectedEof {
                read: expected - remaining + src.len(),
                expected,
            }),
            DecodeState::Chunked(_) => Err(self.unexpected_chunked_eof()),
            DecodeState::UntilClose => {
                self.state = DecodeState::Done;
                if src.is_empty() {
                    Ok(Some(DecodedItem::End))
                } else {
                    Ok(Some(DecodedItem::Data {
                        bytes: src.split().freeze(),
                        end_stream: true,
                    }))
                }
            }
            DecodeState::Done => Ok(Some(DecodedItem::End)),
            DecodeState::Invalid(_) => unreachable!("decode returned the invalid-state error"),
        }
    }
}

fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(2).position(|window| window == b"\r\n")
}

fn chunk_size_line_too_large() -> HttpError {
    HttpError::InvalidLength(format!(
        "chunk-size line exceeded {MAX_CHUNK_SIZE_LINE_SIZE} bytes"
    ))
}

fn trailer_section_too_large() -> HttpError {
    HttpError::InvalidLength(format!(
        "trailer section exceeded {MAX_TRAILER_SECTION_SIZE} bytes"
    ))
}

pub struct ResponseBody<R> {
    framed: BodyFramedReader<R>,
    read_timeout: Duration,
    consumed: usize,
    done: bool,
}

impl<R> std::fmt::Debug for ResponseBody<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponseBody")
            .field("decoder", self.framed.decoder())
            .field("read_timeout", &self.read_timeout)
            .field("consumed", &self.consumed)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin> ResponseBody<R> {
    /// Builds the body from a parsed head and the reader that produced it.
    /// `leftover` contains body bytes that arrived with the response head.
    pub(crate) fn new(
        reader: R,
        strategy: BodyStrategy,
        leftover: Bytes,
        buffer_hint: BufferHint,
        read_timeout: Duration,
    ) -> Self {
        let target = buffer_hint.target.max(MIN_TARGET);
        let leftover_len = leftover.len();
        let decoder = HttpBodyDecoder::new(strategy, target, leftover_len);
        let initial_capacity = decoder.initial_capacity();
        let prefixed_reader = AsyncReadExt::chain(Cursor::new(leftover), reader);
        let timeout_reader = IdleTimeoutReader::new(prefixed_reader, read_timeout);
        let framed = FramedRead::with_capacity(timeout_reader, decoder, initial_capacity);
        let done = matches!(strategy, BodyStrategy::Empty)
            || matches!(strategy, BodyStrategy::Known(0) if leftover_len == 0);
        Self {
            framed,
            read_timeout,
            consumed: 0,
            done,
        }
    }

    /// Number of body bytes yielded so far.
    pub fn consumed(&self) -> usize {
        self.consumed
    }

    fn normalize_error(&self, error: HttpError) -> HttpError {
        match error {
            HttpError::Io(io_error) if io_error.kind() == ErrorKind::TimedOut => {
                HttpError::Timeout(self.read_timeout, "response read".to_string())
            }
            other => other,
        }
    }
}

impl<R: AsyncRead + Unpin> Body for ResponseBody<R> {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.framed).poll_next(cx) {
            Poll::Ready(Some(Ok(DecodedItem::Data { bytes, end_stream }))) => {
                this.consumed += bytes.len();
                this.done = end_stream;
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            Poll::Ready(Some(Ok(DecodedItem::End))) | Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.done = true;
                Poll::Ready(Some(Err(this.normalize_error(error))))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> SizeHint {
        if self.done {
            return SizeHint::with_exact(0);
        }
        self.framed
            .decoder()
            .known_remaining()
            .map(|remaining| SizeHint::with_exact(remaining as u64))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
