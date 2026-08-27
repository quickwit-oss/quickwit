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

use bytes::Bytes;
use futures::{Future, Stream};
use http_body::{Body, Frame, SizeHint};
use tokio::io::{AsyncRead, AsyncReadExt, Chain, ReadBuf};
use tokio::time::Sleep;
use tokio_util::codec::FramedRead;

use crate::error::HttpError;
use crate::response::BodyStrategy;

mod decoder;
use decoder::{DecodedItem, HttpBodyDecoder};

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

pub(crate) const MIN_TARGET: usize = 8 * 1024;

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

    /// Reclaims the underlying reader.
    pub(crate) fn into_inner(self) -> R {
        self.inner
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

struct KnownLen {
    /// Body bytes read alongside headers, to prepend to the output until empty
    leftover: Bytes,
    /// Remaining body bytes to read.
    remaining: usize,
}

struct UntilClose {
    /// Body bytes read alongside headers, to prepend to the output until empty
    leftover: Bytes,
}

enum FastState {
    Known(KnownLen),
    UntilClose(UntilClose),
}

/// The outcome of one frame read on the fast path. The reader is always
/// returned so it can be stored back in `FastBody` (or pooled at EOS).
struct FastReadOutcome<R> {
    reader: R,
    result: Result<(Bytes, bool), HttpError>,
}

/// Read one frame of `want` bytes.
///
/// Can short-read if is_known is false and we reach EOS.
/// May read a bit more than `want` bytes if more is available on the wire
/// depending on how BytesMut decide to actually allocate.
// we might win ever so slightly by having this be a handrolled state machine
// which we can manipulate without boxes, but as is i think it would either cost
// us a zero-initialization of a buffer, or some unsafe to interact with AsyncRead
// efficiently
async fn read_frame_fast<R: AsyncRead + Unpin + Send>(
    mut reader: R,
    want: usize,
    leftover: Bytes,
    is_known: bool,
    total_remaining: usize,
    consumed: usize,
    read_timeout: Duration,
) -> FastReadOutcome<R> {
    use tokio::io::AsyncReadExt;

    let mut buf = bytes::BytesMut::with_capacity(want);
    // Drain the leftover first
    if !leftover.is_empty() {
        buf.extend_from_slice(&leftover);
    }
    let mut sleep = Box::pin(tokio::time::sleep(read_timeout));
    while buf.len() < want {
        sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + read_timeout);
        tokio::select! {
            biased;
            // this might read slighly over `want` depending on how bytes decides
            // to over-allocated. we don't promise strict bound for this reason
            res = reader.read_buf(&mut buf) => match res {
                Ok(0) => {
                    if is_known {
                        let read = consumed + buf.len();
                        return FastReadOutcome {
                            reader,
                            result: Err(HttpError::UnexpectedEof {
                                read,
                                expected: consumed + total_remaining,
                            }),
                        };
                    }
                    return FastReadOutcome {
                        reader,
                        result: Ok((buf.freeze(), true)),
                    };
                }
                Ok(_) => {} // try to read more until we have want bytes
                Err(err) => {
                    return FastReadOutcome {
                        reader,
                        result: Err(HttpError::Io(err)),
                    };
                }
            },
            _ = &mut sleep => {
                return FastReadOutcome {
                    reader,
                    result: Err(HttpError::Timeout(
                        read_timeout,
                        "response read".to_string(),
                    )),
                };
            }
        }
    }
    FastReadOutcome {
        reader,
        result: Ok((buf.freeze(), false)),
    }
}

/// Fast path, when body is not chunked
struct FastBody<R: AsyncRead + Send + 'static> {
    // None only when lend it to read_frame_fast to please the borrowchecker
    reader: Option<R>,
    // In-progress frame read. Only None when reader is Some, and Some when reader is None.
    read_fut: Option<Pin<Box<dyn Future<Output = FastReadOutcome<R>> + Send>>>,
    read_timeout: Duration,
    target: usize,
    // `None` once the body has reached EOS.
    state: Option<FastState>,
    pool_hook: Option<Box<dyn FnOnce(R) + Send + 'static>>,
    poolable: bool,
    clean_eos: bool,
    consumed: usize,
}

impl<R: AsyncRead + Unpin + Send + 'static> FastBody<R> {
    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        // If a read is in progress, poll it.
        if let Some(fut) = self.read_fut.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(outcome) => {
                    self.read_fut = None;
                    self.reader = Some(outcome.reader);
                    return self.finish_frame(outcome.result);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        // Start a new frame read.
        let Some(state) = self.state.take() else {
            self.clean_eos = true;
            return Poll::Ready(None);
        };
        let (want, leftover, is_known, total_remaining) = match &state {
            FastState::Known(k) => {
                if k.leftover.len() > k.remaining {
                    self.state = None;
                    return Poll::Ready(Some(Err(HttpError::InvalidLength(
                        "body longer than Content-Length".to_string(),
                    ))));
                }
                if k.remaining == 0 {
                    self.clean_eos = true;
                    return Poll::Ready(None);
                }
                (
                    k.remaining.min(self.target),
                    k.leftover.clone(),
                    true,
                    k.remaining,
                )
            }
            FastState::UntilClose(u) => (self.target, u.leftover.clone(), false, 0),
        };
        let reader = self
            .reader
            .take()
            .expect("reader is Some when state is Some");
        let consumed = self.consumed;
        let read_timeout = self.read_timeout;
        let fut = Box::pin(read_frame_fast(
            reader,
            want,
            leftover,
            is_known,
            total_remaining,
            consumed,
            read_timeout,
        ));
        self.read_fut = Some(fut);
        self.state = Some(match state {
            FastState::Known(k) => FastState::Known(KnownLen {
                leftover: Bytes::new(),
                remaining: k.remaining,
            }),
            FastState::UntilClose(_) => FastState::UntilClose(UntilClose {
                leftover: Bytes::new(),
            }),
        });
        // Poll the future we just created. This will recurse at most once: read_fut is now set
        self.poll_frame(cx)
    }

    /// Processes the result of a frame read, updating state and pooling on EOS.
    fn finish_frame(
        &mut self,
        result: Result<(Bytes, bool), HttpError>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        match result {
            Ok((bytes, eos)) => {
                self.consumed += bytes.len();
                if let Some(FastState::Known(k)) = &mut self.state {
                    // `read_buf` ran read past `want`, it's fine as long as it doesn't go further
                    // than the actual body content (we don't do pipelining so server shouldn't
                    // send anything more)
                    if bytes.len() > k.remaining {
                        self.state = None;
                        return Poll::Ready(Some(Err(HttpError::InvalidLength(
                            "body longer than Content-Length".to_string(),
                        ))));
                    }
                    k.remaining -= bytes.len();
                }
                if eos || matches!(&self.state, Some(FastState::Known(k)) if k.remaining == 0) {
                    self.clean_eos = true;
                    self.state = None;
                    self.release_to_pool();
                }
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            Err(err) => {
                self.state = None;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

impl<R: AsyncRead + Send + 'static> FastBody<R> {
    fn release_to_pool(&mut self) {
        if self.clean_eos
            && self.poolable
            && let (Some(hook), Some(reader)) = (self.pool_hook.take(), self.reader.take())
        {
            hook(reader);
        }
    }
}

struct ChunkedBody<R: AsyncRead + Send + 'static> {
    framed: Option<BodyFramedReader<R>>,
    pool_hook: Option<Box<dyn FnOnce(R) + Send + 'static>>,
    poolable: bool,
    clean_eos: bool,
}

impl<R: AsyncRead + Send + 'static> ChunkedBody<R> {
    fn release_to_pool(&mut self) {
        if self.poolable
            && let (Some(hook), Some(framed)) = (self.pool_hook.take(), self.framed.take())
        {
            let idle_reader = framed.into_inner();
            let chain = idle_reader.into_inner();
            let (_leftover, reader) = chain.into_inner();
            hook(reader);
        }
    }
}

pub struct ResponseBody<R: AsyncRead + Send + 'static> {
    kind: BodyKind<R>,
    read_timeout: Duration,
    consumed: usize,
    done: bool,
}

enum BodyKind<R: AsyncRead + Send + 'static> {
    /// Fast pass for unframed body (content-lenght or until EOS)
    Fast(FastBody<R>),
    /// Transfer-Encoding: chunked path
    Chunked(ChunkedBody<R>),
    /// Empty body / zero-length body
    Complete {
        reader: Option<R>,
        pool_hook: Option<Box<dyn FnOnce(R) + Send + 'static>>,
        poolable: bool,
    },
}

impl<R: AsyncRead + Send + 'static> std::fmt::Debug for ResponseBody<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponseBody")
            .field("read_timeout", &self.read_timeout)
            .field("consumed", &self.consumed)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> ResponseBody<R> {
    pub(crate) fn new(
        reader: R,
        strategy: BodyStrategy,
        leftover: Bytes,
        buffer_hint: BufferHint,
        read_timeout: Duration,
        pool_hook: Option<Box<dyn FnOnce(R) + Send + 'static>>,
        keep_alive: bool,
    ) -> Self {
        let target = buffer_hint.target.max(MIN_TARGET);
        let leftover_len = leftover.len();
        // `UntilClose` ends on a peer EOF, connection isn't reusable.
        let poolable = keep_alive && !matches!(strategy, BodyStrategy::UntilClose);
        let kind = match strategy {
            BodyStrategy::Empty | BodyStrategy::Known(0) => {
                // An empty body with non-empty leftover means protocol desync,
                // mark it non poolable so we close the connection
                BodyKind::Complete {
                    reader: Some(reader),
                    pool_hook,
                    poolable: poolable && leftover_len == 0,
                }
            }
            BodyStrategy::Known(len) => BodyKind::Fast(FastBody {
                reader: Some(reader),
                read_fut: None,
                read_timeout,
                target,
                state: Some(FastState::Known(KnownLen {
                    leftover,
                    remaining: len,
                })),
                pool_hook,
                poolable,
                clean_eos: false,
                consumed: 0,
            }),
            BodyStrategy::UntilClose => BodyKind::Fast(FastBody {
                reader: Some(reader),
                read_fut: None,
                read_timeout,
                target,
                state: Some(FastState::UntilClose(UntilClose { leftover })),
                pool_hook,
                poolable,
                clean_eos: false,
                consumed: 0,
            }),
            BodyStrategy::Chunked => {
                let decoder = HttpBodyDecoder::new(strategy, target, leftover_len);
                let initial_capacity = decoder.initial_capacity();
                let prefixed_reader = AsyncReadExt::chain(Cursor::new(leftover), reader);
                let timeout_reader = IdleTimeoutReader::new(prefixed_reader, read_timeout);
                let framed = FramedRead::with_capacity(timeout_reader, decoder, initial_capacity);
                BodyKind::Chunked(ChunkedBody {
                    framed: Some(framed),
                    pool_hook,
                    poolable,
                    clean_eos: false,
                })
            }
        };
        let done = matches!(kind, BodyKind::Complete { .. });
        Self {
            kind,
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

impl<R: AsyncRead + Unpin + Send + 'static> Body for ResponseBody<R> {
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
        match &mut this.kind {
            BodyKind::Complete { .. } => {
                this.done = true;
                Poll::Ready(None)
            }
            BodyKind::Fast(fast) => match fast.poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    this.consumed = fast.consumed;
                    if fast.state.is_none() {
                        this.done = true;
                    }
                    Poll::Ready(Some(Ok(frame)))
                }
                Poll::Ready(None) => {
                    this.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Err(err))) => {
                    this.done = true;
                    Poll::Ready(Some(Err(this.normalize_error(err))))
                }
                Poll::Pending => Poll::Pending,
            },
            BodyKind::Chunked(chunked) => {
                let Some(framed) = chunked.framed.as_mut() else {
                    this.done = true;
                    return Poll::Ready(None);
                };
                match Pin::new(framed).poll_next(cx) {
                    Poll::Ready(Some(Ok(DecodedItem::Data { bytes, end_stream }))) => {
                        this.consumed += bytes.len();
                        if end_stream {
                            chunked.clean_eos = true;
                            this.done = true;
                            chunked.release_to_pool();
                        }
                        Poll::Ready(Some(Ok(Frame::data(bytes))))
                    }
                    Poll::Ready(Some(Ok(DecodedItem::End))) | Poll::Ready(None) => {
                        chunked.clean_eos = true;
                        this.done = true;
                        chunked.release_to_pool();
                        Poll::Ready(None)
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.done = true;
                        Poll::Ready(Some(Err(this.normalize_error(error))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> SizeHint {
        if self.done {
            return SizeHint::with_exact(0);
        }
        match &self.kind {
            BodyKind::Fast(fast) => match &fast.state {
                Some(FastState::Known(k)) => SizeHint::with_exact(k.remaining as u64),
                _ => SizeHint::default(),
            },
            // in practice this returns something only after the last chunk
            BodyKind::Chunked(chunked) => chunked
                .framed
                .as_ref()
                .and_then(|framed| framed.decoder().known_remaining())
                .map(|remaining| SizeHint::with_exact(remaining as u64))
                .unwrap_or_default(),
            BodyKind::Complete { .. } => SizeHint::with_exact(0),
        }
    }
}

impl<R: AsyncRead + Send + 'static> Drop for ResponseBody<R> {
    fn drop(&mut self) {
        match &mut self.kind {
            BodyKind::Fast(fast) => {
                // this is probably dead code, we already release earlier on most paths
                fast.release_to_pool();
            }
            BodyKind::Chunked(chunked) => {
                if chunked.clean_eos && chunked.poolable {
                    chunked.release_to_pool();
                }
            }
            BodyKind::Complete {
                reader,
                pool_hook,
                poolable,
            } => {
                if *poolable && let (Some(hook), Some(reader)) = (pool_hook.take(), reader.take()) {
                    hook(reader);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
