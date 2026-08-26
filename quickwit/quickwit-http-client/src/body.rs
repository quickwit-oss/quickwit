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
use futures::Stream;
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

const MIN_TARGET: usize = 8 * 1024;

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

    /// Reclaims the underlying reader. Used by the pool-return `Drop` to get
    /// the `ConnStream` back after a clean EOS.
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

pub struct ResponseBody<R: AsyncRead + 'static> {
    // Always Some(_), an Option<> just so it can be taken out during Drop and
    // sent through `pool_hook`
    framed: Option<BodyFramedReader<R>>,
    pool_hook: Option<Box<dyn FnOnce(R) + Send + 'static>>,
    // Whether the response head allowed keep-alive *and* the body strategy
    // ends on a protocol boundary.
    poolable: bool,
    // Whether the request ended cleanly and might be reusable
    // it might not be if keep-alive is forbiden, this must be checked alongside poolable
    clean_eos: bool,
    read_timeout: Duration,
    consumed: usize,
    done: bool,
}

impl<R: AsyncRead + 'static> std::fmt::Debug for ResponseBody<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponseBody")
            .field("decoder", &self.framed.as_ref().map(|f| f.decoder()))
            .field("read_timeout", &self.read_timeout)
            .field("consumed", &self.consumed)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin + 'static> ResponseBody<R> {
    /// Builds the body from a parsed head and the reader that produced it.
    /// `leftover` contains body bytes that arrived with the response head.
    ///
    /// `pool_hook`, receives the reclaimed reader if it's reusable.
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
        let decoder = HttpBodyDecoder::new(strategy, target, leftover_len);
        let initial_capacity = decoder.initial_capacity();
        let prefixed_reader = AsyncReadExt::chain(Cursor::new(leftover), reader);
        let timeout_reader = IdleTimeoutReader::new(prefixed_reader, read_timeout);
        let framed = FramedRead::with_capacity(timeout_reader, decoder, initial_capacity);
        let done = matches!(strategy, BodyStrategy::Empty)
            || matches!(strategy, BodyStrategy::Known(0) if leftover_len == 0);
        // `UntilClose` ends on a peer EOF, so definitely not reusable
        let poolable = keep_alive && !matches!(strategy, BodyStrategy::UntilClose);
        Self {
            framed: Some(framed),
            pool_hook,
            poolable,
            clean_eos: done,
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

impl<R: AsyncRead + Unpin + 'static> Body for ResponseBody<R> {
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
        // `framed` is only taken by `Drop`; while the body is live it is always
        // `Some` here.
        let Some(framed) = this.framed.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(framed).poll_next(cx) {
            Poll::Ready(Some(Ok(DecodedItem::Data { bytes, end_stream }))) => {
                this.consumed += bytes.len();
                this.done = end_stream;
                if end_stream {
                    this.clean_eos = true;
                }
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            Poll::Ready(Some(Ok(DecodedItem::End))) | Poll::Ready(None) => {
                this.done = true;
                this.clean_eos = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                // An error leaves the connection state suspect: drop, do not
                // pool. `clean_eos` stays `false`.
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
            .as_ref()
            .and_then(|framed| framed.decoder().known_remaining())
            .map(|remaining| SizeHint::with_exact(remaining as u64))
            .unwrap_or_default()
    }
}

/// Returns a pooled connection to its pool on a clean, poolable EOS.
impl<R: AsyncRead + 'static> Drop for ResponseBody<R> {
    fn drop(&mut self) {
        if !self.clean_eos || !self.poolable {
            return;
        }
        let Some(hook) = self.pool_hook.take() else {
            return;
        };
        let Some(framed) = self.framed.take() else {
            return;
        };
        // Dismantle the framed reader and extract the underlying Reader.
        // Any read ahead we might have is lost, but we should have none because
        // we don't pipeline.
        let idle_reader = framed.into_inner();
        let chain = idle_reader.into_inner();
        let (_leftover, reader) = chain.into_inner();
        hook(reader);
    }
}

#[cfg(test)]
mod tests;
