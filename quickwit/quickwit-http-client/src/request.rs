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

use std::pin::Pin;
use std::time::Duration;

use bytes::Buf;
use tokio::io::AsyncWrite;

use crate::error::HttpError;
use crate::io::write_all_timeout;

/// How far [`write_request`] got.
#[derive(Debug, Default)]
pub(crate) struct WriteState {
    /// The entire head was sent, this makes body-less non-idempotent queries
    /// non re-tryable.
    pub(crate) head_sent: bool,
    /// We read part of the request (not response) body: we definitely cannot replay
    /// the query anymore
    pub(crate) body_touched: bool,
}

pub(crate) async fn write_request<W, B>(
    stream: &mut W,
    request: &mut http::Request<B>,
    write_timeout: Duration,
    state: &mut WriteState,
) -> Result<(), HttpError>
where
    W: AsyncWrite + Unpin,
    B: http_body::Body + Unpin,
    B::Error: Into<HttpError>,
{
    let mut head = Vec::with_capacity(512);
    let method = request.method().as_str();
    // TODO: also send proto and autority when talking to an http proxy
    let path = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    head.extend_from_slice(method.as_bytes());
    head.push(b' ');
    head.extend_from_slice(path.as_bytes());
    head.extend_from_slice(b" HTTP/1.1\r\n");
    for (name, value) in request.headers() {
        head.extend_from_slice(name.as_str().as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value.as_bytes());
        head.extend_from_slice(b"\r\n");
    }
    head.extend_from_slice(b"\r\n");
    write_all_timeout(stream, &head, write_timeout).await?;
    // TODO should we not write the last CRLF, flush, mark head_sent, and then push that CRLF?
    state.head_sent = true;

    loop {
        let frame = std::future::poll_fn(|ctx| Pin::new(request.body_mut()).poll_frame(ctx)).await;
        match frame {
            None => break,
            Some(Ok(mut frame)) => {
                state.body_touched = true;
                if let Some(data) = frame.data_mut() {
                    while data.remaining() > 0 {
                        let chunk = data.chunk();
                        write_all_timeout(stream, chunk, write_timeout).await?;
                        let n = chunk.len();
                        data.advance(n);
                    }
                }
                // Trailers are ignored: HTTP/1.1 chunked trailers are not
                // emitted by this GET-only client.
            }
            Some(Err(err)) => return Err(err.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http_body_util::Empty;

    use super::*;

    async fn capture<B>(mut request: http::Request<B>) -> String
    where
        B: http_body::Body + Unpin,
        B::Error: Into<HttpError>,
    {
        let mut writer = Vec::new();
        write_request(
            &mut writer,
            &mut request,
            Duration::from_secs(5),
            &mut WriteState::default(),
        )
        .await
        .unwrap();
        String::from_utf8(writer).unwrap()
    }

    #[tokio::test]
    async fn serialize_get_with_empty_body() {
        let request = http::Request::builder()
            .method("GET")
            .uri("https://bucket.s3.amazonaws.com/key?x=1")
            .header("host", "bucket.s3.amazonaws.com")
            .header("accept", "*/*")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let text = capture(request).await;
        assert!(
            text.starts_with("GET /key?x=1 HTTP/1.1\r\n"),
            "got: {text:?}"
        );
        assert!(text.contains("host: bucket.s3.amazonaws.com\r\n"));
        assert!(text.contains("accept: */*\r\n"));
        assert!(
            text.ends_with("\r\n\r\n"),
            "no body, head ends with blank line"
        );
    }

    #[tokio::test]
    async fn serialize_defaults_missing_path_to_root() {
        let request = http::Request::builder()
            .method("GET")
            .uri("https://example.com")
            .header("host", "example.com")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let text = capture(request).await;
        assert!(text.starts_with("GET / HTTP/1.1\r\n"), "got: {text:?}");
    }

    #[tokio::test]
    async fn serialize_drains_body_frames() {
        let body = http_body_util::combinators::BoxBody::new(http_body_util::StreamBody::new(
            futures::stream::iter([
                Ok::<_, std::convert::Infallible>(http_body::Frame::data(Bytes::from_static(
                    b"hello ",
                ))),
                Ok(http_body::Frame::data(Bytes::from_static(b"world"))),
            ]),
        ));
        let request = http::Request::builder()
            .method("POST")
            .uri("/ingest")
            .header("host", "example.com")
            .header("content-length", "11")
            .body(body)
            .unwrap();
        let text = capture(request).await;
        assert!(text.starts_with("POST /ingest HTTP/1.1\r\n"));
        assert!(
            text.ends_with("\r\n\r\nhello world"),
            "body not drained: {text:?}"
        );
    }

    #[tokio::test]
    async fn write_times_out_when_peer_stops_draining() {
        use tokio::io::AsyncReadExt;
        // A duplex with a tiny buffer and no reader: the write blocks once the
        // buffer fills, and the per-write idle timeout (20 ms) fires.
        let (mut client, mut server) = tokio::io::duplex(16);
        let mut request = http::Request::builder()
            .method("POST")
            .uri("/big")
            .header("host", "example.com")
            .body(http_body_util::Full::new(bytes::Bytes::from(vec![
                b'x';
                1024
            ])))
            .unwrap();
        let err = write_request(
            &mut client,
            &mut request,
            Duration::from_millis(20),
            &mut WriteState::default(),
        )
        .await
        .unwrap_err();
        assert!(err.is_timeout(), "expected a timeout, got {err:?}");
        // Drain to avoid a broken-pipe panic on drop.
        let mut buf = vec![0u8; 1024];
        let _ = server.read(&mut buf).await;
    }
}
