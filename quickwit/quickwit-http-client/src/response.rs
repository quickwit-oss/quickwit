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

use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::HttpError;

/// How to read the response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyStrategy {
    /// A fixed number of bytes are expected (from `Content-Length` or
    /// `Content-Range`).
    Known(usize),
    /// Size unknown (`Transfer-Encoding: chunked`)
    Chunked,
    /// Size unknown, read until end of connection
    UntilClose,
    /// No body to read
    Empty,
}

/// A parsed response head plus the strategy for reading its body.
#[derive(Debug)]
pub struct ResponseHead {
    /// The response Head
    pub parts: http::response::Parts,
    /// Whether the connection may be returned to the pool after the body is
    /// fully consumed. Derived from the `Connection` header and the HTTP
    /// version default.
    pub keep_alive: bool,
    /// How we're going to read the body and detect its end.
    pub body: BodyStrategy,
    /// Body bytes that already arrived in the same read as the head. It must
    /// be consumed before reading more from the stream.
    /// It can't contain response to another request (we don't do pipelining)
    pub leftover: Bytes,
}

/// Maximum response head size we are willing to buffer.
const MAX_HEAD_SIZE: usize = 64 * 1024;
/// Upper bound on the number of headers we parse per response.
const MAX_HEADERS: usize = 128;

/// Reads bytes from `stream` until a complete (non-informational) response
/// head is available, then parses it. Informational 1xx responses (100 Continue,
/// 103 Early Hints, ...) are consumed and skipped; the final response is
/// returned. `101 Switching Protocols` is rejected as unsupported.
///
/// `request_method` is needed to detect HEAD queries, which don't have a body despite
/// their possible content-length.
pub async fn read_head<R>(
    stream: &mut R,
    request_method: &http::Method,
    read_timeout: Duration,
) -> Result<ResponseHead, HttpError>
where
    R: AsyncRead + Unpin,
{
    let mut buf = BytesMut::with_capacity(8192);
    loop {
        if buf.len() > MAX_HEAD_SIZE {
            return Err(HttpError::HeadTooLarge(MAX_HEAD_SIZE));
        }
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut resp = httparse::Response::new(&mut headers);
        match resp.parse(&buf) {
            Ok(httparse::Status::Complete(head_len)) => {
                let status = resp.code.ok_or(HttpError::Parse(httparse::Error::Token))?;
                // discard 1xx (they are informational) and followed by an actual
                // status. 101 Switching Protocols is not supported.
                if (100..200).contains(&status) {
                    if status == 101 {
                        return Err(HttpError::Parse(httparse::Error::Version));
                    }
                    buf.advance(head_len);
                    continue;
                }
                let mut head = build_head(&resp, request_method)?;
                let _ = buf.split_to(head_len);
                head.leftover = buf.freeze();
                return Ok(head);
            }
            Ok(httparse::Status::Partial) => {
                if buf.capacity() - buf.len() < 1024 {
                    buf.reserve(8192);
                }
                let n = match tokio::time::timeout(read_timeout, stream.read_buf(&mut buf)).await {
                    Ok(res) => res?,
                    Err(_) => {
                        return Err(HttpError::Timeout(
                            read_timeout,
                            "response head read".to_string(),
                        ));
                    }
                };
                if n == 0 {
                    return Err(HttpError::UnexpectedEof {
                        read: buf.len(),
                        expected: 0,
                    });
                }
            }
            Err(err) => return Err(HttpError::Parse(err)),
        }
    }
}

fn build_head(
    resp: &httparse::Response<'_, '_>,
    request_method: &http::Method,
) -> Result<ResponseHead, HttpError> {
    let status = resp.code.ok_or(HttpError::Parse(httparse::Error::Token))?;
    let version = resp.version.unwrap_or(0); // 1 for HTTP/1.1, 0 for HTTP/1.0

    let mut header_map = http::HeaderMap::with_capacity(resp.headers.len());
    let mut connection_close = false;
    let mut connection_keepalive = false;
    let mut transfer_encoding_chunked: Option<bool> = None;
    let mut content_length: Option<usize> = None;

    for header in resp.headers.iter() {
        let name = header.name;
        let value = header.value;
        if name.eq_ignore_ascii_case("connection") {
            for tok in std::str::from_utf8(value)
                .unwrap_or("")
                .split(',')
                .map(str::trim)
            {
                if tok.eq_ignore_ascii_case("close") {
                    connection_close = true;
                } else if tok.eq_ignore_ascii_case("keep-alive") {
                    connection_keepalive = true;
                }
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            let value_str = std::str::from_utf8(value).unwrap_or("");
            for tok in value_str.split(',').map(str::trim) {
                if !tok.is_empty() {
                    transfer_encoding_chunked = Some(tok.eq_ignore_ascii_case("chunked"));
                }
            }
        } else if name.eq_ignore_ascii_case("content-length") {
            let Some(parsed) = parse_usize(value)? else {
                continue;
            };
            match content_length {
                Some(existing) if existing != parsed => {
                    return Err(HttpError::InvalidLength(format!(
                        "conflicting Content-Length headers: {existing} vs {parsed}"
                    )));
                }
                None => content_length = Some(parsed),
                _ => {}
            }
        }
        let header_name = http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| HttpError::Parse(httparse::Error::HeaderName))?;
        let header_value = http::HeaderValue::from_bytes(value)
            .map_err(|_| HttpError::Parse(httparse::Error::HeaderName))?;
        header_map.append(header_name, header_value);
    }

    // keep-alive is default in 1.1  unless `connection: close`.
    // with 1.0 keep-alive has to be explicitly mentioned to be used.
    let keep_alive = if connection_close {
        false
    } else if version == 1 {
        true
    } else {
        connection_keepalive
    };

    // 1xx other than 101 are handled in read_head.
    // `HEAD` and  `304 Not Modified` may have headers suggesting content, but they
    // *never* have an actual body.
    // 204, 304, and HEAD responses have no body despite any Content-Length.
    let body = if status == 204 || status == 304 || request_method == http::Method::HEAD {
        BodyStrategy::Empty
    } else if let Some(chunked) = transfer_encoding_chunked {
        // Transfer-Encoding takes precedence over Content-Length.
        // HTTP/1.0 + TE is invalid
        if version == 0 {
            return Err(HttpError::InvalidLength(
                "HTTP/1.0 response with Transfer-Encoding is invalid".to_string(),
            ));
        }
        // The final encoding determines framing: `chunked` -> Chunked,
        // anything else -> UntilClose (Content-Length is ignored).
        if chunked {
            BodyStrategy::Chunked
        } else {
            BodyStrategy::UntilClose
        }
    } else if let Some(len) = content_length {
        BodyStrategy::Known(len)
    } else {
        BodyStrategy::UntilClose
    };

    let http_version = if version == 1 {
        http::Version::HTTP_11
    } else {
        http::Version::HTTP_10
    };
    let response = http::Response::builder()
        .status(
            http::StatusCode::from_u16(status)
                .map_err(|_| HttpError::Parse(httparse::Error::Token))?,
        )
        .version(http_version)
        .body(())
        .map_err(|_| HttpError::Parse(httparse::Error::Token))?;
    let (mut parts, ()) = response.into_parts();
    parts.headers = header_map;

    Ok(ResponseHead {
        parts,
        keep_alive,
        body,
        leftover: Bytes::new(),
    })
}

fn parse_usize(value: &[u8]) -> Result<Option<usize>, HttpError> {
    let s = std::str::from_utf8(value)
        .map_err(|err| HttpError::InvalidLength(format!("non-UTF-8 Content-Length: {err}")))?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(HttpError::InvalidLength("empty Content-Length".to_string()));
    }
    trimmed
        .parse::<usize>()
        .map(Some)
        .map_err(|err| HttpError::InvalidLength(format!("invalid length `{s}`: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_head_from(raw: &[u8]) -> ResponseHead {
        read_head(&mut &raw[..], &http::Method::GET, Duration::from_secs(5))
            .await
            .unwrap()
    }

    async fn read_head_from_err(raw: &[u8]) -> HttpError {
        read_head(&mut &raw[..], &http::Method::GET, Duration::from_secs(5))
            .await
            .unwrap_err()
    }

    async fn read_head_from_method(raw: &[u8], method: http::Method) -> ResponseHead {
        read_head(&mut &raw[..], &method, Duration::from_secs(5))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn known_length_from_content_length() {
        let head = read_head_from(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").await;
        assert_eq!(head.body, BodyStrategy::Known(5));
        assert!(head.keep_alive);
        assert_eq!(head.parts.status, 200);
        assert_eq!(head.parts.version, http::Version::HTTP_11);
        assert_eq!(head.leftover, Bytes::from_static(b"hello"));
        assert_eq!(
            head.parts.headers.get("content-length").unwrap(),
            "5".parse::<http::HeaderValue>().unwrap()
        );
    }

    #[tokio::test]
    async fn chunked() {
        let head = read_head_from(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").await;
        assert_eq!(head.body, BodyStrategy::Chunked);
        assert!(head.keep_alive);
    }

    #[tokio::test]
    async fn transfer_encoding_chunked_overrides_content_length() {
        // transfer-encoding takes precedence over content-length.
        let head = read_head_from(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n",
        )
        .await;
        assert_eq!(head.body, BodyStrategy::Chunked);
    }

    #[tokio::test]
    async fn until_close_when_no_length() {
        let head = read_head_from(b"HTTP/1.0 200 OK\r\n\r\nbody").await;
        assert_eq!(head.body, BodyStrategy::UntilClose);
        assert!(!head.keep_alive, "HTTP/1.0 defaults to close");
    }

    #[tokio::test]
    async fn http10_keep_alive_header() {
        let head = read_head_from(b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\n\r\n").await;
        assert!(head.keep_alive);
    }

    #[tokio::test]
    async fn http11_connection_close() {
        let head = read_head_from(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n").await;
        assert!(!head.keep_alive);
    }

    #[tokio::test]
    async fn empty_body_for_204() {
        let head = read_head_from(b"HTTP/1.1 204 No Content\r\n\r\n").await;
        assert_eq!(head.body, BodyStrategy::Empty);
        assert!(head.keep_alive);
    }

    #[tokio::test]
    async fn empty_body_for_304() {
        let head = read_head_from(b"HTTP/1.1 304 Not Modified\r\n\r\n").await;
        assert_eq!(head.body, BodyStrategy::Empty);
    }

    #[tokio::test]
    async fn conflicting_content_length_headers_error() {
        let err = read_head_from_err(
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
        )
        .await;
        assert!(matches!(err, HttpError::InvalidLength(_)), "{err:?}");
    }

    #[tokio::test]
    async fn skips_informational_100() {
        let head = read_head_from(
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi",
        )
        .await;
        assert_eq!(head.parts.status, 200);
        assert_eq!(head.body, BodyStrategy::Known(2));
        assert_eq!(head.leftover, Bytes::from_static(b"hi"));
    }

    #[tokio::test]
    async fn leftover_only_body_bytes() {
        let head = read_head_from(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabcEXTRA").await;
        // this works because we read on a large buffer from an in memory buffer, on an actual
        // socket we might get less bytes
        assert_eq!(head.leftover, Bytes::from_static(b"abcEXTRA"));
    }

    #[tokio::test]
    async fn read_head_times_out_when_no_bytes_arrive() {
        // The head never arrives: the write side stays open so reads stay
        // pending; the per-read idle timeout (20 ms) fires.
        let (_client, mut server) = tokio::io::duplex(1024);
        let err = read_head(&mut server, &http::Method::GET, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(err.is_timeout(), "expected a timeout, got {err:?}");
    }

    #[tokio::test]
    async fn head_response_with_content_length_is_empty_body() {
        let head = read_head_from_method(
            b"HTTP/1.1 200 OK\r\nContent-Length: 12345\r\n\r\n",
            http::Method::HEAD,
        )
        .await;
        assert_eq!(head.body, BodyStrategy::Empty);
        assert!(head.keep_alive);
        assert_eq!(head.parts.headers.get("content-length").unwrap(), "12345");
    }

    #[tokio::test]
    async fn head_response_with_chunked_is_empty_body() {
        let head = read_head_from_method(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            http::Method::HEAD,
        )
        .await;
        assert_eq!(head.body, BodyStrategy::Empty);
    }

    #[tokio::test]
    async fn transfer_encoding_non_chunked_is_until_close() {
        // Any Transfer-Encoding takes precedence over Content-Length. A
        // non-final `chunked` (or no `chunked` at all) means the body is
        // delimited by close, not by Content-Length.
        let head = read_head_from(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: 5\r\n\r\n",
        )
        .await;
        assert_eq!(head.body, BodyStrategy::UntilClose);
    }

    #[tokio::test]
    async fn transfer_encoding_chunked_final_is_chunked() {
        let head =
            read_head_from(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n").await;
        assert_eq!(head.body, BodyStrategy::Chunked);
    }

    #[tokio::test]
    async fn transfer_encoding_chunked_not_final_is_until_close() {
        let head =
            read_head_from(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n").await;
        assert_eq!(head.body, BodyStrategy::UntilClose);
    }

    #[tokio::test]
    async fn transfer_encoding_multiple_headers_combined() {
        // determines framing.
        let head = read_head_from(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await;
        assert_eq!(head.body, BodyStrategy::Chunked);
    }

    #[tokio::test]
    async fn http10_with_transfer_encoding_errors() {
        let err =
            read_head_from_err(b"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").await;
        assert!(
            matches!(err, HttpError::InvalidLength(_)),
            "expected an error for HTTP/1.0 + TE, got {err:?}"
        );
    }

    #[tokio::test]
    async fn transfer_encoding_ignores_content_length() {
        let head = read_head_from(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: 5\r\n\r\n",
        )
        .await;
        assert_eq!(head.body, BodyStrategy::UntilClose);
        // Content-Length is still surfaced in the headers.
        assert_eq!(head.parts.headers.get("content-length").unwrap(), "5");
    }
}
