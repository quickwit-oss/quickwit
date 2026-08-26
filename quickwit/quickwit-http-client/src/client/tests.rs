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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::body::ResponseBody;
use crate::client::{HttpClient, HttpClientBuilder};
use crate::connection::ConnStream;

async fn spawn_server(
    body: &'static [u8],
    keep_alive: bool,
) -> (
    u16,
    Arc<AtomicUsize>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let conn_count = Arc::new(AtomicUsize::new(0));
    let (host_tx, host_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let count_for_task = conn_count.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };
            let count = count_for_task.clone();
            let host_tx = host_tx.clone();
            tokio::spawn(async move {
                count.fetch_add(1, Ordering::SeqCst);
                let mut buf: Vec<u8> = Vec::new();
                loop {
                    // Read until the end of the request head.
                    loop {
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                        let mut tmp = [0u8; 1024];
                        match sock.read(&mut tmp).await {
                            Ok(0) => return,
                            Ok(n) => buf.extend_from_slice(&tmp[..n]),
                            Err(_) => return,
                        }
                    }
                    let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                    if let Some(host) = parse_host(&buf[..head_end]) {
                        let _ = host_tx.send(host);
                    }
                    buf.drain(..head_end);
                    let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                    if sock.write_all(body).await.is_err() {
                        return;
                    }
                    if !keep_alive {
                        return;
                    }
                }
            });
        }
    });
    (port, conn_count, host_rx)
}

/// Extracts the `Host` header value from a raw request head (case-insensitive
/// name match).
fn parse_host(head: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(head).ok()?;
    for line in text.split("\r\n").skip(1) {
        let mut parts = line.splitn(2, ':');
        let name = parts.next()?.trim();
        let value = parts.next()?.trim();
        if name.eq_ignore_ascii_case("host") {
            return Some(value.to_string());
        }
    }
    None
}

fn get(uri: &str) -> http::Request<Empty<Bytes>> {
    http::Request::builder()
        .method("GET")
        .uri(uri)
        .body(Empty::new())
        .unwrap()
}

async fn collect(response: http::Response<ResponseBody<ConnStream>>) -> Vec<u8> {
    let (_parts, body) = response.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    bytes.to_vec()
}

async fn http_client() -> HttpClient {
    HttpClientBuilder::new().build().unwrap()
}

#[tokio::test]
async fn sequential_requests_reuse_one_connection() {
    let (port, conn_count, _host_rx) = spawn_server(b"hello", true).await;
    let client = http_client().await;
    let uri = format!("http://127.0.0.1:{port}/a");

    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"hello");
    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"hello");

    assert_eq!(
        conn_count.load(Ordering::SeqCst),
        1,
        "second request should have reused the pooled connection"
    );
}

#[tokio::test]
async fn dead_pooled_connection_is_retried_once() {
    // One-shot server: it closes after the first response, so the connection
    // the client pools is dead on reuse.
    let (port, conn_count, _host_rx) = spawn_server(b"world", false).await;
    let client = http_client().await;
    let uri = format!("http://127.0.0.1:{port}/b");

    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"world");

    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"world");

    // First request opened one connection; the second reused the (dead)
    // pooled one, detected the failure, and reconnected -> two connections.
    assert_eq!(conn_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_requests_then_reuse() {
    let (port, conn_count, _host_rx) = spawn_server(b"x", true).await;
    let client = http_client().await;
    let uri = format!("http://127.0.0.1:{port}/c");

    // empty pool: we should create as much connections as needed
    const N: usize = 4;
    let mut first = Vec::new();
    for _ in 0..N {
        first.push(client.execute(get(&uri)));
    }
    let first = futures::future::join_all(first).await;
    for resp in first {
        assert_eq!(&collect(resp.unwrap()).await, b"x");
    }
    assert_eq!(conn_count.load(Ordering::SeqCst), N);

    // now we should reuse connection and not open any new one
    let mut second = Vec::new();
    for _ in 0..N {
        second.push(client.execute(get(&uri)));
    }
    let second = futures::future::join_all(second).await;
    for resp in second {
        assert_eq!(&collect(resp.unwrap()).await, b"x");
    }
    assert_eq!(
        conn_count.load(Ordering::SeqCst),
        N,
        "second batch should reuse pooled connections"
    );
}

#[tokio::test]
async fn max_idle_zero_disables_reuse() {
    let (port, conn_count, _host_rx) = spawn_server(b"z", true).await;
    let client = HttpClientBuilder::new()
        .max_idle_per_host(0)
        .build()
        .unwrap();
    let uri = format!("http://127.0.0.1:{port}/d");

    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"z");
    let body = collect(client.execute(get(&uri)).await.unwrap()).await;
    assert_eq!(&body, b"z");

    assert_eq!(
        conn_count.load(Ordering::SeqCst),
        2,
        "pooling disabled: each request opens a new connection"
    );
}

#[tokio::test]
async fn host_header_is_derived_when_absent() {
    let (port, _conn_count, mut host_rx) = spawn_server(b"h", true).await;
    let client = http_client().await;
    let uri = format!("http://127.0.0.1:{port}/e");

    // No explicit Host header on the request.
    let request = http::Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Empty::<Bytes>::new())
        .unwrap();
    let _ = collect(client.execute(request).await.unwrap()).await;

    let host = host_rx
        .try_recv()
        .expect("server should have received a Host");
    assert_eq!(host, format!("127.0.0.1:{port}"));
}

#[tokio::test]
async fn explicit_host_header_is_preserved() {
    let (port, _conn_count, mut host_rx) = spawn_server(b"h", true).await;
    let client = http_client().await;
    let uri = format!("http://127.0.0.1:{port}/f");

    let request = http::Request::builder()
        .method("GET")
        .uri(&uri)
        .header("host", "example.invalid")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let _ = collect(client.execute(request).await.unwrap()).await;

    let host = host_rx
        .try_recv()
        .expect("server should have received a Host");
    assert_eq!(host, "example.invalid");
}

mod retry_safety {
    use crate::client::retry_is_safe;
    use crate::request::WriteState;

    fn state(head_sent: bool, body_touched: bool) -> WriteState {
        WriteState {
            head_sent,
            body_touched,
        }
    }

    // --- head not fully sent: safe for any method ---
    #[test]
    fn head_not_sent_is_safe_for_get() {
        assert!(retry_is_safe(&http::Method::GET, &state(false, false)));
    }

    #[test]
    fn head_not_sent_is_safe_for_post() {
        assert!(retry_is_safe(&http::Method::POST, &state(false, false)));
    }

    #[test]
    fn head_not_sent_is_safe_for_put() {
        assert!(retry_is_safe(&http::Method::PUT, &state(false, false)));
    }

    // --- head fully sent, bodyless request, side-effect free ---
    #[test]
    fn head_sent_bodyless_get_is_safe() {
        assert!(retry_is_safe(&http::Method::GET, &state(true, false)));
    }

    #[test]
    fn head_sent_bodyless_head_is_safe() {
        assert!(retry_is_safe(&http::Method::HEAD, &state(true, false)));
    }

    #[test]
    fn head_sent_bodyless_options_is_safe() {
        assert!(retry_is_safe(&http::Method::OPTIONS, &state(true, false)));
    }

    // --- head fully sent, might have had an empty body, not side-effect free ---
    #[test]
    fn head_sent_bodyless_post_is_unsafe() {
        assert!(!retry_is_safe(&http::Method::POST, &state(true, false)));
    }

    #[test]
    fn head_sent_bodyless_put_is_unsafe() {
        assert!(!retry_is_safe(&http::Method::PUT, &state(true, false)));
    }

    #[test]
    fn head_sent_bodyless_delete_is_unsafe() {
        assert!(!retry_is_safe(&http::Method::DELETE, &state(true, false)));
    }

    // --- body started: never safe ---
    #[test]
    fn body_touched_get_is_unsafe() {
        assert!(!retry_is_safe(&http::Method::GET, &state(true, true)));
    }

    #[test]
    fn body_touched_post_is_unsafe() {
        assert!(!retry_is_safe(&http::Method::POST, &state(true, true)));
    }
}
