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

use aws_smithy_runtime_api::client::http::HttpConnector;
use aws_smithy_runtime_api::http::Request as SdkRequest;
use aws_smithy_types::body::SdkBody;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::SingleBufferHttp1Connector;
use crate::client::HttpClientBuilder;

async fn read_request_head(sock: &mut tokio::net::TcpStream) -> bool {
    let mut acc: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
            return true;
        }
        let n = match tokio::time::timeout(Duration::from_secs(2), sock.read(&mut tmp)).await {
            Ok(Ok(n)) => n,
            _ => return false,
        };
        if n == 0 {
            return false;
        }
        acc.extend_from_slice(&tmp[..n]);
    }
}

fn sdk_get_request(uri: &str, host: &str) -> SdkRequest<SdkBody> {
    let mut request = SdkRequest::new(SdkBody::empty());
    request.set_uri(uri).unwrap();
    request.set_method("GET").unwrap();
    request
        .headers_mut()
        .try_insert("host", host.to_string())
        .unwrap();
    request
}

#[tokio::test]
async fn s3_adapter_returns_single_segment_sdk_body() {
    // A few MB so a streaming client would necessarily receive it as many
    // chunks; a recognizable pattern verifies integrity over the whole body.
    let body: Vec<u8> = {
        let mut buf = Vec::with_capacity(4 * 1024 * 1024);
        let mut counter: u32 = 0;
        while buf.len() < 4 * 1024 * 1024 {
            counter = counter.wrapping_add(1);
            buf.extend_from_slice(&counter.to_le_bytes());
        }
        buf
    };
    let response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    let mut full = response;
    full.extend_from_slice(&body);
    let response_bytes = bytes::Bytes::from(full);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        loop {
            if !read_request_head(&mut sock).await {
                return;
            }
            sock.write_all(&response_bytes).await.unwrap();
            sock.flush().await.unwrap();
        }
    });

    let client = HttpClientBuilder::new().build().unwrap();
    let connector = SingleBufferHttp1Connector::new(client);
    let request = sdk_get_request(
        &format!("http://127.0.0.1:{port}/bucket/key"),
        &format!("127.0.0.1:{port}"),
    );

    let response = connector.call(request).await.unwrap();
    assert_eq!(response.status().as_u16(), 200);

    let body_bytes = response
        .body()
        .bytes()
        .expect("expected a single buffer");
    assert_eq!(body_bytes.len(), body.len());
    assert_eq!(body_bytes, body.as_slice());

    server.abort();
}

#[tokio::test]
async fn s3_adapter_maps_connect_refused_to_connector_error() {
    let client = HttpClientBuilder::new()
        .connect_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let connector = SingleBufferHttp1Connector::new(client);
    // Grab a free port and drop the listener so the connect is refused.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let request = sdk_get_request(
        &format!("http://127.0.0.1:{port}/bucket/key"),
        &format!("127.0.0.1:{port}"),
    );
    let err = connector.call(request).await.unwrap_err();
    // A refused connect is an I/O-class connector error.
    assert!(err.is_io(), "expected an io connector error, got {err:?}");
}

#[tokio::test]
async fn s3_adapter_maps_read_timeout_to_connector_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _server = tokio::spawn(async move {
        let (_sock, _) = listener.accept().await.unwrap();
        // Hold the connection open without responding
        std::future::pending::<()>().await;
    });

    let client = HttpClientBuilder::new()
        .read_timeout(Duration::from_millis(50))
        .build()
        .unwrap();
    let connector = SingleBufferHttp1Connector::new(client);
    let request = sdk_get_request(
        &format!("http://127.0.0.1:{port}/bucket/key"),
        &format!("127.0.0.1:{port}"),
    );
    let err = connector.call(request).await.unwrap_err();
    assert!(
        err.is_timeout(),
        "expected a timeout connector error, got {err:?}"
    );
}
