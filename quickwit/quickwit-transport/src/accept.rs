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

//! Server-side TLS handshaking for accepted TCP connections, shared by the REST and gRPC servers.

use std::io;
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tracing::error;

/// Maximum number of TLS handshakes performed concurrently. A handshake holds a slot only until it
/// completes or times out, so this bounds the work a burst of new connections can create while
/// still letting independent handshakes make progress.
const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 256;

/// Upper bound on a single TLS handshake. A client that opens a TCP connection but never sends a
/// `ClientHello` would otherwise hold its handshake slot indefinitely; this lets us drop it.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Wraps a stream of accepted TCP connections, performing the server-side TLS handshake on each one
/// and yielding the decrypted stream. A connection whose handshake fails or times out is logged and
/// dropped rather than tearing down the whole server.
///
/// Handshakes run concurrently (up to [`MAX_CONCURRENT_TLS_HANDSHAKES`]), each bounded by
/// [`TLS_HANDSHAKE_TIMEOUT`]. This matters for correctness, not just throughput: performing the
/// handshake inline on the accept path would let a single client that connects but never sends a
/// `ClientHello` stall the `accept` await and block the server from accepting any further
/// connections.
pub fn accept_tls_incoming<S, E>(
    tcp_incoming: S,
    tls_acceptor: TlsAcceptor,
) -> impl Stream<Item = io::Result<TlsStream<TcpStream>>>
where
    S: Stream<Item = Result<TcpStream, E>> + Send + 'static,
    E: std::fmt::Display,
{
    tcp_incoming
        .map(move |tcp_result| {
            let tls_acceptor = tls_acceptor.clone();
            async move {
                let tcp_stream = match tcp_result {
                    Ok(tcp_stream) => tcp_stream,
                    Err(error) => {
                        error!("failed to accept TCP connection: {error:#}");
                        return None;
                    }
                };
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, tls_acceptor.accept(tcp_stream))
                    .await
                {
                    Ok(Ok(tls_stream)) => Some(Ok::<_, io::Error>(tls_stream)),
                    Ok(Err(error)) => {
                        error!("failed to perform TLS handshake: {error:#}");
                        None
                    }
                    Err(_elapsed) => {
                        error!("timed out while performing TLS handshake");
                        None
                    }
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_TLS_HANDSHAKES)
        .filter_map(|item| async move { item })
}
