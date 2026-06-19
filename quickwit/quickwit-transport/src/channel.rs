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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use hyper_util::rt::TokioIo;
use quickwit_config::{GrpcConfig, KeepAliveConfig};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How a [`ChannelFactory`] reaches peers.
#[derive(Clone)]
enum TransportMode {
    Plaintext,
    /// TLS terminated by us so the client identity can be hot-reloaded: the `client_config` holds a
    /// reloadable cert resolver, and we drive the handshake through a `tokio-rustls` connector
    /// rather than tonic's built-in `tls_config` (which bakes the identity in at startup).
    Tls {
        client_config: Arc<ClientConfig>,
        /// Server name the client verifies against the peer's certificate SAN. When `None`, the
        /// peer's IP address is used.
        expected_server_name: Option<ServerName<'static>>,
    },
}

/// Builds gRPC [`Channel`]s to peers. Created once at startup and shared by
/// everything that needs to connect to other nodes. When TLS is configured, every channel the
/// factory makes shares one reloadable `ClientConfig`, so a certificate reload is picked up by all
/// subsequent (re)connections.
#[derive(Clone)]
pub struct ChannelFactory {
    keep_alive_opt: Option<KeepAliveConfig>,
    mode: TransportMode,
}

impl ChannelFactory {
    /// Builds the factory from a node's gRPC config: plaintext, or TLS with a hot-reloadable client
    /// config. Under mTLS (`verify_client_cert`) the client also presents a hot-reloadable identity
    /// toward peers (see [`crate::make_tls_client_config`]).
    pub fn for_grpc(grpc_config: &GrpcConfig) -> anyhow::Result<Self> {
        let keep_alive_opt = grpc_config.keep_alive.clone();
        let Some(tls_config) = &grpc_config.tls_config else {
            return Ok(Self {
                keep_alive_opt,
                mode: TransportMode::Plaintext,
            });
        };
        let client_config = crate::tls::make_tls_client_config(tls_config)?;
        let expected_server_name = match &tls_config.expected_name {
            Some(name) => Some(
                ServerName::try_from(name.clone())
                    .with_context(|| format!("`{name}` is not a valid TLS server name"))?,
            ),
            None => None,
        };
        Ok(Self {
            keep_alive_opt,
            mode: TransportMode::Tls {
                client_config,
                expected_server_name,
            },
        })
    }

    /// Creates a lazily-connected channel to `socket_addr`. The channel reconnects through this
    /// factory's transport, so a reloaded certificate takes effect on the next (re)connection
    /// without rebuilding the channel.
    ///
    /// The function is `async` because `connect_lazy` requires a Tokio runtime context.
    pub async fn make_channel(&self, socket_addr: SocketAddr) -> Channel {
        // The scheme is always `http`: when TLS is enabled our custom connector hands tonic an
        // already-encrypted stream, so tonic must not attempt its own TLS.
        let uri = Uri::builder()
            .scheme("http")
            .authority(socket_addr.to_string())
            .path_and_query("/")
            .build()
            .expect("provided arguments should be valid");

        let mut endpoint = Endpoint::from(uri).connect_timeout(CONNECT_TIMEOUT);

        if let Some(keep_alive) = &self.keep_alive_opt {
            endpoint = endpoint
                .keep_alive_while_idle(true)
                .http2_keep_alive_interval(*keep_alive.interval)
                .keep_alive_timeout(*keep_alive.timeout);
        }
        match &self.mode {
            TransportMode::Plaintext => endpoint.connect_lazy(),
            TransportMode::Tls {
                client_config,
                expected_server_name,
            } => {
                let tls_connector = TlsConnector::from(client_config.clone());
                let server_name = match expected_server_name {
                    Some(server_name) => server_name.clone(),
                    None => ServerName::IpAddress(socket_addr.ip().into()),
                };
                endpoint.connect_with_connector_lazy(service_fn(move |_: Uri| {
                    let tls_connector = tls_connector.clone();
                    let server_name = server_name.clone();
                    async move {
                        let tcp_stream = TcpStream::connect(socket_addr).await?;
                        // Set `TCP_NODELAY` to match tonic's built-in connector (default on); a
                        // custom connector doesn't inherit it.
                        tcp_stream.set_nodelay(true)?;
                        let tls_stream = tls_connector.connect(server_name, tcp_stream).await?;
                        std::io::Result::Ok(TokioIo::new(tls_stream))
                    }
                }))
            }
        }
    }
}
