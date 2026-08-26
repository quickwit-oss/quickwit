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

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::dns::DnsResolver;
use crate::endpoint::Endpoint;
use crate::error::HttpError;

type TlsConnStream = TlsStream<TcpStream>;

/// Either a plain TCP connection, or a TLS connection.
#[derive(Debug)]
pub enum ConnStream {
    Plain(TcpStream),
    // TlsConnStream is rather big, box it to keep the enum small
    Tls(Box<TlsConnStream>),
}

impl AsyncRead for ConnStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ConnStream::Tls(s) => Pin::new(&mut **s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ConnStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ConnStream::Tls(s) => Pin::new(&mut **s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ConnStream::Tls(s) => Pin::new(&mut **s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ConnStream::Tls(s) => Pin::new(&mut **s).poll_shutdown(cx),
        }
    }
}

/// Open a connection to an Endpoint.
///
/// Perform DNS resolution, TCP and optionally TLS handshake.
/// Set `TCP_NODELAY`.
// TODO: happy-eyeballs and connect-timeout division across addresses
// TODO: if multiple connection connect, it would be nice to have a way to
// put them all in the pool. return a stream of ConnStream maybe?
// TODO TCP keepalive
pub async fn connect(
    resolver: &dyn DnsResolver,
    endpoint: &Endpoint,
    tls_connector: Option<&TlsConnector>,
    connect_timeout: Duration,
) -> Result<ConnStream, HttpError> {
    let connect = async {
        let ips = resolver.resolve(&endpoint.host).await?;
        let tls_connector = match (endpoint.tls, tls_connector) {
            (true, Some(connector)) => Some(connector),
            (true, None) => {
                return Err(HttpError::Tls(
                    "https endpoint requested but no TLS connector was provided".to_string(),
                ));
            }
            (false, _) => None,
        };
        let server_name = match tls_connector {
            Some(_) => Some(endpoint.server_name()?),
            None => None,
        };
        let mut last_err: Option<HttpError> = None;
        for ip in ips {
            let addr = SocketAddr::new(ip, endpoint.port);
            match TcpStream::connect(addr).await {
                Ok(tcp) => {
                    let _ = tcp.set_nodelay(true);
                    if let Some(connector) = tls_connector {
                        match connector.connect(server_name.clone().unwrap(), tcp).await {
                            Ok(tls) => return Ok(ConnStream::Tls(Box::new(tls))),
                            Err(err) => {
                                last_err =
                                    Some(HttpError::Tls(format!("tls handshake failed: {err}")));
                                continue;
                            }
                        }
                    }
                    return Ok(ConnStream::Plain(tcp));
                }
                Err(err) => last_err = Some(HttpError::Io(err)),
            }
        }
        Err(last_err.unwrap_or_else(|| HttpError::Dns {
            host: endpoint.host.clone(),
            message: "no addresses resolved".to_string(),
        }))
    };

    tokio::time::timeout(connect_timeout, connect)
        .await
        .map_err(|_| HttpError::Timeout(connect_timeout, "connect".to_string()))?
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use super::connect;
    use crate::dns::{DefaultDnsResolver, DnsResolver, ResolveFuture};
    use crate::endpoint::Endpoint;
    use crate::error::HttpError;

    // The test certificates live in the shared test-resources directory, one
    // level up from this crate. The server cert's SAN includes `127.0.0.1`,
    // so we connect by IP literal and verify against our own CA rather than
    // the native root store.
    const CA_CERT_PATH: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../resources/tests/tls/ca.crt");
    const SERVER_CERT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../resources/tests/tls/server.crt"
    );
    const SERVER_KEY_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../resources/tests/tls/server.key"
    );

    struct HangingResolver;
    impl DnsResolver for HangingResolver {
        fn resolve<'a>(&'a self, _host: &'a str) -> ResolveFuture<'a> {
            Box::pin(std::future::pending::<Result<Vec<IpAddr>, HttpError>>())
        }
    }

    struct EmptyResolver;
    impl DnsResolver for EmptyResolver {
        fn resolve<'a>(&'a self, host: &'a str) -> ResolveFuture<'a> {
            let host = host.to_string();
            Box::pin(async move {
                Err(HttpError::Dns {
                    host,
                    message: "no addresses resolved".to_string(),
                })
            })
        }
    }

    struct FixedResolver(Vec<IpAddr>);
    impl DnsResolver for FixedResolver {
        fn resolve<'a>(&'a self, _host: &'a str) -> ResolveFuture<'a> {
            let ips = self.0.clone();
            Box::pin(async move { Ok(ips) })
        }
    }

    fn load_certs(path: &str) -> Vec<CertificateDer<'static>> {
        let mut reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());
        rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn load_key(path: &str) -> PrivateKeyDer<'static> {
        let mut reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());
        rustls_pemfile::private_key(&mut reader).unwrap().unwrap()
    }

    fn server_config() -> Arc<rustls::ServerConfig> {
        let certs = load_certs(SERVER_CERT_PATH);
        let key = load_key(SERVER_KEY_PATH);
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        Arc::new(config)
    }

    fn client_config() -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in load_certs(CA_CERT_PATH) {
            roots.add(cert).unwrap();
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(config)
    }

    #[tokio::test]
    async fn plain_tcp_echo() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = match sock.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                sock.write_all(&buf[..n]).await.unwrap();
            }
        });

        let endpoint = Endpoint {
            tls: false,
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        };
        let mut conn = connect(&DefaultDnsResolver, &endpoint, None, Duration::from_secs(5))
            .await
            .unwrap();

        let payload = b"hello plain";
        conn.write_all(payload).await.unwrap();
        let mut got = vec![0u8; payload.len()];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, payload);
        drop(conn);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_handshake_echo() {
        // try to install a crypto provider, might fail if another test already ran in the same
        // process, ignore the failure
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let acceptor = TlsAcceptor::from(server_config());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(sock).await.unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = match tls.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                tls.write_all(&buf[..n]).await.unwrap();
            }
        });

        let connector = TlsConnector::from(client_config());
        let endpoint = Endpoint {
            tls: true,
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        };
        let mut conn = connect(
            &DefaultDnsResolver,
            &endpoint,
            Some(&connector),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let payload = b"hello tls";
        conn.write_all(payload).await.unwrap();
        let mut got = vec![0u8; payload.len()];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, payload);
        drop(conn);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_handshake_failure_tries_next_ip() {
        // try to install a crypto provider, might fail if another test already ran in the same
        // process, ignore the failure
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let acceptor = TlsAcceptor::from(server_config());
        let good_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = good_listener.local_addr().unwrap().port();

        let bad_listener = TcpListener::bind(("127.0.0.2", port)).await.unwrap();

        let good_acceptor = acceptor.clone();
        let good_server = tokio::spawn(async move {
            let (sock, _) = good_listener.accept().await.unwrap();
            let mut tls = good_acceptor.accept(sock).await.unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = match tls.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                tls.write_all(&buf[..n]).await.unwrap();
            }
        });
        // The bad server just accepts and immediately closes, so the rustls
        // handshake reads EOF and errors.
        let bad_server = tokio::spawn(async move {
            let (_sock, _) = bad_listener.accept().await.unwrap();
        });

        let connector = TlsConnector::from(client_config());
        let endpoint = Endpoint {
            tls: true,
            // this will get resolved to both ip, but must match the SAN in our certificate
            host: "127.0.0.1".to_string(),
            port,
        };
        let resolver = FixedResolver(vec![
            "127.0.0.2".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
        ]);
        let mut conn = connect(
            &resolver,
            &endpoint,
            Some(&connector),
            Duration::from_secs(5),
        )
        .await
        .expect("should fall back to the second IP");

        let payload = b"hello after fallback";
        conn.write_all(payload).await.unwrap();
        let mut got = vec![0u8; payload.len()];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, payload);
        drop(conn);
        good_server.await.unwrap();
        bad_server.await.unwrap();
    }

    #[tokio::test]
    async fn connect_timeout_fires() {
        let resolver = HangingResolver;
        let endpoint = Endpoint {
            tls: false,
            host: "example.com".to_string(),
            port: 80,
        };
        let err = connect(&resolver, &endpoint, None, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(err.is_timeout(), "expected a timeout, got {err:?}");
    }

    #[tokio::test]
    async fn no_address_resolution_fails() {
        let resolver = EmptyResolver;
        let endpoint = Endpoint {
            tls: false,
            host: "nope.example".to_string(),
            port: 80,
        };
        let err = connect(&resolver, &endpoint, None, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(
            matches!(err, HttpError::Dns { .. }),
            "expected a dns error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn connect_refused_surfaces_io_error() {
        // Bind to grab a free port, then drop the listener so the port has no
        // listener and the connect is refused (ECONNREFUSED on loopback).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let endpoint = Endpoint {
            tls: false,
            host: "127.0.0.1".to_string(),
            port,
        };
        let err = connect(&DefaultDnsResolver, &endpoint, None, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(
            err.is_io(),
            "expected an io error for a refused connect, got {err:?}"
        );
    }

    #[tokio::test]
    async fn https_without_connector_is_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Keep the listener alive so the TCP connect succeeds and we reach the
        // TLS-connector check rather than a connect error.
        let _server = tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
        });
        let endpoint = Endpoint {
            tls: true,
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        };
        let err = connect(&DefaultDnsResolver, &endpoint, None, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(
            matches!(err, HttpError::Tls(_)),
            "expected a tls error for a missing connector, got {err:?}"
        );
    }
}
