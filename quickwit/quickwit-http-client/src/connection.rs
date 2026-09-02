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
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use socket2::TcpKeepalive;
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

/// Default TCP keepalive idle time (matches `reqwest`'s default).
const KEEPALIVE_TIME: Duration = Duration::from_secs(15);
/// Happy-eyeballs fallback delay before starting the second address family
const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(300);

/// Process-wide counter used to round-robin the starting index into the
/// resolved IP list across concurrent fresh connects, so they spread across
/// the fleet instead of all converging on the first reachable IP.
// i wonder if this should be an internal of the DnsResolver instead
static CONNECT_ROTATION: AtomicUsize = AtomicUsize::new(0);

/// Open a connection to an Endpoint.
///
/// Perform DNS resolution, TCP and optionally TLS handshake.
/// Set a few socket options for keepalive and no-delay.
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
        connect_addresses(
            &ips,
            endpoint.port,
            connect_timeout,
            tls_connector,
            server_name.as_ref(),
        )
        .await
    };

    tokio::time::timeout(connect_timeout, connect)
        .await
        .map_err(|_| HttpError::Timeout(connect_timeout, "connect".to_string()))?
}

/// Connects to one of the requested IPs
///
/// Uses happy-eyeball for dual-stacked endpoints
async fn connect_addresses(
    ips: &[IpAddr],
    port: u16,
    connect_timeout: Duration,
    tls_connector: Option<&TlsConnector>,
    server_name: Option<&rustls::pki_types::ServerName<'static>>,
) -> Result<ConnStream, HttpError> {
    if ips.is_empty() {
        return Err(HttpError::Dns {
            host: String::new(),
            message: "no addresses resolved".to_string(),
        });
    }
    let (v4, v6) = split_by_family(ips);
    // clamp so many ips don't cause overly short timeout
    let divisor = v4.len().max(v6.len()).clamp(1, 4) as u32;
    let per_addr = connect_timeout / divisor;

    let v4 = rotate(v4);
    let v6 = rotate(v6);

    if v6.is_empty() {
        return connect_family(v4, port, per_addr, tls_connector, server_name).await;
    }
    if v4.is_empty() {
        return connect_family(v6, port, per_addr, tls_connector, server_name).await;
    }

    let v4_fut = connect_family(v4, port, per_addr, tls_connector, server_name);
    let v6_fut = async {
        tokio::time::sleep(HAPPY_EYEBALLS_DELAY).await;
        connect_family(v6, port, per_addr, tls_connector, server_name).await
    };
    race_first_success(v4_fut, v6_fut).await
}

async fn connect_family(
    ips: Vec<IpAddr>,
    port: u16,
    per_addr: Duration,
    tls_connector: Option<&TlsConnector>,
    server_name: Option<&rustls::pki_types::ServerName<'static>>,
) -> Result<ConnStream, HttpError> {
    let mut last_err: Option<HttpError> = None;
    for ip in ips {
        let addr = SocketAddr::new(ip, port);
        match tokio::time::timeout(per_addr, TcpStream::connect(addr)).await {
            Ok(Ok(tcp)) => {
                set_socket_opts(&tcp);
                if let Some(connector) = tls_connector {
                    match tokio::time::timeout(
                        per_addr,
                        connector.connect(server_name.cloned().unwrap(), tcp),
                    )
                    .await
                    {
                        Ok(Ok(tls)) => return Ok(ConnStream::Tls(Box::new(tls))),
                        Ok(Err(err)) => {
                            last_err = Some(HttpError::Tls(format!("tls handshake failed: {err}")));
                            continue;
                        }
                        Err(_) => {
                            last_err =
                                Some(HttpError::Timeout(per_addr, "tls handshake".to_string()));
                            continue;
                        }
                    }
                }
                return Ok(ConnStream::Plain(tcp));
            }
            Ok(Err(err)) => last_err = Some(HttpError::Io(err)),
            Err(_) => {
                last_err = Some(HttpError::Timeout(per_addr, "tcp connect".to_string()));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| HttpError::Dns {
        host: String::new(),
        message: "no addresses resolved".to_string(),
    }))
}

async fn race_first_success<A, B>(a: A, b: B) -> Result<ConnStream, HttpError>
where
    A: std::future::Future<Output = Result<ConnStream, HttpError>> + Send,
    B: std::future::Future<Output = Result<ConnStream, HttpError>> + Send,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    tokio::select! {
        result = &mut a => match result {
            Ok(conn) => Ok(conn),
            Err(_) => b.await,
        },
        result = &mut b => match result {
            Ok(conn) => Ok(conn),
            Err(_) => a.await,
        },
    }
}

fn set_socket_opts(tcp: &TcpStream) {
    let _ = tcp.set_nodelay(true);
    let keepalive = TcpKeepalive::new()
        .with_time(KEEPALIVE_TIME)
        .with_interval(KEEPALIVE_TIME)
        .with_retries(3);
    if let Err(err) = socket2::SockRef::from(tcp).set_tcp_keepalive(&keepalive) {
        tracing::debug!("failed to set TCP keepalive: {err}");
    }
}

fn split_by_family(ips: &[IpAddr]) -> (Vec<IpAddr>, Vec<IpAddr>) {
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for ip in ips {
        match ip {
            IpAddr::V4(_) => v4.push(*ip),
            IpAddr::V6(_) => v6.push(*ip),
        }
    }
    (v4, v6)
}

/// Rotates the list so concurrent fresh connects start at different IPs,
/// spreading load across the resolved fleet. Uses a process-wide counter
/// so concurrent connects from multiple threads get distinct offsets.
fn rotate(mut ips: Vec<IpAddr>) -> Vec<IpAddr> {
    if ips.len() <= 1 {
        return ips;
    }
    let offset = CONNECT_ROTATION.fetch_add(1, Ordering::Relaxed) % ips.len();
    ips.rotate_left(offset);
    ips
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

    async fn happy_eyeballs_helper(broken: IpAddr, working: IpAddr, working_listener: TcpListener) {
        let port = working_listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = working_listener.accept().await.unwrap();
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
            port,
        };
        let resolver = FixedResolver(vec![broken, working]);

        let start = std::time::Instant::now();
        let mut conn = connect(&resolver, &endpoint, None, Duration::from_secs(2))
            .await
            .expect("should connect via the working family");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "happy-eyeballs should not wait for the broken family"
        );

        let payload = b"hello happy eyeballs";
        conn.write_all(payload).await.unwrap();
        let mut got = vec![0u8; payload.len()];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, payload);
        drop(conn);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn happy_eyeballs_prefers_v4_when_v6_broken() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        happy_eyeballs_helper(
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            "127.0.0.1".parse().unwrap(),
            listener,
        )
        .await;
    }

    #[tokio::test]
    async fn happy_eyeballs_prefers_v6_when_v4_broken() {
        // Bind an IPv6-only listener on ::1.
        let socket =
            socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::STREAM, None).unwrap();
        // Ensure it is not dual-stack: only IPv6 connections are accepted.
        socket.set_only_v6(true).unwrap();
        socket.set_nonblocking(true).unwrap();
        socket
            .bind(&socket2::SockAddr::from(std::net::SocketAddr::new(
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                0u16,
            )))
            .unwrap();
        socket.listen(1024).unwrap();
        let listener = TcpListener::from_std(socket.into()).unwrap();

        happy_eyeballs_helper(
            "127.0.0.1".parse().unwrap(),
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            listener,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connect_timeout_divided_across_addresses() {
        // Grab 4 free ports, then drop the listeners so all 4 connects refuse.
        let ports: Vec<u16> = (0..4)
            .map(|_| {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap().port()
            })
            .collect();

        let ips: Vec<IpAddr> = (0..4).map(|_| "127.0.0.1".parse().unwrap()).collect();
        let resolver = FixedResolver(ips);
        let endpoint = Endpoint {
            tls: false,
            host: "127.0.0.1".to_string(),
            port: ports[0],
        };

        tokio::select! {
            result = connect(&resolver, &endpoint, None, Duration::from_secs(4)) => {
                let err = result.unwrap_err();
                assert!(
                    err.is_io() || err.is_timeout(),
                    "expected an io or timeout error, got {err:?}"
                );
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                panic!("connect should win the race against sleep(5s)");
            }
        }
    }

    #[test]
    fn rotate_changes_starting_index() {
        use super::rotate;
        let ips: Vec<IpAddr> = (0..4)
            .map(|i| format!("127.0.0.{i}").parse().unwrap())
            .collect();
        // Two calls should produce different rotations (the counter increments).
        let r1 = rotate(ips.clone());
        let r2 = rotate(ips.clone());
        assert_ne!(r1, r2, "consecutive rotates should start at different IPs");
        // Each rotation is a valid permutation of the input.
        let mut sorted = r1.clone();
        sorted.sort();
        assert_eq!(sorted, ips, "rotate preserves the set of IPs");
    }
}
