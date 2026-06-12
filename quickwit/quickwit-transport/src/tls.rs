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

//! Hot-reloadable TLS certificates, shared by the gRPC/REST *servers* and the gRPC *client*.
//!
//! The certificate is not baked into the [`rustls::ServerConfig`]/[`rustls::ClientConfig`].
//! Instead, the config holds a [`ReloadableCertResolver`] which rustls calls on *every* TLS
//! handshake. Swapping the resolver's backing certificate therefore takes effect for all new
//! connections without rebuilding the config or restarting the process; in-flight connections keep
//! the certificate they negotiated with.
//!
//! A background task re-reads the certificate files and swaps in a new certificate when it changes
//! on disk. It is driven by two triggers: a periodic poll (`cert_reload_interval`) and an
//! out-of-band [`reload_tls_cert`] (wired to `SIGHUP`) for an immediate reload. The CA used to
//! validate peer certificates (mTLS) is *not* hot-reloaded — rotating trust roots still requires a
//! restart.

use std::sync::{Arc, LazyLock};
use std::time::Duration;
use std::{fmt, fs, io};

use anyhow::Context;
use arc_swap::ArcSwap;
use quickwit_config::TlsConfig;
use quickwit_metrics::{LazyCounter, counter, lazy_counter};
use rustls::client::ResolvesClientCert;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, ServerConfig, SignatureScheme};
use tokio::sync::watch;
use tracing::{error, info, warn};

static TLS_CERT_RELOADS_TOTAL: LazyCounter = lazy_counter!(
        name: "cert_reloads_total",
        description: "Total number of TLS certificate hot-reload attempts, labeled by `result` \
                      (`success`: a new cert was applied without a restart; `error`: the attempt \
                      failed and the current cert was kept).",
        subsystem: "tls",
);

/// Process-wide trigger for an immediate certificate reload, in addition to the periodic poll.
static CERT_RELOAD_TX: LazyLock<watch::Sender<()>> = LazyLock::new(|| watch::channel(()).0);

/// Requests an immediate reload of every hot-reloadable TLS certificate (REST server, gRPC server,
/// and gRPC client identity). Cheap and signal-safe; typically called from a `SIGHUP` handler.
/// Reloads are idempotent — if nothing changed on disk, no certificate is swapped.
pub fn reload_tls_cert() {
    // `send_modify` notifies all receivers unconditionally and, unlike `send`, does not error when
    // there are none (e.g. TLS is not configured).
    CERT_RELOAD_TX.send_modify(|_| {});
}

fn io_error(error: String) -> io::Error {
    io::Error::other(error)
}

/// Loads a certificate chain (PEM) from `filename`.
fn load_certs(filename: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let certfile = fs::File::open(filename)
        .map_err(|error| io_error(format!("failed to open {filename}: {error}")))?;
    let mut reader = io::BufReader::new(certfile);
    rustls_pemfile::certs(&mut reader).collect()
}

/// Loads a single private key (PEM) from `filename`.
fn load_private_key(filename: &str) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = fs::File::open(filename)
        .map_err(|error| io_error(format!("failed to open {filename}: {error}")))?;
    let mut reader = io::BufReader::new(keyfile);
    let key_opt = rustls_pemfile::private_key(&mut reader)?;
    key_opt.ok_or_else(|| io_error(format!("no private key found in {filename}")))
}

/// Reads the certificate chain and private key from disk and assembles a [`CertifiedKey`] using the
/// process-wide default crypto provider (ring, see
/// `quickwit_cli::install_default_crypto_ring_provider`).
fn load_certified_key(cert_path: &str, key_path: &str) -> anyhow::Result<CertifiedKey> {
    let certs = load_certs(cert_path)?;
    if certs.is_empty() {
        anyhow::bail!("no certificate found in `{cert_path}`");
    }
    let key = load_private_key(key_path)?;
    let crypto_provider = rustls::crypto::CryptoProvider::get_default()
        .context("no default rustls crypto provider is installed")?;
    let signing_key = crypto_provider
        .key_provider
        .load_private_key(key)
        .with_context(|| format!("private key in `{key_path}` is not usable"))?;
    let certified_key = CertifiedKey::new(certs, signing_key);
    // Guard against swapping in a mismatched cert/key pair, e.g. if the reload task reads the two
    // files mid-rotation. A definite mismatch is fatal; an inconclusive result (key type that
    // cannot expose its public key) is tolerated since we cannot do better.
    match certified_key.keys_match() {
        Ok(()) => {}
        Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::KeyMismatch)) => {
            anyhow::bail!(
                "private key in `{key_path}` does not match certificate in `{cert_path}`"
            );
        }
        Err(error) => {
            warn!("could not verify that private key matches certificate: {error}");
        }
    }
    Ok(certified_key)
}

/// A cert resolver whose certificate can be swapped atomically at runtime. rustls calls `resolve`
/// on each handshake, so the latest stored certificate is always served. The same resolver works
/// for both server handshakes ([`ResolvesServerCert`]) and client handshakes
/// ([`ResolvesClientCert`], for gRPC mTLS client identities).
pub(crate) struct ReloadableCertResolver {
    cert_path: String,
    key_path: String,
    certified_key: ArcSwap<CertifiedKey>,
}

impl fmt::Debug for ReloadableCertResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReloadableCertResolver")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish()
    }
}

impl ReloadableCertResolver {
    fn load(cert_path: &str, key_path: &str) -> anyhow::Result<Arc<Self>> {
        let certified_key = load_certified_key(cert_path, key_path)?;
        let resolver = Self {
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
            certified_key: ArcSwap::from_pointee(certified_key),
        };
        Ok(Arc::new(resolver))
    }

    /// Re-reads the certificate and key from disk and swaps them in only if they parse and the
    /// certificate chain actually changed. Returns `true` when a new certificate was installed. On
    /// parse error the current certificate is kept and the error is returned (never serve a broken
    /// certificate). This does blocking file I/O and CPU-bound key parsing, so it is expected to
    /// run in a blocking context (see [`spawn_cert_reload_task`]).
    fn reload_and_compare(&self) -> anyhow::Result<bool> {
        let new_certified_key = load_certified_key(&self.cert_path, &self.key_path)?;
        let current_certified_key = self.certified_key.load();

        if new_certified_key.cert == current_certified_key.cert {
            return Ok(false);
        }
        self.certified_key.store(Arc::new(new_certified_key));
        Ok(true)
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.load_full())
    }
}

impl ResolvesClientCert for ReloadableCertResolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sig_schemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.load_full())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Builds a [`ServerConfig`] backed by a hot-reloadable certificate and spawns the background
/// reload task.
///
/// `alpn_protocols` must be set by the caller: the REST server negotiates `h2`/`http1.1`/`http1.0`
/// while the gRPC server negotiates `h2` only.
///
/// The returned config is the *only* owner of the cert resolver. The reload task holds a `Weak`
/// reference and exits on its own once the server drops the config at shutdown.
pub fn make_tls_server_config(
    tls_config: &TlsConfig,
    alpn_protocols: &[&[u8]],
) -> anyhow::Result<Arc<ServerConfig>> {
    let resolver = ReloadableCertResolver::load(&tls_config.cert_path, &tls_config.key_path)?;

    let builder = ServerConfig::builder();
    let builder = if tls_config.verify_client_cert {
        let roots = load_root_cert_store(&tls_config.ca_path)?;
        let client_cert_verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        builder.with_client_cert_verifier(client_cert_verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut server_config = builder.with_cert_resolver(resolver.clone());
    server_config.alpn_protocols = alpn_protocols
        .iter()
        .map(|protocol| protocol.to_vec())
        .collect();

    spawn_cert_reload_task(resolver, *tls_config.cert_reload_interval);
    Ok(Arc::new(server_config))
}

/// Builds a [`ClientConfig`] used by the gRPC client to connect to peers. Server certificates are
/// validated against `ca_path`. When `verify_client_cert` is set (mTLS), the client also presents
/// its own hot-reloadable identity (`cert_path`/`key_path`) and the reload task is spawned. ALPN is
/// fixed to `h2`, which is all gRPC speaks.
pub fn make_tls_client_config(tls_config: &TlsConfig) -> anyhow::Result<Arc<ClientConfig>> {
    let roots = load_root_cert_store(&tls_config.ca_path)?;
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let mut client_config = if tls_config.verify_client_cert {
        let resolver = ReloadableCertResolver::load(&tls_config.cert_path, &tls_config.key_path)?;
        spawn_cert_reload_task(resolver.clone(), *tls_config.cert_reload_interval);
        builder.with_client_cert_resolver(resolver)
    } else {
        builder.with_no_client_auth()
    };
    client_config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(client_config))
}

/// Loads the CA certificate(s) at `ca_path` into a [`RootCertStore`].
fn load_root_cert_store(ca_path: &str) -> anyhow::Result<RootCertStore> {
    let ca_certs = load_certs(ca_path)?;
    let mut roots = RootCertStore::empty();
    for ca_cert in ca_certs {
        roots.add(ca_cert)?;
    }
    Ok(roots)
}

/// Spawns a background task that reloads `resolver`'s certificate, driven by both a periodic poll
/// (`cert_reload_interval`) and the process-wide [`CERT_RELOAD_TX`] trigger (e.g. `SIGHUP`).
///
/// The task only holds a `Weak` reference, plus a transient strong reference while reloading. Once
/// the owner drops the config (the sole strong owner) the next `upgrade` fails and the task
/// returns.
fn spawn_cert_reload_task(resolver: Arc<ReloadableCertResolver>, cert_reload_interval: Duration) {
    let weak_resolver = Arc::downgrade(&resolver);
    drop(resolver);
    let mut cert_reload_rx = CERT_RELOAD_TX.subscribe();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cert_reload_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // The first tick completes immediately; skip it so we don't reload the certificate we just
        // loaded.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                // `CERT_RELOAD_TX` lives in a `static`, so the sender is never dropped and
                // `changed()` never returns an error; the match arm is exhaustive regardless.
                _ = cert_reload_rx.changed() => {}
            }
            let Some(resolver) = weak_resolver.upgrade() else {
                return;
            };
            // The reload does blocking file I/O and CPU-bound key parsing; keep it off the async
            // worker threads.
            let reload_resolver = resolver.clone();
            let reload_result =
                tokio::task::spawn_blocking(move || reload_resolver.reload_and_compare()).await;
            match reload_result {
                Ok(Ok(true)) => {
                    counter!(parent: TLS_CERT_RELOADS_TOTAL, "result" => "success").inc();
                    info!(cert_path = %resolver.cert_path, "reloaded TLS certificate");
                }
                Ok(Ok(false)) => {}
                Ok(Err(error)) => {
                    counter!(parent: TLS_CERT_RELOADS_TOTAL, "result" => "error").inc();
                    error!(
                        cert_path = %resolver.cert_path,
                        "failed to reload TLS certificate: {error:#}"
                    );
                }
                Err(join_error) => {
                    counter!(parent: TLS_CERT_RELOADS_TOTAL, "result" => "error").inc();
                    error!(
                        cert_path = %resolver.cert_path,
                        "failed to reload TLS certificate: {join_error:#}"
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use quickwit_config::HumanDuration;
    use tempfile::NamedTempFile;

    use super::*;

    // Two distinct, self-consistent cert/key pairs. They are unrelated to each other, so swapping
    // one for the other exercises the "certificate changed" path, and crossing them
    // (`SERVER_CERT_PATH` + `CA_KEY_PATH`) exercises the cert/key mismatch guard.
    //
    // The fixtures are shared with the `quickwit-integration-tests` TLS tests rather than
    // duplicated here.
    const SERVER_CERT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../resources/tests/tls/server.crt"
    );
    const SERVER_KEY_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../resources/tests/tls/server.key"
    );
    const CA_CERT_PATH: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../resources/tests/tls/ca.crt");
    const CA_KEY_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../resources/tests/tls/ca.key");

    /// `load_certified_key` needs the process-wide crypto provider to turn the private key into a
    /// signing key. In the binary this is installed by `quickwit_cli` (ring); tests install
    /// aws-lc-rs, which is rustls' default feature. Either provider works here. Installing twice is
    /// harmless, hence the ignored error.
    fn install_crypto_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    /// Copies `src_path` into a fresh temp file so the test can mutate it (simulating an on-disk
    /// rotation) without touching the committed fixture.
    fn temp_copy_of(src_path: &str) -> NamedTempFile {
        let contents = fs::read(src_path).unwrap();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&contents).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn path_str(temp_file: &NamedTempFile) -> &str {
        temp_file.path().to_str().unwrap()
    }

    fn test_tls_config(cert_path: &str, key_path: &str, ca_path: &str) -> TlsConfig {
        TlsConfig {
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
            ca_path: ca_path.to_string(),
            expected_name: None,
            verify_client_cert: true,
            cert_reload_interval: HumanDuration::try_from("5m".to_string()).unwrap(),
        }
    }

    #[test]
    fn test_load_certified_key_accepts_matching_pair() {
        install_crypto_provider();
        let certified_key = load_certified_key(SERVER_CERT_PATH, SERVER_KEY_PATH).unwrap();
        assert!(!certified_key.cert.is_empty());
    }

    #[test]
    fn test_load_certified_key_rejects_mismatched_pair() {
        install_crypto_provider();
        // server certificate with the CA's (unrelated) private key.
        let error = load_certified_key(SERVER_CERT_PATH, CA_KEY_PATH).unwrap_err();
        assert!(
            error.to_string().contains("does not match"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn test_load_certified_key_errors_on_empty_cert() {
        install_crypto_provider();
        let empty_cert = NamedTempFile::new().unwrap();
        let error = load_certified_key(path_str(&empty_cert), SERVER_KEY_PATH).unwrap_err();
        assert!(
            error.to_string().contains("no certificate"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn test_reload_and_compare_detects_no_change() {
        install_crypto_provider();
        let cert_file = temp_copy_of(SERVER_CERT_PATH);
        let key_file = temp_copy_of(SERVER_KEY_PATH);
        let resolver =
            ReloadableCertResolver::load(path_str(&cert_file), path_str(&key_file)).unwrap();
        // Nothing changed on disk, so no swap should happen.
        assert!(!resolver.reload_and_compare().unwrap());
    }

    #[test]
    fn test_reload_and_compare_swaps_on_change() {
        install_crypto_provider();
        let cert_file = temp_copy_of(SERVER_CERT_PATH);
        let key_file = temp_copy_of(SERVER_KEY_PATH);
        let resolver =
            ReloadableCertResolver::load(path_str(&cert_file), path_str(&key_file)).unwrap();
        let cert_before = resolver.certified_key.load_full().cert.clone();

        // Rotate both files on disk to the other (distinct) pair.
        fs::write(cert_file.path(), fs::read(CA_CERT_PATH).unwrap()).unwrap();
        fs::write(key_file.path(), fs::read(CA_KEY_PATH).unwrap()).unwrap();

        assert!(resolver.reload_and_compare().unwrap());
        let cert_after = resolver.certified_key.load_full().cert.clone();
        assert_ne!(cert_before, cert_after);
        // The served certificate now matches what is on disk.
        let expected = load_certs(CA_CERT_PATH).unwrap();
        assert_eq!(cert_after, expected);
    }

    #[test]
    fn test_reload_and_compare_keeps_cert_on_parse_error() {
        install_crypto_provider();
        let cert_file = temp_copy_of(SERVER_CERT_PATH);
        let key_file = temp_copy_of(SERVER_KEY_PATH);
        let resolver =
            ReloadableCertResolver::load(path_str(&cert_file), path_str(&key_file)).unwrap();
        let cert_before = resolver.certified_key.load_full().cert.clone();

        // Simulate reading the certificate mid-rotation (truncated / not yet valid PEM).
        fs::write(cert_file.path(), b"-----BEGIN CERTIFICATE-----\ntruncated").unwrap();

        assert!(resolver.reload_and_compare().is_err());
        // The previous certificate is kept; a broken file is never served.
        let cert_after = resolver.certified_key.load_full().cert.clone();
        assert_eq!(cert_before, cert_after);
    }

    #[tokio::test]
    async fn test_make_tls_client_config_with_mtls_identity() {
        install_crypto_provider();
        // mTLS: a client config that presents an identity and trusts the CA should build. Spawns
        // the reload task, hence the Tokio runtime.
        let tls_config = test_tls_config(SERVER_CERT_PATH, SERVER_KEY_PATH, CA_CERT_PATH);
        let client_config = make_tls_client_config(&tls_config).unwrap();
        assert_eq!(client_config.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn test_reload_tls_cert_notifies_subscribers() {
        let mut reload_request_rx = CERT_RELOAD_TX.subscribe();
        // Clear any pending notification so we only observe the one we trigger.
        reload_request_rx.mark_unchanged();
        reload_tls_cert();
        assert!(reload_request_rx.has_changed().unwrap());
    }
}
