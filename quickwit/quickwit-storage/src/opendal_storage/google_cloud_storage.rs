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

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{GoogleCloudStorageConfig, StorageBackend};
use regex::Regex;
use tracing::info;

use super::OpendalStorage;
use crate::debouncer::DebouncedStorage;
use crate::{Storage, StorageFactory, StorageResolverError};

/// Google cloud storage resolver.
pub struct GoogleCloudStorageFactory {
    storage_config: GoogleCloudStorageConfig,
}

impl GoogleCloudStorageFactory {
    /// Create a new google cloud storage factory via config.
    pub fn new(storage_config: GoogleCloudStorageConfig) -> Self {
        Self { storage_config }
    }
}

#[async_trait]
impl StorageFactory for GoogleCloudStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Google
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = from_uri(&self.storage_config, uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

/// Helpers to configure the GCP local test setup.
#[cfg(feature = "integration-testsuite")]
pub mod test_config_helpers {
    use super::*;

    /// URL of the local GCP emulator.
    pub const LOCAL_GCP_EMULATOR_ENDPOINT: &str = "http://127.0.0.1:4443";
    /// Creates a storage connecting to a local emulated google cloud storage.
    pub fn new_emulated_google_cloud_storage(
        uri: &Uri,
    ) -> Result<OpendalStorage, StorageResolverError> {
        let (bucket, root) = parse_google_uri(uri).expect("must be valid google uri");

        let cfg = opendal::services::Gcs::default()
            .bucket(&bucket)
            .root(&root.to_string_lossy())
            .endpoint(LOCAL_GCP_EMULATOR_ENDPOINT)
            .allow_anonymous() // Disable authentication for fake GCS server
            .disable_vm_metadata(); // Disable GCE metadata server requests
        let store = OpendalStorage::new_google_cloud_storage(uri.clone(), cfg)?;
        Ok(store)
    }
}

fn from_uri(
    google_cloud_storage_config: &GoogleCloudStorageConfig,
    uri: &Uri,
) -> Result<OpendalStorage, StorageResolverError> {
    let (bucket_name, prefix) = parse_google_uri(uri).ok_or_else(|| {
        let message = format!("failed to extract bucket name from google URI: {uri}");
        StorageResolverError::InvalidUri(message)
    })?;

    let mut cfg = opendal::services::Gcs::default()
        .bucket(&bucket_name)
        .root(&prefix.to_string_lossy());

    if let Some(credential_path) = google_cloud_storage_config.resolve_credential_path() {
        info!(path=%credential_path, "fetching google cloud storage credentials from path");
        cfg = cfg.credential_path(&credential_path);
    }
    let store = OpendalStorage::new_google_cloud_storage(uri.clone(), cfg)?;
    Ok(store)
}

fn parse_google_uri(uri: &Uri) -> Option<(String, PathBuf)> {
    // Ex: gs://bucket/prefix.
    static URI_PTN: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"gs(\+[^:]+)?://(?P<bucket>[^/]+)(/(?P<prefix>.*))?$")
            .expect("The regular expression should compile.")
    });

    let captures = URI_PTN.captures(uri.as_str())?;

    let bucket = captures.name("bucket")?.as_str().to_string();
    let prefix = captures
        .name("prefix")
        .map(|prefix_match| PathBuf::from(prefix_match.as_str()))
        .unwrap_or_default();
    Some((bucket, prefix))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use base64::Engine;
    use opendal::raw::HttpClient;
    use quickwit_common::uri::Uri;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivateSec1KeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::{OpendalStorage, parse_google_uri};
    use crate::Storage;

    // Test-only CA and server certificate for 127.0.0.1/localhost, valid from
    // 2020 to 3020. The client trusts only this CA, so the test never depends
    // on the host root store.
    // Regenerate with `quickwit-storage/tests/test_data/regenerate-gcs-certs.sh`.
    const TEST_CA_CERT_DER_BASE64: &str = concat!(
        "MIIBizCCATGgAwIBAgICEAEwCgYIKoZIzj0EAwIwGzEZMBcGA1UEAwwQUXVpY2t3",
        "aXQgVGVzdCBDQTAgFw0yMDAxMDEwMDAwMDBaGA8zMDIwMDEwMTAwMDAwMFowGzEZ",
        "MBcGA1UEAwwQUXVpY2t3aXQgVGVzdCBDQTBZMBMGByqGSM49AgEGCCqGSM49AwEH",
        "A0IABH+1ZvivhT0E5FydtoMGBkyenql8XPyFTPBhTfHycTjfTWJiETjILGadPLKY",
        "OZJky8ThPZUpKAux5M4SaazdX1WjYzBhMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0P",
        "AQH/BAQDAgEGMB0GA1UdDgQWBBQmOMvIHAegmBHwvdVGyguC/57/4zAfBgNVHSME",
        "GDAWgBQmOMvIHAegmBHwvdVGyguC/57/4zAKBggqhkjOPQQDAgNIADBFAiEAnE7M",
        "lcB35MOr+7WKDAhu/c6ZrpgRz+chqqfc3g5YTOECIEDmoPkOigkulNON67opCPaT",
        "y+MQhMA9KDEzE3t/CY9V",
    );

    const TEST_SERVER_CERT_DER_BASE64: &str = concat!(
        "MIIBszCCAVqgAwIBAgICEAIwCgYIKoZIzj0EAwIwGzEZMBcGA1UEAwwQUXVpY2t3",
        "aXQgVGVzdCBDQTAgFw0yMDAxMDEwMDAwMDBaGA8zMDIwMDEwMTAwMDAwMFowFDES",
        "MBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEowPj",
        "3vpXPAkf04MNeGhaDBvtwMsmeipV57lSWx5K2FwXH7JDmt74k4HmQFB6JESy6FbM",
        "tAVhivr7kG5dWKK/sqOBkjCBjzAMBgNVHRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIH",
        "gDATBgNVHSUEDDAKBggrBgEFBQcDATAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8A",
        "AAEwHQYDVR0OBBYEFES7BP5uQpa3+PktDVlgc9zYGIqDMB8GA1UdIwQYMBaAFCY4",
        "y8gcB6CYEfC91UbKC4L/nv/jMAoGCCqGSM49BAMCA0cAMEQCIGtIKEWRn7ec82TY",
        "s1jrUoKWnhzRDbZTUtvXORk190rHAiAosxVgu45TjDyuROKU39TxJ1z+JObhNGk8",
        "J6PkuOTFqg==",
    );

    const TEST_SERVER_KEY_DER_BASE64: &str = concat!(
        "MHcCAQEEIIql19flBaZJE16Ivs8GjdJHedhuU5YFZgvIn4WaOs6HoAoGCCqGSM49",
        "AwEHoUQDQgAEowPj3vpXPAkf04MNeGhaDBvtwMsmeipV57lSWx5K2FwXH7JDmt74",
        "k4HmQFB6JESy6FbMtAVhivr7kG5dWKK/sg==",
    );

    type LocalHttpsGcsServer = (String, JoinHandle<anyhow::Result<()>>);

    #[test]
    fn test_parse_google_uri() {
        assert!(parse_google_uri(&Uri::for_test("gs://")).is_none());

        let (bucket, prefix) = parse_google_uri(&Uri::for_test("gs://test-bucket")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert!(prefix.to_str().unwrap().is_empty());

        let (bucket, prefix) = parse_google_uri(&Uri::for_test("gs://test-bucket/")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert!(prefix.to_str().unwrap().is_empty());

        let (bucket, prefix) =
            parse_google_uri(&Uri::for_test("gs://test-bucket/indexes")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert_eq!(prefix.to_str().unwrap(), "indexes");
    }

    #[tokio::test]
    async fn test_gcs_storage_get_slice_over_https_with_verified_tls() -> anyhow::Result<()> {
        // Nextest runs tests in separate processes, so this test must not rely
        // on another rustls user having already selected a process-wide provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (endpoint, server_task) = start_local_https_gcs_server().await?;
        let ca_cert_der = decode_test_der(TEST_CA_CERT_DER_BASE64)?;
        let ca_cert = reqwest_013::Certificate::from_der(&ca_cert_der)?;
        let reqwest_client = reqwest_013::Client::builder()
            .no_proxy()
            .tls_certs_only([ca_cert])
            .build()?;

        let cfg = opendal::services::Gcs::default()
            .bucket("quickwit-test-bucket")
            .endpoint(&endpoint)
            .allow_anonymous()
            .disable_config_load()
            .disable_vm_metadata();
        let storage = OpendalStorage::new_google_cloud_storage_with_http_client_for_test(
            Uri::for_test("gs://quickwit-test-bucket"),
            cfg,
            HttpClient::with(reqwest_client),
        )?;

        let bytes = storage.get_slice(Path::new("hello.txt"), 0..2).await?;
        assert_eq!(bytes.as_slice(), b"ok");
        server_task.await??;
        Ok(())
    }

    async fn start_local_https_gcs_server() -> anyhow::Result<LocalHttpsGcsServer> {
        let cert_chain = vec![CertificateDer::from(decode_test_der(
            TEST_SERVER_CERT_DER_BASE64,
        )?)];
        let private_key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(decode_test_der(
            TEST_SERVER_KEY_DER_BASE64,
        )?));
        let tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)?;
        let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut stream = tls_acceptor.accept(stream).await?;
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let bytes_read = stream.read(&mut buffer).await?;
                if bytes_read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..bytes_read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            let (header_block, _) = request
                .split_once("\r\n\r\n")
                .expect("request must contain HTTP header terminator");
            let mut header_lines = header_block.lines();
            let request_line = header_lines.next().expect("request line must be present");
            let mut request_line_parts = request_line.split_whitespace();
            let method = request_line_parts.next();
            let target = request_line_parts.next();
            let version = request_line_parts.next();
            assert_eq!(
                method,
                Some("GET"),
                "unexpected request line: {request_line}"
            );
            assert_eq!(
                target,
                Some("/storage/v1/b/quickwit-test-bucket/o/hello.txt?alt=media"),
                "unexpected GCS request target: {request_line}"
            );
            assert!(
                matches!(version, Some(version) if version.starts_with("HTTP/")),
                "unexpected HTTP version in request line: {request_line}"
            );
            assert_eq!(
                request_line_parts.next(),
                None,
                "unexpected extra request line segment: {request_line}"
            );

            let headers: BTreeMap<String, String> = header_lines
                .filter_map(|line| line.split_once(':'))
                .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
                .collect();
            assert_eq!(
                headers.get("range").map(String::as_str),
                Some("bytes=0-1"),
                "expected range read request header: {request}"
            );

            stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\n\
                    Content-Length: 2\r\n\
                    Content-Range: bytes 0-1/2\r\n\
                    Accept-Ranges: bytes\r\n\
                    Connection: close\r\n\
                    \r\n\
                    ok",
                )
                .await?;
            stream.shutdown().await?;
            Ok(())
        });

        Ok((endpoint, server_task))
    }

    fn decode_test_der(base64_der: &str) -> anyhow::Result<Vec<u8>> {
        Ok(base64::engine::general_purpose::STANDARD.decode(base64_der)?)
    }
}
