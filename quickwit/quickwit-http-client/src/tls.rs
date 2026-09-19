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

use crate::error::HttpError;

/// Builds a `rustls::ClientConfig` backed by the aws-lc-rs crypto provider
/// and the OS native root store.
pub fn default_client_config() -> Result<Arc<rustls::ClientConfig>, HttpError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for cert in loaded.certs {
        // `add` only fails on an unparseable DER blob but native-certs
        // should have checked these already.
        let _ = roots.add(cert);
    }
    if loaded.errors.is_empty() {
        tracing::debug!(
            loaded = roots.len(),
            "loaded native root certificates for quickwit-http-client"
        );
    } else {
        tracing::warn!(

            errors = ?loaded.errors,
            loaded = roots.len(),
            "some native root certificates failed to load for quickwit-http-client"
        );
    }

    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| HttpError::Tls(format!("unsupported TLS protocol versions: {err}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    // Force HTTP/1.1 ALPN so the server cannot negotiate HTTP/2
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}
