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

use http::Uri;

use crate::error::HttpError;

/// A connection target: protocol (plain/TLS), host, and port.
///
/// Used to create connections and to key the connection pool.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub tls: bool,
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// Builds an `Endpoint` from a parsed request URI
    pub fn from_uri(uri: &Uri) -> Result<Self, HttpError> {
        let scheme = uri.scheme_str().ok_or_else(|| {
            HttpError::InvalidUri("missing scheme (expected http/https)".to_string())
        })?;
        let tls = match scheme {
            "https" => true,
            "http" => false,
            other => {
                return Err(HttpError::InvalidUri(format!(
                    "unsupported scheme `{other}`"
                )));
            }
        };

        let host = uri
            .host()
            .ok_or_else(|| HttpError::InvalidUri("missing host (authority)".to_string()))?;

        // `Uri::port_u16` returns `None` both when no port is present and when
        // the port is the scheme default; fall back to the scheme default.
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });

        Ok(Self {
            tls,
            host: host.to_string(),
            port,
        })
    }

    /// Parses an absolute URI (`https://host[:port]/...`) into an `Endpoint`.
    ///
    /// Convenience wrapper around [`Self::from_uri`] for tests and callers that build an endpoint
    /// from a string.
    pub fn parse(uri: &str) -> Result<Self, HttpError> {
        let parsed: Uri = uri
            .parse()
            .map_err(|err| HttpError::InvalidUri(format!("`{uri}`: {err}")))?;
        Self::from_uri(&parsed)
    }

    /// The `ServerName` used for TLS validation and SNI. Accepts both DNS
    /// names and IP literals
    pub fn server_name(&self) -> Result<rustls::pki_types::ServerName<'static>, HttpError> {
        rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|err| HttpError::Tls(format!("invalid server name `{}`: {err}", self.host)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_https_default_port() {
        let ep = Endpoint::parse("https://bucket.s3.us-east-1.amazonaws.com/key?x=1").unwrap();
        assert!(ep.tls);
        assert_eq!(ep.host, "bucket.s3.us-east-1.amazonaws.com");
        assert_eq!(ep.port, 443);
    }

    #[test]
    fn parse_http_explicit_port() {
        let ep = Endpoint::parse("http://localhost:4566/bucket/key").unwrap();
        assert!(!ep.tls);
        assert_eq!(ep.host, "localhost");
        assert_eq!(ep.port, 4566);
    }

    #[test]
    fn parse_discards_path_and_query() {
        let ep = Endpoint::parse("https://example.com/some/path?q=1#frag").unwrap();
        assert_eq!(ep.host, "example.com");
        assert_eq!(ep.port, 443);
    }

    #[test]
    fn from_uri_matches_parse() {
        let uri: Uri = "https://example.com:8443/x".parse().unwrap();
        let ep = Endpoint::from_uri(&uri).unwrap();
        assert_eq!(ep, Endpoint::parse("https://example.com:8443/y").unwrap());
    }

    #[test]
    fn parse_rejects_missing_scheme() {
        assert!(Endpoint::parse("localhost:4566/x").is_err());
    }

    #[test]
    fn parse_rejects_unsupported_scheme() {
        assert!(Endpoint::parse("ftp://example.com/x").is_err());
    }

    #[test]
    fn parse_rejects_missing_host() {
        assert!(Endpoint::parse("https:///path").is_err());
    }

    #[test]
    fn server_name_accepts_ip_literal() {
        let ep = Endpoint::parse("https://127.0.0.1:443/x").unwrap();
        let name = ep.server_name().unwrap();
        assert!(matches!(name, rustls::pki_types::ServerName::IpAddress(_)));
    }

    #[test]
    fn endpoint_is_pool_key() {
        // Same authority, different paths -> same endpoint (same pool key).
        let a = Endpoint::parse("https://bucket.s3.amazonaws.com/keyA").unwrap();
        let b = Endpoint::parse("https://bucket.s3.amazonaws.com/keyB?x=1").unwrap();
        assert_eq!(a, b);
        // Different port -> different endpoint.
        let c = Endpoint::parse("https://bucket.s3.amazonaws.com:8443/keyA").unwrap();
        assert_ne!(a, c);
    }
}
