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

use std::net::IpAddr;

use anyhow::{Context, ensure};
use fluent_uri::Uri;
use itertools::Itertools;
use regex::{Regex, RegexSet, RegexSetBuilder};
use serde::{Deserialize, Serialize};

/// Alternatives for authorizing an incoming, cryptographically verified client certificate.
/// A match in any configured list suffices. These rules only apply to the leaf certificate.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedClientIdentities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub common_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dns_sans: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uri_sans: Vec<String>,
}

impl AllowedClientIdentities {
    /// Validates and compiles the policy at startup, never during a TLS handshake.
    pub fn compile(&self) -> anyhow::Result<ClientIdentityMatcher> {
        ensure!(
            !(self.common_names.is_empty() && self.dns_sans.is_empty() && self.uri_sans.is_empty()),
            "`tls.allowed_client_identities` must contain at least one identity"
        );
        let mut common_name_patterns = Vec::new();
        for pattern in &self.common_names {
            ensure!(
                !pattern.is_empty(),
                "empty common name pattern in `tls.allowed_client_identities`"
            );
            let regex_pattern = make_wildcard_regex(pattern, ".*");
            let anchored_pattern = format!(r"\A{regex_pattern}\z");
            common_name_patterns.push(anchored_pattern);
        }
        let mut dns_san_patterns = Vec::new();
        for pattern in &self.dns_sans {
            ensure!(
                is_valid_dns_name(pattern, true),
                "invalid DNS name pattern `{pattern}` in `tls.allowed_client_identities`"
            );
            let regex_pattern = make_wildcard_regex(pattern, "[^.]*");
            let anchored_pattern = format!(r"\A{regex_pattern}\z");
            dns_san_patterns.push(anchored_pattern);
        }
        let mut uri_sans = Vec::new();
        for pattern in &self.uri_sans {
            let matcher = UriSanMatcher::compile(pattern).with_context(|| {
                format!("invalid URI SAN pattern `{pattern}` in `tls.allowed_client_identities`")
            })?;
            uri_sans.push(matcher);
        }
        let common_names = RegexSet::new(common_name_patterns)?;
        let dns_sans = RegexSetBuilder::new(dns_san_patterns)
            .case_insensitive(true)
            .build()?;
        Ok(ClientIdentityMatcher {
            common_names,
            dns_sans,
            uri_sans,
        })
    }
}

/// Compiled identity rules. Matching does not authenticate a certificate: the caller must first
/// perform standard TLS client-certificate validation. URI rules do not enforce identity profiles
/// such as SPIFFE X.509-SVID, even when a URI uses the `spiffe` scheme.
#[derive(Debug)]
pub struct ClientIdentityMatcher {
    common_names: RegexSet,
    dns_sans: RegexSet,
    uri_sans: Vec<UriSanMatcher>,
}

impl ClientIdentityMatcher {
    pub fn has_common_name_rules(&self) -> bool {
        !self.common_names.is_empty()
    }

    pub fn matches_common_name(&self, common_name: &str) -> bool {
        self.common_names.is_match(common_name)
    }

    pub fn matches_dns_san(&self, dns_name: &str) -> bool {
        // Certificate DNS names are concrete identities, never wildcard patterns.
        is_valid_dns_name(dns_name, false) && self.dns_sans.is_match(dns_name)
    }

    pub fn matches_uri_san(&self, uri: &str) -> bool {
        // Parses without normalizing or decoding the certificate's identity.
        let Ok(parsed_uri) = Uri::parse(uri) else {
            return false;
        };
        self.uri_sans
            .iter()
            .any(|matcher| matcher.matches(&parsed_uri))
    }
}

#[derive(Debug)]
struct UriSanMatcher {
    scheme: String,
    // Distinguishes an absent authority from an explicitly empty authority.
    authority: Option<String>,
    path_regex: Regex,
    query: Option<String>,
    fragment: Option<String>,
}

impl UriSanMatcher {
    fn compile(pattern: &str) -> anyhow::Result<Self> {
        let uri = Uri::parse(pattern).map_err(|error| anyhow::anyhow!("{error}"))?;
        let scheme = uri.scheme().as_str().to_string();
        let authority = uri
            .authority()
            .map(|authority| authority.as_str().to_string());
        let query = uri.query().map(|query| query.as_str().to_string());
        let fragment = uri.fragment().map(|fragment| fragment.as_str().to_string());
        let literal_components = [
            Some(scheme.as_str()),
            authority.as_deref(),
            query.as_deref(),
            fragment.as_deref(),
        ];
        for component in literal_components.into_iter().flatten() {
            ensure!(
                !component.contains('*'),
                "URI wildcards are only supported in the path"
            );
        }
        let path_regex = make_uri_path_regex(uri.path().as_str())?;
        Ok(Self {
            scheme,
            authority,
            path_regex,
            query,
            fragment,
        })
    }

    fn matches(&self, uri: &Uri<&str>) -> bool {
        let authority = uri.authority().map(|authority| authority.as_str());
        let query = uri.query().map(|query| query.as_str());
        let fragment = uri.fragment().map(|fragment| fragment.as_str());
        self.scheme == uri.scheme().as_str()
            && self.authority.as_deref() == authority
            && self.query.as_deref() == query
            && self.fragment.as_deref() == fragment
            && self.path_regex.is_match(uri.path().as_str())
    }
}

fn make_wildcard_regex(pattern: &str, wildcard: &str) -> String {
    pattern.split('*').map(regex::escape).join(wildcard)
}

/// Validates ASCII DNS names, without IP literals or a terminal dot. Patterns additionally allow
/// `*` within labels. A wildcard cannot consume a dot; presented names must have nonempty labels.
fn is_valid_dns_name(name: &str, allow_pattern: bool) -> bool {
    if name.is_empty() || name.len() > 253 || name.parse::<IpAddr>().is_ok() {
        return false;
    }
    name.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && !label.contains("**")
            && label.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'-' || (allow_pattern && byte == b'*')
            })
    })
}

/// Builds a wildcard regex for the parsed URI path. Percent escapes, dot segments, case, and
/// repeated separators are preserved, never normalized into another identity.
fn make_uri_path_regex(path: &str) -> anyhow::Result<Regex> {
    let base = path.strip_suffix("/**");
    ensure!(
        !base.unwrap_or(path).contains("**"),
        "URI `**` is only supported as a terminal `/**` path suffix"
    );
    let mut expression = make_wildcard_regex(base.unwrap_or(path), "[^/?#]*");
    if base.is_some() {
        expression.push_str("(?:/[^?#]*)?");
    }
    let anchored_pattern = format!(r"\A{expression}\z");
    Ok(Regex::new(&anchored_pattern)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_names_are_case_sensitive_and_anchored() {
        let matcher = AllowedClientIdentities {
            common_names: vec![
                "collector-*".to_string(),
                "forwarder".to_string(),
                "service.prod".to_string(),
            ],
            ..Default::default()
        }
        .compile()
        .unwrap();
        for name in [
            "collector-",
            "collector-a.prod",
            "forwarder",
            "service.prod",
        ] {
            assert!(matcher.matches_common_name(name), "{name}");
        }
        for name in [
            "Forwarder",
            "collector",
            "forwarder-suffix",
            "prefix-forwarder",
            "serviceXprod",
        ] {
            assert!(!matcher.matches_common_name(name), "{name}");
        }
        assert!(!matcher.matches_dns_san("forwarder"));
    }

    #[test]
    fn test_dns_patterns_respect_label_boundaries() {
        let matcher = AllowedClientIdentities {
            dns_sans: vec![
                "*.collectors.example.com".to_string(),
                "agent-*.example.com".to_string(),
                "collector.example.com".to_string(),
            ],
            ..Default::default()
        }
        .compile()
        .unwrap();
        for name in [
            "COLLECTOR.EXAMPLE.COM",
            "a.collectors.example.com",
            "agent-42.example.com",
        ] {
            assert!(matcher.matches_dns_san(name), "{name}");
        }
        for name in [
            "*.collectors.example.com",
            ".collectors.example.com",
            "127.0.0.1",
            "a.b.collectors.example.com",
            "agent-.example.com",
            "collector.example.com.",
            "collector.example.com.evil",
            "collectors.example.com",
        ] {
            assert!(!matcher.matches_dns_san(name), "{name}");
        }
        assert!(!matcher.matches_common_name("collector.example.com"));
    }

    #[test]
    fn test_uri_patterns_respect_authority_and_path() {
        let matcher = AllowedClientIdentities {
            uri_sans: vec![
                "https://user@example.com:8443/agents/*?role=writer#v1".to_string(),
                "spiffe://example.com/ns/logging/sa/*".to_string(),
                "spiffe://example.com/ns/observability/**".to_string(),
                "spiffe://example.com/workload/exact".to_string(),
                "urn:service:collector-*".to_string(),
            ],
            ..Default::default()
        }
        .compile()
        .unwrap();
        for name in [
            "https://user@example.com:8443/agents/collector?role=writer#v1",
            "spiffe://example.com/ns/logging/sa/collector",
            "spiffe://example.com/ns/observability",
            "spiffe://example.com/ns/observability/a/",
            "spiffe://example.com/ns/observability/a/b",
            "spiffe://example.com/workload/exact",
            "urn:service:collector-42",
        ] {
            assert!(matcher.matches_uri_san(name), "{name}");
        }
        for name in [
            "SPIFFE://EXAMPLE.COM/ns/logging/sa/collector",
            "https://example.com/workload/exact",
            "https://example.com:8443/agents/collector?role=writer#v1",
            "https://user@example.com:8443/agents/a/b?role=writer#v1",
            "https://user@example.com:8443/agents/collector?role=admin#v1",
            "https://user@example.com:8443/agents/collector?role=writer#v2",
            "spiffe://evil.example.com/ns/logging/sa/collector",
            "spiffe://example.com/ns/logging/SA/collector",
            "spiffe://example.com/ns/logging/sa/%invalid",
            "spiffe://example.com/ns/logging/sa/a/b",
            "spiffe://example.com/ns/observability-evil/a",
            "spiffe://example.com/ns/observability/a#fragment",
            "spiffe://example.com/ns/observability/a?query",
            "spiffe://example.com:443/ns/logging/sa/collector",
            "spiffe://user@example.com/ns/logging/sa/collector",
            "urn:service:collector-42/other",
            "urn:service:collector-42?query",
        ] {
            assert!(!matcher.matches_uri_san(name), "{name}");
        }
        assert!(!matcher.matches_dns_san("example.com"));
    }

    #[test]
    fn test_uri_patterns_preserve_authority_boundaries() {
        for (pattern, identity, accepted) in [
            ("file:/**", "file:", true),
            ("file:/**", "file:///secret", false),
            ("file:/**", "file://untrusted.example/secret", false),
            ("file:/**", "file:/secret", true),
            ("file:/*/*", "file://untrusted.example", false),
            ("file:/*/*", "file:/dir/secret", true),
            ("file:///**", "file:", false),
            ("file:///**", "file:///secret", true),
            ("file:///**", "file://untrusted.example/secret", false),
            ("file:///**", "file:/secret", false),
            ("file://trusted.example/**", "file:///secret", false),
            ("file://trusted.example/**", "file://trusted.example", true),
            (
                "file://trusted.example/**",
                "file://trusted.example/secret",
                true,
            ),
            (
                "file://trusted.example/**",
                "file://untrusted.example/secret",
                false,
            ),
            ("file://trusted.example/**", "file:/secret", false),
        ] {
            let matcher = AllowedClientIdentities {
                uri_sans: vec![pattern.to_string()],
                ..Default::default()
            }
            .compile()
            .unwrap();
            assert_eq!(
                matcher.matches_uri_san(identity),
                accepted,
                "{pattern} against {identity}"
            );
        }
    }

    #[test]
    fn test_uri_sans_are_literal_without_scheme_specific_restrictions() {
        for (identity, different_identity) in [
            ("file:/a#", "file:/a"),
            ("file:/a?", "file:/a"),
            ("file:/a?#", "file:/a?"),
            ("https://example.com/%2f", "https://example.com/%2F"),
            ("https://example.com/%61", "https://example.com/a"),
            ("https://example.com//a", "https://example.com/a"),
            ("https://example.com/a/../b", "https://example.com/b"),
            ("https://example.com/a?x#y", "https://example.com/a?x#z"),
            ("spiffe://example.com", "spiffe://example.com/"),
            ("spiffe://example.com/", "spiffe://example.com"),
            ("urn:example:collector", "URN:example:collector"),
        ] {
            let matcher = AllowedClientIdentities {
                uri_sans: vec![identity.to_string()],
                ..Default::default()
            }
            .compile()
            .unwrap();
            assert!(matcher.matches_uri_san(identity), "{identity}");
            assert!(
                !matcher.matches_uri_san(different_identity),
                "{different_identity}"
            );
        }
        let matcher = AllowedClientIdentities {
            uri_sans: vec!["spiffe://example.com/**".to_string()],
            ..Default::default()
        }
        .compile()
        .unwrap();
        // Matching uses literal URI text: no SPIFFE profile checks or path normalization.
        for identity in [
            "spiffe://example.com",
            "spiffe://example.com/",
            "spiffe://example.com/%2f",
            "spiffe://example.com/*",
            "spiffe://example.com//a",
            "spiffe://example.com/a/../b",
        ] {
            assert!(matcher.matches_uri_san(identity), "{identity}");
        }
        assert!(!matcher.matches_uri_san("spiffe://other.com/a"));
    }

    #[test]
    fn test_reject_invalid_identity_policies() {
        assert!(AllowedClientIdentities::default().compile().is_err());
        let empty_common_name_policy = AllowedClientIdentities {
            common_names: vec![String::new()],
            ..Default::default()
        };
        let error = empty_common_name_policy.compile().unwrap_err().to_string();
        assert_eq!(
            error,
            "empty common name pattern in `tls.allowed_client_identities`"
        );
        for name in [
            "",
            "**.example.com",
            "-host.example.com",
            "127.0.0.1",
            "[::1]",
            "a..example.com",
            "a.example.com.",
            "foo?.example.com",
            "é.example.com",
        ] {
            let policy = AllowedClientIdentities {
                dns_sans: vec![name.to_string()],
                ..Default::default()
            };
            assert!(policy.compile().is_err(), "{name}");
        }
        for name in [
            "",
            "/relative/path",
            "https://example.com/a#*",
            "https://example.com/a?x=*",
            "spiffe://*.example.com/a",
            "spiffe://example.com/%invalid",
            "spiffe://example.com/**/**",
            "spiffe://example.com/a b",
            "spiffe://example.com/a**",
            "spiffe://example.com/a/**/b",
        ] {
            let policy = AllowedClientIdentities {
                uri_sans: vec![name.to_string()],
                ..Default::default()
            };
            assert!(policy.compile().is_err(), "{name}");
        }
    }
}
