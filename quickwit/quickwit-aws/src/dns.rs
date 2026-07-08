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

//! Caching DNS resolver used by the AWS/S3 HTTP client.
//!
//! The AWS SDK's default HTTP client performs a blocking `getaddrinfo`
//! lookup on every new connection without any caching.
//!
//! [`HickoryDnsResolver`] wraps a Hickory resolver, which keeps an in-memory
//! cache keyed by hostname and honors record TTLs.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use hickory_resolver::TokioResolver;
use quickwit_common::rate_limited_warn;

/// Minimum TTL enforced on positive DNS responses.
///
/// S3 endpoints advertise TTLs of only a few seconds. Without a floor, the
/// Hickory cache would expire almost immediately and provide little benefit.
const DNS_CACHE_MIN_TTL: Duration = Duration::from_mins(1);

/// Maximum number of cached records.
const DNS_CACHE_SIZE: u64 = 1_024;

/// A [`ResolveDns`] implementation backed by a caching Hickory resolver.
#[derive(Debug, Clone)]
pub struct HickoryDnsResolver {
    // `TokioResolver` is internally reference-counted and cheap to clone,
    // but the `Arc` makes the shared-cache contract explicit: every clone of
    // this resolver hits the same DNS cache.
    resolver: Arc<TokioResolver>,
}

impl HickoryDnsResolver {
    /// Builds a resolver from the host's DNS configuration (`/etc/resolv.conf`
    /// on Unix), overriding the cache options so short-TTL records are still
    /// cached for at least `DNS_CACHE_MIN_TTL`.
    pub fn from_system_conf() -> anyhow::Result<Self> {
        let mut builder =
            TokioResolver::builder_tokio().context("failed to read system DNS configuration")?;
        let options = builder.options_mut();
        options.positive_min_ttl = Some(DNS_CACHE_MIN_TTL);
        options.cache_size = DNS_CACHE_SIZE;
        let resolver = builder.build().context("failed to build DNS resolver")?;
        Ok(HickoryDnsResolver {
            resolver: Arc::new(resolver),
        })
    }
}

impl ResolveDns for HickoryDnsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let resolver = self.resolver.clone();
        // The lookup takes ownership of the host name so the returned future is
        // `'static` and does not borrow from `name` (the trait allows `'a`, but
        // owning is simpler and side-steps lifetime coupling).
        let host = name.to_string();
        DnsFuture::new(async move {
            if let Ok(lookup) = resolver.lookup_ip(host.clone()).await {
                let ip_addresses: Vec<IpAddr> = lookup.iter().collect();
                if !ip_addresses.is_empty() {
                    return Ok(ip_addresses);
                }
            }
            // Hickory only speaks the DNS protocol (plus `/etc/hosts`), so it
            // misses hostnames resolved through other NSS sources (mDNS, LDAP,
            // custom `nsswitch.conf` plugins, etc). Fall back to the OS
            // resolver (`getaddrinfo`) for those.
            rate_limited_warn!(
                limit_per_min = 1,
                "hickory dns resolver could not resolve {host}, falling back to the os resolver"
            );
            let socket_addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(ResolveDnsError::new)?;
            Ok(socket_addrs.map(|socket_addr| socket_addr.ip()).collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use hickory_resolver::config::{NameServerConfig, ResolveHosts, ResolverConfig};
    use hickory_resolver::net::runtime::TokioRuntimeProvider;

    use super::*;

    #[tokio::test]
    async fn test_hickory_dns_resolver_resolves_localhost() {
        // `localhost` resolves from `/etc/hosts` (Hickory reads it by default),
        // so this test does not depend on any network name server.
        let resolver = HickoryDnsResolver::from_system_conf()
            .expect("resolver should build from system configuration");
        let ip_addresses = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve");
        assert!(
            ip_addresses
                .iter()
                .any(|ip_address| ip_address.is_loopback()),
            "expected a loopback address, got {ip_addresses:?}"
        );
    }

    #[tokio::test]
    async fn test_hickory_dns_resolver_falls_back_to_os_resolver() {
        // Points the Hickory resolver at a non-routable name server and disables its
        // `/etc/hosts` lookup, so it can never resolve anything itself. `localhost`
        // must still resolve through the OS resolver fallback.
        let mut builder = TokioResolver::builder_with_config(
            ResolverConfig::from_parts(
                None,
                vec![],
                vec![NameServerConfig::udp(IpAddr::V4(Ipv4Addr::new(
                    203, 0, 113, 1,
                )))],
            ),
            TokioRuntimeProvider::default(),
        );
        let options = builder.options_mut();
        options.timeout = Duration::from_millis(200);
        options.attempts = 1;
        options.use_hosts_file = ResolveHosts::Never;
        let resolver = HickoryDnsResolver {
            resolver: Arc::new(
                builder
                    .build()
                    .expect("resolver should build from explicit configuration"),
            ),
        };
        let ip_addresses = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve via the os resolver fallback");
        assert!(
            ip_addresses
                .iter()
                .any(|ip_address| ip_address.is_loopback()),
            "expected a loopback address, got {ip_addresses:?}"
        );
    }
}
