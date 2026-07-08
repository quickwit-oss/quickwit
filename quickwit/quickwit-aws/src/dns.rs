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
//! [`CachingDnsResolver`] resolves names the same way (via
//! [`tokio::net::lookup_host`], which goes through `getaddrinfo` and
//! therefore honors every NSS source configured on the host: DNS,
//! `/etc/hosts`, mDNS, LDAP, etc), but keeps an in-memory cache of the
//! results so repeated lookups for the same host don't each pay for a
//! blocking `getaddrinfo` call.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use mini_moka::sync::Cache;

/// TTL applied to cached lookups.
const DNS_CACHE_TTL: Duration = Duration::from_mins(1);

/// Maximum number of cached hostnames.
const DNS_CACHE_SIZE: u64 = 1_024;

/// A [`ResolveDns`] implementation that caches `getaddrinfo` lookups.
#[derive(Debug, Clone)]
pub struct CachingDnsResolver {
    cache: Arc<Cache<String, Vec<IpAddr>>>,
}

impl Default for CachingDnsResolver {
    fn default() -> Self {
        let cache = Cache::builder()
            .max_capacity(DNS_CACHE_SIZE)
            .time_to_live(DNS_CACHE_TTL)
            .build();
        CachingDnsResolver {
            cache: Arc::new(cache),
        }
    }
}

impl ResolveDns for CachingDnsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let cache = self.cache.clone();
        let host = name.to_string();
        DnsFuture::new(async move {
            if let Some(ip_addresses) = cache.get(&host) {
                return Ok(ip_addresses);
            }
            let socket_addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(ResolveDnsError::new)?;
            let ip_addresses: Vec<IpAddr> =
                socket_addrs.map(|socket_addr| socket_addr.ip()).collect();
            cache.insert(host, ip_addresses.clone());
            Ok(ip_addresses)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_caching_dns_resolver_resolves_localhost() {
        let resolver = CachingDnsResolver::default();
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
    async fn test_caching_dns_resolver_caches_lookups() {
        let resolver = CachingDnsResolver::default();
        let first = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve");
        assert!(resolver.cache.get(&"localhost".to_string()).is_some());
        let second = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve from cache");
        assert_eq!(first, second);
    }
}
