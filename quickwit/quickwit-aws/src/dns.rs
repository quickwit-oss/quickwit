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
//!
//! Cached entries never expire outright: once a host has been resolved at
//! least once, lookups always return the cached (possibly stale) addresses
//! immediately. Entries older than [`DNS_REFRESH_COOLDOWN`] instead trigger a
//! background refresh that updates the cache in place once it completes, so
//! callers are never blocked waiting on a fresh `getaddrinfo` call for a
//! host they already have an answer for.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use mini_moka::sync::Cache;
use tokio::sync::watch;

// We refresh once every 5 seconds. S3's DNS varies very rapidly.
const DNS_REFRESH_COOLDOWN: Duration = Duration::from_secs(5);

// On the first call, we do not have any answer to give back,
// so we wait up to DNS_TIMEOUT seconds for an entry to have been populated
// by the background task.
const DNS_TIMEOUT: Duration = Duration::from_secs(1);

/// Maximum number of cached hostnames.
/// In practise this cache should only contain a few entries to connect to s3's endpoints.
const DNS_CACHE_SIZE: u64 = 1_024;

// An instant expressed as duration elapsed from a reference time, expressed in millisecs.
type IInstant = u64;

fn now() -> IInstant {
    static REFERENCE_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);
    Instant::now().duration_since(*REFERENCE_TIME).as_millis() as u64
}

/// A cached DNS entry.
#[derive(Debug)]
struct DnsEntry {
    ip_addresses_rx: watch::Receiver<Vec<IpAddr>>,
    ip_addresses_tx: watch::Sender<Vec<IpAddr>>,
    next_resolve_attempt: AtomicU64,
}

impl DnsEntry {
    fn new() -> Self {
        let (ip_addresses_tx, ip_addresses_rx) = tokio::sync::watch::channel(Vec::new());
        DnsEntry {
            ip_addresses_rx,
            ip_addresses_tx,
            next_resolve_attempt: AtomicU64::new(0u64),
        }
    }

    fn should_resolve(&self) -> bool {
        let next_resolve_attempt: u64 = self.next_resolve_attempt.load(Ordering::Relaxed);
        let now = now();
        if now >= next_resolve_attempt {
            // Our current entry is stale. Let's see if we want to trigger
            // the refresh by atomically updating the next_resolve_attempt instant.
            //
            // Relaxed suffices on both arms: this CAS only elects a single winner.
            self.next_resolve_attempt
                .compare_exchange(
                    next_resolve_attempt,
                    now + DNS_REFRESH_COOLDOWN.as_millis() as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        } else {
            false
        }
    }
}

/// A [`ResolveDns`] implementation that caches `getaddrinfo` lookups.
#[derive(Debug, Clone)]
pub struct CachingDnsResolver {
    cache: Arc<Cache<String, Arc<DnsEntry>>>,
}

impl Default for CachingDnsResolver {
    fn default() -> Self {
        let cache = Cache::builder().max_capacity(DNS_CACHE_SIZE).build();
        CachingDnsResolver {
            cache: Arc::new(cache),
        }
    }
}

fn spawn_refresh_dns(ips_tx: tokio::sync::watch::Sender<Vec<IpAddr>>, host: String) {
    tokio::task::spawn(async move {
        let Ok(socket_addrs) = tokio::net::lookup_host((host.as_str(), 0)).await else {
            quickwit_common::rate_limited_error!(limit_per_min = 10, %host, "failed to refresh DNS");
            return;
        };
        let ips: Vec<IpAddr> = socket_addrs.map(|socket_addr| socket_addr.ip()).collect();
        if ips.is_empty() {
            return;
        }
        let _ = ips_tx.send(ips);
    });
}

impl ResolveDns for CachingDnsResolver {
    fn resolve_dns<'a>(&'a self, host: &'a str) -> DnsFuture<'a> {
        let cache = self.cache.clone();
        let host = host.to_string();
        let dns_entry_opt: Option<Arc<DnsEntry>> = cache.get(&host);
        // First insertion CAN trigger several DNS call, but this is not a problem.
        let dns_entry: Arc<DnsEntry> = if let Some(dns_entry) = dns_entry_opt {
            dns_entry.clone()
        } else {
            let dns_entry = Arc::new(DnsEntry::new());
            cache.insert(host.clone(), dns_entry.clone());
            dns_entry
        };
        if dns_entry.should_resolve() {
            spawn_refresh_dns(dns_entry.ip_addresses_tx.clone(), host.clone());
        }
        DnsFuture::new(async move {
            {
                // Happy path! If we have ips, we return directly.
                let ips_view = dns_entry.ip_addresses_rx.borrow();
                if !ips_view.is_empty() {
                    return Ok(ips_view.clone());
                }
            }
            let mut ip_addresses_rx = dns_entry.ip_addresses_rx.clone();
            let ips: Vec<IpAddr> = tokio::time::timeout(DNS_TIMEOUT, async move {
                ip_addresses_rx
                    .wait_for(|ips| !ips.is_empty())
                    .await
                    .map_err(ResolveDnsError::new)
                    .map(|res| res.clone())
            })
            .await
            .map_err(ResolveDnsError::new)??;
            Ok(ips)
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
