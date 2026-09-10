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

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;

use crate::error::HttpError;

/// The future returned by [`DnsResolver::resolve`].
pub type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, HttpError>> + Send + 'a>>;

/// Resolves a hostname to a list of IP addresses.
pub trait DnsResolver: Send + Sync {
    fn resolve<'a>(&'a self, host: &'a str) -> ResolveFuture<'a>;
}

/// The default resolver, backed by `tokio::net::lookup_host`.
#[derive(Clone, Default, Debug)]
pub struct DefaultDnsResolver;

impl DnsResolver for DefaultDnsResolver {
    fn resolve<'a>(&'a self, host: &'a str) -> ResolveFuture<'a> {
        Box::pin(async move {
            // `lookup_host` needs a `ToSocketAddrs`, so a port is required,
            // but it is only stamped onto the results; it does not change the
            // resolved IPs. Use port `0` as a placeholder and discard it.
            let ips: Vec<IpAddr> = tokio::net::lookup_host((host, 0u16))
                .await
                .map_err(|err| HttpError::Dns {
                    host: host.to_string(),
                    message: err.to_string(),
                })?
                .map(|addr| addr.ip())
                .collect();
            if ips.is_empty() {
                return Err(HttpError::Dns {
                    host: host.to_string(),
                    message: "no addresses resolved".to_string(),
                });
            }
            Ok(ips)
        })
    }
}
