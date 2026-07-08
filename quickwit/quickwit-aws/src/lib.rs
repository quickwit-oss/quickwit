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

use aws_config::retry::RetryConfig;
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::{BehaviorVersion, Region};
pub use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::{Builder as HttpClientBuilder, Connector, tls};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use tokio::sync::OnceCell;

use crate::dns::CachingDnsResolver;

pub mod dns;
pub mod error;
pub mod retry;

pub const DEFAULT_AWS_REGION: Region = Region::from_static("us-east-1");

/// Builds the HTTP client used by all AWS SDK clients.
///
/// This mirrors `aws-smithy-runtime`'s `default_https_client()` (Rustls +
/// aws-lc-rs crypto, proxy settings read from the environment,
/// but wraps the default `getaddrinfo`-based DNS resolver with a cache, see [`CachingDnsResolver`].
fn build_http_client() -> SharedHttpClient {
    HttpClientBuilder::new().build_with_connector_fn(move |settings, runtime_components| {
        let mut conn_builder =
            Connector::builder().tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc));
        conn_builder.set_connector_settings(settings.cloned());
        if let Some(components) = runtime_components {
            conn_builder.set_sleep_impl(components.sleep_impl());
        }
        // Handling `HTTP_PROXY`/`HTTPS_PROXY`/ `NO_PROXY`
        conn_builder.set_proxy_config(Some(ProxyConfig::from_env()));
        conn_builder.build_with_resolver(CachingDnsResolver::default())
    })
}

/// Initialises and returns the AWS config.
pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

    SDK_CONFIG
        .get_or_init(|| async {
            aws_config::defaults(aws_behavior_version())
                .http_client(build_http_client())
                .stalled_stream_protection(StalledStreamProtectionConfig::enabled().build())
                // Currently handle this ourselves so probably best for now to leave it as is.
                .retry_config(RetryConfig::disabled())
                .sleep_impl(TokioSleep::default())
                .load()
                .await
        })
        .await
}

/// Returns the AWS behavior version.
pub fn aws_behavior_version() -> BehaviorVersion {
    BehaviorVersion::v2026_01_12()
}
