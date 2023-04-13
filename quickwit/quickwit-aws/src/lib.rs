// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#![deny(clippy::disallowed_methods)]

use aws_config::retry::RetryConfig;
use aws_smithy_client::hyper_ext;
use hyper_rustls::HttpsConnectorBuilder;
use tokio::sync::OnceCell;

pub mod error;
pub mod retry;

static AWS_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

/// Attempts to get the current AWS SDK config.
///
/// If the config has not yet been initialised this will return `None`.
pub fn try_get_aws_config() -> Option<&'static aws_config::SdkConfig> {
    AWS_CONFIG.get()
}

/// Attempts to initialise the AWS SDK config.
///
/// If the config is already initialised, this is a no-op.
pub async fn try_init_aws_config() -> &'static aws_config::SdkConfig {
    AWS_CONFIG
        .get_or_init(|| async move {
            let builder = HttpsConnectorBuilder::new();
            let builder = builder.with_native_roots();
            let connector = builder
                .https_or_http()
                // We do not enable HTTP2.
                // It is not enabled on S3 and it does not seem to work with Google Cloud Storage at
                // this point. https://github.com/quickwit-oss/quickwit/issues/1584
                //
                // (HTTP2 would be awesome since we do a lot of concurrent requests and
                // HTTP2 enables multiplexing a given connection.)
                .enable_http1()
                .build();

            let smithy_connector = hyper_ext::Adapter::builder().build(connector);

            aws_config::from_env()
                .http_connector(smithy_connector)
                // Currently handle this ourselves so probably best for now to leave it as is.
                .retry_config(RetryConfig::disabled())
                .load()
                .await
        })
        .await
}

/// Get the set S3 endpoint if applicable.
pub fn get_s3_endpoint() -> Option<String> {
    std::env::var("QW_S3_ENDPOINT").ok()
}
