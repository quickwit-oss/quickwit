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

use std::time::Duration;

use aws_config::retry::RetryConfig;
pub use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_client::hyper_ext;
use aws_types::region::Region;
use hyper::client::{Client as HyperClient, HttpConnector};
use hyper_rustls::HttpsConnectorBuilder;
use tokio::sync::OnceCell;

pub mod error;
pub mod retry;

pub const DEFAULT_AWS_REGION: Region = Region::from_static("us-east-1");

/// Initialises and returns the AWS config.
pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

    SDK_CONFIG
        .get_or_init(|| async {
            let mut http_connector = HttpConnector::new();
            http_connector.enforce_http(false); // Enforced by `HttpsConnector`.
            http_connector.set_nodelay(true);

            let https_connector = HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                // We do not enable HTTP2.
                // It is not enabled on S3 and it does not seem to work with Google Cloud Storage at
                // this point. https://github.com/quickwit-oss/quickwit/issues/1584
                //
                // (HTTP2 would be awesome since we do a lot of concurrent requests and
                // HTTP2 enables multiplexing a given connection.)
                .enable_http1()
                .wrap_connector(http_connector);

            let mut hyper_client_builder = HyperClient::builder();
            hyper_client_builder.pool_idle_timeout(Duration::from_secs(30));

            let mut smithy_connector_builder = hyper_ext::Adapter::builder();
            smithy_connector_builder.set_hyper_builder(Some(hyper_client_builder));

            let smithy_connector = smithy_connector_builder.build(https_connector);

            aws_config::from_env()
                .http_connector(smithy_connector)
                // Currently handle this ourselves so probably best for now to leave it as is.
                .retry_config(RetryConfig::disabled())
                .sleep_impl(TokioSleep::default())
                .load()
                .await
        })
        .await
}
