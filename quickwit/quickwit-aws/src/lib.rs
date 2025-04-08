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
use aws_config::BehaviorVersion;
pub use aws_smithy_async::rt::sleep::TokioSleep;
use aws_types::region::Region;
use tokio::sync::OnceCell;

pub mod error;
pub mod retry;

pub const DEFAULT_AWS_REGION: Region = Region::from_static("us-east-1");

/// Initialises and returns the AWS config.
pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

    SDK_CONFIG
        .get_or_init(|| async {

            // TODO implement the logic below!
            //
            // let mut http_connector = HttpConnector::new();
            // http_connector.enforce_http(false); // Enforced by `HttpsConnector`.
            // http_connector.set_nodelay(true);

            // let https_connector = HttpsConnectorBuilder::new()
            //     .with_native_roots()
            //     .https_or_http()
            //     // We do not enable HTTP2.
            //     // It is not enabled on S3 and it does not seem to work with Google Cloud Storage at
            //     // this point. https://github.com/quickwit-oss/quickwit/issues/1584
            //     //
            //     // (HTTP2 would be awesome since we do a lot of concurrent requests and
            //     // HTTP2 enables multiplexing a given connection.)
            //     .enable_http1()
            //     .wrap_connector(http_connector);

            // let mut hyper_client_builder = HyperClient::builder();
            // hyper_client_builder.pool_idle_timeout(Duration::from_secs(30));
            // let hyper_client = HyperClientBuilder::new()
            //     .hyper_builder(hyper_client_builder)
            //     .build(https_connector);

            aws_config::defaults(BehaviorVersion::v2024_03_28())
                .stalled_stream_protection(StalledStreamProtectionConfig::enabled().build())
                // READD me
                // .http_client(hyper_client)
                // Currently handle this ourselves so probably best for now to leave it as is.
                .retry_config(RetryConfig::disabled())
                .sleep_impl(TokioSleep::default())
                .load()
                .await
        })
        .await
}
