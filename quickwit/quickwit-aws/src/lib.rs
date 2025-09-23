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
use tokio::sync::OnceCell;

pub mod error;
pub mod retry;

pub const DEFAULT_AWS_REGION: Region = Region::from_static("us-east-1");

/// Initialises and returns the AWS config.
pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

    SDK_CONFIG
        .get_or_init(|| async {
            aws_config::defaults(aws_behavior_version())
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
    BehaviorVersion::v2025_08_07()
}
