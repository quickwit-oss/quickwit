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

mod config;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod traces;

use std::time::Duration;

pub(crate) use config::{OtlpExporterConfig, OtlpProtocol, quickwit_resource};
use opentelemetry_otlp::RetryPolicy;

/// Common retry policy for OTLP exporters.
///
/// We set it explicitly instead of relying on `RetryPolicy::default()` so that our retry budget
/// does not change when the library's recommended defaults do.
fn retry_policy() -> RetryPolicy {
    RetryPolicy::disabled()
        .with_max_retries(5)
        .with_initial_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_millis(30_000))
        .with_max_jitter(Duration::from_millis(100))
}
