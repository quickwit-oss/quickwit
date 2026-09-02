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

pub(crate) use config::{OtlpExporterConfig, OtlpProtocol, quickwit_resource};
use opentelemetry_otlp::retry::RetryPolicy;

//Common retry policy for OTLP exporters
const RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 5,
    initial_delay_ms: 500,
    max_delay_ms: 30_000,
    jitter_ms: 100,
};
