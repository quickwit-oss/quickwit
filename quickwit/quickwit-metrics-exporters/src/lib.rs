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

#![deny(clippy::disallowed_methods)]

use metrics_util::MetricKindMask;
use metrics_util::layers::{FanoutBuilder, RouterBuilder};
pub use opentelemetry_sdk::metrics::SdkMeterProvider as SdkMetricsProvider;

mod dogstatsd;
pub mod otlp;
mod prometheus;

pub use otlp::{OtlpExporterConfig, OtlpProtocol};
pub use prometheus::metrics_text_payload;

/// Sets up the global metrics recorder and invariant recorder.
pub fn init_metrics_provider(
    service_version: &str,
    otlp_config: &OtlpExporterConfig,
) -> anyhow::Result<Option<SdkMetricsProvider>> {
    let prometheus_recorder = prometheus::build_recorder()?;

    let (quickwit_recorder, meter_provider) = if otlp_config.is_enabled() {
        let (otlp_recorder, meter_provider) = otlp::build_recorder(service_version, otlp_config)?;
        let recorder = FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .add_recorder(otlp_recorder)
            .build();
        (recorder, Some(meter_provider))
    } else {
        let recorder = FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .build();
        (recorder, None)
    };

    let dogstatsd_recorder = dogstatsd::build_recorder(service_version)?;

    let mut router = RouterBuilder::from_recorder(metrics::NoopRecorder);
    router
        .add_route(MetricKindMask::ALL, "quickwit_", quickwit_recorder)
        .add_route(MetricKindMask::ALL, "pomsky.invariant.", dogstatsd_recorder);
    let recorder = router.build();
    metrics::set_global_recorder(recorder)
        .map_err(|_| anyhow::anyhow!("failed to install global metrics recorder"))?;
    quickwit_metrics::describe_metrics();

    Ok(meter_provider)
}
