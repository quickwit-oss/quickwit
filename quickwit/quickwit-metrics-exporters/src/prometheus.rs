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

use std::sync::OnceLock;
#[cfg(not(test))]
use std::time::Duration;

use anyhow::Context;
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub(crate) fn build_recorder() -> anyhow::Result<PrometheusRecorder> {
    let mut prometheus_builder = PrometheusBuilder::new();
    for (name, buckets) in quickwit_metrics::histogram_buckets() {
        prometheus_builder = prometheus_builder
            .set_buckets_for_metric(Matcher::Full(name.to_string()), &buckets)
            .with_context(|| {
                format!("failed to configure Prometheus histogram buckets for `{name}`")
            })?;
    }
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();
    set_prometheus_handle(prometheus_handle.clone()).map_err(anyhow::Error::msg)?;
    Ok(prometheus_recorder)
}

fn set_prometheus_handle(handle: PrometheusHandle) -> Result<(), String> {
    #[cfg(not(test))]
    let upkeep_handle = handle.clone();
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| "Prometheus metrics renderer is already installed".to_string())?;
    #[cfg(not(test))]
    spawn_prometheus_upkeep(upkeep_handle)?;
    Ok(())
}

pub fn metrics_text_payload() -> Result<String, String> {
    let handle = PROMETHEUS_HANDLE
        .get()
        .ok_or_else(|| "Prometheus metrics rendering is not installed yet".to_string())?;
    Ok(handle.render())
}

#[cfg(not(test))]
fn spawn_prometheus_upkeep(handle: PrometheusHandle) -> Result<(), String> {
    // Quickwit serves the existing `/metrics` route itself, so we build only the
    // Prometheus recorder instead of using the exporter's HTTP listener. That lower-level
    // API does not spawn the upkeep task that periodically drains histogram buffers.
    std::thread::Builder::new()
        .name("metrics-exporter-prometheus-upkeep".to_string())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(5));
                handle.run_upkeep();
            }
        })
        .map(|_| ())
        .map_err(|error| format!("failed to spawn Prometheus metrics upkeep thread: {error}"))
}

#[cfg(test)]
mod tests {
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use quickwit_metrics::{gauge, labels};

    use super::*;

    #[test]
    fn metrics_text_payload_renders_prometheus_handle() {
        let recorder = PrometheusBuilder::new().build_recorder();
        set_prometheus_handle(recorder.handle()).expect("Prometheus handle should be set once");

        with_local_recorder(&recorder, || {
            let info_metric = gauge!(
                name: "prometheus_payload_info",
                description: "prometheus payload info",
                subsystem: "",
            );
            quickwit_metrics::describe_metrics();
            gauge!(parent: info_metric, labels: [labels!("version" => "test")]).set(1.0);
        });

        let payload = metrics_text_payload().expect("Prometheus payload should render");
        assert!(payload.contains("# HELP quickwit_prometheus_payload_info"));
        assert!(payload.contains(r#"quickwit_prometheus_payload_info{version="test"} 1"#));
    }
}
