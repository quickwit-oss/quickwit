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

use std::io::Write;
use std::path::Path;

use anyhow::Context;
use clap::Parser;
use tracing::info;
use vector::app::Application;
use vector::cli::Opts;
use vector::extra_context::ExtraContext;

use crate::IntakeConfig;
use crate::host_tags::HostTagsStore;
use crate::host_tags_poller::{self, HostTagsPollerConfig, UnknownHostsCollector};

fn build_vector_config(data_dir: &Path, config: &IntakeConfig, print: bool) -> String {
    let data_dir = data_dir.display();
    let logs_endpoint = &config.logs_endpoint;
    let metrics_endpoint = &config.metrics_endpoint;
    let traces_endpoint = &config.traces_endpoint;
    let org_id = &config.org_id;
    let dd_site = config.resolve_dd_site();
    let metadata_svc_url = format!("https://{}", dd_site.trim_end_matches('/'));
    let metric_metadata_persist_file_path = config.metric_metadata_persist_file_path.display();

    let print_sink = if print {
        r#"
  print:
    type: console
    inputs:
      - add_log_host_tags
      - add_metric_host_tags
      - add_trace_host_tags
    encoding:
      codec: json
"#
    } else {
        ""
    };

    format!(
        r#"
data_dir: "{data_dir}"

# Enable the API for exposing the healthcheck endpoint.
api:
  enabled: true
  address: "0.0.0.0:8686"

sources:
  datadog_agent:
    type: datadog_agent
    address: "0.0.0.0:8181"
    multiple_outputs: true
    disable_logs: false
    disable_metrics: false
    disable_traces: false

  http:
    type: http_server
    address: "0.0.0.0:8282"

  otlp:
    type: opentelemetry
    grpc:
      address: "0.0.0.0:8383"
    http:
      address: "0.0.0.0:8384"

  # Receives raw agent CollectorConnections payloads.
  # Handles V8 envelope stripping + zstd decompression at the source level,
  # emits decoded protobuf bytes for the connections_to_apm_metrics transform.
  connections:
    type: connections
    address: "0.0.0.0:8585"

transforms:
  preprocess_logs:
    type: preprocess_log
    inputs:
      - datadog_agent.logs
      - http
      - otlp.logs

  # Decode connection payloads and produce universal.* metrics.
  connections_to_apm_metrics:
    type: connections_to_apm_metrics
    inputs:
      - connections

  preprocess_metrics:
    type: preprocess_metric
    inputs:
      - datadog_agent.metrics
      - otlp.metrics
      - connections_to_apm_metrics

  metric_metadata:
    type: metric_metadata
    inputs:
      - add_metric_host_tags
    org_id: "{org_id}"
    metadata_svc_url: "{metadata_svc_url}"
    persist_file_path: "{metric_metadata_persist_file_path}"

  preprocess_dd_traces:
    type: preprocess_dd_trace
    inputs:
      - datadog_agent.traces

  explode_dd_trace_spans:
    type: explode_trace_spans
    inputs:
      - preprocess_dd_traces

  preprocess_spans:
    type: preprocess_span
    inputs:
      - explode_dd_trace_spans
      - otlp.traces

  normalize_log_names:
    type: name_normalizer
    inputs:
      - preprocess_logs

  normalize_metric_names:
    type: name_normalizer
    inputs:
      - preprocess_metrics

  normalize_trace_names:
    type: name_normalizer
    inputs:
      - preprocess_spans

  add_log_host_tags:
    type: add_host_tags
    inputs:
      - normalize_log_names

  add_metric_host_tags:
    type: add_host_tags
    inputs:
      - normalize_metric_names

  add_trace_host_tags:
    type: add_host_tags
    inputs:
      - normalize_trace_names

sinks:
  logs_out:
    type: http
    inputs:
      - add_log_host_tags
    uri: "{logs_endpoint}"
    method: post
    encoding:
      codec: json
    framing:
      method: newline_delimited

  metrics_out:
    type: arrow_ipc_metrics
    inputs:
      - metric_metadata
    uri: "{metrics_endpoint}"

  traces_out:
    type: http
    inputs:
      - add_trace_host_tags
    uri: "{traces_endpoint}"
    method: post
    encoding:
      codec: json
    framing:
      method: newline_delimited
{print_sink}
"#
    )
}

/// Spawns the host-tags poller on the Vector runtime.
///
/// Panics if `dd_site` or `dd_api_key` cannot be resolved from either the
/// intake config or the `DD_SITE` / `DD_API_KEY` environment variables —
/// both are required for the intake service to run.
fn spawn_host_tags_poller(config: &IntakeConfig, handle: &tokio::runtime::Handle) {
    let dd_site = config.resolve_dd_site();
    let dd_api_key = config
        .resolve_dd_api_key()
        .expect("DD API key should be set via config or DD_API_KEY env var");
    let host_tags_config = &config.host_tags;
    let poller_config = HostTagsPollerConfig {
        store: HostTagsStore::global(),
        collector: UnknownHostsCollector::global(),
        metadata_service_url: host_tags_config.metadata_service_url(&dd_site),
        dd_api_key,
        poll_interval: host_tags_config.poll_interval(),
        fetch_timeout: host_tags_config.fetch_timeout(),
        ttl_min: host_tags_config.ttl_min(),
        ttl_max: host_tags_config.ttl_max(),
        cache_path: host_tags_config.cache_path.clone(),
    };
    handle.spawn(host_tags_poller::run_host_tags_poller(poller_config));
    info!("spawned host-tags poller");
}

/// Starts Vector in-process with the default processing pipeline config.
///
/// This function is **blocking** — Vector creates its own tokio runtime and
/// runs until it shuts down (via SIGINT / SIGTERM).
pub fn run_intake(config: IntakeConfig, print: bool) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.data_dir).with_context(|| {
        format!(
            "failed to create data directory `{}`",
            config.data_dir.display()
        )
    })?;
    let config_content = build_vector_config(&config.data_dir, &config, print);
    let mut config_file =
        tempfile::NamedTempFile::new().context("failed to create temporary intake config file")?;
    config_file
        .write_all(config_content.as_bytes())
        .context("failed to write intake config")?;
    config_file
        .flush()
        .context("failed to flush intake config")?;

    let config_path = config_file.path().to_path_buf();
    info!(config_path=%config_path.display(), "starting intake service");

    let opts = Opts::parse_from(["vector", "--config-yaml", &config_path.to_string_lossy()]);

    let extra_context: ExtraContext = std::iter::empty().collect();
    let (runtime, app) = Application::prepare_from_opts(opts, extra_context)
        .map_err(|code| anyhow::anyhow!("failed to prepare intake service (exit code {code})"))?;

    spawn_host_tags_poller(&config, runtime.handle());

    let started = app
        .start(runtime.handle())
        .map_err(|code| anyhow::anyhow!("failed to start intake service (exit code {code})"))?;

    runtime.block_on(async {
        let finished = started.main().await;
        finished.shutdown().await;
    });
    // Keep the temp config file alive until after shutdown.
    drop(config_file);
    info!("intake service terminated");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use vector::config::{Format, load_from_str};

    use super::*;

    /// Validates that the YAML emitted by [`build_vector_config`] is a
    /// well-formed Vector topology — every transform/sink references an
    /// upstream source/transform that actually exists, every component type
    /// is registered, and the schema parses. This is the cheapest check
    /// available for catching pipeline-wiring typos (e.g. an `inputs:` ref
    /// that points at a non-existent transform name) without spinning up the
    /// full intake binary or hitting the network.
    fn assert_vector_config_loads(print: bool) {
        let config = IntakeConfig::default();
        let yaml = build_vector_config(&PathBuf::from("/tmp/pomsky-intake-test"), &config, print);
        if let Err(errors) = load_from_str(&yaml, Format::Yaml) {
            panic!(
                "vector rejected the generated config:\n--- yaml ---\n{yaml}\n--- errors ---\n{}",
                errors.join("\n"),
            );
        }
    }

    #[test]
    fn build_vector_config_topology_is_valid() {
        assert_vector_config_loads(false);
    }

    #[test]
    fn build_vector_config_topology_with_print_sink_is_valid() {
        assert_vector_config_loads(true);
    }
}
