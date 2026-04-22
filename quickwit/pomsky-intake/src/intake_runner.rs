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

fn build_vector_config(data_dir: &Path, config: &IntakeConfig, print: bool) -> String {
    let data_dir = data_dir.display();
    let logs_endpoint = &config.logs_endpoint;
    let metrics_endpoint = &config.metrics_endpoint;
    let traces_endpoint = &config.traces_endpoint;
    let org_id = &config.org_id;
    let metadata_svc_url = &config.metadata_svc_url;
    format!(
        r#"
data_dir: "{data_dir}"

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

transforms:
  preprocess_logs:
    type: preprocess_log
    inputs:
      - datadog_agent.logs
      - http
      - otlp.logs

  preprocess_metrics:
    type: preprocess_metric
    inputs:
      - datadog_agent.metrics
      - otlp.metrics

  metric_metadata:
    type: metric_metadata
    inputs:
      - preprocess_metrics
    org_id: "{org_id}"
    metadata_svc_url: "{metadata_svc_url}"

  explode_dd_trace_spans:
    type: explode_trace_spans
    inputs:
      - datadog_agent.traces

  preprocess_traces:
    type: preprocess_trace
    inputs:
      - explode_dd_trace_spans
      - otlp.traces

sinks:
  logs_out:
    type: http
    inputs:
      - preprocess_logs
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
      - preprocess_traces
    uri: "{traces_endpoint}"
    method: post
    encoding:
      codec: json
    framing:
      method: newline_delimited
{print_sink}
"#,
        print_sink = if print {
            r#"
  print:
    type: console
    inputs:
      - preprocess_logs
      - preprocess_metrics
      - preprocess_traces
    encoding:
      codec: json
"#
        } else {
            ""
        }
    )
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
