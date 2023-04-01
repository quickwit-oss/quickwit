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

use std::env;

use anyhow::Context;
use colored::Colorize;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use quickwit_cli::cli::{build_cli, CliCommand};
#[cfg(feature = "jemalloc")]
use quickwit_cli::jemalloc::start_jemalloc_metrics_loop;
use quickwit_cli::{
    QW_ENABLE_JAEGER_EXPORTER_ENV_KEY, QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY,
};
use quickwit_common::RED_COLOR;
use quickwit_serve::{quickwit_build_info, QuickwitBuildInfo};
use quickwit_telemetry::payload::TelemetryEvent;
use tonic::metadata::MetadataMap;
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

fn setup_logging_and_tracing(
    level: Level,
    ansi: bool,
    build_info: &QuickwitBuildInfo,
) -> anyhow::Result<()> {
    #[cfg(feature = "tokio-console")]
    {
        if std::env::var_os(quickwit_cli::QW_ENABLE_TOKIO_CONSOLE_ENV_KEY).is_some() {
            console_subscriber::init();
            return Ok(());
        }
    }
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={level}")))
        .context("Failed to set up tracing env filter.")?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let registry = tracing_subscriber::registry().with(env_filter);
    let event_format = tracing_subscriber::fmt::format()
        .with_target(true)
        .with_ansi(ansi)
        .with_timer(
            // We do not rely on the Rfc3339 implementation, because it has a nanosecond precision.
            // See discussion here: https://github.com/time-rs/time/discussions/418
            UtcTime::new(
                time::format_description::parse(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
                )
                .expect("Time format invalid."),
            ),
        );
    if std::env::var_os(QW_ENABLE_JAEGER_EXPORTER_ENV_KEY).is_some() {
        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name("quickwit")
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)
            .context("Failed to initialize Jaeger exporter.")?;
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(tracing_subscriber::fmt::layer().event_format(event_format))
            .try_init()
            .context("Failed to set up tracing.")?;
    } else if std::env::var_os(QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY).is_some() {
        let mut metadata_map = MetadataMap::with_capacity(2);
        metadata_map.insert("version", build_info.version.parse()?);
        metadata_map.insert("commit", build_info.commit_short_hash.parse()?);

        let otlp_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_env()
            .with_metadata(metadata_map);
        let trace_config = trace::config()
            .with_resource(Resource::new([KeyValue::new("service.name", "quickwit")]));
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(otlp_exporter)
            .with_trace_config(trace_config)
            .install_batch(opentelemetry::runtime::Tokio)
            .context("Failed to initialize OpenTelemetry OTLP exporter.")?;
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(tracing_subscriber::fmt::layer().event_format(event_format))
            .try_init()
            .context("Failed to set up tracing.")?;
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().event_format(event_format))
            .try_init()
            .context("Failed to set up tracing.")?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "openssl-support")]
    openssl_probe::init_ssl_cert_env_vars();

    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();
    let about_text = about_text();
    let build_info = quickwit_build_info();
    let version_text = format!(
        "{} ({} {})",
        build_info.version, build_info.commit_short_hash, build_info.build_date
    );

    let app = build_cli()
        .about(about_text.as_str())
        .version(version_text.as_str());
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {err:?}");
            std::process::exit(1);
        }
    };

    #[cfg(feature = "jemalloc")]
    start_jemalloc_metrics_loop();

    setup_logging_and_tracing(
        command.default_log_level(),
        !matches.is_present("no-color"),
        build_info,
    )?;
    let return_code: i32 = if let Err(err) = command.execute().await {
        eprintln!("{} Command failed: {:?}\n", "✘".color(RED_COLOR), err);
        1
    } else {
        0
    };
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::EndCommand { return_code }).await;
    telemetry_handle.terminate_telemetry().await;
    global::shutdown_tracer_provider();
    std::process::exit(return_code)
}

/// Return the about text with telemetry info.
fn about_text() -> String {
    let mut about_text = String::from(
        "Sub-second search & analytics engine on cloud storage.\n  Find more information at https://quickwit.io/docs\n\n",
    );
    if quickwit_telemetry::is_telemetry_enabled() {
        about_text += "Telemetry: enabled";
    }
    about_text
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    use quickwit_cli::cli::{build_cli, CliCommand};
    use quickwit_cli::index::{
        ClearIndexArgs, CreateIndexArgs, DeleteIndexArgs, DescribeIndexArgs, IndexCliCommand,
        IngestDocsArgs, SearchIndexArgs,
    };
    use quickwit_cli::split::{DescribeSplitArgs, SplitCliCommand};
    use quickwit_cli::tool::{
        ExtractSplitArgs, GarbageCollectIndexArgs, LocalIngestDocsArgs, MergeArgs, ToolCliCommand,
    };
    use quickwit_common::uri::Uri;
    use quickwit_rest_client::rest_client::CommitType;
    use reqwest::Url;

    #[test]
    fn test_parse_clear_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "clear", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Clear(ClearIndexArgs {
            cluster_endpoint: Url::from_str("http://127.0.0.1:7280").unwrap(),
            index_id: "wikipedia".to_string(),
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "clear", "--index", "wikipedia", "--yes"])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Clear(ClearIndexArgs {
            cluster_endpoint: Url::from_str("http://127.0.0.1:7280").unwrap(),
            index_id: "wikipedia".to_string(),
            assume_yes: true,
        }));
        assert_eq!(command, expected_cmd);
    }

    #[test]
    fn test_parse_create_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let _ = app
            .try_get_matches_from(["new", "--index-uri", "file:///indexes/wikipedia"])
            .unwrap_err();

        let app = build_cli().no_binary_name(true);
        let matches =
            app.try_get_matches_from(["index", "create", "--index-config", "index-conf.yaml"])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_index_config_uri = Uri::from_str(&format!(
            "file://{}/index-conf.yaml",
            std::env::current_dir().unwrap().display()
        ))
        .unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            cluster_endpoint: Url::from_str("http://127.0.0.1:7280").unwrap(),
            index_config_uri: expected_index_config_uri.clone(),
            overwrite: false,
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "create",
            "--index-config",
            "index-conf.yaml",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            cluster_endpoint: Url::from_str("http://127.0.0.1:7280").unwrap(),
            index_config_uri: expected_index_config_uri,
            overwrite: true,
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        Ok(())
    }

    #[test]
    fn test_parse_ingest_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--endpoint",
            "http://127.0.0.1:8000",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    cluster_endpoint,
                    index_id,
                    input_path_opt: None,
                    commit_type: CommitType::Auto,
                })) if &index_id == "wikipedia"
                       && cluster_endpoint == Url::from_str("http://127.0.0.1:8000").unwrap()
        ));

        let app = build_cli().no_binary_name(true);
        let matches =
            app.try_get_matches_from(["index", "ingest", "--index", "wikipedia", "--force"])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    cluster_endpoint,
                    index_id,
                    input_path_opt: None,
                    commit_type: CommitType::Force,
                })) if &index_id == "wikipedia"
                        && cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
        ));

        let app = build_cli().no_binary_name(true);
        let matches =
            app.try_get_matches_from(["index", "ingest", "--index", "wikipedia", "--wait"])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    cluster_endpoint,
                    index_id,
                    input_path_opt: None,
                    commit_type: CommitType::WaitFor,
                })) if &index_id == "wikipedia"
                        && cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
        ));

        let app = build_cli().no_binary_name(true);
        assert_eq!(
            app.try_get_matches_from([
                "index",
                "ingest",
                "--index",
                "wikipedia",
                "--wait",
                "--force",
            ])
            .unwrap_err()
            .kind(),
            clap::ErrorKind::ArgumentConflict
        );
        Ok(())
    }

    #[test]
    fn test_parse_local_ingest_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "tool",
                "local-ingest",
                "--index",
                "wikipedia",
                "--config",
                "/config.yaml",
                "--overwrite",
                "--keep-cache",
                "--transform-script",
                ".message = downcase(string!(.message))",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        println!("{command:?}");
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::LocalIngest(
                LocalIngestDocsArgs {
                    config_uri,
                    index_id,
                    input_path_opt: None,
                    overwrite,
                    vrl_script: Some(vrl_script),
                    clear_cache,
                })) if &index_id == "wikipedia"
                       && config_uri == Uri::from_str("file:///config.yaml").unwrap()
                       && vrl_script == ".message = downcase(string!(.message))"
                       && overwrite
                       && !clear_cache
        ));
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                snippet_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                aggregation: None,
                ..
            })) if &index_id == "wikipedia" && &query == "Barack Obama"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--max-hits",
            "50",
            "--start-offset",
            "100",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
            "--search-fields",
            "title",
            "url",
            "--snippet-fields",
            "body",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let _cluster_endpoint = Uri::from_str("http://127.0.0.1:7280").unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                aggregation: None,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(search_field_names),
                snippet_fields: Some(snippet_field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
                cluster_endpoint: _cluster_endpoint,
                sort_by_score: false,
            })) if &index_id == "wikipedia"
                  && query == "Barack Obama"
                  && search_field_names == vec!["title".to_string(), "url".to_string()]
                  && snippet_field_names == vec!["body".to_string()]
        ));
        Ok(())
    }

    #[test]
    fn test_parse_delete_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "delete", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "delete", "--index", "wikipedia", "--dry-run"])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: true,
                ..
            })) if &index_id == "wikipedia"
        ));
    }

    #[test]
    fn test_parse_describe_index_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "describe", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Describe(DescribeIndexArgs {
                index_id,
                ..
            })) if &index_id == "wikipedia"
        ));
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "split",
            "describe",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Describe(DescribeSplitArgs {
                index_id,
                split_id,
                verbose: false,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_extract_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "extract-split",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--target-dir",
            "datadir",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::ExtractSplit(ExtractSplitArgs {
                index_id,
                split_id,
                target_dir,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC" && target_dir == PathBuf::from("datadir")
        ));
        Ok(())
    }

    #[test]
    fn test_parse_garbage_collect_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "gc",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(60 * 60)
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "gc",
            "--index",
            "wikipedia",
            "--grace-period",
            "5m",
            "--config",
            "/config.yaml",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                config_uri,
                dry_run: true,
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(5 * 60) && config_uri == expected_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_merge_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "merge",
            "--index",
            "wikipedia",
            "--source",
            "ingest-source",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::Merge(MergeArgs {
                index_id,
                source_id,
                ..
            })) if &index_id == "wikipedia" && source_id == "ingest-source"
        ));
        Ok(())
    }
}
