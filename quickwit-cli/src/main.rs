// Copyright (C) 2021 Quickwit, Inc.
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
use std::fmt::Debug;

use anyhow::{bail, Context};
use clap::{load_yaml, App, AppSettings, ArgMatches};
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use quickwit_cli::index::IndexCliCommand;
use quickwit_cli::service::ServiceCliCommand;
use quickwit_cli::split::SplitCliCommand;
use quickwit_cli::*;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Debug, PartialEq)]
enum CliCommand {
    Index(IndexCliCommand),
    Split(SplitCliCommand),
    Service(ServiceCliCommand),
}

impl CliCommand {
    fn default_log_level(&self) -> Level {
        match self {
            CliCommand::Index(subcommand) => subcommand.default_log_level(),
            CliCommand::Split(_) => Level::ERROR,
            CliCommand::Service(_) => Level::INFO,
        }
    }

    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let subcommand_opt = matches.subcommand();
        let (subcommand, submatches) =
            subcommand_opt.ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "index" => IndexCliCommand::parse_cli_args(submatches).map(CliCommand::Index),
            "split" => SplitCliCommand::parse_cli_args(submatches).map(CliCommand::Split),
            "service" => ServiceCliCommand::parse_cli_args(submatches).map(CliCommand::Service),
            _ => bail!("Subcommand `{}` is not implemented.", subcommand),
        }
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            CliCommand::Index(sub_command) => sub_command.execute().await,
            CliCommand::Split(sub_command) => sub_command.execute().await,
            CliCommand::Service(sub_command) => sub_command.execute().await,
        }
    }
}

/// Describes the way events should be formatted in the logs.
fn event_format(
) -> Format<tracing_subscriber::fmt::format::Full, tracing_subscriber::fmt::time::ChronoUtc> {
    tracing_subscriber::fmt::format()
        .with_target(true)
        .with_timer(tracing_subscriber::fmt::time::ChronoUtc::with_format(
            // We do not rely on ChronoUtc::from_rfc3339, because it has a nanosecond precision.
            "%Y-%m-%dT%H:%M:%S%.3f%:z".to_string(),
        ))
}

fn setup_logging_and_tracing(level: Level) -> anyhow::Result<()> {
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={}", level)))
        .context("Failed to set up tracing env filter.")?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let registry = tracing_subscriber::registry().with(env_filter);
    if std::env::var_os(QUICKWIT_JAEGER_ENABLED_ENV_KEY).is_some() {
        // TODO: use install_batch once this issue is fixed: https://github.com/open-telemetry/opentelemetry-rust/issues/545
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("quickwit")
            //.install_batch(opentelemetry::runtime::Tokio)
            .install_simple()
            .context("Failed to initialize Jaeger exporter.")?;
        registry
            .with(tracing_subscriber::fmt::layer().event_format(event_format()))
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .context("Failed to set up tracing.")?
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().event_format(event_format()))
            .try_init()
            .context("Failed to set up tracing.")?
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "openssl-support")]
    openssl_probe::init_ssl_cert_env_vars();

    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();
    let about_text = about_text();
    let version_text = format!(
        "{} (commit-hash: {})",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_COMMIT_HASH")
    );

    let yaml = load_yaml!("cli.yaml");
    let matches = App::from(yaml)
        .version(version_text.as_str())
        .about(about_text.as_str())
        .license("AGPLv3.0")
        .setting(AppSettings::DisableHelpSubcommand)
        .get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };

    setup_logging_and_tracing(command.default_log_level())?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        commit = env!("GIT_COMMIT_HASH"),
    );

    let return_code: i32 = if let Err(err) = command.execute().await {
        eprintln!("Command failed: {:?}", err);
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
        "Index your dataset on object storage & making it searchable from the command line.\n  Find more information at https://quickwit.io/docs\n\n",
    );
    if quickwit_telemetry::is_telemetry_enabled() {
        about_text += "Telemetry: enabled";
    }
    about_text
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use clap::{load_yaml, App, AppSettings};
    use quickwit_cli::index::{
        CreateIndexArgs, DeleteIndexArgs, DescribeIndexArgs, GarbageCollectIndexArgs,
        IngestDocsArgs, MergeOrDemuxArgs, SearchIndexArgs,
    };
    use quickwit_cli::service::{RunIndexerArgs, RunSearcherArgs, ServiceCliCommand};
    use quickwit_cli::split::{DescribeSplitArgs, ExtractSplitArgs};

    use super::*;
    use crate::CliCommand;

    #[test]
    fn test_parse_create_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);

        let _ = app
            .try_get_matches_from(vec!["new", "--index-uri", "file:///indexes/wikipedia"])
            .unwrap_err();

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "create",
            "--index-config-uri",
            "conf.yaml",
            "--metastore-uri",
            "/indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_index_config_uri = format!(
            "file://{}/conf.yaml",
            std::env::current_dir().unwrap().display()
        );
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            metastore_uri: "file:///indexes".to_string(),
            index_config_uri: expected_index_config_uri.clone(),
            overwrite: false,
        }));
        assert_eq!(command, expected_cmd);

        const QUICKWIT_METASTORE_URI_ENV_KEY: &str = "QUICKWIT_METASTORE_URI";
        env::set_var(QUICKWIT_METASTORE_URI_ENV_KEY, "/indexes");

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "create",
            "--index-config-uri",
            "conf.yaml",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            metastore_uri: "file:///indexes".to_string(),
            index_config_uri: expected_index_config_uri,
            overwrite: true,
        }));
        assert_eq!(command, expected_cmd);
        env::remove_var(QUICKWIT_METASTORE_URI_ENV_KEY);

        Ok(())
    }

    #[test]
    fn test_parse_ingest_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "ingest",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "/indexes",
            "--data-dir-path",
            "/var/lib/quickwit/data",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    metastore_uri,
                    index_id,
                    input_path_opt: None,
                    data_dir_path,
                    overwrite: false,
                })) if &index_id == "wikipedia"
                       && &metastore_uri == "file:///indexes"
                       && data_dir_path == Path::new("/var/lib/quickwit/data")
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "ingest",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "/indexes",
            "--data-dir-path",
            "/var/lib/quickwit/data",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    metastore_uri,
                    index_id,
                    input_path_opt: None,
                    data_dir_path,
                    overwrite: true,
                })) if &index_id == "wikipedia"
                        && metastore_uri == "file:///indexes"
                        && data_dir_path == Path::new("/var/lib/quickwit/data")
        ));
        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "search",
            "--index-id",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--metastore-uri",
            "file:///indexes",
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
                start_timestamp: None,
                end_timestamp: None,
                tags: None,
                metastore_uri,
            })) if &index_id == "wikipedia" && &query == "Barack Obama" && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "search",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
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
            "--tags",
            "device:rpi",
            "city:paris",
            "--search-fields",
            "title",
            "url",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
                tags: Some(tags),
                metastore_uri,
            })) if &index_id == "wikipedia" && query == "Barack Obama"
                && field_names == vec!["title".to_string(), "url".to_string()]
                && tags == vec!["device:rpi".to_string(), "city:paris".to_string()] && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "delete",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                metastore_uri,
                dry_run: false
            })) if &index_id == "wikipedia" && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "delete",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                metastore_uri,
                dry_run: true
            })) if &index_id == "wikipedia" && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_garbage_collect_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "gc",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                metastore_uri,
                dry_run: false
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(60 * 60) && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "gc",
            "--index-id",
            "wikipedia",
            "--grace-period",
            "5m",
            "--metastore-uri",
            "file:///indexes",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                metastore_uri,
                dry_run: true
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(5 * 60) && metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_searcher_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "service",
            "run",
            "searcher",
            "--server-config-uri",
            "conf.toml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_server_config_uri = format!(
            "file://{}/conf.toml",
            std::env::current_dir().unwrap().display()
        );
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand::RunSearcher(RunSearcherArgs {
                server_config_uri,
            })) if server_config_uri == expected_server_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_indexer_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "service",
            "run",
            "indexer",
            "--index-id",
            "wikipedia",
            "--server-config-uri",
            "conf.toml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_server_config_uri = format!(
            "file://{}{}conf.toml",
            std::env::current_dir().unwrap().display(),
            std::path::MAIN_SEPARATOR
        );
        println!("{} {:?}", expected_server_config_uri, command);
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand::RunIndexer(RunIndexerArgs {
                server_config_uri,
                index_id,
            })) if index_id == "wikipedia" && server_config_uri == expected_server_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_merge_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "merge",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--data-dir-path",
            "datadir",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Merge(MergeOrDemuxArgs {
                index_id,
                metastore_uri,
                data_dir_path,
            })) if &index_id == "wikipedia" && data_dir_path == PathBuf::from("datadir") && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_demux_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "demux",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--data-dir-path",
            "datadir",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Demux(MergeOrDemuxArgs {
                index_id,
                metastore_uri,
                data_dir_path,
            })) if &index_id == "wikipedia" && data_dir_path == PathBuf::from("datadir") && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_describe_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "index",
            "describe",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Describe(DescribeIndexArgs {
                index_id,
                metastore_uri,
            })) if &index_id == "wikipedia" && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "split",
            "describe",
            "--index-id",
            "wikipedia",
            "--split-id",
            "ABC",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Describe(DescribeSplitArgs {
                index_id,
                split_id,
                metastore_uri,
                verbose: false,
            })) if &index_id == "wikipedia" && &split_id == "ABC" && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_extract_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "split",
            "extract",
            "--index-id",
            "wikipedia",
            "--split-id",
            "ABC",
            "--target-dir",
            "datadir",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Extract(ExtractSplitArgs {
                index_id,
                split_id,
                metastore_uri,
                target_dir
            })) if &index_id == "wikipedia" && &split_id == "ABC" && &metastore_uri == "file:///indexes" && target_dir == PathBuf::from("datadir")
        ));
        Ok(())
    }
}
