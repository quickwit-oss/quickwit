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
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use byte_unit::Byte;
use clap::{load_yaml, value_t, App, AppSettings, ArgMatches};
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use quickwit_cli::*;
use quickwit_common::net::socket_addr_from_str;
use quickwit_serve::{serve_cli, ServeArgs};
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::Level;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Debug, PartialEq)]
enum CliCommand {
    ExtractSplit(ExtractSplitArgs),
    InspectSplit(InspectSplitArgs),
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Serve(ServeArgs),
    GarbageCollect(GarbageCollectIndexArgs),
    Delete(DeleteIndexArgs),
}

impl CliCommand {
    fn default_log_level(&self) -> Level {
        match self {
            CliCommand::ExtractSplit(_) => Level::INFO,
            CliCommand::InspectSplit(_) => Level::INFO,
            CliCommand::New(_) => Level::WARN,
            CliCommand::Index(_) => Level::INFO,
            CliCommand::Search(_) => Level::WARN,
            CliCommand::Serve(_) => Level::INFO,
            CliCommand::GarbageCollect(_) => Level::WARN,
            CliCommand::Delete(_) => Level::WARN,
        }
    }

    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;

        match subcommand {
            "new" => Self::parse_new_args(submatches),
            "index" => Self::parse_index_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "serve" => Self::parse_serve_args(submatches),
            "gc" => Self::parse_garbage_collect_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            "extract-split" => Self::parse_extract_split_args(submatches),
            "inspect-split" => Self::parse_inspect_split_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_new_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let index_config_path = matches
            .value_of("index-config-path")
            .map(PathBuf::from)
            .context("'index-config-path' is a required arg")?;
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::New(CreateIndexArgs::new(
            metastore_uri,
            index_id,
            index_uri,
            index_config_path,
            overwrite,
        )?))
    }

    fn parse_extract_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;

        let target_folder = matches
            .value_of("target-folder")
            .map(PathBuf::from)
            .context("'target-folder' is a required arg")?;

        Ok(CliCommand::ExtractSplit(ExtractSplitArgs::new(
            metastore_uri,
            index_id,
            split_id,
            target_folder,
        )?))
    }

    fn parse_inspect_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;

        let verbose = matches.is_present("verbose");

        Ok(CliCommand::InspectSplit(InspectSplitArgs::new(
            metastore_uri,
            index_id,
            split_id,
            verbose,
        )?))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .expect("`metastore-uri` is a required arg.");
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let input_path: Option<PathBuf> = matches.value_of("input-path").map(PathBuf::from);
        let source_config_path: Option<PathBuf> =
            matches.value_of("source-config-path").map(PathBuf::from);
        let data_dir_path: PathBuf = matches
            .value_of("data-dir-path")
            .map(PathBuf::from)
            .expect("`data-dir-path` is a required arg.");
        let heap_size = matches
            .value_of("heap-size")
            .map(Byte::from_str)
            .expect("`heap-size` has a default value.")?;
        let overwrite = matches.is_present("overwrite");
        let demux = matches.is_present("demux");
        let merge = !matches.is_present("no-merge");

        Ok(CliCommand::Index(IndexDataArgs {
            index_id,
            input_path,
            source_config_path,
            data_dir_path,
            heap_size,
            metastore_uri,
            overwrite,
            demux,
            merge,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let query = matches
            .value_of("query")
            .context("query is a required arg")?
            .to_string();
        let max_hits = value_t!(matches, "max-hits", usize)?;
        let start_offset = value_t!(matches, "start-offset", usize)?;
        let search_fields = matches
            .values_of("search-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
        let start_timestamp = if matches.is_present("start-timestamp") {
            Some(value_t!(matches, "start-timestamp", i64)?)
        } else {
            None
        };
        let end_timestamp = if matches.is_present("end-timestamp") {
            Some(value_t!(matches, "end-timestamp", i64)?)
        } else {
            None
        };
        let tags = matches
            .values_of("tags")
            .map(|values| values.map(|value| value.to_string()).collect());

        Ok(CliCommand::Search(SearchIndexArgs {
            index_id,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
            tags,
            metastore_uri,
        }))
    }

    fn parse_serve_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let host = matches
            .value_of("host")
            .context("'host' has a default value")?
            .to_string();
        let port = value_t!(matches, "port", u16)?;
        let rest_addr = format!("{}:{}", host, port);
        let rest_socket_addr = socket_addr_from_str(&rest_addr)?;
        let host_key_path_prefix = matches
            .value_of("host-key-path-prefix")
            .context("'host-key-path-prefix' has a default  value")?
            .to_string();
        let host_key_path =
            Path::new(format!("{}-{}-{}", host_key_path_prefix, host, port.to_string()).as_str())
                .to_path_buf();
        let mut peer_socket_addrs: Vec<SocketAddr> = Vec::new();
        if matches.is_present("peer-seed") {
            if let Some(values) = matches.values_of("peer-seed") {
                for value in values {
                    peer_socket_addrs.push(socket_addr_from_str(value)?);
                }
            }
        }

        Ok(CliCommand::Serve(ServeArgs {
            rest_socket_addr,
            host_key_path,
            peer_socket_addrs,
            metastore_uri,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let dry_run = matches.is_present("dry-run");

        Ok(CliCommand::Delete(DeleteIndexArgs {
            index_id,
            metastore_uri,
            dry_run,
        }))
    }

    fn parse_garbage_collect_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let grace_period = matches
            .value_of("grace-period")
            .map(parse_duration_with_unit)
            .context("'grace-period' should have default")??;
        let dry_run = matches.is_present("dry-run");

        Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            metastore_uri,
            dry_run,
        }))
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
    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();
    let about_text = about_text();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml)
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .about(about_text.as_str());
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };

    setup_logging_and_tracing(command.default_log_level())?;

    let command_res = match command {
        CliCommand::ExtractSplit(args) => extract_split_cli(args).await,
        CliCommand::InspectSplit(args) => inspect_split_cli(args).await,
        CliCommand::New(args) => create_index_cli(args).await,
        CliCommand::Index(args) => index_data_cli(args).await,
        CliCommand::Search(args) => search_index_cli(args).await,
        CliCommand::Serve(args) => serve_cli(args).await,
        CliCommand::GarbageCollect(args) => garbage_collect_index_cli(args).await,
        CliCommand::Delete(args) => delete_index_cli(args).await,
    };

    let return_code: i32 = if let Err(err) = command_res {
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
    let mut about_text = format!(
        "Indexing your large dataset on object storage & making it searchable from the command \
         line.\nCommit hash: {}\n",
        env!("GIT_COMMIT_HASH")
    );
    if quickwit_telemetry::is_telemetry_enabled() {
        about_text += "Telemetry Enabled";
    }
    about_text
}

/// Parse duration with unit.
/// examples: 1s 2m 3h 5d
pub fn parse_duration_with_unit(duration: &str) -> anyhow::Result<Duration> {
    let mut value = "".to_string();
    let mut unit = "".to_string();
    for character in duration.chars() {
        if character.is_numeric() {
            value.push(character);
        } else {
            unit.push(character);
        }
    }

    match value.parse::<u64>() {
        Ok(value) => match unit.as_str() {
            "s" => Ok(Duration::from_secs(value)),
            "m" => Ok(Duration::from_secs(value * 60)),
            "h" => Ok(Duration::from_secs(value * 60 * 60)),
            "d" => Ok(Duration::from_secs(value * 60 * 60 * 24)),
            _ => Err(anyhow::anyhow!("Invalid duration format: `[0-9]+[smhd]`")),
        },
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use std::time::Duration;

    use clap::{load_yaml, App, AppSettings};
    use quickwit_serve::ServeArgs;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        parse_duration_with_unit, CliCommand, CreateIndexArgs, DeleteIndexArgs,
        GarbageCollectIndexArgs, IndexDataArgs, SearchIndexArgs,
    };

    #[test]
    fn test_parse_new_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches_result =
            app.get_matches_from_safe(vec!["new", "--index-uri", "file:///indexes/wikipedia"]);
        assert!(matches!(matches_result, Err(_)));
        let mut index_config_file = NamedTempFile::new()?;
        let index_config_str = r#"{
            "type": "default",
            "store_source": true,
            "default_search_fields": ["timestamp"],
            "timestamp_field": "timestamp",
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                }
            ]
        }"#;
        index_config_file.write_all(index_config_str.as_bytes())?;
        let path = index_config_file.into_temp_path();
        let path_str = path.to_string_lossy().to_string();
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--index-id",
            "wikipedia",
            "--index-config-path",
            &path_str,
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes".to_string(),
                "wikipedia".to_string(),
                "file:///indexes/wikipedia".to_string(),
                path.to_path_buf(),
                false,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);

        const QUICKWIT_METASTORE_URI_ENV_KEY: &str = "QUICKWIT_METASTORE_URI";
        env::set_var(QUICKWIT_METASTORE_URI_ENV_KEY, "file:///indexes");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--index-id",
            "wikipedia",
            "--index-config-path",
            &path_str,
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes".to_string(),
                "wikipedia".to_string(),
                "file:///indexes/wikipedia".to_string(),
                path.to_path_buf(),
                true,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);
        env::remove_var(QUICKWIT_METASTORE_URI_ENV_KEY);

        Ok(())
    }

    #[test]
    fn test_parse_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--data-dir-path",
            "/var/lib/quickwit/data",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_id,
                input_path: None,
                source_config_path: None,
                data_dir_path,
                heap_size,
                metastore_uri,
                overwrite: false,
                demux: false,
                merge: false,
            })) if &index_id == "wikipedia"
                    && &metastore_uri == "file:///indexes"
                    && data_dir_path == Path::new("/var/lib/quickwit/data")
                    && heap_size.get_bytes() == 2_000_000_000
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-id",
            "wikipedia",
            "--source-config-path",
            "/conf/source_config.json",
            "--data-dir-path",
            "/var/lib/quickwit/data",
            "--heap-size",
            "4gib",
            "--metastore-uri",
            "file:///indexes",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_id,
                input_path: None,
                source_config_path: Some(source_config_path),
                data_dir_path,
                heap_size,
                metastore_uri,
                overwrite: true,
                demux: false,
                merge: false,
            })) if &index_id == "wikipedia"
                    && metastore_uri == "file:///indexes"
                    && source_config_path == Path::new("/conf/source_config.json")
                    && data_dir_path == Path::new("/var/lib/quickwit/data")
                    && heap_size.get_bytes() == 4_294_967_296
        ));
        Ok(())
    }

    #[test]
    fn test_source_config_path_and_input_path_args_are_mutually_exclusive() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--input-path",
            "/data/wikipedia.json",
            "--source-config-path",
            "/conf/source_config.json",
            "--data-dir-path",
            "/var/lib/quickwit/data",
        ]);
        assert!(matches.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-id",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
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
        let matches = app.get_matches_from_safe(vec![
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
            "--search-fields",
            "title",
            "url",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
            "--tags",
            "device:rpi",
            "city:paris",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
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
        let matches = app.get_matches_from_safe(vec![
            "delete",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_id,
                metastore_uri,
                dry_run: false
            })) if &index_id == "wikipedia" && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "delete",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
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
        let matches = app.get_matches_from_safe(vec![
            "gc",
            "--index-id",
            "wikipedia",
            "--metastore-uri",
            "file:///indexes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                metastore_uri,
                dry_run: false
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(60 * 60) && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "gc",
            "--index-id",
            "wikipedia",
            "--grace-period",
            "5m",
            "--metastore-uri",
            "file:///indexes",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                metastore_uri,
                dry_run: true
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(5 * 60) && &metastore_uri == "file:///indexes"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_serve_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "serve",
            "--metastore-uri",
            "file:///indexes",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "--host-key-path-prefix",
            "/etc/quickwit-host-key",
            "--peer-seed",
            "192.168.1.13:9090",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Serve(ServeArgs {
                rest_socket_addr, host_key_path, peer_socket_addrs, metastore_uri,
            })) if rest_socket_addr == socket_addr_from_str("127.0.0.1:9090").unwrap() && host_key_path == Path::new("/etc/quickwit-host-key-127.0.0.1-9090").to_path_buf() && peer_socket_addrs == vec![socket_addr_from_str("192.168.1.13:9090").unwrap()] && &metastore_uri == "file:///indexes"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "serve",
            "--metastore-uri",
            "file:///indexes",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "--host-key-path-prefix",
            "/etc/quickwit-host-key",
            "--peer-seed",
            "192.168.1.13:9090,192.168.1.14:9090",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Serve(ServeArgs {
                rest_socket_addr, host_key_path, peer_socket_addrs, metastore_uri,
            })) if rest_socket_addr == socket_addr_from_str("127.0.0.1:9090").unwrap() && host_key_path == Path::new("/etc/quickwit-host-key-127.0.0.1-9090").to_path_buf() && peer_socket_addrs == vec![socket_addr_from_str("192.168.1.13:9090").unwrap(), socket_addr_from_str("192.168.1.14:9090").unwrap()] && &metastore_uri == "file:///indexes"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_duration_with_unit() -> anyhow::Result<()> {
        assert_eq!(parse_duration_with_unit("8s")?, Duration::from_secs(8));
        assert_eq!(parse_duration_with_unit("5m")?, Duration::from_secs(5 * 60));
        assert_eq!(
            parse_duration_with_unit("2h")?,
            Duration::from_secs(2 * 60 * 60)
        );
        assert_eq!(
            parse_duration_with_unit("3d")?,
            Duration::from_secs(3 * 60 * 60 * 24)
        );

        assert!(parse_duration_with_unit("").is_err());
        assert!(parse_duration_with_unit("a2d").is_err());
        assert!(parse_duration_with_unit("3 d").is_err());
        assert!(parse_duration_with_unit("3").is_err());
        Ok(())
    }
}
