/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use anyhow::{bail, Context};
use byte_unit::Byte;
use clap::{load_yaml, value_t, App, AppSettings, ArgMatches};
use quickwit_cli::*;
use quickwit_serve::serve_cli;
use quickwit_serve::ServeArgs;
use quickwit_telemetry::payload::TelemetryEvent;
use std::env;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
#[derive(Debug, PartialEq)]
enum CliCommand {
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Serve(ServeArgs),
    Delete(DeleteIndexArgs),
}
impl CliCommand {
    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Unable to parse sub matches"))?;

        match subcommand {
            "new" => Self::parse_new_args(submatches),
            "index" => Self::parse_index_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "serve" => Self::parse_serve_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_new_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let doc_mapper_type = matches
            .value_of("doc-mapper-type")
            .context("doc-mapper-type has a default value")?;
        let doc_mapper_config_path = matches
            .value_of("doc-mapper-config-path")
            .map(PathBuf::from);
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::New(CreateIndexArgs::new(
            index_uri,
            doc_mapper_type,
            doc_mapper_config_path,
            overwrite,
        )?))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("index-uri is required")?
            .to_string();
        let input_path: Option<PathBuf> = matches.value_of("input-path").map(PathBuf::from);
        let temp_dir: Option<PathBuf> = matches.value_of("temp-dir").map(PathBuf::from);
        let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
        let heap_size_str = matches
            .value_of("heap-size")
            .context("heap-size has a default value")?;
        let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
        let overwrite = matches.is_present("overwrite");
        Ok(CliCommand::Index(IndexDataArgs {
            index_uri,
            input_path,
            temp_dir,
            num_threads,
            heap_size,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
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

        Ok(CliCommand::Search(SearchIndexArgs {
            index_uri,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
        }))
    }

    fn parse_serve_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uris = matches
            .values_of("index-uri")
            .map(|values| {
                values
                    .into_iter()
                    .map(|index_uri| index_uri.to_string())
                    .collect()
            })
            .context("At least one 'index-uri' is required.")?;
        let peers: Vec<SocketAddr> = matches
            .values_of("peer_seeds")
            .map(|values| {
                values
                    .map(|peer_socket_addr_str| SocketAddr::from_str(peer_socket_addr_str))
                    .collect::<Result<Vec<SocketAddr>, _>>()
            })
            .unwrap_or_else(|| Ok(Vec::new()))?;
        let host_str = matches
            .value_of("host")
            .context("'host' has a default  value")?
            .to_string();
        let rest_port = value_t!(matches, "port", u16)?;
        let rest_ip_addr = IpAddr::from_str(&host_str)?;
        let rest_socket_addr = SocketAddr::new(rest_ip_addr, rest_port);
        Ok(CliCommand::Serve(ServeArgs {
            index_uris,
            rest_socket_addr,
            peers,
        }))
    }
    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let dry_run = matches.is_present("dry-run");
        Ok(CliCommand::Delete(DeleteIndexArgs { index_uri, dry_run }))
    }
}

#[tracing::instrument]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml)
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"));
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };
    let command_res = match command {
        CliCommand::New(args) => create_index_cli(args).await,
        CliCommand::Index(args) => index_data_cli(args).await,
        CliCommand::Search(args) => search_index_cli(args).await,
        CliCommand::Serve(args) => serve_cli(args).await,
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

    std::process::exit(return_code);
}

#[cfg(test)]
mod tests {
    use crate::{CliCommand, CreateIndexArgs, DeleteIndexArgs, IndexDataArgs, SearchIndexArgs};
    use clap::{load_yaml, App, AppSettings};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_new_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches_result =
            app.get_matches_from_safe(vec!["new", "--index-uri", "file:///indexes/wikipedia"]);
        assert!(matches!(matches_result, Err(_)));
        let mut mapper_file = NamedTempFile::new()?;
        let mapper_str = r#"{
            "type": "default",
            "store_source": true,
            "default_search_fields": ["timestamp"],
            "timestamp_field": "timestamp",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                }
            ]
        }"#;
        mapper_file.write_all(mapper_str.as_bytes())?;
        let path = mapper_file.into_temp_path();
        let path_str = path.to_string_lossy().to_string();
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--doc-mapper-config-path",
            &path_str,
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes/wikipedia".to_string(),
                "wikipedia",
                None,
                false,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--doc-mapper-type",
            "wikipedia",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes/wikipedia".to_string(),
                "wikipedia",
                None,
                true,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);

        Ok(())
    }

    #[test]
    fn test_parse_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["index", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: None,
                temp_dir: None,
                num_threads: 2,
                heap_size: 2_000_000_000,
                overwrite: false,
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--input-path",
            "/data/wikipedia.json",
            "--temp-dir",
            "./tmp",
            "--num-threads",
            "4",
            "--heap-size",
            "4gib",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: Some(input_path),
                temp_dir,
                num_threads: 4,
                heap_size: 4_294_967_296,
                overwrite: true,
            })) if &index_uri == "file:///indexes/wikipedia" && input_path == Path::new("/data/wikipedia.json") && temp_dir == Some(PathBuf::from("./tmp"))
        ));

        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
            "--query",
            "Barack Obama",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                start_timestamp: None,
                end_timestamp: None,
            })) if index_uri == "./wikipedia" && query == "Barack Obama"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
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
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
            })) if index_uri == "./wikipedia" && query == "Barack Obama" && field_names == vec!["title".to_string(), "url".to_string()]
        ));

        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["delete", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: false
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "delete",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: true
            })) if &index_uri == "file:///indexes/wikipedia"
        ));
        Ok(())
    }
}
