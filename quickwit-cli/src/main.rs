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

use anyhow::bail;
use byte_unit::Byte;
use clap::{load_yaml, value_t, App, ArgMatches};
use std::path::PathBuf;
use tracing::debug;

use quickwit_core::index::{create_index, delete_index};
use quickwit_doc_mapping::DocMapping;

struct CreateIndexArgs {
    index_uri: PathBuf,
    timestamp_field: Option<String>,
    overwrite: bool,
}

struct IndexDataArgs {
    index_uri: PathBuf,
    input_uri: Option<PathBuf>,
    temp_dir: PathBuf,
    num_threads: usize,
    heap_size: u64,
    overwrite: bool,
}

struct SearchIndexArgs {
    index_uri: PathBuf,
    query: String,
    max_hits: usize,
    start_offset: usize,
    search_fields: Option<Vec<String>>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

struct DeleteIndexArgs {
    index_uri: PathBuf,
    dry_run: bool,
}

enum CliCommand {
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Delete(DeleteIndexArgs),
}
impl CliCommand {
    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches = submatches_opt.unwrap();

        match subcommand {
            "new" => Self::parse_new_args(submatches),
            "index" => Self::parse_index_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_new_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri_str = matches.value_of("index-uri").unwrap().to_string(); // 'index-uri' is a required arg
        let index_uri = PathBuf::from(index_uri_str);
        let timestamp_field = matches
            .value_of("timestamp-field")
            .map(|field| field.to_string());
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::New(CreateIndexArgs {
            index_uri,
            timestamp_field,
            overwrite,
        }))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri_str = matches.value_of("index-uri").unwrap(); // 'index-uri' is a required arg
        let index_uri = PathBuf::from(index_uri_str);
        let input_uri = matches.value_of("input-uri").map(PathBuf::from);
        let temp_dir_str = matches.value_of("temp-dir").unwrap(); // 'temp-dir' has a default value
        let temp_dir = PathBuf::from(temp_dir_str);
        let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
        let heap_size_str = matches.value_of("heap-size").unwrap(); // 'heap-size' has a default value
        let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::Index(IndexDataArgs {
            index_uri,
            input_uri,
            temp_dir,
            num_threads,
            heap_size,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri_str = matches.value_of("index-uri").unwrap(); // 'index-uri' is a required arg
        let index_uri = PathBuf::from(index_uri_str);
        let query = matches.value_of("query").unwrap().to_string(); // 'query' is a required arg
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

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri_str = matches.value_of("index-uri").unwrap(); // 'index-uri' is a required arg
        let index_uri = PathBuf::from(index_uri_str);
        let dry_run = matches.is_present("dry-run");

        Ok(CliCommand::Delete(DeleteIndexArgs { index_uri, dry_run }))
    }
}

async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        timestamp_field =? args.timestamp_field,
        overwrite = args.overwrite,
        "create-index"
    );
    let index_uri = args.index_uri.to_string_lossy().to_string();
    let doc_mapping = DocMapping::Dynamic;

    if args.overwrite {
        delete_index(index_uri.clone()).await?;
    }
    create_index(index_uri, doc_mapping).await?;
    Ok(())
}

async fn index_data_cli(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        input_uri =% args.input_uri.unwrap_or_else(|| PathBuf::from("stdin")).display(),
        temp_dir =% args.temp_dir.display(),
        num_threads = args.num_threads,
        heap_size = args.heap_size,
        overwrite = args.overwrite,
        "indexing"
    );
    Ok(())
}

async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        query =% args.query,
        max_hits = args.max_hits,
        start_offset = args.start_offset,
        search_fields =? args.search_fields,
        start_timestamp =? args.start_timestamp,
        end_timestamp =? args.end_timestamp,
        "search-index"
    );
    Ok(())
}

async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        dry_run = args.dry_run,
        "delete-index"
    );
    Ok(())
}

#[tracing::instrument]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml).version(env!("CARGO_PKG_VERSION"));
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
        CliCommand::Delete(args) => delete_index_cli(args).await,
    };
    if let Err(err) = command_res {
        eprintln!("Command failed: {:?}", err);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use crate::{CliCommand, CreateIndexArgs, DeleteIndexArgs, IndexDataArgs, SearchIndexArgs};
    use clap::{load_yaml, App, AppSettings};
    use std::path::PathBuf;

    #[test]
    fn test_parse_new_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "./wikipedia",
            "--no-timestamp-field",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::New(CreateIndexArgs {
                index_uri,
                timestamp_field: None,
                overwrite: false
            })) if index_uri == PathBuf::from("./wikipedia")
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "./wikipedia",
            "--timestamp-field",
            "ts",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::New(CreateIndexArgs {
                index_uri,
                timestamp_field: Some(field_name),
                overwrite: true
            })) if index_uri == PathBuf::from("./wikipedia") && field_name == "ts"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec!["index", "--index-uri", "./wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_uri: None,
                temp_dir,
                num_threads: 2,
                heap_size: 2_000_000_000,
                overwrite: false,
            })) if index_uri == PathBuf::from("./wikipedia") && temp_dir == PathBuf::from("/tmp")
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-uri",
            "./wikipedia",
            "--input-uri",
            "./data/wikipedia",
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
                input_uri: Some(input_uri),
                temp_dir,
                num_threads: 4,
                heap_size: 4_294_967_296,
                overwrite: true,
            })) if index_uri == PathBuf::from("./wikipedia") && input_uri == PathBuf::from("./data/wikipedia") && temp_dir == PathBuf::from("./tmp")
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
            })) if index_uri == PathBuf::from("./wikipedia") && query == "Barack Obama"
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
            })) if index_uri == PathBuf::from("./wikipedia") && query == "Barack Obama" && field_names == vec!["title".to_string(), "url".to_string()]
        ));

        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec!["delete", "--index-uri", "./wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: false
            })) if index_uri == PathBuf::from("./wikipedia")
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["delete", "--index-uri", "./wikipedia", "--dry-run"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: true
            })) if index_uri == PathBuf::from("./wikipedia")
        ));

        Ok(())
    }
}
