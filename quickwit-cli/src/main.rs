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

struct CreateIndexArgs {
    index_path: PathBuf,
    timestamp_field: Option<String>,
    overwrite: bool,
}

struct IndexDataArgs {
    index_path: PathBuf,
    input_path: Option<PathBuf>,
    temp_dir: PathBuf,
    num_threads: usize,
    heap_size: u64,
    overwrite: bool,
}

struct SearchIndexArgs {
    index_path: PathBuf,
    query: String,
    max_hits: usize,
    start_offset: usize,
    search_fields: Option<Vec<String>>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

struct DeleteIndexArgs {
    index_path: PathBuf,
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
        let index_path_str = matches.value_of("index-path").unwrap().to_string(); // 'index-path' is a required arg
        let index_path = PathBuf::from(index_path_str);
        let timestamp_field = matches
            .value_of("timestamp-field")
            .map(|field| field.to_string());
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::New(CreateIndexArgs {
            index_path,
            timestamp_field,
            overwrite,
        }))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_path_str = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
        let index_path = PathBuf::from(index_path_str);
        let input_path = matches.value_of("input-path").map(PathBuf::from);
        let temp_dir_str = matches.value_of("temp-dir").unwrap(); // 'temp-dir' has a default value
        let temp_dir = PathBuf::from(temp_dir_str);
        let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
        let heap_size_str = matches.value_of("heap-size").unwrap(); // 'heap-size' has a default value
        let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
        let overwrite = matches.is_present("overwrite");

        Ok(CliCommand::Index(IndexDataArgs {
            index_path,
            input_path,
            temp_dir,
            num_threads,
            heap_size,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_path_str = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
        let index_path = PathBuf::from(index_path_str);
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
            index_path,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_path_str = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
        let index_path = PathBuf::from(index_path_str);
        let dry_run = matches.is_present("dry-run");

        Ok(CliCommand::Delete(DeleteIndexArgs {
            index_path,
            dry_run,
        }))
    }
}

fn create_index(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_path =% args.index_path.display(),
        timestamp_field =? args.timestamp_field,
        overwrite = args.overwrite,
        "create-index"
    );
    Ok(())
}

fn index_data(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(
        index_path =% args.index_path.display(),
        input_path =% args.input_path.unwrap_or_else(|| PathBuf::from("stdin")).display(),
        temp_dir =% args.temp_dir.display(),
        num_threads = args.num_threads,
        heap_size = args.heap_size,
        overwrite = args.overwrite,
        "indexing"
    );
    Ok(())
}

fn search_index(args: SearchIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_path =% args.index_path.display(),
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

fn delete_index(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_path =% args.index_path.display(),
        dry_run = args.dry_run,
        "delete-index"
    );
    Ok(())
}

#[tracing::instrument]
fn main() {
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
        CliCommand::New(args) => create_index(args),
        CliCommand::Index(args) => index_data(args),
        CliCommand::Search(args) => search_index(args),
        CliCommand::Delete(args) => delete_index(args),
    };
    if let Err(err) = command_res {
        eprintln!("Command failed: {:?}", err);
        std::process::exit(1);
    }
}
