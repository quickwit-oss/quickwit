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

use byte_unit::Byte;
use clap::{load_yaml, value_t, App, ArgMatches};
use tracing::debug;

fn run_new_subcommand(matches: &ArgMatches) -> anyhow::Result<()> {
    let index_path = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
    let timestamp_field = matches.value_of("timestamp-field");
    let overwrite = matches.is_present("overwrite");
    create_index(index_path, timestamp_field, overwrite)
}

fn create_index(
    index_path: &str,
    timestamp_field: Option<&str>,
    overwrite: bool,
) -> anyhow::Result<()> {
    debug!(
        index_path = index_path,
        timestamp_field =? timestamp_field,
        overwrite = overwrite,
        "create-index"
    );
    Ok(())
}

fn run_index_subcommand(matches: &ArgMatches) -> anyhow::Result<()> {
    let index_path = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
    let input_path = matches.value_of("input-path");
    let temp_dir = matches.value_of("temp-dir").unwrap(); // 'temp-dir' has a default value
    let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
    let heap_size_str = matches.value_of("heap-size").unwrap(); // 'heap-size' has a default value
    let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
    let overwrite = matches.is_present("overwrite");
    index(
        index_path,
        input_path,
        temp_dir,
        num_threads,
        heap_size,
        overwrite,
    )
}

fn index(
    index_path: &str,
    input_path: Option<&str>,
    temp_dir: &str,
    num_threads: usize,
    heap_size: u64,
    overwrite: bool,
) -> anyhow::Result<()> {
    debug!(
        index_path = index_path,
        input_path = input_path.unwrap_or("stdin"),
        temp_dir = temp_dir,
        num_threads = num_threads,
        heap_size = heap_size,
        overwrite = overwrite,
        "indexing"
    );
    Ok(())
}

fn run_search_subcommand(matches: &ArgMatches) -> anyhow::Result<()> {
    let index_path = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
    let query = matches.value_of("query").unwrap(); // 'query' is a required arg
    let max_hits = value_t!(matches, "max-hits", usize)?;
    let start_offset = value_t!(matches, "start-offset", usize)?;
    let search_fields = matches
        .values_of("search-fields")
        .map(|values| values.collect());
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
    search_index(
        index_path,
        query,
        max_hits,
        start_offset,
        search_fields,
        start_timestamp,
        end_timestamp,
    )
}

fn search_index(
    index_path: &str,
    query: &str,
    max_hits: usize,
    start_offset: usize,
    search_fields: Option<Vec<&str>>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
) -> anyhow::Result<()> {
    debug!(
        index_path = index_path,
        query = query,
        max_hits = max_hits,
        start_offset = start_offset,
        search_fields =? search_fields,
        start_timestamp =? start_timestamp,
        end_timestamp =? end_timestamp,
        "search-index"
    );
    Ok(())
}

fn run_delete_subcommand(matches: &ArgMatches) -> anyhow::Result<()> {
    let index_path = matches.value_of("index-path").unwrap(); // 'index-path' is a required arg
    let dry_run = matches.is_present("dry-run");
    delete_index(index_path, dry_run)
}

fn delete_index(index_path: &str, dry_run: bool) -> anyhow::Result<()> {
    debug!(index_path = index_path, dry_run = dry_run, "delete-index");
    Ok(())
}

#[tracing::instrument]
fn main() {
    tracing_subscriber::fmt::init();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml).version(env!("CARGO_PKG_VERSION"));
    let matches = app.get_matches();
    let (subcommand, submatches) = matches.subcommand();

    let run_subcommand = match subcommand {
        "new" => run_new_subcommand,
        "index" => run_index_subcommand,
        "delete" => run_delete_subcommand,
        "search" => run_search_subcommand,
        _ => panic!("Subcommand '{}' is not implemented", subcommand),
    };

    if let Err(err) = run_subcommand(submatches.unwrap()) {
        eprintln!("Command failed: {:?}", err);
        std::process::exit(1);
    }
}
