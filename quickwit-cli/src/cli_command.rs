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
use clap::{value_t, ArgMatches};
use std::path::PathBuf;

pub struct CreateIndexArgs {
    pub index_uri: PathBuf,
    pub timestamp_field: Option<String>,
    pub overwrite: bool,
}

#[derive(Debug, Clone)]
pub struct IndexDataArgs {
    pub index_uri: PathBuf,
    pub input_uri: Option<PathBuf>,
    pub temp_dir: PathBuf,
    pub num_threads: usize,
    pub heap_size: u64,
    pub overwrite: bool,
}

pub struct SearchIndexArgs {
    pub index_uri: PathBuf,
    pub query: String,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
}

pub struct DeleteIndexArgs {
    pub index_uri: PathBuf,
    pub dry_run: bool,
}

pub enum CliCommand {
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Delete(DeleteIndexArgs),
}

impl CliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Unable to parse sub matches"))?;

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
