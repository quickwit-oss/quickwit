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

mod cli_command;
mod indexing;

use clap::{load_yaml, App, AppSettings};
use cli_command::*;
use indexing::index_data;
use tracing::debug;

fn create_index(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        timestamp_field =? args.timestamp_field,
        overwrite = args.overwrite,
        "create-index"
    );
    Ok(())
}

fn search_index(args: SearchIndexArgs) -> anyhow::Result<()> {
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

fn delete_index(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        dry_run = args.dry_run,
        "delete-index"
    );
    Ok(())
}

#[tracing::instrument]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml)
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"));
    let matches = app.get_matches();

    let command = CliCommand::parse_cli_args(&matches)?;

    let command_res = match command {
        CliCommand::New(args) => create_index(args),
        CliCommand::Index(args) => index_data(args).await,
        CliCommand::Search(args) => search_index(args),
        CliCommand::Delete(args) => delete_index(args),
    };

    command_res
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
