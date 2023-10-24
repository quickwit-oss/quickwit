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

use std::str::FromStr;

use anyhow::{bail, Context};
use bytes::Bytes;
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::{validate_identifier, ConfigFormat, SourceConfig};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_storage::{load_file, StorageResolver};
use serde_json::Value as JsonValue;
use tabled::{Table, Tabled};
use tracing::debug;

use crate::checklist::GREEN_COLOR;
use crate::{client_args, make_table, prompt_confirmation, ClientArgs};

pub fn build_source_command() -> Command {
    Command::new("source")
        .about("Manages sources: creates, updates, deletes sources...")
        .args(client_args())
        .subcommand(
            Command::new("create")
                .about("Adds a new source to an index.")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"source-config" <SOURCE_CONFIG> "Path to source config file. Please, refer to the documentation for more details.")
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("enable")
                .about("Enables a source for an index.")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("disable")
                .about("Disables a source for an index.")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("ingest-api")
                .about("Enables/disables the ingest API of an index.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--enable "Enables the ingest API.")
                        .display_order(2),
                    arg!(--disable "Disables the ingest API.")
                        .display_order(3)
                        .conflicts_with("enable"),
                ])
            )
        .subcommand(
            Command::new("delete")
                .about("Deletes a source from an index.")
                .alias("del")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("describe")
                .about("Describes a source.")
                .alias("desc")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("list")
                .about("Lists the sources of an index.")
                .alias("ls")
                .args(&[
                    arg!(--index <INDEX_ID> "ID of the target index")
                        .display_order(1)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("reset-checkpoint")
                .about("Resets a source checkpoint.")
                .alias("reset")
                .args(&[
                    arg!(--index <INDEX_ID> "Index ID")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "Source ID")
                        .display_order(2)
                        .required(true),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateSourceArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub source_config_uri: Uri,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ToggleSourceArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub source_id: String,
    pub enable: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DeleteSourceArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub source_id: String,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSourceArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub source_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListSourcesArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ResetCheckpointArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub source_id: String,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub enum SourceCliCommand {
    CreateSource(CreateSourceArgs),
    ToggleSource(ToggleSourceArgs),
    DeleteSource(DeleteSourceArgs),
    DescribeSource(DescribeSourceArgs),
    ListSources(ListSourcesArgs),
    ResetCheckpoint(ResetCheckpointArgs),
}

impl SourceCliCommand {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::CreateSource(args) => create_source_cli(args).await,
            Self::ToggleSource(args) => toggle_source_cli(args).await,
            Self::DeleteSource(args) => delete_source_cli(args).await,
            Self::DescribeSource(args) => describe_source_cli(args).await,
            Self::ListSources(args) => list_sources_cli(args).await,
            Self::ResetCheckpoint(args) => reset_checkpoint_cli(args).await,
        }
    }

    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse source subcommand")?;
        match subcommand.as_str() {
            "create" => Self::parse_create_args(submatches).map(Self::CreateSource),
            "enable" => {
                Self::parse_toggle_source_args(&subcommand, submatches).map(Self::ToggleSource)
            }
            "disable" => {
                Self::parse_toggle_source_args(&subcommand, submatches).map(Self::ToggleSource)
            }
            "delete" => Self::parse_delete_args(submatches).map(Self::DeleteSource),
            "describe" => Self::parse_describe_args(submatches).map(Self::DescribeSource),
            "list" => Self::parse_list_args(submatches).map(Self::ListSources),
            "reset-checkpoint" => {
                Self::parse_reset_checkpoint_args(submatches).map(Self::ResetCheckpoint)
            }
            _ => bail!("unknown source subcommand `{subcommand}`"),
        }
    }

    fn parse_create_args(mut matches: ArgMatches) -> anyhow::Result<CreateSourceArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let source_config_uri = matches
            .remove_one::<String>("source-config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`source-config` should be a required arg.")?;
        Ok(CreateSourceArgs {
            client_args,
            index_id,
            source_config_uri,
        })
    }

    fn parse_toggle_source_args(
        subcommand: &str,
        mut matches: ArgMatches,
    ) -> anyhow::Result<ToggleSourceArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("`source` should be a required arg.");
        let enable = matches!(subcommand, "enable");
        Ok(ToggleSourceArgs {
            client_args,
            index_id,
            source_id,
            enable,
        })
    }

    fn parse_delete_args(mut matches: ArgMatches) -> anyhow::Result<DeleteSourceArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("`source` should be a required arg.");
        let assume_yes = matches.get_flag("yes");
        Ok(DeleteSourceArgs {
            client_args,
            index_id,
            source_id,
            assume_yes,
        })
    }

    fn parse_describe_args(mut matches: ArgMatches) -> anyhow::Result<DescribeSourceArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("`source` should be a required arg.");
        Ok(DescribeSourceArgs {
            client_args,
            index_id,
            source_id,
        })
    }

    fn parse_list_args(mut matches: ArgMatches) -> anyhow::Result<ListSourcesArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        Ok(ListSourcesArgs {
            client_args,
            index_id,
        })
    }

    fn parse_reset_checkpoint_args(mut matches: ArgMatches) -> anyhow::Result<ResetCheckpointArgs> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("`source` should be a required arg.");
        let assume_yes = matches.get_flag("yes");
        Ok(ResetCheckpointArgs {
            client_args,
            index_id,
            source_id,
            assume_yes,
        })
    }
}

async fn create_source_cli(args: CreateSourceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "create-source");
    println!("❯ Creating source...");
    let storage_resolver = StorageResolver::unconfigured();
    let source_config_content = load_file(&storage_resolver, &args.source_config_uri).await?;
    let config_format = ConfigFormat::sniff_from_uri(&args.source_config_uri)?;
    let qw_client = args.client_args.client();
    qw_client
        .sources(&args.index_id)
        .create(Bytes::from(source_config_content.to_vec()), config_format)
        .await?;
    println!("{} Source successfully created.", "✔".color(GREEN_COLOR));
    Ok(())
}

async fn toggle_source_cli(args: ToggleSourceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "toggle-source");
    println!("❯ Toggling source...");
    let qw_client = args.client_args.client();
    qw_client
        .sources(&args.index_id)
        .toggle(&args.source_id, args.enable)
        .await
        .context("failed to update source")?;

    let toggled_state_name = if args.enable { "enabled" } else { "disabled" };
    println!(
        "{} Source successfully {}.",
        toggled_state_name,
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

async fn delete_source_cli(args: DeleteSourceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "delete-source");
    println!("❯ Deleting source...");
    validate_identifier("Source ID", &args.source_id)?;

    if !args.assume_yes {
        let prompt = "This operation will delete the source. Do you want to proceed?".to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }

    let qw_client = args.client_args.client();
    qw_client
        .sources(&args.index_id)
        .delete(&args.source_id)
        .await
        .context("failed to delete source")?;
    println!("{} Source successfully deleted.", "✔".color(GREEN_COLOR));
    Ok(())
}

async fn describe_source_cli(args: DescribeSourceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "describe-source");
    let qw_client = args.client_args.client();
    let index_metadata = qw_client
        .indexes()
        .get(&args.index_id)
        .await
        .context("failed to fetch index metadata")?;
    let source_checkpoint = index_metadata
        .checkpoint
        .source_checkpoint(&args.source_id)
        .cloned()
        .unwrap_or_default();
    let (source_table, params_table, checkpoint_table) = make_describe_source_tables(
        source_checkpoint,
        index_metadata.sources.into_values(),
        &args.source_id,
    )?;
    display_tables(&[source_table, params_table, checkpoint_table]);
    Ok(())
}

fn make_describe_source_tables<I>(
    checkpoint: SourceCheckpoint,
    sources: I,
    source_id: &str,
) -> anyhow::Result<(Table, Table, Table)>
where
    I: IntoIterator<Item = SourceConfig>,
{
    let source = sources
        .into_iter()
        .find(|source| source.source_id == source_id)
        .with_context(|| format!("source `{source_id}` does not exist"))?;

    let source_rows = vec![SourceRow {
        source_id: source.source_id.clone(),
        source_type: source.source_type().as_str().to_string(),
        enabled: source.enabled.to_string(),
    }];
    let source_table = make_table("Source", source_rows, true);

    let params_rows = flatten_json(source.params())
        .into_iter()
        .map(|(key, value)| ParamsRow { key, value })
        .sorted_by(|left, right| left.key.cmp(&right.key));
    let params_table = make_table("Parameters", params_rows, false);

    let checkpoint_rows = checkpoint
        .iter()
        .map(|(partition_id, position)| CheckpointRow {
            partition_id: partition_id.0.to_string(),
            offset: position.as_str().to_string(),
        })
        .sorted_by(|left, right| left.partition_id.cmp(&right.partition_id));
    let checkpoint_table = make_table("Checkpoint", checkpoint_rows, false);
    Ok((source_table, params_table, checkpoint_table))
}

async fn list_sources_cli(args: ListSourcesArgs) -> anyhow::Result<()> {
    let qw_client = args.client_args.client();
    let index_metadata = qw_client
        .indexes()
        .get(&args.index_id)
        .await
        .context("failed to fetch indexes metadatas")?;
    let table = make_list_sources_table(index_metadata.sources.into_values());
    display_tables(&[table]);
    Ok(())
}

fn make_list_sources_table<I>(sources: I) -> Table
where I: IntoIterator<Item = SourceConfig> {
    let rows = sources
        .into_iter()
        .map(|source| SourceRow {
            source_type: source.source_type().as_str().to_string(),
            source_id: source.source_id,
            enabled: source.enabled.to_string(),
        })
        .sorted_by(|left, right| left.source_id.cmp(&right.source_id));
    make_table("Sources", rows, false)
}

#[derive(Tabled)]
struct SourceRow {
    #[tabled(rename = "ID")]
    source_id: String,
    #[tabled(rename = "Type")]
    source_type: String,
    #[tabled(rename = "Enabled")]
    enabled: String,
}

#[derive(Tabled)]
struct ParamsRow {
    #[tabled(rename = "Key")]
    key: String,
    #[tabled(rename = "Value")]
    value: JsonValue,
}

#[derive(Tabled)]
struct CheckpointRow {
    #[tabled(rename = "Partition ID")]
    partition_id: String,
    #[tabled(rename = "Offset")]
    offset: String,
}

fn display_tables(tables: &[Table]) {
    println!(
        "{}",
        tables.iter().map(|table| table.to_string()).join("\n\n")
    );
}

async fn reset_checkpoint_cli(args: ResetCheckpointArgs) -> anyhow::Result<()> {
    debug!(args=?args, "reset-checkpoint-source");
    println!("❯ Resetting source checkpoint...");
    if !args.assume_yes {
        let prompt =
            "This operation will reset the source checkpoints. Do you want to proceed?".to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
    let qw_client = args.client_args.client();
    qw_client
        .sources(&args.index_id)
        .reset_checkpoint(&args.source_id)
        .await?;
    println!(
        "{} Checkpoint successfully deleted.",
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

/// Recursively flattens a JSON object into a vector of `(path, value)` tuples where `path`
/// represents the full path of each property in the original object. For instance, `{"root": true,
/// "parent": {"child": 0}}` yields `[("root", true), ("parent.child", 0)]`. Arrays are not
/// flattened.
fn flatten_json(value: JsonValue) -> Vec<(String, JsonValue)> {
    let mut acc = Vec::new();
    let mut values = vec![(String::new(), value)];

    while let Some((root, value)) = values.pop() {
        if let JsonValue::Object(obj) = value {
            for (key, val) in obj {
                values.push((
                    if root.is_empty() {
                        key
                    } else {
                        format!("{root}.{key}")
                    },
                    val,
                ));
            }
            continue;
        }
        acc.push((root, value))
    }
    acc
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use quickwit_config::{SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::PartitionId;
    use quickwit_proto::types::Position;
    use serde_json::json;

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_flatten_json() {
        assert!(flatten_json(json!({})).is_empty());

        assert_eq!(
            flatten_json(json!(JsonValue::Null)),
            vec![("".to_string(), JsonValue::Null)]
        );
        assert_eq!(
            flatten_json(
                json!({"foo": {"bar": JsonValue::Bool(true)}, "baz": JsonValue::Bool(false)})
            ),
            vec![
                ("foo.bar".to_string(), JsonValue::Bool(true)),
                ("baz".to_string(), JsonValue::Bool(false)),
            ]
        );
    }

    #[test]
    fn test_parse_create_source_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "create",
                "--index",
                "hdfs-logs",
                "--source-config",
                "/source-conf.yaml",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::CreateSource(CreateSourceArgs {
                client_args: ClientArgs::default(),
                index_id: "hdfs-logs".to_string(),
                source_config_uri: Uri::from_str("file:///source-conf.yaml").unwrap(),
            }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_parse_toggle_source_args() {
        {
            let app = build_cli().no_binary_name(true);
            let matches = app
                .try_get_matches_from(vec![
                    "source",
                    "enable",
                    "--index",
                    "hdfs-logs",
                    "--source",
                    "kafka-foo",
                ])
                .unwrap();
            let command = CliCommand::parse_cli_args(matches).unwrap();
            let expected_command =
                CliCommand::Source(SourceCliCommand::ToggleSource(ToggleSourceArgs {
                    client_args: ClientArgs::default(),
                    index_id: "hdfs-logs".to_string(),
                    source_id: "kafka-foo".to_string(),
                    enable: true,
                }));
            assert_eq!(command, expected_command);
        }
        {
            let app = build_cli().no_binary_name(true);
            let matches = app
                .try_get_matches_from(vec![
                    "source",
                    "disable",
                    "--index",
                    "hdfs-logs",
                    "--source",
                    "kafka-foo",
                ])
                .unwrap();
            let command = CliCommand::parse_cli_args(matches).unwrap();
            let expected_command =
                CliCommand::Source(SourceCliCommand::ToggleSource(ToggleSourceArgs {
                    client_args: ClientArgs::default(),
                    index_id: "hdfs-logs".to_string(),
                    source_id: "kafka-foo".to_string(),
                    enable: false,
                }));
            assert_eq!(command, expected_command);
        }
    }

    #[test]
    fn test_parse_delete_source_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "delete",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
                "--yes",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::DeleteSource(DeleteSourceArgs {
                client_args: ClientArgs::default(),
                index_id: "hdfs-logs".to_string(),
                source_id: "hdfs-logs-source".to_string(),
                assume_yes: true,
            }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_parse_describe_source_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "describe",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::DescribeSource(DescribeSourceArgs {
                client_args: ClientArgs::default(),
                index_id: "hdfs-logs".to_string(),
                source_id: "hdfs-logs-source".to_string(),
            }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_parse_reset_checkpoint_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "reset-checkpoint",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
                "--yes",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::ResetCheckpoint(ResetCheckpointArgs {
                client_args: ClientArgs::default(),
                index_id: "hdfs-logs".to_string(),
                source_id: "hdfs-logs-source".to_string(),
                assume_yes: true,
            }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_make_describe_source_tables() {
        assert!(make_describe_source_tables(
            SourceCheckpoint::default(),
            [],
            "source-does-not-exist"
        )
        .is_err());

        let checkpoint: SourceCheckpoint = vec![("shard-000", ""), ("shard-001", "1234567890")]
            .into_iter()
            .map(|(partition_id, offset)| (PartitionId::from(partition_id), Position::from(offset)))
            .collect();
        let sources = vec![SourceConfig {
            source_id: "foo-source".to_string(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::file("path/to/file"),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }];
        let expected_source = vec![SourceRow {
            source_id: "foo-source".to_string(),
            source_type: "file".to_string(),
            enabled: "true".to_string(),
        }];
        let expected_params = vec![ParamsRow {
            key: "filepath".to_string(),
            value: JsonValue::String("path/to/file".to_string()),
        }];
        let expected_checkpoint = vec![
            CheckpointRow {
                partition_id: "shard-000".to_string(),
                offset: "".to_string(),
            },
            CheckpointRow {
                partition_id: "shard-001".to_string(),
                offset: "1234567890".to_string(),
            },
        ];
        let (source_table, params_table, checkpoint_table) =
            make_describe_source_tables(checkpoint, sources, "foo-source").unwrap();
        assert_eq!(
            source_table.to_string(),
            make_table("Source", expected_source, true).to_string()
        );
        assert_eq!(
            params_table.to_string(),
            make_table("Parameters", expected_params, false).to_string()
        );
        assert_eq!(
            checkpoint_table.to_string(),
            make_table("Checkpoint", expected_checkpoint, false).to_string()
        );
    }

    #[test]
    fn test_parse_list_sources_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(vec!["source", "list", "--index", "hdfs-logs"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_command = CliCommand::Source(SourceCliCommand::ListSources(ListSourcesArgs {
            client_args: ClientArgs::default(),
            index_id: "hdfs-logs".to_string(),
        }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_make_list_sources_table() {
        let sources = [
            SourceConfig {
                source_id: "foo-source".to_string(),
                desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                enabled: true,
                source_params: SourceParams::stdin(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
            SourceConfig {
                source_id: "bar-source".to_string(),
                desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                enabled: true,
                source_params: SourceParams::stdin(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
        ];
        let expected_sources = [
            SourceRow {
                source_id: "bar-source".to_string(),
                source_type: "file".to_string(),
                enabled: "true".to_string(),
            },
            SourceRow {
                source_id: "foo-source".to_string(),
                source_type: "file".to_string(),
                enabled: "true".to_string(),
            },
        ];
        assert_eq!(
            make_list_sources_table(sources).to_string(),
            make_table("Sources", expected_sources, false).to_string()
        );
    }
}
