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

use anyhow::{bail, Context};
use clap::ArgMatches;
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::{SourceConfig, SourceType};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_metastore::{quickwit_metastore_uri_resolver, IndexMetadata};
use quickwit_storage::load_file;
use serde_json::{Map, Value};
use tabled::{Table, Tabled};

use crate::{load_quickwit_config, make_table};

#[derive(Debug, PartialEq)]
pub struct AddSourceArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub source_id: String,
    pub source_type: String,
    /// Can be an inline JSON object or a path to a file holding a JSON object.
    pub params: String,
}

#[derive(Debug, PartialEq)]
pub struct DeleteSourceArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub source_id: String,
}

#[derive(Debug, PartialEq)]
pub struct DescribeSourceArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub source_id: String,
}

#[derive(Debug, PartialEq)]
pub struct ListSourcesArgs {
    pub config_uri: Uri,
    pub index_id: String,
}

#[derive(Debug, PartialEq)]
pub enum SourceCliCommand {
    AddSource(AddSourceArgs),
    DeleteSource(DeleteSourceArgs),
    DescribeSource(DescribeSourceArgs),
    ListSources(ListSourcesArgs),
}

impl SourceCliCommand {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::AddSource(args) => add_source_cli(args).await,
            Self::DeleteSource(args) => delete_source_cli(args).await,
            Self::DescribeSource(args) => describe_source_cli(args).await,
            Self::ListSources(args) => list_sources_cli(args).await,
        }
    }

    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse source subcommand arguments."))?;
        match subcommand {
            "add" => Self::parse_add_args(submatches).map(Self::AddSource),
            "delete" => Self::parse_delete_args(submatches).map(Self::DeleteSource),
            "describe" => Self::parse_describe_args(submatches).map(Self::DescribeSource),
            "list" => Self::parse_list_args(submatches).map(Self::ListSources),
            _ => bail!("Source subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_add_args(matches: &ArgMatches) -> anyhow::Result<AddSourceArgs> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let source_id = matches
            .value_of("source")
            .map(String::from)
            .expect("`source` is a required arg.");
        let source_type = matches
            .value_of("type")
            .map(String::from)
            .expect("`type` is a required arg.");
        let params = matches
            .value_of("params")
            .map(String::from)
            .expect("`params` is a required arg.");
        Ok(AddSourceArgs {
            config_uri,
            index_id,
            source_id,
            source_type,
            params,
        })
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<DeleteSourceArgs> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let source_id = matches
            .value_of("source")
            .map(String::from)
            .expect("`source` is a required arg.");
        Ok(DeleteSourceArgs {
            config_uri,
            index_id,
            source_id,
        })
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<DescribeSourceArgs> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let source_id = matches
            .value_of("source")
            .map(String::from)
            .expect("`source` is a required arg.");
        Ok(DescribeSourceArgs {
            config_uri,
            index_id,
            source_id,
        })
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<ListSourcesArgs> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        Ok(ListSourcesArgs {
            config_uri,
            index_id,
        })
    }
}

async fn add_source_cli(args: AddSourceArgs) -> anyhow::Result<()> {
    let config = load_quickwit_config(args.config_uri, None).await?;
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&config.metastore_uri)
        .await?;
    let mut params = sniff_params(&args.params).await?;
    params.insert("source_type".to_string(), Value::String(args.source_type));
    let source_type_value = Value::Object(params);
    let source_type: SourceType = serde_json::from_value(source_type_value)?;

    let source = SourceConfig {
        source_id: args.source_id.clone(),
        source_type,
    };
    check_source_connectivity(&source).await?;
    metastore.add_source(&args.index_id, source).await?;
    println!(
        "Source `{}` successfully created for index `{}`.",
        args.source_id, args.index_id
    );
    Ok(())
}

async fn delete_source_cli(args: DeleteSourceArgs) -> anyhow::Result<()> {
    let config = load_quickwit_config(args.config_uri, None).await?;
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&config.metastore_uri)
        .await?;
    metastore
        .delete_source(&args.index_id, &args.source_id)
        .await?;
    println!(
        "Source `{}` successfully deleted for index `{}`.",
        args.source_id, args.index_id
    );
    Ok(())
}

async fn describe_source_cli(args: DescribeSourceArgs) -> anyhow::Result<()> {
    let quickwit_config = load_quickwit_config(args.config_uri, None).await?;
    let index_metadata = resolve_index(&quickwit_config.metastore_uri, &args.index_id).await?;
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
        .with_context(|| format!("Source `{}` does not exist.", source_id))?;

    let source_rows = vec![SourceRow {
        source_id: source.source_id.clone(),
        source_type: source.source_type().to_string(),
    }];
    let source_table = make_table("Source", source_rows);

    let params_rows = flatten_json(source.params())
        .into_iter()
        .map(|(key, value)| ParamsRow { key, value })
        .sorted_by(|left, right| left.key.cmp(&right.key));
    let params_table = make_table("Parameters", params_rows);

    let checkpoint_rows = checkpoint
        .iter()
        .map(|(partition_id, position)| CheckpointRow {
            partition_id: partition_id.0.to_string(),
            offset: position.as_str().to_string(),
        })
        .sorted_by(|left, right| left.partition_id.cmp(&right.partition_id));
    let checkpoint_table = make_table("Checkpoint", checkpoint_rows);
    Ok((source_table, params_table, checkpoint_table))
}

async fn list_sources_cli(args: ListSourcesArgs) -> anyhow::Result<()> {
    let quickwit_config = load_quickwit_config(args.config_uri, None).await?;
    let index_metadata = resolve_index(&quickwit_config.metastore_uri, &args.index_id).await?;
    let table = make_list_sources_table(index_metadata.sources.into_values());
    display_tables(&[table]);
    Ok(())
}

fn make_list_sources_table<I>(sources: I) -> Table
where I: IntoIterator<Item = SourceConfig> {
    let rows = sources
        .into_iter()
        .map(|source| SourceRow {
            source_type: source.source_type().to_string(),
            source_id: source.source_id,
        })
        .sorted_by(|left, right| left.source_id.cmp(&right.source_id));
    make_table("Sources", rows)
}

#[derive(Tabled)]
struct SourceRow {
    #[header("ID")]
    source_id: String,
    #[header("Type")]
    source_type: String,
}

#[derive(Tabled)]
struct ParamsRow {
    #[header("Key")]
    key: String,
    #[header("Value")]
    value: Value,
}

#[derive(Tabled)]
struct CheckpointRow {
    #[header("Partition ID")]
    partition_id: String,
    #[header("Offset")]
    offset: String,
}

fn display_tables(tables: &[Table]) {
    println!(
        "{}",
        tables.iter().map(|table| table.to_string()).join("\n\n")
    );
}

/// Recursively flattens a JSON object into a vector of `(path, value)` tuples where `path`
/// represents the full path of each property in the original object. For instance, `{"root": true,
/// "parent": {"child": 0}}` yields `[("root", true), ("parent.child", 0)]`. Arrays are not
/// flattened.
fn flatten_json(value: Value) -> Vec<(String, Value)> {
    let mut acc = Vec::new();
    let mut values = vec![(String::new(), value)];

    while let Some((root, value)) = values.pop() {
        if let Value::Object(obj) = value {
            for (key, val) in obj {
                values.push((
                    if root.is_empty() {
                        key
                    } else {
                        format!("{}.{}", root, key)
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

/// Tries to read a JSON object from a string, assuming the string is an inline JSON object or a
/// path to a file holding a JSON object.
async fn sniff_params(params: &str) -> anyhow::Result<Map<String, Value>> {
    if let Ok(Value::Object(values)) = serde_json::from_str(params) {
        return Ok(values);
    }
    let params_uri = Uri::try_new(params)?;
    let params_bytes = load_file(&params_uri).await?;
    if let Ok(Value::Object(values)) = serde_json::from_slice(params_bytes.as_slice()) {
        return Ok(values);
    }
    bail!("Failed to parse JSON object from `{}`.", params)
}

async fn resolve_index(metastore_uri: &str, index_id: &str) -> anyhow::Result<IndexMetadata> {
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    let index_metadata = metastore.index_metadata(index_id).await?;
    Ok(index_metadata)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::{load_yaml, App, AppSettings};
    use quickwit_metastore::checkpoint::{PartitionId, Position};
    use quickwit_storage::{quickwit_storage_uri_resolver, PutPayload};
    use serde_json::json;

    use super::*;
    use crate::cli::CliCommand;

    #[test]
    fn test_flatten_json() {
        assert!(flatten_json(json!({})).is_empty());

        assert_eq!(
            flatten_json(json!(Value::Null)),
            vec![("".to_string(), Value::Null)]
        );
        assert_eq!(
            flatten_json(json!({"foo": {"bar": Value::Bool(true)}, "baz": Value::Bool(false)})),
            vec![
                ("foo.bar".to_string(), Value::Bool(true)),
                ("baz".to_string(), Value::Bool(false)),
            ]
        );
    }

    #[tokio::test]
    async fn test_sniff_params() {
        sniff_params("").await.unwrap_err();
        sniff_params("foo").await.unwrap_err();
        sniff_params("null").await.unwrap_err();
        sniff_params("0").await.unwrap_err();
        sniff_params("[]").await.unwrap_err();

        assert!(sniff_params(r#"{"foo": 0}"#)
            .await
            .unwrap()
            .contains_key("foo"));

        let storage = quickwit_storage_uri_resolver()
            .resolve("ram:///tmp")
            .unwrap();
        let payload: Box<dyn PutPayload> = Box::new(r#"{"bar": 1}"#.to_string().into_bytes());
        storage
            .put(Path::new("params.json"), payload)
            .await
            .unwrap();

        assert!(
            sniff_params("ram:///tmp/params.json").await.unwrap().contains_key("bar")
        );
    }

    #[test]
    fn test_parse_add_source_args() {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "add",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
                "--type",
                "kafka",
                "--params",
                "{}",
                "--config",
                "/conf.yaml",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_command = CliCommand::Source(SourceCliCommand::AddSource(AddSourceArgs {
            config_uri: Uri::try_new("file:///conf.yaml").unwrap(),
            index_id: "hdfs-logs".to_string(),
            source_id: "hdfs-logs-source".to_string(),
            source_type: "kafka".to_string(),
            params: "{}".to_string(),
        }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_parse_delete_source_args() {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "delete",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
                "--config",
                "/conf.yaml",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::DeleteSource(DeleteSourceArgs {
                config_uri: Uri::try_new("file:///conf.yaml").unwrap(),
                index_id: "hdfs-logs".to_string(),
                source_id: "hdfs-logs-source".to_string(),
            }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_parse_describe_source_args() {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "describe",
                "--index",
                "hdfs-logs",
                "--source",
                "hdfs-logs-source",
                "--config",
                "/conf.yaml",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_command =
            CliCommand::Source(SourceCliCommand::DescribeSource(DescribeSourceArgs {
                config_uri: Uri::try_new("file:///conf.yaml").unwrap(),
                index_id: "hdfs-logs".to_string(),
                source_id: "hdfs-logs-source".to_string(),
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

        let checkpoint: SourceCheckpoint = vec![("shard-000", ""), ("shard-001", "42")]
            .into_iter()
            .map(|(partition_id, offset)| (PartitionId::from(partition_id), Position::from(offset)))
            .collect();
        let sources = vec![SourceConfig {
            source_id: "foo-source".to_string(),
            source_type: SourceType::file("path/to/file"),
        }];
        let expected_source = vec![SourceRow {
            source_id: "foo-source".to_string(),
            source_type: "file".to_string(),
        }];
        let expected_params = vec![ParamsRow {
            key: "filepath".to_string(),
            value: Value::String("path/to/file".to_string()),
        }];
        let expected_checkpoint = vec![
            CheckpointRow {
                partition_id: "shard-000".to_string(),
                offset: "".to_string(),
            },
            CheckpointRow {
                partition_id: "shard-001".to_string(),
                offset: "42".to_string(),
            },
        ];
        let (source_table, params_table, checkpoint_table) =
            make_describe_source_tables(checkpoint, sources, "foo-source").unwrap();
        assert_eq!(
            source_table.to_string(),
            make_table("Source", expected_source).to_string()
        );
        assert_eq!(
            params_table.to_string(),
            make_table("Parameters", expected_params).to_string()
        );
        assert_eq!(
            checkpoint_table.to_string(),
            make_table("Checkpoint", expected_checkpoint).to_string()
        );
    }

    #[test]
    fn test_parse_list_sources_args() {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app
            .try_get_matches_from(vec![
                "source",
                "list",
                "--index",
                "hdfs-logs",
                "--config",
                "/conf.yaml",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_command = CliCommand::Source(SourceCliCommand::ListSources(ListSourcesArgs {
            config_uri: Uri::try_new("file:///conf.yaml").unwrap(),
            index_id: "hdfs-logs".to_string(),
        }));
        assert_eq!(command, expected_command);
    }

    #[test]
    fn test_make_list_sources_table() {
        let sources = [
            SourceConfig {
                source_id: "foo-source".to_string(),
                source_type: SourceType::stdin(),
            },
            SourceConfig {
                source_id: "bar-source".to_string(),
                source_type: SourceType::stdin(),
            },
        ];
        let expected_sources = [
            SourceRow {
                source_id: "bar-source".to_string(),
                source_type: "file".to_string(),
            },
            SourceRow {
                source_id: "foo-source".to_string(),
                source_type: "file".to_string(),
            },
        ];
        assert_eq!(
            make_list_sources_table(sources).to_string(),
            make_table("Sources", expected_sources).to_string()
        );
    }
}
