// Copyright (C) 2024 Quickwit, Inc.
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
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use quickwit_common::uri::Uri;
use quickwit_config::ConfigFormat;
use quickwit_proto::bytes::Bytes;
use quickwit_rest_client::rest_client::UpdateConfigField;
use quickwit_storage::{load_file, StorageResolver};
use tracing::debug;

use crate::checklist::{BLUE_COLOR, GREEN_COLOR};
use crate::ClientArgs;

pub fn build_index_update_command() -> Command {
    Command::new("update")
        .args(&[
            arg!(--index <INDEX> "ID of the target index")
            .required(true),
        ])
        .subcommand_required(true)
        .subcommand(
            Command::new("search-settings")
                .about("Updates search settings.")
                .args(&[
                    arg!(--"config-file" <PATH> "Location of a json, yaml or toml file containing the new search settings. See https://quickwit.io/docs/configuration/index-config#search-settings.")
                        .required(true),
                ]))
        .subcommand(
            Command::new("retention-policy")
                .about("Updates or disables the retention policy.")
                .args(&[
                    arg!(--"config-file" <PATH> "Location of a json, yaml or toml file containing the new retention policy. See https://quickwit.io/docs/configuration/index-config#retention-policy.")
                        .required(false),
                    arg!(--disable "Disables the retention policy. Old indexed data will not be cleaned up anymore.")
                        .required(false),
                ])
        )
        .subcommand(
            Command::new("indexing-settings")
                .about("Updates indexing settings.")
                .args(&[
                    arg!(--"config-file" <PATH> "Location of a json, yaml or toml file containing the new indexing settings. See https://quickwit.io/docs/configuration/index-config#indexing-settings.")
                        .required(true),
                ])

        )
}

#[derive(Debug, Eq, PartialEq)]
pub struct OptionalFieldArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub config_file_opt: Option<Uri>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RequiredFieldArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub config_file: Uri,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IndexingSettingsArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub config_file: Uri,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IndexUpdateCliCommand {
    RetentionPolicy(OptionalFieldArgs),
    SearchSettings(RequiredFieldArgs),
    IndexingSettings(RequiredFieldArgs),
}

impl IndexUpdateCliCommand {
    pub fn parse_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse index update subcommand")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        match subcommand.as_str() {
            "retention-policy" => Self::parse_update_optional_field_args(submatches, index_id),
            "search-settings" => Self::parse_update_required_field_args(submatches, index_id),
            "indexing-settings" => Self::parse_update_required_field_args(submatches, index_id),
            _ => bail!("unknown index update subcommand `{subcommand}`"),
        }
    }

    /// Parse args for optional fields (i.e retention policy).
    fn parse_update_optional_field_args(
        mut matches: ArgMatches,
        index_id: String,
    ) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;

        let config_file_opt = matches
            .remove_one::<String>("config-file")
            .map(|uri| Uri::from_str(&uri))
            .transpose()?;
        let disable = matches.get_flag("disable");
        if !disable && config_file_opt.is_none() {
            bail!("either `--config-file` or `--disable` must be specified");
        }
        if disable && config_file_opt.is_some() {
            bail!("both `--config-file` and `--disable` cannot be specified");
        }
        Ok(Self::RetentionPolicy(OptionalFieldArgs {
            client_args,
            index_id,
            config_file_opt,
        }))
    }

    /// Parse args for required fields (i.e. search and indexing settings).
    fn parse_update_required_field_args(
        mut matches: ArgMatches,
        index_id: String,
    ) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;

        let config_file = matches
            .remove_one::<String>("config-file")
            .map(|uri| Uri::from_str(&uri))
            .expect("`config-file` should be a required arg.")?;
        Ok(Self::SearchSettings(RequiredFieldArgs {
            client_args,
            index_id,
            config_file,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::RetentionPolicy(args) => update_retention_policy_cli(args).await,
            Self::SearchSettings(args) => update_search_settings_cli(args).await,
            Self::IndexingSettings(args) => update_indexing_settings_cli(args).await,
        }
    }
}

async fn update_from_file(
    client_args: ClientArgs,
    index_id: &str,
    config_file: &Uri,
    field: UpdateConfigField,
) -> anyhow::Result<()> {
    let storage_resolver = StorageResolver::unconfigured();
    let content = load_file(&storage_resolver, config_file).await?;
    client_args
        .client()
        .indexes()
        .update(
            index_id,
            field,
            Bytes::from(content.as_slice().to_owned()),
            ConfigFormat::sniff_from_uri(config_file)?,
        )
        .await?;
    Ok(())
}

pub async fn update_retention_policy_cli(args: OptionalFieldArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-retention-policy");
    println!("❯ Updating index retention policy...");
    if let Some(uri) = args.config_file_opt {
        update_from_file(
            args.client_args,
            &args.index_id,
            &uri,
            UpdateConfigField::RetentionPolicy,
        )
        .await?;
    } else {
        args.client_args
            .client()
            .indexes()
            .delete_retention_policy(&args.index_id)
            .await?;
    }
    println!(
        "{} Index retention policy successfully updated.",
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

pub async fn update_search_settings_cli(args: RequiredFieldArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-search-settings");
    println!("❯ Updating index search settings...");
    update_from_file(
        args.client_args,
        &args.index_id,
        &args.config_file,
        UpdateConfigField::SearchSettings,
    )
    .await?;
    println!(
        "{} Index search settings successfully updated.",
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

pub async fn update_indexing_settings_cli(args: RequiredFieldArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-indexing-settings");
    println!("❯ Updating index indexing settings...");
    update_from_file(
        args.client_args,
        &args.index_id,
        &args.config_file,
        UpdateConfigField::IndexingSettings,
    )
    .await?;
    println!(
        "{} Index indexing settings successfully updated.",
        "✔".color(GREEN_COLOR)
    );
    println!(
        "{} Restart indexer nodes for the new configuration to take effect.",
        "!".color(BLUE_COLOR)
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cli::{build_cli, CliCommand};
    use crate::index::IndexCliCommand;

    #[test]
    fn test_cmd_update_subsubcommand() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "index",
                "update",
                "--index",
                "my-index",
                "retention-policy",
                "--config-file",
                "/tmp/hello.json",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Update(
                IndexUpdateCliCommand::RetentionPolicy(OptionalFieldArgs {
                    client_args: _,
                    index_id,
                    config_file_opt: Some(uri),
                })
            )) if &index_id == "my-index" &&  uri.as_str() == "file:///tmp/hello.json"
        ));
    }

    #[test]
    fn test_cmd_invalid_update_subsubcommand() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "index",
                "update",
                "--index",
                "my-index",
                "retention-policy",
                "--config-file",
                "/tmp/hello.json",
                "--disable",
            ])
            .unwrap();
        CliCommand::parse_cli_args(matches)
            .expect_err("command with both `--config-file` and `--disable` should fail");
    }
}
