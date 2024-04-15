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

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use quickwit_config::{RetentionPolicy, SearchSettings};
use quickwit_serve::IndexUpdates;
use tracing::debug;

use crate::checklist::GREEN_COLOR;
use crate::ClientArgs;

pub fn build_index_update_command() -> Command {
    Command::new("update")
        .subcommand_required(true)
        .subcommand(
            Command::new("search-settings")
                .about("Updates default search settings.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"default-search-fields" <FIELD_NAME> "List of fields that Quickwit will search into if the user query does not explicitly target a field. Space-separated list, e.g. \"field1 field2\". If no value is provided, existing defaults are removed and queries without target field will fail.")
                        .display_order(2)
                        .num_args(0..)
                        .required(true),
                ]))
        .subcommand(
            Command::new("retention-policy")
                .about("Configures or disables the retention policy.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"period" <RETENTION_PERIOD> "Duration after which splits are dropped. Expressed in a human-readable way (`1 day`, `2 hours`, `1 week`, ...)")
                        .display_order(2)
                        .required(false),
                    arg!(--"schedule" <RETENTION_SCHEDULE> "Frequency at which the retention policy is evaluated and applied. Expressed as a cron expression (0 0 * * * *) or human-readable form (hourly, daily, weekly, ...).")
                        .display_order(3)
                        .required(false),
                    arg!(--"disable" "Disables the retention policy. Old indexed data will not be cleaned up anymore.")
                        .display_order(4)
                        .required(false),
                ])
        )
}

#[derive(Debug, Eq, PartialEq)]
pub struct RetentionPolicyArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub disable: bool,
    pub period: Option<String>,
    pub schedule: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchSettingsArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub default_search_fields: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IndexUpdateCliCommand {
    RetentionPolicy(RetentionPolicyArgs),
    SearchSettings(SearchSettingsArgs),
}

impl IndexUpdateCliCommand {
    pub fn parse_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse index update subcommand")?;
        match subcommand.as_str() {
            "retention-policy" => Self::parse_update_retention_policy_args(submatches),
            "search-settings" => Self::parse_update_search_settings_args(submatches),
            _ => bail!("unknown index update subcommand `{subcommand}`"),
        }
    }

    fn parse_update_retention_policy_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let disable = matches.get_flag("disable");
        let period = matches.remove_one::<String>("period");
        let schedule = matches.remove_one::<String>("schedule");
        Ok(Self::RetentionPolicy(RetentionPolicyArgs {
            client_args,
            index_id,
            disable,
            period,
            schedule,
        }))
    }

    fn parse_update_search_settings_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let default_search_fields = matches
            .remove_many::<String>("default-search-fields")
            .map(|values| values.collect())
            // --default-search-fields should be made optional if other fields
            // are added to SearchSettings
            .expect("`default-search-fields` should be a required arg.");
        Ok(Self::SearchSettings(SearchSettingsArgs {
            client_args,
            index_id,
            default_search_fields,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::RetentionPolicy(args) => update_retention_policy_cli(args).await,
            Self::SearchSettings(args) => update_search_settings_cli(args).await,
        }
    }
}

pub async fn update_retention_policy_cli(args: RetentionPolicyArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-retention-policy");
    println!("❯ Updating index retention policy...");
    let qw_client = args.client_args.client();
    let metadata = qw_client.indexes().get(&args.index_id).await?;
    let new_retention_policy_opt = match (
        args.disable,
        args.period,
        args.schedule,
        metadata.index_config.retention_policy_opt,
    ) {
        (true, Some(_), Some(_), _) | (true, None, Some(_), _) | (true, Some(_), None, _) => {
            bail!("`--period` and `--schedule` cannot be used together with `--disable`")
        }
        (false, None, None, _) => bail!("either `--period` or `--disable` must be specified"),
        (false, None, Some(_), None) => {
            bail!("`--period` is required when creating a retention policy")
        }
        (true, None, None, _) => None,
        (false, None, Some(schedule), Some(policy)) => Some(RetentionPolicy {
            retention_period: policy.retention_period,
            evaluation_schedule: schedule,
        }),
        (false, Some(period), schedule_opt, None) => Some(RetentionPolicy {
            retention_period: period,
            evaluation_schedule: schedule_opt.unwrap_or(RetentionPolicy::default_schedule()),
        }),
        (false, Some(period), schedule_opt, Some(policy)) => Some(RetentionPolicy {
            retention_period: period,
            evaluation_schedule: schedule_opt.unwrap_or(policy.evaluation_schedule.clone()),
        }),
    };
    if let Some(new_retention_policy) = new_retention_policy_opt.as_ref() {
        println!(
            "New retention policy: {}",
            serde_json::to_string(&new_retention_policy)?
        );
    } else {
        println!("Disable retention policy.");
    }
    qw_client
        .indexes()
        .update(
            &args.index_id,
            IndexUpdates {
                retention_policy_opt: new_retention_policy_opt,
                search_settings: metadata.index_config.search_settings,
            },
        )
        .await?;
    println!("{} Index successfully updated.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn update_search_settings_cli(args: SearchSettingsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-search-settings");
    println!("❯ Updating index search settings...");
    let qw_client = args.client_args.client();
    let metadata = qw_client.indexes().get(&args.index_id).await?;
    let search_settings = SearchSettings {
        default_search_fields: args.default_search_fields,
    };
    println!(
        "New search settings: {}",
        serde_json::to_string(&search_settings)?
    );
    qw_client
        .indexes()
        .update(
            &args.index_id,
            IndexUpdates {
                retention_policy_opt: metadata.index_config.retention_policy_opt,
                search_settings,
            },
        )
        .await?;
    println!("{} Index successfully updated.", "✔".color(GREEN_COLOR));
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
                "retention-policy",
                "--index",
                "my-index",
                "--period",
                "1 day",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Update(
                IndexUpdateCliCommand::RetentionPolicy(RetentionPolicyArgs {
                    client_args: _,
                    index_id,
                    disable: false,
                    period: Some(period),
                    schedule: None,
                })
            )) if &index_id == "my-index" &&  &period == "1 day"
        ));
    }
}
