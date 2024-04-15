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
        .display_order(2)
        .about("Updates an index configuration.")
        .subcommand(
            Command::new("search-settings")
                .about("Updates an default search settings.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"default-search-fields" <FIELD_NAME> "Comma separated list of fields that will be searched by default.")
                        .display_order(2)
                        .required(false),
                ])
        .subcommand(
            Command::new("retention-policy")
                .about("Set or unset a retention policy.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"retention-policy-period" <RETENTION_PERIOD> "Duration after which splits are dropped.")
                        .display_order(2)
                        .required(false),
                    arg!(--"retention-policy-schedule" <RETENTION_SCHEDULE> "Frequency at which the retention policy is evaluated and applied.")
                        .display_order(3)
                        .required(false),
                    arg!(--"disable-retention-policy" "Disables the retention policy.")
                        .display_order(4)
                        .required(false),
                ])
        )
    )
}

// Structured representation of the retention policy args
#[derive(Debug, Eq, PartialEq)]
pub enum RetentionPolicyUpdate {
    Noop,
    Update {
        period: Option<String>,
        schedule: Option<String>,
    },
    Disable,
}

impl RetentionPolicyUpdate {
    pub fn apply_update(
        self,
        retention_policy_opt: Option<RetentionPolicy>,
    ) -> anyhow::Result<Option<RetentionPolicy>> {
        match (self, retention_policy_opt) {
            (Self::Noop, policy_opt) => Ok(policy_opt),
            (Self::Update { period: None, .. }, None) => {
                bail!("`--retention-policy-period` is required when creating a retention policy");
            }
            (
                Self::Update {
                    period: None,
                    schedule,
                },
                Some(mut policy),
            ) => {
                policy.evaluation_schedule = schedule.unwrap_or(policy.evaluation_schedule.clone());
                Ok(Some(policy))
            }
            (
                Self::Update {
                    period: Some(period),
                    schedule,
                },
                None,
            ) => Ok(Some(RetentionPolicy {
                retention_period: period,
                evaluation_schedule: schedule.unwrap_or(RetentionPolicy::default_schedule()),
            })),
            (
                Self::Update {
                    period: Some(period),
                    schedule,
                },
                Some(policy),
            ) => Ok(Some(RetentionPolicy {
                retention_period: period,
                evaluation_schedule: schedule.unwrap_or(policy.evaluation_schedule.clone()),
            })),
            (Self::Disable, _) => Ok(None),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RetentionPolicyArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub retention_policy_update: RetentionPolicyUpdate,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchSettingsArgs {
    pub client_args: ClientArgs,
    pub index_id: String,
    pub default_search_fields: Option<Vec<String>>,
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
            .context("failed to parse index subcommand")?;
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
        let disable_retention_policy = matches.get_flag("disable-retention-policy");
        let retention_policy_period = matches.remove_one::<String>("retention-policy-period");
        let retention_policy_schedule = matches.remove_one::<String>("retention-policy-schedule");

        let retention_policy_update = match (
            disable_retention_policy,
            retention_policy_period,
            retention_policy_schedule,
        ) {
            (true, Some(_), Some(_)) | (true, None, Some(_)) | (true, Some(_), None) => bail!(
                "`--retention-policy-period` and `--retention-policy-schedule` cannot be used \
                 together with `--disable-retention-policy`"
            ),
            (true, None, None) => RetentionPolicyUpdate::Disable,
            (false, None, None) => RetentionPolicyUpdate::Noop,
            (false, period, schedule) => RetentionPolicyUpdate::Update { period, schedule },
        };

        Ok(Self::RetentionPolicy(RetentionPolicyArgs {
            client_args,
            index_id,
            retention_policy_update,
        }))
    }

    fn parse_update_search_settings_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let default_search_fields = matches
            .remove_many::<String>("default-search-fields")
            .map(|values| values.collect());
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
    let new_retention_policy_opt = args
        .retention_policy_update
        .apply_update(metadata.index_config.retention_policy_opt)?;
    if let Some(new_retention_policy) = new_retention_policy_opt.as_ref() {
        println!(
            "New retention policy: {}",
            serde_json::to_string(&new_retention_policy)?
        );
    } else {
        println!("Retention policy disabled.");
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
    Ok(())
}

pub async fn update_search_settings_cli(args: SearchSettingsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index-search-settings");
    println!("❯ Updating index search settings...");
    let qw_client = args.client_args.client();
    let metadata = qw_client.indexes().get(&args.index_id).await?;
    let search_settings = SearchSettings {
        default_search_fields: args.default_search_fields.unwrap_or(vec![]),
        ..metadata.index_config.search_settings
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
