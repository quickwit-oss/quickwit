// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, bail};
use clap::{ArgMatches, Command};
use colored::Colorize;
use tracing::debug;

use crate::checklist::{GREEN_COLOR, RED_COLOR};
use crate::{ClientArgs, client_args};

pub fn build_maintenance_command() -> Command {
    Command::new("maintenance")
        .about("Manages cluster maintenance mode for safe rolling upgrades.")
        .args(client_args())
        .subcommand(Command::new("enable").about(
            "Enables maintenance mode. Freezes the indexing plan; metadata mutations are accepted \
             but the plan is not rebuilt.",
        ))
        .subcommand(
            Command::new("disable")
                .about("Disables maintenance mode and triggers a full indexing plan rebuild."),
        )
        .subcommand(Command::new("status").about("Shows the current maintenance mode status."))
        .subcommand_required(true)
        .arg_required_else_help(true)
}

#[derive(Debug, PartialEq)]
pub struct EnableMaintenanceArgs {
    pub client_args: ClientArgs,
}

#[derive(Debug, PartialEq)]
pub struct DisableMaintenanceArgs {
    pub client_args: ClientArgs,
}

#[derive(Debug, PartialEq)]
pub struct MaintenanceStatusArgs {
    pub client_args: ClientArgs,
}

#[derive(Debug, PartialEq)]
pub enum MaintenanceCliCommand {
    Enable(EnableMaintenanceArgs),
    Disable(DisableMaintenanceArgs),
    Status(MaintenanceStatusArgs),
}

impl MaintenanceCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse maintenance subcommand")?;
        match subcommand.as_str() {
            "enable" => Self::parse_enable_args(submatches),
            "disable" => Self::parse_disable_args(submatches),
            "status" => Self::parse_status_args(submatches),
            _ => bail!("unknown maintenance subcommand `{subcommand}`"),
        }
    }

    fn parse_enable_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        Ok(Self::Enable(EnableMaintenanceArgs { client_args }))
    }

    fn parse_disable_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        Ok(Self::Disable(DisableMaintenanceArgs { client_args }))
    }

    fn parse_status_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        Ok(Self::Status(MaintenanceStatusArgs { client_args }))
    }

    pub fn default_log_level(&self) -> tracing::Level {
        tracing::Level::ERROR
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Enable(args) => enable_maintenance_cli(args).await,
            Self::Disable(args) => disable_maintenance_cli(args).await,
            Self::Status(args) => maintenance_status_cli(args).await,
        }
    }
}

async fn enable_maintenance_cli(args: EnableMaintenanceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "enable-maintenance");
    println!("❯ Enabling maintenance mode...");
    let qw_client = args.client_args.client();
    let response = qw_client.maintenance().enable().await?;
    println!(
        "{} Maintenance mode enabled. Indexing plan frozen.",
        "✔".color(GREEN_COLOR)
    );
    debug!(frozen_plan_json_len = response.frozen_plan_json.len());
    Ok(())
}

async fn disable_maintenance_cli(args: DisableMaintenanceArgs) -> anyhow::Result<()> {
    debug!(args=?args, "disable-maintenance");
    println!("❯ Disabling maintenance mode...");
    let qw_client = args.client_args.client();
    qw_client.maintenance().disable().await?;
    println!(
        "{} Maintenance mode disabled. Indexing plan rebuild triggered.",
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

async fn maintenance_status_cli(args: MaintenanceStatusArgs) -> anyhow::Result<()> {
    debug!(args=?args, "maintenance-status");
    let qw_client = args.client_args.client();
    let status = qw_client.maintenance().status().await?;
    if status.is_maintenance_mode {
        println!(
            "{} Maintenance mode is {}",
            "●".color(RED_COLOR),
            "ENABLED".color(RED_COLOR).bold()
        );
        if let Some(enabled_at) = status.enabled_at {
            println!("  Enabled at: {enabled_at}");
        }
    } else {
        println!(
            "{} Maintenance mode is {}",
            "●".color(GREEN_COLOR),
            "DISABLED".color(GREEN_COLOR).bold()
        );
    }
    Ok(())
}
