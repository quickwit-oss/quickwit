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

use anyhow::bail;
use clap::{AppSettings, ArgMatches, Command};
use tracing::Level;

use crate::index::{build_index_command, IndexCliCommand};
use crate::service::{build_service_command, ServiceCliCommand};
use crate::source::{build_source_command, SourceCliCommand};
use crate::split::{build_split_command, SplitCliCommand};

pub fn build_cli<'a>() -> Command<'a> {
    Command::new("Quickwit")
        .subcommand(build_source_command())
        .subcommand(build_service_command())
        .subcommand(build_split_command())
        .subcommand(build_index_command())
        .disable_help_subcommand(true)
        .arg_required_else_help(true)
}

#[derive(Debug, PartialEq)]
pub enum CliCommand {
    Index(IndexCliCommand),
    Service(ServiceCliCommand),
    Source(SourceCliCommand),
    Split(SplitCliCommand),
}

impl CliCommand {
    pub fn default_log_level(&self) -> Level {
        match self {
            CliCommand::Index(subcommand) => subcommand.default_log_level(),
            CliCommand::Service(_) => Level::INFO,
            CliCommand::Source(_) => Level::ERROR,
            CliCommand::Split(_) => Level::ERROR,
        }
    }

    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse command arguments."))?;
        match subcommand {
            "index" => IndexCliCommand::parse_cli_args(submatches).map(CliCommand::Index),
            "service" => ServiceCliCommand::parse_cli_args(submatches).map(CliCommand::Service),
            "source" => SourceCliCommand::parse_cli_args(submatches).map(CliCommand::Source),
            "split" => SplitCliCommand::parse_cli_args(submatches).map(CliCommand::Split),
            _ => bail!("Subcommand `{}` is not implemented.", subcommand),
        }
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            CliCommand::Index(subcommand) => subcommand.execute().await,
            CliCommand::Service(subcommand) => subcommand.execute().await,
            CliCommand::Source(subcommand) => subcommand.execute().await,
            CliCommand::Split(subcommand) => subcommand.execute().await,
        }
    }
}
