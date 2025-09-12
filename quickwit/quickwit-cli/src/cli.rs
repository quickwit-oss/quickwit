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
use clap::{Arg, ArgAction, ArgMatches, Command, arg};
use quickwit_serve::EnvFilterReloadFn;
use tracing::Level;

use crate::index::{IndexCliCommand, build_index_command};
use crate::service::{RunCliCommand, build_run_command};
use crate::source::{SourceCliCommand, build_source_command};
use crate::split::{SplitCliCommand, build_split_command};
use crate::tool::{ToolCliCommand, build_tool_command};

pub fn build_cli() -> Command {
    Command::new("Quickwit")
        .arg(
            // Following https://no-color.org/
            Arg::new("no-color")
                .long("no-color")
                .help(
                    "Disable ANSI terminal codes (colors, etc...) being injected into the logging \
                     output",
                )
                .env("NO_COLOR")
                .value_parser(clap::builder::FalseyValueParser::new())
                .global(true)
                .action(ArgAction::SetTrue),
        )
        .arg(arg!(-y --"yes" "Assume \"yes\" as an answer to all prompts and run non-interactively.")
            .global(true)
            .required(false)
        )
        .subcommand(build_run_command().display_order(1))
        .subcommand(build_index_command().display_order(2))
        .subcommand(build_source_command().display_order(3))
        .subcommand(build_split_command().display_order(4))
        .subcommand(build_tool_command().display_order(5))
        .arg_required_else_help(true)
        .disable_help_subcommand(true)
        .subcommand_required(true)
}

#[derive(Debug, PartialEq)]
pub enum CliCommand {
    Run(RunCliCommand),
    Index(IndexCliCommand),
    Split(SplitCliCommand),
    Source(SourceCliCommand),
    Tool(ToolCliCommand),
}

impl CliCommand {
    pub fn default_log_level(&self) -> Level {
        match self {
            CliCommand::Run(_) => Level::INFO,
            CliCommand::Index(subcommand) => subcommand.default_log_level(),
            CliCommand::Source(_) => Level::ERROR,
            CliCommand::Split(_) => Level::ERROR,
            CliCommand::Tool(_) => Level::ERROR,
        }
    }

    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse command")?;
        match subcommand.as_str() {
            "index" => IndexCliCommand::parse_cli_args(submatches).map(CliCommand::Index),
            "run" => RunCliCommand::parse_cli_args(submatches).map(CliCommand::Run),
            "source" => SourceCliCommand::parse_cli_args(submatches).map(CliCommand::Source),
            "split" => SplitCliCommand::parse_cli_args(submatches).map(CliCommand::Split),
            "tool" => ToolCliCommand::parse_cli_args(submatches).map(CliCommand::Tool),
            _ => bail!("unknown command `{subcommand}`"),
        }
    }

    pub async fn execute(self, env_filter_reload_fn: EnvFilterReloadFn) -> anyhow::Result<()> {
        match self {
            CliCommand::Index(subcommand) => subcommand.execute().await,
            CliCommand::Run(subcommand) => subcommand.execute(env_filter_reload_fn).await,
            CliCommand::Source(subcommand) => subcommand.execute().await,
            CliCommand::Split(subcommand) => subcommand.execute().await,
            CliCommand::Tool(subcommand) => subcommand.execute().await,
        }
    }
}
