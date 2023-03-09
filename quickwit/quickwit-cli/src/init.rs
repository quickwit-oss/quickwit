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

use anyhow::bail;
use clap::{Arg, ArgMatches, Command};
use quickwit_init::InitOption;

pub fn build_init_command<'a>() -> Command<'a> {
    Command::new("init")
        .about(
            "The one stop shop for setting up Quickwit configs, projects and more with a helpful \
             walkthrough prompt.",
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .required(false)
                .takes_value(false)
                .help(
                    "Hide the additional help text for each parameter to reduce noise in the \
                     terminal.",
                ),
        )
        .subcommand(
            Command::new("index")
                .display_order(1)
                .about("Create a new Quickwit index config using the prompt-base walkthrough."),
        )
        .subcommand(
            Command::new("source")
                .display_order(2)
                .about("Create a new Quickwit source config using the prompt-base walkthrough."),
        )
}

#[derive(Debug, Eq, PartialEq)]
pub struct InitCliCommand {
    pub option: InitOption,
    pub quiet: bool,
}

impl InitCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let quiet = matches.is_present("quiet");
        let subcommand = matches.subcommand();
        let option = match subcommand.map(|v| v.0) {
            Some("index") => InitOption::Index,
            Some("source") => InitOption::Source,
            None => InitOption::Quickwit,
            Some(other) => bail!("Init subcommand `{other}` is not implemented."),
        };

        Ok(Self { option, quiet })
    }

    pub fn execute(self) -> anyhow::Result<()> {
        quickwit_init::init(self.option, self.quiet)
    }
}
