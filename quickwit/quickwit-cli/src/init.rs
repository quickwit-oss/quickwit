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
