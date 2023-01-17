use structopt::StructOpt;
use xtask::{download_artifacts, generate_api};

/// Elasticsearch compatible API generator helper script.
#[derive(StructOpt, Debug)]
enum CommandOption {
    DownloadApiSpecs,
    GenerateApi,
}

fn main() -> anyhow::Result<()> {
    let opt = CommandOption::from_args();
    match opt {
        CommandOption::DownloadApiSpecs => download_artifacts(),
        CommandOption::GenerateApi => generate_api(),
    }
}
