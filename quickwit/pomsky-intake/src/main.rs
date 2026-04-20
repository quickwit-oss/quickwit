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

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(name = "pomsky-intake", about = "Pomsky intake service (Vector-based)")]
struct Cli {
    /// Path to the YAML configuration file. Uses defaults if not provided.
    #[arg(long, short)]
    config: Option<PathBuf>,

    /// Print events to stdout for debugging. For metrics, events are printed
    /// before the Arrow IPC sink (i.e. after preprocess_metric).
    #[arg(long)]
    print: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = match &cli.config {
        Some(path) => {
            let config_content = std::fs::read_to_string(path).with_context(|| {
                format!(
                    "failed to read confile file located at `{}`",
                    path.display()
                )
            })?;
            let config: pomsky_intake::IntakeConfig =
                serde_yaml::from_str(&config_content).context("failed to parse config")?;
            info!(config_path=%path.display(), "loaded intake config");
            config
        }
        None => {
            info!("no config provided, using defaults");
            pomsky_intake::IntakeConfig::default()
        }
    };

    pomsky_intake::run_intake(config, cli.print)
}
