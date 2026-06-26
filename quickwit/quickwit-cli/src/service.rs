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

use std::collections::HashSet;
use std::pin::pin;
use std::str::FromStr;

use clap::{ArgAction, ArgMatches, Command, arg};
use colored::Colorize;
use futures::future::select;
use itertools::Itertools;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_serve::tcp_listener::DefaultTcpListenerResolver;
use quickwit_serve::{BuildInfo, EnvFilterReloadFn, reload_tls_cert, serve_quickwit};
use tokio::signal;
use tracing::{debug, info};

use crate::checklist::{BLUE_COLOR, RED_COLOR};
use crate::{config_cli_arg, get_resolvers, load_node_config, start_actor_runtimes};

pub fn build_run_command() -> Command {
    Command::new("run")
        .about("Starts a Quickwit node.")
        .long_about("Starts a Quickwit node with the default services enabled: `indexer`, `searcher`, `metastore`, `control-plane`, and `janitor`.")
        .arg(config_cli_arg())
        .args(&[
            arg!(--"service" <SERVICE> "Services (`indexer`, `searcher`, `metastore`, `metastore-read-replica`, `control-plane`, or `janitor`) to run. If unspecified, services from the config are used.")
                .action(ArgAction::Append)
                .required(false),
        ])
}

#[derive(Debug, Eq, PartialEq)]
pub struct RunCliCommand {
    pub config_uri: Uri,
    pub services: Option<HashSet<QuickwitService>>,
}

async fn listen_interrupt() {
    async fn ctrl_c() {
        signal::ctrl_c()
            .await
            .expect("registering a signal handler for SIGINT should not fail");
        // carriage return to hide the ^C echo from the terminal
        print!("\r");
    }
    ctrl_c().await;
    println!(
        "{} Graceful shutdown initiated. Waiting for ingested data to be indexed. This may take a \
         few minutes. Press Ctrl+C again to force shutdown.",
        "❢".color(BLUE_COLOR)
    );
    tokio::spawn(async {
        ctrl_c().await;
        println!(
            "{} Quickwit was forcefully shut down. Some data might not have been indexed.",
            "✘".color(RED_COLOR)
        );
        std::process::exit(1);
    });
}

async fn listen_sigterm() {
    signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("registering a signal handler for SIGTERM should not fail")
        .recv()
        .await;
    info!("SIGTERM received");
}

async fn listen_sighup() {
    let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())
        .expect("registering a signal handler for SIGHUP should not fail");

    while sighup.recv().await.is_some() {
        info!("SIGHUP received");
        reload_tls_cert();
    }
}

impl RunCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let services = matches
            .remove_many::<String>("service")
            .map(|values| {
                let services: Result<HashSet<_>, _> = values
                    .into_iter()
                    .map(|service_str| QuickwitService::from_str(&service_str))
                    .collect();
                services
            })
            .transpose()?;
        Ok(RunCliCommand {
            config_uri,
            services,
        })
    }

    pub async fn execute(&self, env_filter_reload_fn: EnvFilterReloadFn) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let version_text = BuildInfo::get_version_text();
        info!("quickwit version: {version_text}");
        let mut node_config = load_node_config(&self.config_uri).await?;
        let (storage_resolver, metastore_resolver) =
            get_resolvers(&node_config.storage_configs, &node_config.metastore_configs);
        crate::busy_detector::set_enabled(true);

        if let Some(services) = &self.services {
            info!(services = %services.iter().join(", "), "setting services from override");
            node_config.enabled_services.clone_from(services);
        }
        // TODO move in serve quickwit?
        let runtimes_config = RuntimesConfig::default();
        start_actor_runtimes(runtimes_config, &node_config.enabled_services)?;
        let shutdown_signal = Box::pin(async {
            select(pin!(listen_interrupt()), pin!(listen_sigterm())).await;
        });
        // Reload TLS certificates on SIGHUP for the lifetime of the process.
        tokio::spawn(listen_sighup());
        serve_quickwit(
            node_config,
            runtimes_config,
            metastore_resolver,
            storage_resolver,
            DefaultTcpListenerResolver,
            shutdown_signal,
            env_filter_reload_fn,
        )
        .await?;
        info!("quickwit successfully terminated");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{CliCommand, build_cli};

    #[test]
    fn test_parse_service_run_args_all_services() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec!["run", "--config", "/config.yaml"])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.is_none()
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_args_indexer_only() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--config",
            "/config.yaml",
            "--service",
            "indexer",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 1 && services.as_ref().unwrap().iter().cloned().next().unwrap() == QuickwitService::Indexer
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_args_searcher_and_metastore() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--config",
            "/config.yaml",
            "--service",
            "searcher",
            "--service",
            "metastore",
        ])?;
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        let expected_services =
            HashSet::from_iter([QuickwitService::Metastore, QuickwitService::Searcher]);
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 2 && services.as_ref().unwrap() == &expected_services
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_indexer_only_args() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--config",
            "/config.yaml",
            "--service",
            "indexer",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 1 && services.as_ref().unwrap().contains(&QuickwitService::Indexer)
        ));
        Ok(())
    }
}
