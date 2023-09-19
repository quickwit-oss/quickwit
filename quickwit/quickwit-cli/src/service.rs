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

use std::collections::HashSet;
use std::str::FromStr;

use clap::{arg, ArgAction, ArgMatches, Command};
use itertools::Itertools;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::NodeConfig;
use quickwit_serve::serve_quickwit;
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
use tokio::signal;
use tracing::debug;

use crate::{config_cli_arg, get_resolvers, load_node_config, start_actor_runtimes};

pub fn build_run_command() -> Command {
    Command::new("run")
        .about("Starts a Quickwit node.")
        .long_about("Starts a Quickwit node with all services enabled by default: `indexer`, `searcher`, `metastore`, `control-plane`, and `janitor`.")
        .arg(config_cli_arg())
        .args(&[
            arg!(--"service" <SERVICE> "Services (`indexer`, `searcher`, `metastore`, `control-plane`, or `janitor`) to run. If unspecified, all the supported services are started.")
                .action(ArgAction::Append)
                .required(false),
        ])
}

#[derive(Debug, Eq, PartialEq)]
pub struct RunCliCommand {
    pub config_uri: Uri,
    pub services: Option<HashSet<QuickwitService>>,
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

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let mut node_config = load_node_config(&self.config_uri).await?;
        let (storage_resolver, metastore_resolver) =
            get_resolvers(&node_config.storage_configs, &node_config.metastore_configs);
        crate::busy_detector::set_enabled(true);

        if let Some(services) = &self.services {
            tracing::info!(services = %services.iter().join(", "), "Setting services from override.");
            node_config.enabled_services = services.clone();
        }
        let telemetry_handle_opt =
            quickwit_telemetry::start_telemetry_loop(quickwit_telemetry_info(&node_config));
        quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;
        // TODO move in serve quickwit?
        let runtimes_config = RuntimesConfig::default();
        start_actor_runtimes(runtimes_config, &node_config.enabled_services)?;
        let shutdown_signal = Box::pin(async move {
            signal::ctrl_c()
                .await
                .expect("Registering a signal handler for SIGINT should not fail.");
        });
        let serve_result = serve_quickwit(
            node_config,
            runtimes_config,
            metastore_resolver,
            storage_resolver,
            shutdown_signal,
        )
        .await;
        let return_code = match serve_result {
            Ok(_) => 0,
            Err(_) => 1,
        };
        quickwit_telemetry::send_telemetry_event(TelemetryEvent::EndCommand { return_code }).await;
        if let Some(telemetry_handle) = telemetry_handle_opt {
            telemetry_handle.terminate_telemetry().await;
        }
        serve_result?;
        Ok(())
    }
}

fn quickwit_telemetry_info(config: &NodeConfig) -> QuickwitTelemetryInfo {
    let mut features = HashSet::new();
    if config.indexer_config.enable_otlp_endpoint {
        features.insert(QuickwitFeature::Otlp);
    }
    if config.jaeger_config.enable_endpoint {
        features.insert(QuickwitFeature::Jaeger);
    }
    // The metastore URI is only relevant if the metastore is enabled.
    if config
        .enabled_services
        .contains(&QuickwitService::Metastore)
    {
        if config.metastore_uri.protocol().is_postgresql() {
            features.insert(QuickwitFeature::PostgresqMetastore);
        } else {
            features.insert(QuickwitFeature::FileBackedMetastore);
        }
    }
    let services = config
        .enabled_services
        .iter()
        .map(|service| service.to_string())
        .collect();
    QuickwitTelemetryInfo::new(services, features)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{build_cli, CliCommand};

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
