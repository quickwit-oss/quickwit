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
use quickwit_config::node_role::NodeRole;
use quickwit_config::NodeConfig;
use quickwit_serve::serve_quickwit;
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
use tokio::signal;
use tracing::debug;

use crate::{config_cli_arg, get_resolvers, load_node_config, start_actor_runtimes};

pub fn build_run_command() -> Command {
    Command::new("run")
        .about("Starts a Quickwit node.")
        .long_about("Starts a node with assigned roles.")
        .arg(config_cli_arg())
        .args(&[
            arg!(--"role" <ROLE> "Role among `indexer`, `searcher`, `metastore`, `control-plane`, and `janitor` to assign to the node.")
                .action(ArgAction::Append)
                .alias("service")
                .required(false),
        ])
}

#[derive(Debug, Eq, PartialEq)]
pub struct RunCliCommand {
    pub config_uri: Uri,
    pub roles: Option<HashSet<NodeRole>>,
}

impl RunCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let roles = matches
            .remove_many::<String>("role")
            .map(|values| {
                let roles: Result<HashSet<_>, _> = values
                    .into_iter()
                    .map(|role_str| NodeRole::from_str(&role_str))
                    .collect();
                roles
            })
            .transpose()?;
        Ok(RunCliCommand { config_uri, roles })
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "start-node");
        let mut config = load_node_config(&self.config_uri).await?;
        let (storage_resolver, metastore_resolver) =
            get_resolvers(&config.storage_configs, &config.metastore_configs);
        crate::busy_detector::set_enabled(true);

        if let Some(roles) = &self.roles {
            tracing::info!(roles = %roles.iter().join(", "), "Setting roles from override.");
            config.assigned_roles = roles.clone();
        }
        let telemetry_handle_opt =
            quickwit_telemetry::start_telemetry_loop(quickwit_telemetry_info(&config));
        quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;
        // TODO move in serve quickwit?
        let runtimes_config = RuntimesConfig::default();
        start_actor_runtimes(runtimes_config, &config.assigned_roles)?;
        let shutdown_signal = Box::pin(async move {
            signal::ctrl_c()
                .await
                .expect("Registering a signal handler for SIGINT should not fail.");
        });
        let serve_result = serve_quickwit(
            config,
            runtimes_config,
            storage_resolver,
            metastore_resolver,
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
    if config.assigned_roles.contains(&NodeRole::Metastore) {
        if config.metastore_uri.protocol().is_postgresql() {
            features.insert(QuickwitFeature::PostgresqMetastore);
        } else {
            features.insert(QuickwitFeature::FileBackedMetastore);
        }
    }
    let roles = config
        .assigned_roles
        .iter()
        .map(|role| role.to_string())
        .collect();
    QuickwitTelemetryInfo::new(roles, features)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_run_args_all_roles() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec!["run", "--config", "/config.yaml"])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                roles,
                ..
            })
            if config_uri == expected_config_uri && roles.is_none()
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_args_indexer_only() -> anyhow::Result<()> {
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
                roles,
                ..
            })
            if config_uri == expected_config_uri && roles.as_ref().unwrap().len() == 1 && roles.as_ref().unwrap().iter().cloned().next().unwrap() == NodeRole::Indexer
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_args_searcher_and_metastore() -> anyhow::Result<()> {
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
        let expected_roles = HashSet::from_iter([NodeRole::Metastore, NodeRole::Searcher]);
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                roles,
                ..
            })
            if config_uri == expected_config_uri && roles.as_ref().unwrap().len() == 2 && roles.as_ref().unwrap() == &expected_roles
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_indexer_only_args() -> anyhow::Result<()> {
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
                roles,
                ..
            })
            if config_uri == expected_config_uri && roles.as_ref().unwrap().len() == 1 && roles.as_ref().unwrap().contains(&NodeRole::Indexer)
        ));
        Ok(())
    }
}
