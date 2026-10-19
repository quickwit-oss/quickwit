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
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_serve::tcp_listener::DefaultTcpListenerResolver;
use quickwit_serve::{BuildInfo, EnvFilterReloadFn, reload_tls_cert, serve_quickwit};
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
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
        if let Some(services) = &self.services {
            info!(services = %services.iter().join(", "), "setting services from override");
        }
        let node_config = load_node_config(&self.config_uri, self.services.as_ref()).await?;
        let (storage_resolver, metastore_resolver) =
            get_resolvers(&node_config.storage_configs, &node_config.metastore_configs);
        crate::busy_detector::set_enabled(true);
        let telemetry_handle_opt =
            quickwit_telemetry::start_telemetry_loop(quickwit_telemetry_info(&node_config));
        quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;
        // TODO move in serve quickwit?
        let runtimes_config = RuntimesConfig::default();
        start_actor_runtimes(runtimes_config, &node_config.enabled_services)?;
        let shutdown_signal = Box::pin(async {
            select(pin!(listen_interrupt()), pin!(listen_sigterm())).await;
        });
        // Reload TLS certificates on SIGHUP for the lifetime of the process.
        tokio::spawn(listen_sighup());
        let serve_result = serve_quickwit(
            node_config,
            runtimes_config,
            metastore_resolver,
            storage_resolver,
            DefaultTcpListenerResolver,
            shutdown_signal,
            env_filter_reload_fn,
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
        info!("quickwit successfully terminated");
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
    if config.is_service_enabled(QuickwitService::Metastore) {
        let feature = if config.metastore_uri.protocol() == Protocol::PostgreSQL {
            QuickwitFeature::PostgresqMetastore
        } else {
            QuickwitFeature::FileBackedMetastore
        };
        features.insert(feature);
    }
    let storage_feature_opt = match config.default_index_root_uri.protocol() {
        Protocol::Azure => Some(QuickwitFeature::AzureStorage),
        Protocol::File => Some(QuickwitFeature::FileStorage),
        Protocol::Google => Some(QuickwitFeature::GoogleCloudStorage),
        Protocol::S3 => Some(QuickwitFeature::S3Storage),
        Protocol::Actor | Protocol::Grpc | Protocol::PostgreSQL | Protocol::Ram => None,
    };
    if let Some(storage_feature) = storage_feature_opt {
        features.insert(storage_feature);
    }
    // The Lambda and split cache settings are only relevant if the searcher is enabled.
    if config.is_service_enabled(QuickwitService::Searcher) {
        if config.searcher_config.lambda.is_some() {
            features.insert(QuickwitFeature::LambdaLeafSearch);
        }
        if config.searcher_config.split_cache.is_some() {
            features.insert(QuickwitFeature::SplitCache);
        }
    }
    if config.rest_config.tls_config.is_some() || config.grpc_config.tls_config.is_some() {
        features.insert(QuickwitFeature::Tls);
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

    use quickwit_config::LambdaConfig;
    use tempfile::tempdir;

    use super::*;
    use crate::cli::{CliCommand, build_cli};

    #[tokio::test]
    async fn test_service_override_is_applied_before_config_validation() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let config_path = temp_dir.path().join("quickwit.yaml");
        let config = format!(
            r#"
            version: 0.8
            data_dir: {}
            searcher:
              use_metastore_read_replica: true
            "#,
            temp_dir.path().display()
        );
        std::fs::write(&config_path, config)?;
        let config_uri = Uri::from_str(&format!("file://{}", config_path.display()))?;
        let enabled_services = HashSet::from([QuickwitService::Searcher]);

        let node_config = load_node_config(&config_uri, Some(&enabled_services)).await?;

        assert_eq!(node_config.enabled_services, enabled_services);
        assert!(node_config.searcher_config.use_metastore_read_replica);
        Ok(())
    }

    #[test]
    fn test_quickwit_telemetry_info_features() {
        let mut node_config = NodeConfig::for_test();
        node_config.enabled_services = HashSet::from([QuickwitService::Metastore]);
        node_config.metastore_uri = Uri::from_str("file:///qwdata/indexes").unwrap();
        node_config.default_index_root_uri = Uri::from_str("file:///qwdata/indexes").unwrap();
        node_config.indexer_config.enable_otlp_endpoint = false;
        node_config.jaeger_config.enable_endpoint = false;
        node_config.searcher_config.lambda = Some(LambdaConfig::for_test());
        node_config.searcher_config.split_cache =
            Some(serde_json::from_value(serde_json::json!({"max_num_bytes": "1GB"})).unwrap());

        // The searcher settings are ignored because the searcher is not enabled.
        let telemetry_info = quickwit_telemetry_info(&node_config);
        assert_eq!(
            telemetry_info.features,
            HashSet::from([
                QuickwitFeature::FileBackedMetastore,
                QuickwitFeature::FileStorage
            ])
        );

        node_config.enabled_services =
            HashSet::from([QuickwitService::Metastore, QuickwitService::Searcher]);
        node_config.metastore_uri = Uri::from_str("postgresql://localhost/metastore").unwrap();
        node_config.default_index_root_uri = Uri::from_str("s3://bucket/indexes").unwrap();
        node_config.indexer_config.enable_otlp_endpoint = true;
        node_config.jaeger_config.enable_endpoint = true;
        node_config.grpc_config.tls_config = Some(
            serde_json::from_value(serde_json::json!({
                "cert_path": "/certs/cert.pem",
                "key_path": "/certs/key.pem",
            }))
            .unwrap(),
        );

        let telemetry_info = quickwit_telemetry_info(&node_config);
        assert_eq!(
            telemetry_info.features,
            HashSet::from([
                QuickwitFeature::Jaeger,
                QuickwitFeature::LambdaLeafSearch,
                QuickwitFeature::Otlp,
                QuickwitFeature::PostgresqMetastore,
                QuickwitFeature::S3Storage,
                QuickwitFeature::SplitCache,
                QuickwitFeature::Tls,
            ])
        );
    }

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
