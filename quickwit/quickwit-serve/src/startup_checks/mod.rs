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

//! Production startup checks for Pomsky.
//!
//! When `QW_PROD_STARTUP_CHECKS=true`, these checks run during node startup and
//! prevent the node from serving traffic if any check fails. Two phases:
//!
//! - **Static checks**: config-only, no I/O. Run before service initialization.
//! - **Environment checks**: require live clients. Run after resolvers exist.

mod environment_checks;
mod static_checks;

use anyhow::bail;
use quickwit_config::NodeConfig;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::{error, info, warn};

pub enum CheckStatus {
    Passed { detail: String },
    Failed { expected: String, found: String, fix: String },
    Skipped { reason: String },
}

pub struct StartupCheckResult {
    name: &'static str,
    status: CheckStatus,
}

impl StartupCheckResult {
    pub fn passed(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: CheckStatus::Passed {
                detail: detail.into(),
            },
        }
    }

    pub fn failed(
        name: &'static str,
        expected: impl Into<String>,
        found: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: CheckStatus::Failed {
                expected: expected.into(),
                found: found.into(),
                fix: fix.into(),
            },
        }
    }

    pub fn skipped(name: &'static str, reason: impl Into<String>) -> Self {
        Self {
            name,
            status: CheckStatus::Skipped {
                reason: reason.into(),
            },
        }
    }

    fn is_failed(&self) -> bool {
        matches!(self.status, CheckStatus::Failed { .. })
    }

    fn log(&self) {
        match &self.status {
            CheckStatus::Passed { detail } => {
                info!("[x] {}: {detail}", self.name);
            }
            CheckStatus::Failed {
                expected,
                found,
                fix,
            } => {
                error!(
                    "[!] {}: expected {expected}, found {found} (fix: {fix})",
                    self.name
                );
            }
            CheckStatus::Skipped { reason } => {
                warn!("[ ] {}: skipped ({reason})", self.name);
            }
        }
    }
}

fn evaluate_results(results: &[StartupCheckResult]) -> anyhow::Result<()> {
    for result in results {
        result.log();
    }
    let n_failed = results.iter().filter(|r| r.is_failed()).count();
    if n_failed > 0 {
        bail!(
            "startup checks failed ({n_failed} of {} checks failed), \
             the node cannot start — check the logs above for details",
            results.len()
        );
    }
    Ok(())
}

/// Run static startup checks (config-only, no I/O).
pub fn run_static_checks(node_config: &NodeConfig) -> anyhow::Result<()> {
    let results = static_checks::check_resource_ratios(node_config);
    evaluate_results(&results)
}

/// Run environment startup checks (requires live clients, does I/O).
pub async fn run_environment_checks(
    node_config: &NodeConfig,
    metastore_client: &MetastoreServiceClient,
    storage_resolver: &StorageResolver,
) -> anyhow::Result<()> {
    let mut results = environment_checks::check_metastore(node_config, metastore_client).await;
    results.extend(
        environment_checks::check_storage_permissions(node_config, storage_resolver).await,
    );
    evaluate_results(&results)
}
