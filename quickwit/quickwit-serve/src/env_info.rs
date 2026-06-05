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

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

use serde::Serialize;

/// Explicit allow list of environment variables exposed by [`EnvInfo`].
///
/// This is the authoritative gate: only variables listed here are ever
/// captured. Every entry must start with `CP_`, `DD_`, or `QW_`.
///
/// [`DENIED_ENV_VARS`] is a redundant belt-and-suspenders check on top of this
/// list.
static ALLOWED_ENV_VARS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        // CloudPrem (CP_*).
        "CP_CREATE_DD_LOGS_INDEX",
        "CP_CREATE_DD_METRICS_INDEX",
        "CP_CREATE_DD_SKETCHES_INDEX",
        "CP_CREATE_DD_TRACES_INDEX",
        "CP_DISABLE_CERTIFICATE_VERIFICATION",
        "CP_DOGSTATSD_SERVER_HOST",
        "CP_DOGSTATSD_SERVER_PORT",
        "CP_ENABLE_PIPELINE_INTEGRATIONS",
        "CP_ENABLE_REVERSE_CONNECTION",
        "CP_MIN_SHARDS",
        // Datadog (DD_*).
        "DD_SITE",
        // Quickwit (QW_*).
        "QW_ACTOR_HEARTBEAT_SECS",
        "QW_ADVERTISE_ADDRESS",
        "QW_AVAILABILITY_ZONE",
        "QW_AZURE_STORAGE_ACCOUNT",
        "QW_CLOUDPREM_LISTEN_PORT",
        "QW_CLUSTER_ENDPOINT",
        "QW_CLUSTER_ID",
        "QW_CONFIG",
        "QW_DATA_DIR",
        "QW_DEFAULT_INDEX_ROOT_URI",
        "QW_DEFAULT_LOAD_PER_SHARD",
        "QW_DISABLE_DELETE_TASK_SERVICE",
        "QW_DISABLE_DOCUMENT_VALIDATION",
        "QW_DISABLE_INGEST_V1",
        "QW_DISABLE_PER_INDEX_METRICS",
        "QW_DISABLE_TELEMETRY",
        "QW_DISABLE_TOKIO_LIFO_SLOT",
        "QW_DISABLE_VARIABLE_SHARD_LOAD",
        "QW_ENABLE_CORS_DEBUG",
        "QW_ENABLE_DATAFUSION_ENDPOINT",
        "QW_ENABLE_INGEST_V2",
        "QW_ENABLE_JAEGER_ENDPOINT",
        "QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER",
        "QW_ENABLE_OTLP_ENDPOINT",
        "QW_ENABLE_TOKIO_CONSOLE",
        "QW_ENABLE_VARIABLE_SHARD_LOAD",
        "QW_ENABLED_SERVICES",
        "QW_FIELD_LIST_SIZE_LIMIT",
        "QW_GOOGLE_CLOUD_STORAGE_CREDENTIAL_PATH",
        "QW_GOSSIP_INTERVAL_MS",
        "QW_GOSSIP_LISTEN_PORT",
        "QW_GRPC_LISTEN_PORT",
        "QW_IDLE_SHARD_TIMEOUT_SECS",
        "QW_INDEX_GC_CONCURRENCY",
        "QW_INGEST_BATCH_NUM_BYTES",
        "QW_INGEST_REPLICATION_FACTOR",
        "QW_INGEST_REQUEST_TIMEOUT_MS",
        "QW_INGEST_ROUTER_BUFFER_SIZE_BYTES",
        "QW_LISTEN_ADDRESS",
        "QW_LISTEN_PORT",
        "QW_LOG_FORMAT",
        "QW_MAX_LOG_FUTURE_AGE_HOURS",
        "QW_MAX_LOG_PAST_AGE_HOURS",
        "QW_MAX_SPLIT_DELETION_RATE_PER_SEC",
        "QW_METASTORE_CLIENT_MAX_CONCURRENCY",
        "QW_METASTORE_URI",
        "QW_METRICS_MAX_BYTES",
        "QW_METRICS_MAX_ROWS",
        "QW_MINIMUM_COMPRESSION_SIZE",
        "QW_NODE_ID",
        "QW_NUM_CPUS",
        "QW_PEER_SEEDS",
        "QW_PIPELINE_CONFIG_PATH",
        "QW_POSTGRES_READ_ONLY",
        "QW_POSTGRES_SKIP_MIGRATION_LOCKING",
        "QW_POSTGRES_SKIP_MIGRATIONS",
        "QW_REST_LISTEN_PORT",
        "QW_S3_ENDPOINT",
        "QW_S3_FORCE_PATH_STYLE_ACCESS",
        "QW_S3_MAX_CONCURRENCY",
        "QW_SPLIT_DELETION_GRACE_PERIOD_SECS",
        "QW_TOKIO_RUNTIME_NUM_THREADS",
    ])
});

/// Belt-and-suspenders deny list of credential-bearing variables.
///
/// **This list is technically redundant.** [`ALLOWED_ENV_VARS`] is already the
/// only source of truth for what gets captured, so a sensitive variable simply
/// being absent from the allow list is sufficient to keep it out of
/// [`EnvInfo`]. We keep this deny list anyway because:
///
/// - The cost of being wrong (leaking a secret via logs, status endpoints, or support bundles) is
///   much higher than the cost of maintaining a few extra lines.
/// - It documents which variables are known to carry secrets, so future maintainers think twice
///   before allow-listing them.
/// - The disjointness check (see test) turns an accidental allow-list addition of a known-sensitive
///   variable into a compile-time-ish failure instead of a silent leak.
///
/// Entries must start with `CP_`, `DD_`, or `QW_`.
static DENIED_ENV_VARS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        // Datadog (DD_*).
        "DD_API_KEY_FILE",
        "DD_API_KEY",
        "DD_APP_KEY",
        // Quickwit (QW_*).
        "QW_METASTORE_URI",
        "QW_TEST_DATABASE_URL",
    ])
});

fn is_allowed_prefix(name: &str) -> bool {
    name.starts_with("CP_") || name.starts_with("DD_") || name.starts_with("QW_")
}

#[derive(Debug, Eq, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct EnvInfo {
    pub env_vars: BTreeMap<String, String>,
}

impl EnvInfo {
    /// Returns the environment variables matching the allow list that are set
    /// in the process environment.
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<EnvInfo> = LazyLock::new(EnvInfo::from_current_env);
        &INSTANCE
    }

    fn from_current_env() -> Self {
        let mut env_vars = BTreeMap::new();

        for (key, value) in std::env::vars() {
            if ALLOWED_ENV_VARS.contains(key.as_str()) {
                env_vars.insert(key, value);
            } else if !DENIED_ENV_VARS.contains(key.as_str()) && is_allowed_prefix(&key) {
                env_vars.insert(key, "env var not in allow list".to_string());
            }
        }
        EnvInfo { env_vars }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_info() {
        EnvInfo::get();
    }
}
