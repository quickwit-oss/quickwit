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

const ENV_VAR_NOT_IN_ALLOW_LIST: &str = "env var not in allow list";

/// Explicit allow list of environment variables whose values are exposed by [`EnvInfo`].
///
/// Variables listed here are captured with their real value. Other variables
/// with a `CP_`, `DD_`, or `QW_` prefix are reported by name only with the
/// `"env var not in allow list"` placeholder.
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

/// Deny list of credential-bearing variables whose values must not be exposed.
///
/// Variables in this list are reported by name only with the
/// `"env var not in allow list"` placeholder. This keeps the diagnostic signal
/// that the variable is set while ensuring the value is never exposed, even if
/// the variable is accidentally added to [`ALLOWED_ENV_VARS`].
///
/// - The cost of being wrong (leaking a secret via logs, status endpoints, or support bundles) is
///   much higher than the cost of maintaining a few extra lines.
/// - It documents which variables are known to carry secrets, so future maintainers think twice
///   before allow-listing them.
/// - `test_env_var_lists_are_pairwise_disjoint` turns an accidental allow-list addition of a denied
///   variable into a test failure instead of a silent leak.
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
    /// Returns diagnostic environment information for the process.
    ///
    /// Allowed variables are returned with their values. Denied variables and
    /// other `CP_`, `DD_`, or `QW_` variables are returned by name with a
    /// placeholder value. Other variables are omitted.
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<EnvInfo> = LazyLock::new(EnvInfo::from_current_env);
        &INSTANCE
    }

    fn from_current_env() -> Self {
        EnvInfo::from_env_iter(std::env::vars())
    }

    fn from_env_iter<I, K, V>(vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut env_vars = BTreeMap::new();

        for (key, value) in vars {
            let key = key.as_ref();
            if DENIED_ENV_VARS.contains(key) {
                env_vars.insert(key.to_string(), ENV_VAR_NOT_IN_ALLOW_LIST.to_string());
            } else if ALLOWED_ENV_VARS.contains(key) {
                env_vars.insert(key.to_string(), value.into());
            } else if is_allowed_prefix(key) {
                env_vars.insert(key.to_string(), ENV_VAR_NOT_IN_ALLOW_LIST.to_string());
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

    #[test]
    fn test_env_var_lists_are_pairwise_disjoint() {
        let allowed_denied_overlap: Vec<&str> = ALLOWED_ENV_VARS
            .intersection(&DENIED_ENV_VARS)
            .copied()
            .collect();
        assert!(
            allowed_denied_overlap.is_empty(),
            "env vars cannot be both allowed and denied: {allowed_denied_overlap:?}"
        );
    }

    #[test]
    fn test_denied_allowed_and_unlisted_env_vars() {
        assert!(!ALLOWED_ENV_VARS.contains("QW_METASTORE_URI"));
        assert!(DENIED_ENV_VARS.contains("QW_METASTORE_URI"));

        let env_info = EnvInfo::from_env_iter([
            (
                "QW_METASTORE_URI",
                "postgres://user:password@db:5432/cloudprem",
            ),
            ("DD_API_KEY", "secret-api-key"),
            ("QW_CLUSTER_ID", "cluster-a"),
            ("QW_NOT_IN_ALLOW_LIST_FOR_TEST", "visible-value"),
            ("PATH", "/bin"),
        ]);

        assert_eq!(
            env_info
                .env_vars
                .get("QW_METASTORE_URI")
                .map(String::as_str),
            Some(ENV_VAR_NOT_IN_ALLOW_LIST)
        );
        assert_eq!(
            env_info.env_vars.get("DD_API_KEY").map(String::as_str),
            Some(ENV_VAR_NOT_IN_ALLOW_LIST)
        );
        assert_eq!(
            env_info.env_vars.get("QW_CLUSTER_ID").map(String::as_str),
            Some("cluster-a")
        );
        assert_eq!(
            env_info
                .env_vars
                .get("QW_NOT_IN_ALLOW_LIST_FOR_TEST")
                .map(String::as_str),
            Some(ENV_VAR_NOT_IN_ALLOW_LIST)
        );
        assert!(!env_info.env_vars.contains_key("PATH"));
    }
}
