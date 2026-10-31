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
use std::path::Path;

use quickwit_common::uri::Protocol;
use quickwit_config::service::QuickwitService;
use quickwit_config::{NodeConfig, StorageBackendFlavor, disable_ingest_v1, enable_ingest_v2};
use quickwit_serve::BuildInfo;
use quickwit_telemetry::payload::{BuildTelemetryInfo, QuickwitFeature, QuickwitTelemetryInfo};
use tracing::warn;

pub(crate) fn quickwit_telemetry_info(config: &NodeConfig) -> QuickwitTelemetryInfo {
    let services = config
        .enabled_services
        .iter()
        .map(|service| service.to_string())
        .collect();
    let build_info = BuildInfo::get();
    QuickwitTelemetryInfo {
        build: BuildTelemetryInfo {
            commit: build_info.commit_short_hash.to_string(),
            target: build_info.build_target.to_string(),
            release: !build_info.version.ends_with("-nightly"),
        },
        ..QuickwitTelemetryInfo::new(services, telemetry_features(config))
    }
}

/// Returns `true` when the install ID was just created. Does nothing when telemetry is disabled, so
/// that a node with telemetry off never writes the ID.
pub(crate) fn set_install_uuid(
    telemetry_info: &mut QuickwitTelemetryInfo,
    data_dir: &Path,
) -> bool {
    if quickwit_telemetry::is_telemetry_disabled() {
        return false;
    }
    match quickwit_telemetry::load_or_create_install_id(data_dir) {
        Ok((install_uuid, created)) => {
            telemetry_info.install_uuid = Some(install_uuid);
            created
        }
        Err(error) => {
            warn!(%error, "failed to load or create the telemetry install ID");
            false
        }
    }
}

fn telemetry_features(config: &NodeConfig) -> HashSet<QuickwitFeature> {
    let mut features = HashSet::new();
    if config.is_service_enabled(QuickwitService::Indexer)
        && config.indexer_config.enable_otlp_endpoint
    {
        features.insert(QuickwitFeature::Otlp);
    }
    // The metastore URI is only relevant on the nodes that serve the metastore.
    let served_metastore_uri_opt = if config.is_service_enabled(QuickwitService::Metastore) {
        Some(&config.metastore_uri)
    } else if config.is_service_enabled(QuickwitService::MetastoreReadReplica) {
        config.metastore_read_replica_uri.as_ref()
    } else {
        None
    };
    if let Some(served_metastore_uri) = served_metastore_uri_opt {
        let feature = if served_metastore_uri.protocol() == Protocol::PostgreSQL {
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
    // Same fallback as the storage resolver, so that `QW_S3_ENDPOINT` alone is reported.
    let s3_storage_config = config
        .storage_configs
        .find_s3()
        .cloned()
        .unwrap_or_default();
    if s3_storage_config.endpoint().is_some() {
        features.insert(QuickwitFeature::S3CustomEndpoint);
    }
    let flavor_feature_opt = match s3_storage_config.flavor {
        Some(StorageBackendFlavor::DigitalOcean) => Some(QuickwitFeature::S3FlavorDigitalOcean),
        Some(StorageBackendFlavor::Garage) => Some(QuickwitFeature::S3FlavorGarage),
        Some(StorageBackendFlavor::Gcs) => Some(QuickwitFeature::S3FlavorGcs),
        Some(StorageBackendFlavor::MinIO) => Some(QuickwitFeature::S3FlavorMinio),
        None => None,
    };
    if let Some(flavor_feature) = flavor_feature_opt {
        features.insert(flavor_feature);
    }
    if config.is_service_enabled(QuickwitService::Searcher) {
        if config.jaeger_config.enable_endpoint {
            features.insert(QuickwitFeature::Jaeger);
        }
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
    if !disable_ingest_v1() {
        features.insert(QuickwitFeature::IngestV1);
    }
    if enable_ingest_v2() {
        features.insert(QuickwitFeature::IngestV2);
    }
    if config.docs_clustering_config.is_some() {
        features.insert(QuickwitFeature::DocsClustering);
    }
    features
}

#[cfg(test)]
mod tests {
    use quickwit_common::uri::Uri;
    use quickwit_config::{
        DocsClusteringConfig, LambdaConfig, S3StorageConfig, StorageConfig, StorageConfigs,
    };

    use super::*;

    #[test]
    fn test_telemetry_features() {
        let mut node_config = NodeConfig::for_test();
        node_config.enabled_services = HashSet::from([QuickwitService::Metastore]);
        node_config.indexer_config.enable_otlp_endpoint = true;
        node_config.jaeger_config.enable_endpoint = true;
        let features = telemetry_features(&node_config);
        assert!(!features.contains(&QuickwitFeature::Otlp));
        assert!(!features.contains(&QuickwitFeature::Jaeger));

        node_config.enabled_services = HashSet::from([QuickwitService::MetastoreReadReplica]);
        node_config.metastore_read_replica_uri =
            Some(Uri::for_test("postgresql://replica/metastore"));
        let features = telemetry_features(&node_config);
        assert!(features.contains(&QuickwitFeature::PostgresqMetastore));

        node_config.enabled_services = HashSet::from([
            QuickwitService::Indexer,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ]);
        node_config.metastore_uri = Uri::for_test("postgresql://localhost/metastore");
        node_config.default_index_root_uri = Uri::for_test("s3://bucket/indexes");
        node_config.storage_configs =
            StorageConfigs::new(vec![StorageConfig::S3(S3StorageConfig {
                flavor: Some(StorageBackendFlavor::MinIO),
                endpoint: Some("http://minio:9000".to_string()),
                ..Default::default()
            })]);
        node_config.searcher_config.lambda = Some(LambdaConfig::for_test());
        node_config.searcher_config.split_cache =
            Some(serde_json::from_value(serde_json::json!({"max_num_bytes": "1GB"})).unwrap());
        node_config.grpc_config.tls_config = Some(
            serde_json::from_value(serde_json::json!({
                "cert_path": "/certs/cert.pem",
                "key_path": "/certs/key.pem",
            }))
            .unwrap(),
        );
        node_config.docs_clustering_config = Some(DocsClusteringConfig {
            policies: Vec::new(),
        });
        let features = telemetry_features(&node_config);
        for feature in [
            QuickwitFeature::DocsClustering,
            QuickwitFeature::Jaeger,
            QuickwitFeature::LambdaLeafSearch,
            QuickwitFeature::Otlp,
            QuickwitFeature::PostgresqMetastore,
            QuickwitFeature::S3CustomEndpoint,
            QuickwitFeature::S3FlavorMinio,
            QuickwitFeature::S3Storage,
            QuickwitFeature::SplitCache,
            QuickwitFeature::Tls,
        ] {
            assert!(features.contains(&feature), "{feature:?}");
        }
    }
}
