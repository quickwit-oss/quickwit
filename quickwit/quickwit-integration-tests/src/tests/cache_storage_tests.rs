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
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use byte_unit::{Byte, ByteUnit};
use bytes::Bytes;
use quickwit_common::is_split_file;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{CacheStorageConfig, StorageConfig, StorageConfigs};
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::{build_node_configs, ingest_with_retry, ClusterSandbox};
use crate::tests::basic_tests::get_ndjson_filepath;

fn test_cache_storage_config(uri: &str, size_in_mb: f64) -> StorageConfig {
    StorageConfig::Cache(CacheStorageConfig {
        cache_uri: Some(Uri::from_well_formed(uri)),
        max_cache_storage_disk_usage: if size_in_mb == 0.0 {
            None
        } else {
            Some(Byte::from_unit(size_in_mb, ByteUnit::MB).unwrap())
        },
    })
}

#[tokio::test]
async fn test_basic_storage_cache_workflow() {
    quickwit_common::setup_logging_for_tests();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut remaining_services = QuickwitService::supported_services();
    let metastore_services = HashSet::from([QuickwitService::Metastore]);
    remaining_services = remaining_services
        .difference(&metastore_services)
        .cloned()
        .collect();
    let controlplane_services = HashSet::from([QuickwitService::ControlPlane]);
    remaining_services = remaining_services
        .difference(&controlplane_services)
        .cloned()
        .collect();
    let search_services = HashSet::from([QuickwitService::Searcher]);
    remaining_services = remaining_services
        .difference(&search_services)
        .cloned()
        .collect();
    let indexer_services = remaining_services;
    let mut configs = build_node_configs(
        temp_dir.path().to_path_buf(),
        &[
            metastore_services,
            controlplane_services,
            indexer_services,
            search_services,
        ],
    );
    let [metastore_node_config, controlplane_node_config, indexer_node_config, search_node_config] =
        &mut configs[..]
    else {
        panic!("failed to construct node configs");
    };
    let cache_path = temp_dir.path().join("cache");
    let cache_uri = format!("file://{}", &cache_path.as_os_str().to_str().unwrap());

    // TODO: Relax checking so if the maximum cache size on the node is None
    // the uri woul not required and the storage would behave like upstream storage.
    metastore_node_config.node_config.storage_configs =
        StorageConfigs::new(vec![test_cache_storage_config(&cache_uri, 0.0)]);
    controlplane_node_config.node_config.storage_configs =
        StorageConfigs::new(vec![test_cache_storage_config(&cache_uri, 0.0)]);
    indexer_node_config.node_config.storage_configs =
        StorageConfigs::new(vec![test_cache_storage_config(&cache_uri, 0.0)]);
    search_node_config.node_config.storage_configs =
        StorageConfigs::new(vec![test_cache_storage_config(&cache_uri, 0.1)]);

    let index_id = "test-index-with-cache";
    let index_path = temp_dir.path().join("indices").join(index_id);
    let index_uri = format!(
        "cache://file://{}",
        &index_path.as_os_str().to_str().unwrap()
    );

    let sandbox = ClusterSandbox::start_cluster_with_configs(temp_dir, configs)
        .await
        .unwrap();

    sandbox.wait_for_cluster_num_ready_nodes(4).await.unwrap();

    let index_config = Bytes::from(format!(
        r#"
            version: 0.6
            index_id: {}
            index_uri: {}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
            "#,
        index_id, index_uri,
    ));

    // Create the index.
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            index_config.clone(),
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    // Wait fo the pipeline to start.
    // TODO: there should be a better way to do this.
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    // Check that ingest request send to searcher is forwarded to indexer and thus indexed.
    let ndjson_filepath = get_ndjson_filepath("documents_to_ingest.json");
    let ingest_source = IngestSource::File(PathBuf::from_str(&ndjson_filepath).unwrap());
    ingest_with_retry(
        &sandbox.searcher_rest_client,
        index_id,
        ingest_source,
        CommitType::Force,
    )
    .await
    .unwrap();

    let paths: Vec<_> = fs::read_dir(index_path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| is_split_file(path.as_path()))
        .collect();
    assert!(!paths.is_empty());
    let first_split = paths[0].file_name().unwrap();

    wait_until_predicate(
        || {
            let cache_path = cache_path.clone();
            async move {
                cache_path
                    .join(index_id)
                    .join(first_split)
                    .try_exists()
                    .unwrap()
            }
        },
        Duration::from_secs(10),
        Duration::from_millis(100),
    )
    .await
    .unwrap();

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:bar".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        1
    );

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    wait_until_predicate(
        || {
            let cache_path = cache_path.clone();
            async move {
                !cache_path
                    .join(index_id)
                    .join(first_split)
                    .try_exists()
                    .unwrap()
            }
        },
        Duration::from_secs(10),
        Duration::from_millis(100),
    )
    .await
    .unwrap();

    sandbox.shutdown().await.unwrap();
}
