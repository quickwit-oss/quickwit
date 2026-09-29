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

use std::num::NonZeroUsize;

use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceInputFormat};
use quickwit_indexing::actors::{MergePipeline, MergeSchedulerService};
use quickwit_indexing::source::quickwit_supported_sources;
use quickwit_metastore::metastore_for_test;
use quickwit_storage::StorageResolver;

use super::*;

#[tokio::test]
async fn test_spawn_pipelines_without_merges() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = NodeConfig::for_test();
    config.data_dir_path = temp_dir.path().to_path_buf();
    let metastore = metastore_for_test();
    let storage_resolver = StorageResolver::for_test();
    let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
    index_service
        .create_index(
            IndexConfig::for_test("test-index", "ram:///test-index"),
            false,
        )
        .await
        .unwrap();
    let args = LocalIngestDocsArgs {
        config_uri: Uri::for_test("file:///unused-config"),
        index_id: "test-index".to_string(),
        input_path_opt: None,
        input_format: SourceInputFormat::Json,
        overwrite: false,
        vrl_script: None,
        clear_cache: false,
        num_pipelines: NonZeroUsize::new(2).unwrap(),
        batch_num_rows_opt: None,
    };
    let source_config = SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        num_pipelines: args.num_pipelines,
        enabled: true,
        // Keep pipelines alive while checking that no merge actors were created.
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    let universe = Universe::new();
    let (mailbox, service_handle) = spawn_indexing_service(
        &universe,
        &config,
        metastore,
        storage_resolver,
        quickwit_supported_sources().clone(),
        false,
    )
    .await
    .unwrap();
    let spawn_result = spawn_pipelines(&mailbox, &args, source_config).await;
    let num_indexing_pipelines = universe.get::<IndexingPipeline>().len();
    let num_merge_pipelines = universe.get::<MergePipeline>().len();
    let num_merge_schedulers = universe.get::<MergeSchedulerService>().len();
    universe.kill();
    service_handle.kill().await;
    lifecycle::quiesce(&universe).await;

    assert_eq!(spawn_result.unwrap().len(), 2);
    assert_eq!(num_indexing_pipelines, 2);
    assert_eq!(num_merge_pipelines, 0);
    assert_eq!(num_merge_schedulers, 0);
}
