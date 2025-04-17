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

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::CommitType;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

// TODO(#5604)

/// This tests checks our happy path for ingesting one doc.
#[tokio::test]
async fn test_ingest_v1_happy_path() {
    let sandbox = ClusterSandboxBuilder::default()
        .use_legacy_ingest()
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::Searcher])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Janitor,
            QuickwitService::Metastore,
        ])
        .build_and_start()
        .await;

    let index_id = "test-ingest-v1-happy-path";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 1
        "#
    );
    let indexer_client = sandbox.rest_client_legacy_indexer();
    indexer_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    ingest(
        &indexer_client,
        index_id,
        ingest_json!({"body": "my-doc"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "*", 1).await;

    // Delete the index to avoid potential hanging on shutdown #5068
    indexer_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
