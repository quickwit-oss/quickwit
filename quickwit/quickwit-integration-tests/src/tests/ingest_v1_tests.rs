// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::CommitType;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ingest, ClusterSandboxBuilder};

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
    sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    ingest(
        &sandbox.indexer_rest_client,
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
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
