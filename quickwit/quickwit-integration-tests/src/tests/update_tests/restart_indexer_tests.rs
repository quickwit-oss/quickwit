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

use std::fmt::Write;
use std::time::Duration;

use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_proto::types::DocMappingUid;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::ListSplitsQueryParams;
use serde_json::json;

use crate::test_utils::ClusterSandboxBuilder;

#[tokio::test]
async fn test_update_doc_mapping_restart_indexing_pipeline() {
    let index_id = "update-restart-ingest";
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Indexer,
            QuickwitService::ControlPlane,
            QuickwitService::Janitor,
        ])
        .build_and_start()
        .await;

    {
        // Wait for indexer to fully start.
        // The starting time is a bit long for a cluster.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .rest_client(QuickwitService::Indexer)
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    // usually these are choosen by quickwit, but actually the client can specify them
    // and we do here to simplify the test
    let initial_mapping_uid = DocMappingUid::for_test(1);
    let final_mapping_uid = DocMappingUid::for_test(2);

    // Create index
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": {
                    "doc_mapping_uid": initial_mapping_uid,
                    "field_mappings": [
                        {"name": "body", "type": "u64"}
                    ]
                },
                "indexing_settings": {
                    "commit_timeout_secs": 1
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
            false,
        )
        .await
        .unwrap();

    assert!(sandbox
        .rest_client(QuickwitService::Indexer)
        .node_health()
        .is_live()
        .await
        .unwrap());

    // Wait until indexing pipelines are started.
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    let payload = (0..1000).fold(String::new(), |mut buffer, id| {
        writeln!(&mut buffer, "{{\"body\": {id}}}").unwrap();
        buffer
    });

    // ingest some documents with old doc mapping.
    // we *don't* use local ingest to use a normal indexing pipeline
    sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            index_id,
            IngestSource::Str(payload.clone()),
            None,
            None,
            CommitType::Auto,
        )
        .await
        .unwrap();

    // we wait for a new split. We don't want to force commits to let the pipeline behave as if in
    // a steady state.
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    // we ingest again, this might end up with the new or old doc mapping depending on how quickly
    // the pipeline gets killed and restarted (in practice as this cluster is very lightly loaded,
    // it will almost always kill the pipeline before these documents are commited)
    sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            index_id,
            IngestSource::Str(payload.clone()),
            None,
            None,
            CommitType::Auto,
        )
        .await
        .unwrap();

    // Update index
    sandbox
        .rest_client(QuickwitService::Searcher)
        .indexes()
        .update(
            index_id,
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": {
                    "doc_mapping_uid": final_mapping_uid,
                    "field_mappings": [
                        {"name": "body", "type": "i64"}
                    ]
                },
                "indexing_settings": {
                    "commit_timeout_secs": 1,
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
        )
        .await
        .unwrap();

    // we ingest again, this might end up with the new or old doc mapping depending on how quickly
    // the pipeline gets killed and restarted. In practice this will almost always use the new
    // mapping on a lightly loaded cluster.
    sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            index_id,
            IngestSource::Str(payload.clone()),
            None,
            None,
            CommitType::Auto,
        )
        .await
        .unwrap();

    // we wait for a 2nd split, though it might still be there if it contains only batch 2 and not
    // batch 3.
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 2)
        .await
        .unwrap();

    // we ingest again, definitely with the up to date doc mapper this time
    sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            index_id,
            IngestSource::Str(payload.clone()),
            None,
            None,
            CommitType::Auto,
        )
        .await
        .unwrap();

    // wait for a last commit
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 3)
        .await
        .unwrap();

    let splits = sandbox
        .rest_client(QuickwitService::Indexer)
        .splits(index_id)
        .list(ListSplitsQueryParams::default())
        .await
        .unwrap();

    // we expect 3 splits, with all docs, and at least one split under old mapping and one under
    // new mapping
    assert_eq!(splits.len(), 3);
    assert!(
        splits
            .iter()
            .filter(|split| split.split_metadata.doc_mapping_uid == initial_mapping_uid)
            .count()
            > 0
    );
    assert!(
        splits
            .iter()
            .filter(|split| split.split_metadata.doc_mapping_uid == final_mapping_uid)
            .count()
            > 0
    );
    assert_eq!(
        splits
            .iter()
            .map(|split| split.split_metadata.num_docs)
            .sum::<usize>(),
        4000
    );

    sandbox.shutdown().await.unwrap();
}
