// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use byte_unit::Byte;
use fail::FailScenario;
use quickwit_index_config::default_config_for_tests;
use quickwit_indexing::actors::IndexerParams;
use quickwit_indexing::index_data;
use quickwit_indexing::models::CommitPolicy;
use quickwit_indexing::models::ScratchDirectory;
use quickwit_indexing::source::SourceConfig;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::IndexMetadata;
use quickwit_metastore::Metastore;
use quickwit_metastore::SingleFileMetastore;
use quickwit_metastore::SplitState;
use quickwit_storage::StorageUriResolver;
use serde_json::json;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[tokio::test]
async fn test_failpoint_no_failure() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

fn deterministic_panic_sequence(mut panics: Vec<bool>) -> impl Fn() + Send + Sync {
    panics.reverse();
    let panics = Mutex::new(panics);
    move || {
        let should_panic = panics.lock().unwrap().pop().unwrap_or(false);
        if should_panic {
            panic!("panicked");
        }
    }
}

#[tokio::test]
async fn test_failpoint_packager_panics_right_away() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback("packager:before", deterministic_panic_sequence(vec![true])).unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_packager_panics_after_one_success() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback(
        "packager:before",
        deterministic_panic_sequence(vec![false, true]),
    )
    .unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_publisher_panics_after_one_success() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback(
        "publisher:before",
        deterministic_panic_sequence(vec![false, true]),
    )
    .unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_publisher_panics_right_away() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback("publisher:before", deterministic_panic_sequence(vec![true])).unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_publisher_after_panics_right_away() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback("publisher:after", deterministic_panic_sequence(vec![true])).unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_uploader_panics_right_away() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback(
        "uploader:before",
        deterministic_panic_sequence(vec![false, true]),
    )
    .unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_uploader_panics_after_one_sucess() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback("uploader:before", deterministic_panic_sequence(vec![true])).unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

#[tokio::test]
async fn test_failpoint_uploader_after_panics_right_away() -> anyhow::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg_callback("uploader:after", deterministic_panic_sequence(vec![true])).unwrap();
    aux_test_failpoints().await?;
    scenario.teardown();
    Ok(())
}

async fn aux_test_failpoints() -> anyhow::Result<()> {
    quickwit_common::setup_logging_for_tests();
    let metastore = Arc::new(SingleFileMetastore::for_test());
    let index_config = default_config_for_tests();
    metastore
        .create_index(IndexMetadata {
            index_id: "test-index".to_string(),
            index_uri: "ram://test-index/".to_string(),
            index_config: Arc::new(index_config),
            checkpoint: Checkpoint::default(),
        })
        .await?;
    let params = IndexerParams {
        scratch_directory: ScratchDirectory::try_new_temp()?,
        heap_size: Byte::from_bytes(30_000_000),
        commit_policy: CommitPolicy {
            timeout: Duration::from_secs(3),
            num_docs_threshold: 2,
        },
    };
    let source_config = SourceConfig {
        id: "test-source".to_string(),
        source_type: "vec".to_string(),
        params: json!({
            "items": [
                r#"{"timestamp": 1629889530, "body": "1"}"#,
                r#"{"timestamp": 1629889531, "body": "2"}"#,
                r#"{"timestamp": 1629889532, "body": "3"}"#,
                r#"{"timestamp": 1629889533, "body": "4"}"#
            ],
            "batch_num_docs": 1
        }),
    };
    let storage_uri_resolver = StorageUriResolver::default();
    index_data(
        "test-index".to_string(),
        metastore.clone(),
        params,
        source_config,
        storage_uri_resolver,
    )
    .await?;
    let mut splits = metastore
        .list_splits("test-index", SplitState::Published, None, &[])
        .await?;
    splits.sort_by_key(|split| *split.split_metadata.time_range.clone().unwrap().start());
    assert_eq!(splits.len(), 2);
    assert_eq!(
        splits[0].split_metadata.time_range.clone().unwrap(),
        1629889530..=1629889531
    );
    assert_eq!(
        splits[1].split_metadata.time_range.clone().unwrap(),
        1629889532..=1629889533
    );
    Ok(())
}
