// Copyright (C) 2021 Quickwit, Inc.
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

//! Fail points are a form of code instrumentation that allow errors and other behaviors
//! to be injected dynamically at runtime, primarily for testing purposes. Fail
//! points are flexible and can be configured to exhibit a variety of behaviors,
//! including panics, early returns, and sleeps. They can be controlled both
//! programmatically and via the environment, and can be triggered conditionally
//! and probabilistically.
//!
//! They rely on a global variable, which requires them to be executed in a single
//! thread.
//! For this reason, we isolate them from the other unit tests and define an
//! independant binary target.
//!
//! They are not executed by default.
//! They are executed in CI and can be executed locally
//! `cargo test --features fail/failpoints test_failpoint -- --test-threads`
//!
//! Below we test panics at different steps in the indexing pipeline.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use byte_unit::Byte;
use fail::FailScenario;
use quickwit_actors::{create_test_mailbox, ActorExitStatus, Universe};
use quickwit_common::split_file;
use quickwit_doc_mapper::{default_config_for_tests, DefaultIndexConfigBuilder};
use quickwit_indexing::actors::{IndexerParams, MergeExecutor};
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_indexing::models::{CommitPolicy, IndexingDirectory, MergeScratch, ScratchDirectory};
use quickwit_indexing::source::SourceConfig;
use quickwit_indexing::{
    get_tantivy_directory_from_split_bundle, index_data, new_split_id, TestSandbox,
};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_metastore::{
    FileBackedMetastore, IndexMetadata, Metastore, SplitMetadata, SplitState,
};
use quickwit_storage::quickwit_storage_uri_resolver;
use serde_json::json;
use tantivy::Directory;

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
    let metastore = Arc::new(FileBackedMetastore::for_test());
    let index_config = default_config_for_tests();
    metastore
        .create_index(IndexMetadata {
            index_id: "test-index".to_string(),
            index_uri: "ram://test-index/".to_string(),
            index_config: Arc::new(index_config),
            checkpoint: SourceCheckpoint::default(),
        })
        .await?;
    let params = IndexerParams {
        indexing_directory: IndexingDirectory::for_test().await?,
        heap_size: Byte::from_bytes(30_000_000),
        commit_policy: CommitPolicy {
            timeout: Duration::from_secs(3),
            num_docs_threshold: 2,
        },
    };
    let source_config = SourceConfig {
        source_id: "test-source".to_string(),
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
    let storage_uri_resolver = quickwit_storage_uri_resolver().clone();
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

const TEST_TEXT: &'static str = r#"His sole child, my lord, and bequeathed to my
overlooking. I have those hopes of her good that
her education promises; her dispositions she
inherits, which makes fair gifts fairer; for where
an unclean mind carries virtuous qualities, there
commendations go with pity; they are virtues and
traitors too; in her they are the better for their
simpleness; she derives her honesty and achieves her goodness."#;

#[tokio::test]
async fn test_merge_executor_controlled_directory_kill_switch() -> anyhow::Result<()> {
    // This tests checks that if a merger is killed in a middle of
    // a merge, then the controlled directory makes it possible to
    // abort the merging operation and return quickly.
    quickwit_common::setup_logging_for_tests();
    let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "tag_fields": [],
            "field_mappings": [
                { "name": "body", "type": "text" },
                { "name": "ts", "type": "i64", "fast": true }
            ]
        }"#;

    let index_config =
        Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
    let index_id = "test-index";
    let test_index_builder = TestSandbox::create(index_id, index_config).await?;

    let batch: Vec<serde_json::Value> =
        std::iter::repeat_with(|| serde_json::json!({"body ": TEST_TEXT, "ts": 1631072713 }))
            .take(500_000)
            .collect();
    for _ in 0..2 {
        test_index_builder.add_documents(batch.clone()).await?;
    }

    let metastore = test_index_builder.metastore();
    let split_infos = metastore.list_all_splits(index_id).await?;
    let splits: Vec<SplitMetadata> = split_infos
        .into_iter()
        .map(|split| split.split_metadata)
        .collect();
    let merge_scratch_directory = ScratchDirectory::for_test()?;

    let downloaded_splits_directory =
        merge_scratch_directory.named_temp_child("downloaded-splits-")?;
    let storage = test_index_builder.storage(index_id)?;
    let mut tantivy_dirs: Vec<Box<dyn Directory>> = vec![];
    for split in &splits {
        let split_filename = split_file(split.split_id());
        let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
        storage
            .copy_to_file(Path::new(&split_filename), &dest_filepath)
            .await?;

        tantivy_dirs.push(get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap());
    }

    let merge_scratch = MergeScratch {
        merge_operation: MergeOperation::Merge {
            merge_split_id: new_split_id(),
            splits,
        },
        merge_scratch_directory,
        downloaded_splits_directory,
        tantivy_dirs,
    };
    let (merge_packager_mailbox, _merge_packager_inbox) = create_test_mailbox();
    let merge_executor = MergeExecutor::new(
        index_id.to_string(),
        merge_packager_mailbox,
        None,
        None,
        10_000_000,
        20_000_000,
    );
    let universe = Universe::new();
    let (merge_executor_mailbox, merge_executor_handle) =
        universe.spawn_actor(merge_executor).spawn_sync();

    // We want to make sure that the processing of the message gets
    // aborted not by the actor framework, before the message is being processed.
    //
    // To do so, we
    // - pause the actor right before the merge operation
    // - send the message
    // - wait 500ms to make sure the test has reached the "pause" point
    // - kill the universe
    // - unpause
    //
    // Before the controlled directory, the merge operation would have continued until it
    // finished, taking hundreds of millisecs to terminate.
    fail::cfg("before-merge-split", "pause").unwrap();
    universe
        .send_message(&merge_executor_mailbox, merge_scratch)
        .await?;

    std::mem::drop(merge_executor_mailbox);

    tokio::time::sleep(Duration::from_millis(500)).await;
    universe.kill();

    let start = Instant::now();
    fail::cfg("before-merge-split", "off").unwrap();

    let (exit_status, _) = merge_executor_handle.join().await;
    assert!(start.elapsed() < Duration::from_millis(10));
    assert!(matches!(exit_status, ActorExitStatus::Killed));
    Ok(())
}
