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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use fail::FailScenario;
use quickwit_actors::{create_test_mailbox, ActorExitStatus, Universe};
use quickwit_common::rand::append_random_suffix;
use quickwit_common::split_file;
use quickwit_indexing::actors::MergeExecutor;
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_indexing::models::{MergeScratch, ScratchDirectory};
use quickwit_indexing::{get_tantivy_directory_from_split_bundle, new_split_id, TestSandbox};
use quickwit_metastore::{SplitMetadata, SplitState};
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
    let doc_mapper_yaml = r#"
        field_mappings:
          - name: body
            type: text
          - name: ts
            type: i64
            fast: true
        "#;
    let indexing_setting_yaml = r#"
        timestamp_field: ts
    "#;
    let search_fields = ["body"];
    let index_id = append_random_suffix("test-index");
    let test_index_builder = TestSandbox::create(
        &index_id,
        doc_mapper_yaml,
        indexing_setting_yaml,
        &search_fields,
    )
    .await?;
    let batch_1: Vec<serde_json::Value> = vec![
        serde_json::json!({"body ": "1", "ts": 1629889530 }),
        serde_json::json!({"body ": "2", "ts": 1629889531 }),
    ];
    let batch_2: Vec<serde_json::Value> = vec![
        serde_json::json!({"body ": "3", "ts": 1629889532 }),
        serde_json::json!({"body ": "4", "ts": 1629889533 }),
    ];
    test_index_builder.add_documents(batch_1).await?;
    test_index_builder.add_documents(batch_2).await?;
    let mut splits = test_index_builder
        .metastore()
        .list_splits(&index_id, SplitState::Published, None, None)
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
    // NOTE(fmassot): This test is working but not as exactly we would want.
    // Ideally we want the actor to stop while merging which is a long task and we
    // don't want to wait until it's finished. But... the merging phase is
    // currently in a protected zone and thus there will be not kill switch activated
    // during this period. We added the protected zone because without we observe from
    // time to time a kill switch activation because the ControlledDirectory did not
    // do any write during a HEARTBEAT... Before removing the protect zone, we need
    // to investigate this instability. Then this test will finally be really helpful.
    quickwit_common::setup_logging_for_tests();
    let doc_mapper_yaml = r#"
        field_mappings:
          - name: body
            type: text
          - name: ts
            type: i64
            fast: true
        "#;
    let indexing_setting_yaml = r#"
        timestamp_field: ts
        split_num_docs_target: 1000
    "#;
    let search_fields = ["body"];
    let index_id = "test-index";
    let test_index_builder = TestSandbox::create(
        index_id,
        doc_mapper_yaml,
        indexing_setting_yaml,
        &search_fields,
    )
    .await?;

    let batch: Vec<serde_json::Value> =
        std::iter::repeat_with(|| serde_json::json!({"body ": TEST_TEXT, "ts": 1631072713 }))
            .take(500)
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
    let storage = test_index_builder.storage();
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
    assert!(matches!(exit_status, ActorExitStatus::Failure(_)));
    Ok(())
}
