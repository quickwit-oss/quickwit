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