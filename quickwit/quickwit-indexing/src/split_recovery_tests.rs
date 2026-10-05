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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quickwit_common::shared_consts::SPLIT_RECOVERY_METADATA_FILE_NAME;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::IndexConfig;
use quickwit_metastore::{
    CreateIndexRequestExt, FileBackedMetastore, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState, StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, ListSplitsRequest, MetastoreService, PublishSplitsRequest,
    SplitRecoveryMetadata, StageSplitsRequest,
};
use quickwit_proto::types::SplitId;
use quickwit_storage::{BundleStorage, RamStorage, Storage};

use crate::test_utils::TestSandbox;

async fn recover_and_publish_split(
    test_sandbox: &TestSandbox,
    index_id: &str,
    original_metadata: &SplitMetadata,
) -> anyhow::Result<Vec<SplitId>> {
    // Load the split bundle from object storage and read its embedded recovery entry.
    let split_filename = quickwit_common::split_file(original_metadata.split_id());
    let split_path = Path::new(&split_filename);
    let storage = test_sandbox.storage();
    let (split_bundle, _hotcache, footer_offsets) =
        BundleStorage::open_from_storage(storage, split_path.to_path_buf()).await?;
    let serialized_recovery_metadata = split_bundle
        .get_all(Path::new(SPLIT_RECOVERY_METADATA_FILE_NAME))
        .await?;
    let recovery_metadata =
        SplitRecoveryMetadata::deserialize(serialized_recovery_metadata.as_ref())?;
    let (mut recovered_metadata, parent_split_ids) =
        SplitMetadata::try_from_recovery_metadata(recovery_metadata, footer_offsets)?;

    assert_eq!(&recovered_metadata, original_metadata);

    // Simulate a lost metastore by creating the index in a new one. Index creation assigns a
    // new incarnation UID, so the importer explicitly remaps the recovered split to it.
    let fresh_metastore =
        FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None).await?;
    let index_config = IndexConfig::for_test(index_id, "ram:///recovered-index");
    let recovered_index_uid = fresh_metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();
    recovered_metadata.index_uid = recovered_index_uid.clone();

    fresh_metastore
        .stage_splits(StageSplitsRequest::try_from_split_metadata(
            recovered_index_uid.clone(),
            &recovered_metadata,
        )?)
        .await?;
    fresh_metastore
        .publish_splits(PublishSplitsRequest {
            index_uid: Some(recovered_index_uid.clone()),
            staged_split_ids: vec![recovered_metadata.split_id.to_string()],
            ..Default::default()
        })
        .await?;

    let published_splits = fresh_metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(recovered_index_uid)?)
        .await?
        .collect_splits()
        .await?;
    assert_eq!(published_splits.len(), 1);
    assert_eq!(published_splits[0].split_state, SplitState::Published);
    assert_eq!(published_splits[0].split_metadata, recovered_metadata);

    Ok(parent_split_ids)
}
#[tokio::test]
async fn test_recover_split_from_bundle_and_publish_it() {
    let index_id = quickwit_common::rand::append_random_suffix("split-recovery");
    let test_sandbox = TestSandbox::create(
        &index_id,
        r#"
            timestamp_field: timestamp
            tag_fields:
              - tenant
            field_mappings:
              - name: timestamp
                type: datetime
                fast: true
              - name: body
                type: text
              - name: tenant
                type: text
                tokenizer: raw
        "#,
        "{}",
        &["body"],
    )
    .await
    .unwrap();
    test_sandbox
        .add_documents([serde_json::json!({
            "timestamp": 1_700_000_000,
            "body": "recover me",
            "tenant": "acme"
        })])
        .await
        .unwrap();

    let original_splits = test_sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(test_sandbox.index_uid()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert_eq!(original_splits.len(), 1);
    let original_metadata = &original_splits[0].split_metadata;
    assert_eq!(
        original_metadata
            .tags
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["tenant!", "tenant:acme"]
    );

    let parent_split_ids = recover_and_publish_split(&test_sandbox, &index_id, original_metadata)
        .await
        .unwrap();
    assert!(parent_split_ids.is_empty());

    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_recover_merged_split_from_bundle_and_publish_it() {
    let index_id = quickwit_common::rand::append_random_suffix("merged-split-recovery");
    let test_sandbox = TestSandbox::create(
        &index_id,
        r#"
            timestamp_field: timestamp
            field_mappings:
              - name: timestamp
                type: datetime
                fast: true
              - name: body
                type: text
        "#,
        r#"
            split_num_docs_target: 1000
            merge_policy:
              type: stable_log
              merge_factor: 2
              max_merge_factor: 2
        "#,
        &["body"],
    )
    .await
    .unwrap();
    test_sandbox
        .add_documents([serde_json::json!({
            "timestamp": 1_700_000_000,
            "body": "first parent"
        })])
        .await
        .unwrap();
    test_sandbox
        .add_documents([serde_json::json!({
            "timestamp": 1_700_000_001,
            "body": "second parent"
        })])
        .await
        .unwrap();

    wait_until_predicate(
        || {
            let metastore = test_sandbox.metastore();
            let index_uid = test_sandbox.index_uid();
            async move {
                let query =
                    ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published);
                let Ok(request) = ListSplitsRequest::try_from_list_splits_query(&query) else {
                    return false;
                };
                let Ok(split_stream) = metastore.list_splits(request).await else {
                    return false;
                };
                let Ok(splits) = split_stream.collect_splits().await else {
                    return false;
                };
                splits.len() == 1 && splits[0].split_metadata.num_merge_ops == 1
            }
        },
        Duration::from_secs(10),
        Duration::from_millis(25),
    )
    .await
    .unwrap();

    let published_query = ListSplitsQuery::for_index(test_sandbox.index_uid())
        .with_split_state(SplitState::Published);
    let published_splits = test_sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&published_query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert_eq!(published_splits.len(), 1);
    let merged_metadata = &published_splits[0].split_metadata;
    assert_eq!(merged_metadata.num_docs, 2);
    assert_eq!(merged_metadata.num_merge_ops, 1);

    let replaced_query = ListSplitsQuery::for_index(test_sandbox.index_uid())
        .with_split_state(SplitState::MarkedForDeletion);
    let replaced_splits = test_sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&replaced_query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert_eq!(replaced_splits.len(), 2);
    let mut expected_parent_ids: Vec<SplitId> = replaced_splits
        .iter()
        .map(|split| split.split_metadata.split_id.clone())
        .collect();
    expected_parent_ids.sort();

    let mut recovered_parent_ids =
        recover_and_publish_split(&test_sandbox, &index_id, merged_metadata)
            .await
            .unwrap();
    recovered_parent_ids.sort();
    assert_eq!(recovered_parent_ids, expected_parent_ids);

    test_sandbox.assert_quit().await;
}
