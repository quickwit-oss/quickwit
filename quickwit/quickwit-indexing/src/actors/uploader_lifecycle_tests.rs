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

use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::tower::BoxService;
use quickwit_proto::metastore::{EmptyResponse, MetastoreError, MockMetastoreService};
use quickwit_proto::types::{DocMappingUid, NodeId};
use quickwit_storage::RamStorage;
use tower::ServiceExt;

use super::*;
use crate::merge_policy::NopMergePolicy;
use crate::models::SplitAttrs;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_uploader_shutdown_waits_for_in_flight_upload() {
    for kill in [false, true] {
        assert_uploader_waits_for_upload(kill, false).await;
    }
}

#[tokio::test]
async fn test_uploader_panic_waits_for_in_flight_upload() {
    assert_uploader_waits_for_upload(false, true).await;
}

async fn assert_uploader_waits_for_upload(kill: bool, panic: bool) {
    let universe = Universe::new();
    let (publisher, _inbox) = universe.create_test_mailbox::<Publisher>();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let entered_clone = entered.clone();
    let release_clone = release.clone();
    let hold_stage = tower::layer::layer_fn(
        move |service: BoxService<StageSplitsRequest, EmptyResponse, MetastoreError>| {
            let entered = entered_clone.clone();
            let release = release_clone.clone();
            tower::service_fn(move |request| {
                let service = service.clone();
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    service.oneshot(request).await
                }
            })
        },
    );
    let mut metastore = MockMetastoreService::new();
    metastore
        .expect_stage_splits()
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));
    let metastore = MetastoreServiceClient::tower()
        .stack_stage_splits_layer(hold_stage)
        .build_from_mock(metastore);
    let storage = RamStorage::default();
    let uploader = Uploader::new(
        UploaderType::IndexUploader,
        metastore,
        Arc::new(NopMergePolicy),
        None,
        IndexingSplitStore::create_without_local_store_for_test(Arc::new(storage.clone())),
        publisher.into(),
        4,
        EventBroker::default(),
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(uploader);
    let batch = PackagedSplitBatch::new(
        vec![PackagedSplit {
            split_attrs: SplitAttrs {
                node_id: NodeId::from_str("test-node"),
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
                doc_mapping_uid: DocMappingUid::default(),
                partition_id: 0,
                time_range: None,
                uncompressed_docs_size_in_bytes: 1,
                num_docs: 1,
                replaced_split_ids: Vec::new(),
                split_id: "held-upload".into(),
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            serialized_split_fields: Vec::new(),
            split_scratch_directory: TempDirectory::for_test(),
            tags: Default::default(),
            hotcache_bytes: Vec::new(),
            split_files: Vec::new(),
        }],
        None,
        PublishLock::default(),
        None,
        Span::none(),
    );
    mailbox.ask(batch).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let shutdown = async move {
        if panic {
            // A malformed batch panics after dispatching the first, held upload.
            mailbox
                .send_message(PackagedSplitBatch {
                    splits: Vec::new(),
                    checkpoint_delta_opt: None,
                    publish_lock: PublishLock::default(),
                    merge_task_opt: None,
                    batch_parent_span: Span::none(),
                })
                .await
                .unwrap();
            handle.join().await
        } else if kill {
            handle.kill().await
        } else {
            handle.quit().await
        }
    };
    tokio::pin!(shutdown);
    let early_result = tokio::time::timeout(Duration::from_millis(100), &mut shutdown).await;
    release.notify_one();
    let returned_early = early_result.is_ok();
    if !returned_early {
        let (status, _) = tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
            .await
            .unwrap();
        if panic {
            assert!(matches!(status, ActorExitStatus::Panicked), "{status:?}");
        }
    }
    universe.quit().await;
    assert!(
        !returned_early,
        "shutdown returned while staging/uploading could still write"
    );
    assert_eq!(
        storage.list_files().await,
        vec![std::path::PathBuf::from("held-upload.split")]
    );
}
