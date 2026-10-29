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
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus};
use quickwit_common::fs::get_cache_directory_path;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceInputFormat, StorageBackend};
use quickwit_metastore::{SplitMetadata, StageSplitsRequestExt, metastore_for_test};
use quickwit_proto::metastore::{PublishSplitsRequest, StageSplitsRequest};
use quickwit_storage::{BulkDeleteError, MockStorage, MockStorageFactory, StorageResolver};
use tokio::sync::oneshot;

use super::*;

struct LateInitializer {
    gates: Vec<oneshot::Receiver<()>>,
    finalized: Arc<AtomicUsize>,
}

#[async_trait]
impl Actor for LateInitializer {
    type ObservableState = ();

    fn observable_state(&self) {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.gates.remove(0).await.unwrap();
        if !self.gates.is_empty() {
            ctx.spawn_actor().spawn(LateInitializer {
                gates: std::mem::take(&mut self.gates),
                finalized: self.finalized.clone(),
            });
        }
        Ok(())
    }

    async fn finalize(
        &mut self,
        _: &ActorExitStatus,
        _: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        self.finalized.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_quiesce_joins_late_initialization_descendants() {
    let universe = Universe::new();
    let finalized = Arc::new(AtomicUsize::new(0));
    let (senders, gates): (Vec<_>, Vec<_>) = (0..3).map(|_| oneshot::channel()).unzip();
    universe.spawn_builder().spawn(LateInitializer {
        gates,
        finalized: finalized.clone(),
    });
    let shutdown = quiesce(&universe);
    tokio::pin!(shutdown);
    let mut returned_early = false;
    for sender in senders {
        if !returned_early {
            returned_early = tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_ok();
        }
        sender.send(()).unwrap();
    }
    if !returned_early {
        tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
            .await
            .unwrap();
    }
    // Also clean up descendants when testing a broken, single-snapshot implementation.
    for _ in 0..4 {
        universe.quit().await;
    }
    assert!(
        !returned_early,
        "rollback barrier missed an initializing descendant"
    );
    assert_eq!(finalized.load(Ordering::SeqCst), 3);
}

fn args() -> LocalIngestDocsArgs {
    LocalIngestDocsArgs {
        config_uri: Uri::for_test("file:///unused-config"),
        index_id: "test-index".to_string(),
        input_path_opt: None,
        input_format: SourceInputFormat::Json,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
        num_pipelines: NonZeroUsize::MIN,
        batch_num_rows_opt: None,
    }
}

async fn populated_index() -> (IndexService, IndexUid, StorageResolver) {
    let metastore = metastore_for_test();
    let resolver = StorageResolver::for_test();
    let mut service = IndexService::new(metastore.clone(), resolver.clone());
    let metadata = service
        .create_index(
            IndexConfig::for_test("test-index", "ram:///test-index"),
            false,
        )
        .await
        .unwrap();
    let split = SplitMetadata {
        index_uid: metadata.index_uid.clone(),
        split_id: "published".into(),
        ..Default::default()
    };
    metastore
        .stage_splits(
            StageSplitsRequest::try_from_splits_metadata(metadata.index_uid.clone(), vec![split])
                .unwrap(),
        )
        .await
        .unwrap();
    metastore
        .publish_splits(PublishSplitsRequest {
            index_uid: Some(metadata.index_uid.clone()),
            staged_split_ids: vec!["published".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let storage = resolver.resolve(metadata.index_uri()).await.unwrap();
    storage
        .put(Path::new("published.split"), Box::new(vec![1u8]))
        .await
        .unwrap();
    (service, metadata.index_uid, resolver)
}

#[tokio::test]
async fn test_cache_cleanup_failure_rolls_back_published_splits() {
    let (mut service, index_uid, resolver) = populated_index().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = NodeConfig::for_test();
    config.data_dir_path = temp_dir.path().to_path_buf();
    let cache_path = get_cache_directory_path(temp_dir.path());
    std::fs::create_dir_all(cache_path.parent().unwrap()).unwrap();
    // An uncleanable cache path fails deterministically, including when tests run as root.
    std::fs::write(&cache_path, b"not a directory").unwrap();
    let result = finish_load(Ok(()), &mut service, &args(), &config, &index_uid).await;
    assert!(result.is_err());
    let splits = service
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();
    assert!(splits.is_empty());
    let storage = resolver
        .resolve(&Uri::for_test("ram:///test-index"))
        .await
        .unwrap();
    assert!(!storage.exists(Path::new("published.split")).await.unwrap());
}

#[tokio::test]
#[allow(clippy::result_large_err)] // MockStorage's bulk_delete signature.
async fn test_rollback_reports_suppressed_deletion_error_and_load_error() {
    let (service, index_uid, _) = populated_index().await;
    let mut storage = MockStorage::new();
    storage.expect_bulk_delete().times(1).returning(|paths| {
        Err(BulkDeleteError {
            unattempted: paths.iter().map(|path| path.to_path_buf()).collect(),
            ..Default::default()
        })
    });
    let storage = Arc::new(storage);
    let mut factory = MockStorageFactory::new();
    factory.expect_backend().returning(|| StorageBackend::Ram);
    factory
        .expect_resolve()
        .returning(move |_| Ok(storage.clone()));
    let resolver = StorageResolver::builder()
        .register(factory)
        .build()
        .unwrap();
    let mut service = IndexService::new(service.metastore(), resolver);
    let mut args = args();
    args.clear_cache = false;
    let error = finish_load(
        Err(anyhow::anyhow!("injected load failure")),
        &mut service,
        &args,
        &NodeConfig::for_test(),
        &index_uid,
    )
    .await
    .unwrap_err();
    let details = format!("{error:#}");
    assert!(details.contains("injected load failure"), "{details}");
    assert!(
        details.contains("index cleanup left split metadata behind"),
        "{details}"
    );
}
