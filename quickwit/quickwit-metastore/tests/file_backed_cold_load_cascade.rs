//! Regression test for #6840: the file-backed metastore used to hold its global state
//! guard across a lazy index load, so a single queued writer stalled every later reader --
//! including readers of unrelated indexes already resident in memory.
//!
//! tokio's `RwLock` is write-preferring, so the cascade needs three things at once: an index
//! loading from storage, a writer queued behind it, and a reader arriving after that writer.
//! The test arranges exactly that and asserts the unrelated reader still completes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_metastore::{CreateIndexRequestExt, FileBackedMetastore};
use quickwit_proto::metastore::{CreateIndexRequest, IndexMetadataRequest, MetastoreService};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{MockStorage, RamStorage, Storage};
use tokio::sync::Semaphore;
use tokio::time::timeout;

async fn seed_index(metastore: &FileBackedMetastore, index_id: &str) -> IndexUid {
    let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .expect("create_index response must carry the index uid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_index_load_plus_queued_writer_does_not_stall_unrelated_reads() {
    // 1. Seed two indexes in a file-backed metastore over RAM storage.
    let ram_storage = RamStorage::default();
    let seeding_metastore = FileBackedMetastore::try_new(Arc::new(ram_storage.clone()), None)
        .await
        .unwrap();
    let cold_uid = seed_index(&seeding_metastore, "index-cold").await;
    let warm_uid = seed_index(&seeding_metastore, "index-warm").await;
    drop(seeding_metastore);

    // 2. Snapshot the seeded files. The mocked metastore below serves them directly, and blocks
    //    `index-cold/metastore.json` until the test opens the gate.
    let manifest_bytes = ram_storage
        .get_all(Path::new("manifest.json"))
        .await
        .unwrap();
    let cold_path = PathBuf::from(format!("{}/metastore.json", cold_uid.index_id));
    let warm_path = PathBuf::from(format!("{}/metastore.json", warm_uid.index_id));
    let cold_bytes = ram_storage.get_all(&cold_path).await.unwrap();
    let warm_bytes = ram_storage.get_all(&warm_path).await.unwrap();

    let block_enabled = Arc::new(AtomicBool::new(true));
    let load_started = Arc::new(Semaphore::new(0));

    let mut mock_storage = MockStorage::default();
    mock_storage
        .expect_uri()
        .return_const(Uri::for_test("ram:///"));
    mock_storage
        .expect_exists()
        .returning(|path| Ok(path == Path::new("manifest.json")));
    {
        let block_enabled = block_enabled.clone();
        let load_started = load_started.clone();
        mock_storage.expect_get_all().returning(move |path| {
            if path == Path::new("manifest.json") {
                return Ok(manifest_bytes.clone());
            }
            if path == warm_path.as_path() {
                return Ok(warm_bytes.clone());
            }
            if path == cold_path.as_path() {
                if block_enabled.load(Ordering::SeqCst) {
                    // Signal that the gated load started. At this point the caller
                    // (`FileBackedMetastore::index`) is holding the metastore state read guard.
                    load_started.add_permits(1);
                    // Simulate a slow object-store download. The deadline only protects the test
                    // from hanging forever if it fails before opening the gate.
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while block_enabled.load(Ordering::SeqCst) && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                return Ok(cold_bytes.clone());
            }
            panic!("unexpected storage path `{}`", path.display());
        });
    }
    mock_storage.expect_put().returning(|_, _| Ok(()));

    // 3. Rebuild the metastore over the mock storage. `try_from_manifest` registers every active
    //    index as a *lazy* (not yet loaded) index, so the next access to `index-cold` downloads
    //    `index-cold/metastore.json` while holding the global state read guard.
    let metastore = Arc::new(
        FileBackedMetastore::try_new(Arc::new(mock_storage), None)
            .await
            .unwrap(),
    );

    // 4. Warm up the unrelated index: its metadata is now loaded and its read path is pure
    //    in-memory work.
    metastore
        .index_metadata(IndexMetadataRequest::for_index_uid(warm_uid.clone()))
        .await
        .unwrap();

    // 5. Trigger the cold load and let it block inside `storage.get_all(...)` while holding the
    //    metastore state read guard.
    let metastore_reader = metastore.clone();
    let cold_request = IndexMetadataRequest::for_index_uid(cold_uid.clone());
    let cold_load_task =
        tokio::spawn(async move { metastore_reader.index_metadata(cold_request).await });
    let _ = timeout(Duration::from_secs(5), load_started.acquire())
        .await
        .expect("cold index load should start within 5s")
        .expect("start handshake semaphore is never closed");

    // 6. Baseline: while the cold load holds the read guard and no writer is queued, the unrelated,
    //    already-loaded index is read without blocking (read locks are shared).
    let baseline_started = Instant::now();
    metastore
        .index_metadata(IndexMetadataRequest::for_index_uid(warm_uid.clone()))
        .await
        .unwrap();
    println!(
        "[baseline] unrelated warm read while cold load holds the read lock: {:?}",
        baseline_started.elapsed()
    );

    // 7. Queue one writer on the same metastore state lock.
    let metastore_writer = metastore.clone();
    let writer_config =
        IndexConfig::for_test("index-queued-writer", "ram:///indexes/index-queued-writer");
    let writer_request = CreateIndexRequest::try_from_index_config(&writer_config).unwrap();
    let writer_task =
        tokio::spawn(async move { metastore_writer.create_index(writer_request).await });

    // 8. With a writer queued behind the cold load, reads of the unrelated, already-loaded index
    //    must still complete. Before the fix they blocked until the cold load finished, because the
    //    load held the state read guard and the queued writer barred new readers.
    let probe_started = Instant::now();
    for probe in 0..20 {
        let warm_request = IndexMetadataRequest::for_index_uid(warm_uid.clone());
        timeout(
            Duration::from_millis(500),
            metastore.index_metadata(warm_request),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "probe {probe}: read of unrelated index `{}` stalled behind the queued writer \
                 while `{}` was still loading; the state guard must not be held across the lazy \
                 load",
                warm_uid.index_id, cold_uid.index_id,
            )
        })
        .expect("unrelated index metadata read should succeed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!(
        "[no-cascade] 20 unrelated warm reads completed while the cold load was gated and a \
         writer was queued: {:?}",
        probe_started.elapsed()
    );

    // 9. Open the gate: the cold load, the queued writer and the unrelated reads must all complete.
    block_enabled.store(false, Ordering::SeqCst);
    let cold_result = timeout(Duration::from_secs(5), cold_load_task)
        .await
        .expect("cold load should complete once released")
        .unwrap();
    assert!(cold_result.is_ok(), "cold load failed: {cold_result:?}");
    let writer_result = timeout(Duration::from_secs(5), writer_task)
        .await
        .expect("queued writer should complete once the cold load is released")
        .unwrap();
    assert!(
        writer_result.is_ok(),
        "queued writer failed: {writer_result:?}"
    );

    metastore
        .index_metadata(IndexMetadataRequest::for_index_uid(warm_uid))
        .await
        .unwrap();
    metastore
        .index_metadata(IndexMetadataRequest::for_index_uid(cold_uid))
        .await
        .unwrap();
    metastore
        .index_metadata(IndexMetadataRequest::for_index_id(
            "index-queued-writer".to_string(),
        ))
        .await
        .unwrap();
    println!("[recovered] cold load, queued writer and unrelated reads all completed");
}
