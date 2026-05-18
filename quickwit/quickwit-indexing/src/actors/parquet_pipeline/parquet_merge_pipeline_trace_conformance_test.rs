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

//! Trace-conformance test: production emits a stream of merge-pipeline
//! lifecycle events; the test rebuilds the formal state machine from
//! the events and checks every TLA+/Stateright invariant after each
//! event. A divergence between production and the model surfaces here
//! as a predicate violation on real production state.
//!
//! Compared to `parquet_merge_pipeline_crash_test.rs` (which only checks
//! that re-seeding happened), this test exercises the *strong* safety
//! claims: row preservation across merges, no orphaned splits, level
//! homogeneity, etc. — using the same predicate functions verified by
//! TLA+ and Stateright.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, set_merge_pipeline_event_observer};
use quickwit_dst::invariants::merge_pipeline::{
    InFlightMerge, MergePipelineState, SplitInfo, bounded_write_amp, leak_is_object_store_only,
    mp1_level_homogeneity, no_duplicate_merge, no_orphan_in_planner, no_split_loss,
    restart_re_seeds_all_immature, rows_conserved,
};
use quickwit_metastore::{
    ListParquetSplitsResponseExt, ParquetSplitRecord, SplitState, StageParquetSplitsRequestExt,
};
use quickwit_parquet_engine::merge::policy::{
    ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
};
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_parquet_engine::storage::ParquetWriterConfig;
use quickwit_proto::metastore::{
    EmptyResponse, ListMetricsSplitsResponse, MetastoreServiceClient, MockMetastoreService,
    StageMetricsSplitsRequest,
};
use quickwit_storage::{RamStorage, Storage};

use super::parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};
use super::parquet_merge_pipeline_test::{
    create_custom_test_batch, make_test_split_metadata, write_test_parquet_file,
};

// ---------------------------------------------------------------------------
// Event collector — single global Vec, drained between tests.
// ---------------------------------------------------------------------------
//
// The observer infrastructure uses `fn` pointers (no captures), so we can't
// install a fresh closure per test. Instead we install a single
// process-wide observer that pushes into a global Vec, and tests drain it.
// `serial_test` ensures only one trace test runs at a time.

static EVENT_LOG: OnceLock<Mutex<Vec<MergePipelineEvent>>> = OnceLock::new();
static OBSERVER_INSTALLED: OnceLock<()> = OnceLock::new();

fn observer(event: &MergePipelineEvent) {
    let log = EVENT_LOG.get_or_init(|| Mutex::new(Vec::new()));
    log.lock().unwrap().push(event.clone());
}

fn install_observer_once() {
    OBSERVER_INSTALLED.get_or_init(|| {
        set_merge_pipeline_event_observer(observer);
    });
}

fn drain_events() -> Vec<MergePipelineEvent> {
    let log = EVENT_LOG.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = log.lock().unwrap();
    std::mem::take(&mut *guard)
}

// ---------------------------------------------------------------------------
// State mirror — rebuild the formal state from events and check predicates.
// ---------------------------------------------------------------------------

/// String-indexed projection of the formal state. Each production split
/// ID maps to one model `SplitId` (u32) via interning.
struct StateMirror {
    state: MergePipelineState,
    split_ids: HashMap<String, u32>,
    /// Window keys (Range<i64> as (start, end)) -> u32 model window id.
    windows: HashMap<(i64, i64), u32>,
    /// Total ingested rows (set up-front from the test scenario, since the
    /// merge pipeline doesn't emit IngestSplit events for splits provided
    /// via `initial_immature_splits_opt`).
    expected_total_rows: u64,
    /// Whether the very first Restart event has been processed. The first
    /// is treated as the bootstrap (state is already seeded by `new`);
    /// every subsequent Restart represents a fresh process invocation,
    /// implicitly preceded by a Crash that orphans uploaded-but-unpublished
    /// merges and clears the in-memory in-flight set.
    bootstrap_restart_seen: bool,
}

impl StateMirror {
    fn new(initial_splits: &[ParquetSplitMetadata]) -> Self {
        let mut state = MergePipelineState::initial();
        let mut split_ids: HashMap<String, u32> = HashMap::new();
        let mut windows: HashMap<(i64, i64), u32> = HashMap::new();
        let mut next_window_id = 1u32;
        let mut expected_total_rows = 0u64;

        // Seed from initial splits as if they had just been ingested.
        // The merge pipeline doesn't emit IngestSplit events for these —
        // they enter via the Restart re-seed mechanism. We mirror that.
        for split in initial_splits {
            let id = state.next_id;
            state.next_id += 1;
            let window_range = split
                .window
                .clone()
                .unwrap_or(split.time_range.start_secs as i64..split.time_range.end_secs as i64);
            let window_key = (window_range.start, window_range.end);
            let w = *windows.entry(window_key).or_insert_with(|| {
                let v = next_window_id;
                next_window_id += 1;
                v
            });
            split_ids.insert(split.split_id.as_str().to_string(), id);
            state.splits.insert(
                id,
                SplitInfo {
                    rows: split.num_rows,
                    merge_ops: split.num_merge_ops,
                    window: w,
                },
            );
            state.published_splits.insert(id);
            state.cold_windows.entry(w).or_default().insert(id);
            state.total_ingested_rows += split.num_rows;
            expected_total_rows += split.num_rows;
        }

        Self {
            state,
            split_ids,
            windows,
            expected_total_rows,
            bootstrap_restart_seen: false,
        }
    }

    fn intern_split(&mut self, prod_id: &str) -> u32 {
        if let Some(&v) = self.split_ids.get(prod_id) {
            return v;
        }
        let v = self.state.next_id;
        self.state.next_id += 1;
        self.split_ids.insert(prod_id.to_string(), v);
        v
    }

    fn intern_window(&mut self, range: &std::ops::Range<i64>) -> u32 {
        let key = (range.start, range.end);
        if let Some(&v) = self.windows.get(&key) {
            return v;
        }
        let v = (self.windows.len() as u32) + 1;
        self.windows.insert(key, v);
        v
    }

    /// Apply an event by updating the mirrored state. Returns `Err` with
    /// a description if the event can't be applied because production
    /// reached a state the model says is unreachable.
    fn apply(&mut self, event: &MergePipelineEvent) -> Result<(), String> {
        match event {
            MergePipelineEvent::IngestSplit {
                split_id,
                num_rows,
                window,
                ..
            } => {
                let id = self.intern_split(split_id);
                let w = self.intern_window(window);
                self.state.splits.insert(
                    id,
                    SplitInfo {
                        rows: *num_rows,
                        merge_ops: 0,
                        window: w,
                    },
                );
                self.state.published_splits.insert(id);
                self.state.cold_windows.entry(w).or_default().insert(id);
                self.state.total_ingested_rows += num_rows;
                self.expected_total_rows += num_rows;
            }
            MergePipelineEvent::PlanMerge {
                merge_id,
                input_split_ids,
                level,
                window,
                ..
            } => {
                let m_id = self.intern_split(merge_id);
                let w = self.intern_window(window);
                let inputs: BTreeSet<u32> = input_split_ids
                    .iter()
                    .map(|s| {
                        *self.split_ids.get(s).unwrap_or_else(|| {
                            panic!("PlanMerge references unknown split {s}");
                        })
                    })
                    .collect();
                // Remove inputs from cold_windows.
                if let Some(set) = self.state.cold_windows.get_mut(&w) {
                    for id in &inputs {
                        set.remove(id);
                    }
                    if set.is_empty() {
                        self.state.cold_windows.remove(&w);
                    }
                }
                self.state.in_flight_merges.insert(
                    m_id,
                    InFlightMerge {
                        id: m_id,
                        inputs,
                        level: *level,
                        window: w,
                        uploaded: false,
                        output_id: None,
                    },
                );
            }
            MergePipelineEvent::UploadMergeOutput {
                merge_id,
                output_split_id,
                output_num_rows,
                output_window,
                output_merge_ops,
                ..
            } => {
                let m_id = *self
                    .split_ids
                    .get(merge_id)
                    .ok_or_else(|| format!("UploadMergeOutput merge_id={merge_id} not interned"))?;
                let out_id = self.intern_split(output_split_id);
                let w = self.intern_window(output_window);
                self.state.splits.insert(
                    out_id,
                    SplitInfo {
                        rows: *output_num_rows,
                        merge_ops: *output_merge_ops,
                        window: w,
                    },
                );
                if let Some(merge) = self.state.in_flight_merges.get_mut(&m_id) {
                    merge.uploaded = true;
                    merge.output_id = Some(out_id);
                } else {
                    return Err(format!(
                        "UploadMergeOutput for unknown in-flight merge {merge_id}"
                    ));
                }
            }
            MergePipelineEvent::PublishMergeAndFeedback {
                merge_id,
                output_split_id,
                replaced_split_ids,
                output_window,
                output_merge_ops,
                ..
            } => {
                // The merge_id is the output split_id in production. It
                // may not have an UploadMergeOutput predecessor for some
                // edge cases (e.g., empty merge output where executor
                // skips the upload step) — in that case treat it as an
                // implicit upload here.
                let m_id = *self
                    .split_ids
                    .get(merge_id)
                    .or_else(|| self.split_ids.get(output_split_id))
                    .copied()
                    .as_ref()
                    .unwrap_or(&self.intern_split(merge_id));
                let out_id = self.intern_split(output_split_id);
                let w = self.intern_window(output_window);
                if !self.state.splits.contains_key(&out_id) {
                    // Compute output rows from inputs (the empty-output
                    // path doesn't go through UploadMergeOutput).
                    let input_rows: u64 = replaced_split_ids
                        .iter()
                        .map(|s| {
                            self.split_ids
                                .get(s)
                                .and_then(|id| self.state.splits.get(id))
                                .map(|info| info.rows)
                                .unwrap_or(0)
                        })
                        .sum();
                    self.state.splits.insert(
                        out_id,
                        SplitInfo {
                            rows: input_rows,
                            merge_ops: *output_merge_ops,
                            window: w,
                        },
                    );
                }
                // Apply atomic metastore replace.
                let input_ids: BTreeSet<u32> = replaced_split_ids
                    .iter()
                    .map(|s| {
                        *self.split_ids.get(s).unwrap_or_else(|| {
                            panic!("PublishMergeAndFeedback replaces unknown split {s}");
                        })
                    })
                    .collect();
                for id in &input_ids {
                    self.state.published_splits.remove(id);
                }
                self.state.published_splits.insert(out_id);
                self.state.in_flight_merges.remove(&m_id);
                // Feedback to planner if connected and output is immature.
                if self.state.planner_connected
                    && self.state.planner_alive
                    && *output_merge_ops < /*max_merge_ops sentinel*/ u32::MAX
                {
                    self.state.cold_windows.entry(w).or_default().insert(out_id);
                }
            }
            MergePipelineEvent::DisconnectMergePlanner { .. } => {
                self.state.planner_connected = false;
            }
            MergePipelineEvent::RunFinalizeAndQuit { .. } => {
                self.state.finalize_requested = true;
                self.state.planner_alive = false;
            }
            MergePipelineEvent::DrainComplete { .. } => {
                self.state.shutdown_complete = true;
            }
            MergePipelineEvent::Restart {
                re_seeded_immature_split_ids,
                ..
            } => {
                if !self.bootstrap_restart_seen {
                    // First Restart of the run — production startup with
                    // initial splits already seeded by `StateMirror::new`.
                    // Sanity-check that the production-reported set
                    // matches our seeded published_splits.
                    let prod_set: BTreeSet<u32> = re_seeded_immature_split_ids
                        .iter()
                        .filter_map(|id| self.split_ids.get(id).copied())
                        .collect();
                    let our_set: BTreeSet<u32> = self.state.published_splits.clone();
                    if prod_set != our_set {
                        return Err(format!(
                            "initial Restart re-seeds {} ids; mirror has {}",
                            prod_set.len(),
                            our_set.len()
                        ));
                    }
                    self.bootstrap_restart_seen = true;
                    return Ok(());
                }
                // Subsequent Restart — model the implicit Crash + re-seed.
                // Crash semantics from the TLA+ spec: in-flight merges
                // are lost; uploaded-but-unpublished outputs become
                // orphan_outputs.
                let new_orphans: Vec<u32> = self
                    .state
                    .in_flight_merges
                    .values()
                    .filter(|m| m.uploaded)
                    .filter_map(|m| m.output_id)
                    .collect();
                self.state.orphan_outputs.extend(new_orphans);
                self.state.in_flight_merges.clear();
                self.state.cold_windows.clear();
                self.state.crashes_performed += 1;
                // Then re-seed.
                self.state.planner_alive = true;
                self.state.planner_connected = true;
                self.state.finalize_requested = false;
                self.state.finalize_ops_emitted = 0;
                self.state.shutdown_complete = false;
                self.state.restarts_performed += 1;
                // Re-seed cold_windows from published_splits using
                // production's authoritative list. Cross-check: every
                // re-seeded id should exist in our mirror's published set.
                for prod_id in re_seeded_immature_split_ids {
                    let id = self.intern_split(prod_id);
                    if let Some(info) = self.state.splits.get(&id) {
                        let w = info.window;
                        self.state.cold_windows.entry(w).or_default().insert(id);
                    }
                }
            }
        }
        Ok(())
    }

    /// Run all formal invariants over the current mirrored state.
    /// Returns the list of violations (empty when all hold).
    fn check_all_invariants(&self, max_merge_ops: u32) -> Vec<&'static str> {
        let mut violations = Vec::new();
        if !rows_conserved(&self.state) {
            violations.push("MP-4 RowsConserved");
        }
        if !no_split_loss(&self.state) {
            violations.push("MP-6 NoSplitLoss");
        }
        if !no_duplicate_merge(&self.state) {
            violations.push("MP-7 NoDuplicateMerge");
        }
        if !no_orphan_in_planner(&self.state) {
            violations.push("MP-8 NoOrphanInPlanner");
        }
        if !leak_is_object_store_only(&self.state) {
            violations.push("MP-10 LeakIsObjectStoreOnly");
        }
        if !mp1_level_homogeneity(&self.state) {
            violations.push("MP-1 LevelHomogeneity");
        }
        if !bounded_write_amp(&self.state, max_merge_ops) {
            violations.push("MP-5 BoundedWriteAmp");
        }
        violations
    }
}

// ---------------------------------------------------------------------------
// Test scenarios
// ---------------------------------------------------------------------------

async fn create_and_upload_splits(
    temp_dir: &Path,
    storage: &Arc<dyn Storage>,
    count: usize,
) -> Vec<ParquetSplitMetadata> {
    let mut metas = Vec::with_capacity(count);
    for i in 0..count {
        let split_id = format!("trace-split-{i}");
        let filename = format!("{split_id}.parquet");
        let metric = if i % 2 == 0 { "cpu.usage" } else { "mem.usage" };
        let ts_start = 100 + (i as u64) * 100;
        let batch = create_custom_test_batch(metric, ts_start, 25, "web", "host-1");
        let meta = make_test_split_metadata(&split_id, 25, 0, ts_start, metric);
        let size = write_test_parquet_file(temp_dir, &filename, &batch, &meta);
        let mut meta = meta;
        meta.size_bytes = size;
        meta.parquet_file = filename.clone();
        let content = std::fs::read(temp_dir.join(&filename)).unwrap();
        storage
            .put(Path::new(&filename), Box::new(content))
            .await
            .unwrap();
        metas.push(meta);
    }
    metas
}

fn merge_policy(
    merge_factor: usize,
    max_merge_ops: u32,
) -> Arc<dyn quickwit_parquet_engine::merge::policy::ParquetMergePolicy> {
    Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
        ParquetMergePolicyConfig {
            merge_factor,
            max_merge_factor: merge_factor,
            max_merge_ops,
            target_split_size_bytes: 256 * 1024 * 1024,
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 3,
        },
    ))
}

/// Stateful mock metastore matching real metastore semantics:
/// - `stage_metrics_splits` adds to `staged`.
/// - `publish_metrics_splits` atomically promotes staged → published AND removes replaced inputs
///   (only on success).
/// - `list_metrics_splits` returns only `published`. Staged-but-not-yet- published splits are
///   invisible to query (they only exist as object-store blobs until promoted).
struct MockMetastoreState {
    staged: Mutex<HashMap<String, ParquetSplitMetadata>>,
    published: Mutex<HashMap<String, ParquetSplitMetadata>>,
    replaced_history: Mutex<BTreeMap<String, ()>>,
    publish_call_count: AtomicUsize,
    fail_publish_at_call: Option<usize>,
    publish_done: Arc<AtomicBool>,
}

fn build_mock_metastore(tracker: Arc<MockMetastoreState>) -> MetastoreServiceClient {
    let mut mock = MockMetastoreService::new();

    // Pre-populate published with any initial splits — the merge pipeline
    // expects them to already be queryable when it starts.

    let staged_clone = tracker.clone();
    mock.expect_stage_metrics_splits()
        .returning(move |req: StageMetricsSplitsRequest| {
            let splits = req.deserialize_splits_metadata().unwrap();
            let mut staged = staged_clone.staged.lock().unwrap();
            for s in splits {
                staged.insert(s.split_id.as_str().to_string(), s);
            }
            Ok(EmptyResponse {})
        });

    let publish_clone = tracker.clone();
    mock.expect_publish_metrics_splits().returning(move |req| {
        let n = publish_clone
            .publish_call_count
            .fetch_add(1, Ordering::SeqCst);
        if Some(n) == publish_clone.fail_publish_at_call {
            // Failed publish: do NOT promote staged → published.
            return Err(quickwit_proto::metastore::MetastoreError::Internal {
                message: "injected failure for trace conformance test".to_string(),
                cause: "test".to_string(),
            });
        }
        // Atomic: promote each staged_split_id to published AND remove
        // each replaced_split_id from published.
        let mut staged = publish_clone.staged.lock().unwrap();
        let mut published = publish_clone.published.lock().unwrap();
        let mut replaced_history = publish_clone.replaced_history.lock().unwrap();
        for staged_id in &req.staged_split_ids {
            if let Some(meta) = staged.remove(staged_id) {
                published.insert(staged_id.clone(), meta);
            }
        }
        for replaced_id in &req.replaced_split_ids {
            published.remove(replaced_id);
            replaced_history.insert(replaced_id.clone(), ());
        }
        if n >= publish_clone
            .fail_publish_at_call
            .map(|x| x + 1)
            .unwrap_or(0)
        {
            publish_clone.publish_done.store(true, Ordering::SeqCst);
        }
        Ok(EmptyResponse {})
    });

    let list_clone = tracker.clone();
    mock.expect_list_metrics_splits().returning(move |_| {
        let published = list_clone.published.lock().unwrap();
        let records: Vec<ParquetSplitRecord> = published
            .values()
            .cloned()
            .map(|metadata| ParquetSplitRecord {
                state: SplitState::Published,
                update_timestamp: 0,
                metadata,
            })
            .collect();
        Ok(
            ListMetricsSplitsResponse::try_from_splits(&records).unwrap_or_else(|_| {
                ListMetricsSplitsResponse {
                    splits_serialized_json: Vec::new(),
                }
            }),
        )
    });

    MetastoreServiceClient::from_mock(mock)
}

/// Pre-populate the mock's published table with the initial splits so the
/// pipeline's first list_metrics_splits (on respawn) sees them. Mirrors
/// real metastore behavior — initial splits are already published before
/// the merge pipeline starts.
fn seed_published(tracker: &Arc<MockMetastoreState>, splits: &[ParquetSplitMetadata]) {
    let mut published = tracker.published.lock().unwrap();
    for s in splits {
        published.insert(s.split_id.as_str().to_string(), s.clone());
    }
}

/// Normal happy-path scenario: 4 splits, merge_factor=2, max_merge_ops=2.
/// Expects 2 first-level merges then 1 second-level merge (mature).
/// Verifies every formal invariant holds at every event in the trace.
#[tokio::test]
async fn test_trace_conformance_normal_path() {
    quickwit_common::setup_logging_for_tests();
    install_observer_once();
    let _ = drain_events();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    let initial_splits = create_and_upload_splits(temp_dir.path(), &ram_storage, 4).await;
    let total_initial_rows: u64 = initial_splits.iter().map(|s| s.num_rows).sum();

    let publish_done = Arc::new(AtomicBool::new(false));
    let tracker = Arc::new(MockMetastoreState {
        staged: Mutex::new(HashMap::new()),
        published: Mutex::new(HashMap::new()),
        replaced_history: Mutex::new(BTreeMap::new()),
        publish_call_count: AtomicUsize::new(0),
        fail_publish_at_call: None,
        publish_done: publish_done.clone(),
    });
    seed_published(&tracker, &initial_splits);
    let metastore = build_mock_metastore(tracker.clone());

    let params = ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("trace-conformance-index", 0),
        indexing_directory: TempDirectory::for_test(),
        metastore,
        storage: ram_storage.clone(),
        merge_policy: merge_policy(2, 2),
        merge_scheduler_service: universe.get_or_spawn_one(),
        max_concurrent_split_uploads: 4,
        event_broker: EventBroker::default(),
        writer_config: ParquetWriterConfig::default(),
    };

    let pipeline =
        ParquetMergePipeline::new(params, Some(initial_splits.clone()), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    // Wait for at least 3 publishes (2 first-level + 1 second-level).
    wait_until_predicate(
        || {
            let count = tracker.publish_call_count.load(Ordering::SeqCst);
            async move { count >= 3 }
        },
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for merge cascade");

    // Allow events to flush.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_events();
    universe.assert_quit().await;

    println!("captured {} events:", events.len());
    for (i, e) in events.iter().enumerate() {
        println!("  [{i:>2}] {e:?}");
    }

    // --- Replay through state mirror, asserting invariants at every step ---

    let mut mirror = StateMirror::new(&initial_splits);
    assert_eq!(mirror.expected_total_rows, total_initial_rows);

    // Verify invariants on the initial seeded state.
    let initial_violations = mirror.check_all_invariants(2);
    assert!(
        initial_violations.is_empty(),
        "invariants violated on initial state: {initial_violations:?}"
    );

    for (i, event) in events.iter().enumerate() {
        if let Err(reason) = mirror.apply(event) {
            panic!(
                "event [{i}] {event:?} could not be applied — production diverged from model: \
                 {reason}"
            );
        }
        let violations = mirror.check_all_invariants(2);
        assert!(
            violations.is_empty(),
            "invariants violated after event [{i}] {event:?}: {violations:?}"
        );
    }

    // Final-state checks: total ingested rows should still equal sum of
    // rows in published_splits.
    assert!(
        rows_conserved(&mirror.state),
        "final state violates RowsConserved"
    );

    // Verify MP-11 holds in any reachable post-restart state.
    if mirror.state.restarts_performed > 0 {
        assert!(
            restart_re_seeds_all_immature(&mirror.state, 2),
            "MP-11 RestartReSeedsAllImmature violated in final state"
        );
    }

    println!(
        "trace conformance passed: {} events, final state has {} published splits, {} in-flight, \
         {} ingested rows, {} crashes, {} restarts",
        events.len(),
        mirror.state.published_splits.len(),
        mirror.state.in_flight_merges.len(),
        mirror.state.total_ingested_rows,
        mirror.state.crashes_performed,
        mirror.state.restarts_performed
    );
}

/// Adversarial scenario: inject a publish failure after the first merge
/// publishes, forcing the pipeline to restart mid-cascade. Trace
/// conformance should still hold across the crash boundary.
#[tokio::test]
async fn test_trace_conformance_crash_mid_cascade() {
    quickwit_common::setup_logging_for_tests();
    install_observer_once();
    let _ = drain_events();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
    let initial_splits = create_and_upload_splits(temp_dir.path(), &ram_storage, 4).await;

    let publish_done = Arc::new(AtomicBool::new(false));
    let tracker = Arc::new(MockMetastoreState {
        staged: Mutex::new(HashMap::new()),
        published: Mutex::new(HashMap::new()),
        replaced_history: Mutex::new(BTreeMap::new()),
        publish_call_count: AtomicUsize::new(0),
        // Fail on the second publish (first merge done, second crashes).
        fail_publish_at_call: Some(1),
        publish_done: publish_done.clone(),
    });
    seed_published(&tracker, &initial_splits);
    let metastore = build_mock_metastore(tracker.clone());

    let params = ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("trace-crash-index", 0),
        indexing_directory: TempDirectory::for_test(),
        metastore,
        storage: ram_storage.clone(),
        merge_policy: merge_policy(2, 2),
        merge_scheduler_service: universe.get_or_spawn_one(),
        max_concurrent_split_uploads: 4,
        event_broker: EventBroker::default(),
        writer_config: ParquetWriterConfig::default(),
    };

    let pipeline =
        ParquetMergePipeline::new(params, Some(initial_splits.clone()), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    // Wait for post-crash publish to complete.
    wait_until_predicate(
        || {
            let done = publish_done.clone();
            async move { done.load(Ordering::SeqCst) }
        },
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for post-crash publish");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let events = drain_events();
    universe.assert_quit().await;

    println!("captured {} events (crash scenario):", events.len());
    for (i, e) in events.iter().enumerate() {
        println!("  [{i:>2}] {e:?}");
    }

    // We expect at least one Restart event AFTER the initial bootstrap.
    let restart_count = events
        .iter()
        .filter(|e| matches!(e, MergePipelineEvent::Restart { .. }))
        .count();
    assert!(
        restart_count >= 2,
        "expected initial + post-crash Restart, got {restart_count}"
    );

    // Replay and check invariants at every state.
    let mut mirror = StateMirror::new(&initial_splits);
    for (i, event) in events.iter().enumerate() {
        if let Err(reason) = mirror.apply(event) {
            panic!("event [{i}] {event:?} could not be applied — divergence: {reason}");
        }
        let violations = mirror.check_all_invariants(2);
        assert!(
            violations.is_empty(),
            "invariants violated after event [{i}] {event:?}: {violations:?}"
        );
    }

    // Final assertion: total rows preserved across the crash.
    assert!(
        rows_conserved(&mirror.state),
        "row conservation broken across crash boundary"
    );
    assert!(
        mirror.state.restarts_performed >= 1,
        "model state should reflect at least one restart"
    );
    println!(
        "trace conformance under crash passed: {} events, final state has {} published splits, \
         restarts={}",
        events.len(),
        mirror.state.published_splits.len(),
        mirror.state.restarts_performed
    );
}
