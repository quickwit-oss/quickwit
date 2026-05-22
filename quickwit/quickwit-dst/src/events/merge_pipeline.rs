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

//! Merge-pipeline lifecycle events emitted by production code.
//!
//! Each variant mirrors one of the actions in
//! `MergePipelineShutdown.tla` and the Stateright model
//! `crate::models::merge_pipeline::MergePipelineAction`, but uses
//! production-flavored types: string split IDs (UUIDs in production),
//! `Range<i64>` windows (epoch-second time ranges).
//!
//! Trace-conformance tests install an observer that captures events
//! into a `Vec<MergePipelineEvent>`, intern the string IDs and time
//! windows into the model's u32 IDs, and replay them through
//! `MergePipelineModel::next_state`. If a property is violated during
//! replay, production behavior diverges from the formally-verified
//! model — that's a production-visible bug.
//!
//! Note: there is no `Crash` event variant. Process death by definition
//! cannot emit anything; crashes are inferred by replay context (a gap
//! between the last emitted event and a `Restart`).

use std::ops::Range;
use std::sync::OnceLock;

/// One discrete state transition in the metrics merge pipeline.
///
/// Each event is emitted exactly once per transition, on the actor that
/// caused the transition, after the transition has been committed
/// (i.e., not before the metastore call returns successfully).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergePipelineEvent {
    /// A new ingest split was published to the metastore.
    IngestSplit {
        index_uid: String,
        split_id: String,
        num_rows: u64,
        window: Range<i64>,
    },
    /// The planner dispatched a merge operation to the scheduler.
    PlanMerge {
        index_uid: String,
        merge_id: String,
        input_split_ids: Vec<String>,
        level: u32,
        window: Range<i64>,
    },
    /// The merge executor finished writing output to object storage.
    /// The output exists as a blob but is not yet referenced by the
    /// metastore — a crash here orphans it.
    UploadMergeOutput {
        index_uid: String,
        merge_id: String,
        output_split_id: String,
        output_num_rows: u64,
        output_window: Range<i64>,
        output_merge_ops: u32,
    },
    /// The publisher atomically replaced inputs with the output in the
    /// metastore (and, if connected, fed the output back to the planner).
    PublishMergeAndFeedback {
        index_uid: String,
        merge_id: String,
        output_split_id: String,
        replaced_split_ids: Vec<String>,
        output_window: Range<i64>,
        output_merge_ops: u32,
    },
    /// The publisher disconnected from the planner (graceful shutdown
    /// phase 1).
    DisconnectMergePlanner { index_uid: String },
    /// The planner ran finalize and is exiting (graceful shutdown phase 2).
    RunFinalizeAndQuit {
        index_uid: String,
        finalize_merges_emitted: u32,
    },
    /// All in-flight merges drained and the supervisor confirmed
    /// shutdown.
    DrainComplete { index_uid: String },
    /// A fresh process invocation re-seeded the planner from
    /// `published_splits`. The list of immature splits is what the
    /// planner saw — the trace-conformance test verifies this matches
    /// the model's expectation.
    Restart {
        index_uid: String,
        re_seeded_immature_split_ids: Vec<String>,
    },
}

impl MergePipelineEvent {
    /// The `index_uid` of the pipeline that emitted this event.
    pub fn index_uid(&self) -> &str {
        match self {
            Self::IngestSplit { index_uid, .. }
            | Self::PlanMerge { index_uid, .. }
            | Self::UploadMergeOutput { index_uid, .. }
            | Self::PublishMergeAndFeedback { index_uid, .. }
            | Self::DisconnectMergePlanner { index_uid }
            | Self::RunFinalizeAndQuit { index_uid, .. }
            | Self::DrainComplete { index_uid }
            | Self::Restart { index_uid, .. } => index_uid,
        }
    }
}

/// Observer signature. Use `fn` rather than `Box<dyn Fn>` so production
/// hot paths cost a single atomic load when no observer is installed.
pub type MergePipelineEventObserver = fn(&MergePipelineEvent);

/// Global observer slot. `None` (the default) makes
/// [`record_merge_pipeline_event`] a no-op.
static OBSERVER: OnceLock<MergePipelineEventObserver> = OnceLock::new();

/// Register the global observer. Should be called once at process
/// startup or test setup. Subsequent calls are ignored (first writer wins);
/// for repeated re-installation (e.g. across multiple tests), use
/// `Arc<Mutex<Vec<MergePipelineEvent>>>` collector indirection — see
/// the conformance test in `quickwit-indexing` for the pattern.
pub fn set_merge_pipeline_event_observer(observer: MergePipelineEventObserver) {
    let _ = OBSERVER.set(observer);
}

/// Emit a merge-pipeline event. Called by production code at every
/// lifecycle transition. If no observer has been installed, this is a
/// no-op — a single atomic load.
#[inline]
pub fn record_merge_pipeline_event(event: &MergePipelineEvent) {
    if let Some(observer) = OBSERVER.get() {
        observer(event);
    }
}
