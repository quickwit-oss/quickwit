use std::cmp::Ordering;
use std::collections::BinaryHeap;
use async_trait::async_trait;
use tantivy::{Order, TrackedObject};
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_proto::opentelemetry::proto::logs::v1::SeverityNumber::Trace;
use crate::merge_policy::MergeOperation;


struct ScoredMergeOperation {
    score: u64,
    merge_operation: TrackedObject<MergeOperation>,
}

fn merge_operation_score(merge_op: &MergeOperation) -> u64 {
    // A good merge operation is one that reduce the number of splits the most,
    // while not being too expensive.
    let num_bytes: u64 = merge_op
        .splits
        .iter()
        .map(|split| split.footer_offsets.end)
        .sum();
    let num_splits: u64 = (merge_op.splits.len() as u64).checked_sub(1).unwrap_or(0);
    // We prefer to return an integer, to avoid struggling with floating point non-ord silliness.
    num_splits * (1u64 << 50) / (1u64 + num_bytes)
}

impl From<TrackedObject<MergeOperation>> for ScoredMergeOperation {
    fn from(merge_op: TrackedObject<MergeOperation>) -> Self {
        let score = merge_operation_score(&*merge_op);
        ScoredMergeOperation {
            score,
            merge_operation: merge_op,
        }
    }
}


impl Eq for ScoredMergeOperation {}

impl PartialEq<Self> for ScoredMergeOperation {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for ScoredMergeOperation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredMergeOperation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.cmp(&other.score)
            .then_with(|| {
                self.merge_operation.merge_split_id.cmp(&other.merge_operation.merge_split_id)
            })
    }
}


pub struct GlobalMergeExecutor {
    merge_ops_to_execute: BinaryHeap<ScoredMergeOperation>,
    num_running_merge_ops: usize,
    max_running_merge_ops: usize,
}

impl Actor for GlobalMergeExecutor {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}
}

#[async_trait]
impl Handler<TrackedObject<MergeOperation>> for GlobalMergeExecutor {
    type Reply = ();

    async fn handle(&mut self, merge_op: TrackedObject<MergeOperation>, ctx: &ActorContext<Self>) -> Result<Self::Reply, ActorExitStatus> {
        let scored_merge_op = ScoredMergeOperation::from(merge_op);
        self.merge_ops_to_execute.push(scored_merge_op);
        self.consider_starting_merge_op(ctx).await?;
        Ok(())
    }
}

impl GlobalMergeExecutor {
    async fn consider_starting_merge_op(&mut self, ctx: &ActorContext<Self>) {
        if self.num_running_merge_ops >= self.max_running_merge_ops {
            return;
        }
        let Some(merge_op) = self.merge_ops_to_execute.pop() else {
            return;
        };
        self.num_running_merge_ops += 1;
        // run merge op
    }
}

#[derive(Copy, Debug, Clone)]
struct WakeUp;

#[async_trait]
impl Handler<WakeUp> for GlobalMergeExecutor {
    type Reply = ();

    async fn handle(&mut self, wake_up: WakeUp, ctx: &ActorContext<GlobalMergeExecutor>) -> Result<Self::Reply, ActorExitStatus> {
        self.consider_starting_merge_op(ctx).await?;
        Ok(())
    }
}