use std::sync::Arc;
use std::time::Duration;

use quickwit_proto::ingest::ShardState;
use quickwit_proto::ingest::ingester::IngesterStatus;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::local_shards_utils::ShardThroughputReadings;
use super::metrics::{
    CLOSED_SHARDS, OPEN_SHARDS, SHARD_LT_THROUGHPUT_MIB, SHARD_ST_THROUGHPUT_MIB,
};
use super::state::WeakIngesterState;

const LOCAL_SHARDS_SAMPLE_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};

/// The ShardReadingsPublisher is responsinble for harvesting shard state and throughput readings
/// from the ingester state, for use in reporting tasks. The legacy, gossip-based LocalShardsUpdate
/// task reads from the data produced here until all indexers are migrated to the new gRPC-based
/// IndexerReportingTask, which communicates shard readings directly with the control plane.
pub(super) struct ShardReadingsPublisher {
    weak_state: WeakIngesterState,
    local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
}

impl ShardReadingsPublisher {
    pub fn spawn(
        weak_state: WeakIngesterState,
        local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
    ) -> JoinHandle<()> {
        let publisher = Self {
            weak_state,
            local_shards_tx,
        };
        tokio::spawn(publisher.run())
    }

    async fn run(self) {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + LOCAL_SHARDS_SAMPLE_INTERVAL,
            LOCAL_SHARDS_SAMPLE_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(state) = self.weak_state.upgrade() else {
                warn!("stopping ShardReadingsPublisher: failed to upgrade state");
                // unexpected: something went wrong upstream. Terminate.
                return;
            };
            match *state.status_rx.borrow() {
                IngesterStatus::Initializing => continue, // WAL loading is async; retry next tick
                IngesterStatus::Failed => return,         // This pod should be killed.
                _ => {}
            }
            let shared_rate_meter_opt = state.shared_rate_meter_rx.borrow().clone();
            let Some(shared_rate_meter) = shared_rate_meter_opt else {
                // possible, but unlikely if we're initialized but haven't sent a reading yet.
                continue;
            };
            let snapshot = Arc::new(shared_rate_meter.harvest());
            self.local_shards_tx.send_replace(Some(snapshot.clone()));
            report_local_shards_metrics(&snapshot);
        }
    }
}

fn report_local_shards_metrics(snapshot: &ShardThroughputReadings) {
    let mut num_open_shards = 0;
    let mut num_closed_shards = 0;

    for shard_infos in snapshot.per_source_shard_infos.values() {
        for shard_info in shard_infos {
            match shard_info.shard_state {
                ShardState::Open => num_open_shards += 1,
                ShardState::Closed => num_closed_shards += 1,
                ShardState::Unavailable | ShardState::Unspecified => {}
            }
            SHARD_ST_THROUGHPUT_MIB.observe(shard_info.short_term_ingestion_rate.as_mib().ceil());
            SHARD_LT_THROUGHPUT_MIB.observe(shard_info.long_term_ingestion_rate.as_mib().ceil());
        }
    }
    OPEN_SHARDS.set(num_open_shards as f64);
    CLOSED_SHARDS.set(num_closed_shards as f64);
}
