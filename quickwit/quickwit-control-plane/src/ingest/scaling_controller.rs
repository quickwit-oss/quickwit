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

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use fnv::FnvHashSet;
use quickwit_actors::Mailbox;
use quickwit_common::Progress;
use quickwit_ingest::SourceShardReport;
use quickwit_proto::control_plane::ShardsUpdate;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::{NodeId, SourceUid};
use tracing::{error, info, warn};

use crate::control_plane::ControlPlane;
use crate::ingest::IngestController;
use crate::model::{ControlPlaneModel, ShardStats};

const SCALE_DOWN_COOLDOWN: Duration = Duration::from_mins(5);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ScalingDecision {
    ScaleUp { target_num_open_shards: usize },
    ScaleDown { target_num_open_shards: usize },
}

pub(crate) struct ScalingController {
    // The target shard throughput in a quiescent cluster in bytes per second.
    target_shard_throughput: ByteSize,
    // The threshold above which the scale up logic is triggered.
    scale_up_shard_throughput_threshold: ByteSize,
    // The threshold below which the scale down logic is triggered.
    scale_down_shard_throughput_threshold: ByteSize,
    // Last time a scaling occurred, per source. Used to gate scale-downs, which have a cooldown.
    last_shard_count_changes: HashMap<SourceUid, Instant>,
}

impl ScalingController {
    /// Given a throughput limit, scale up/down logic is as followed. Example with the default
    /// 5MiB/s:
    /// * Target: 80% of limit: 4mib/s per shard. We're happy if every shard is here.
    /// * Scale up: >=100% of limit: 5mib/s per shard. At this level, scale-up is required.
    /// * Scale down: <40% of limit: 2mib/s per shard. At this level, we can scale down.
    pub fn with_shard_throughput_limit(shard_throughput_limit: ByteSize) -> ScalingController {
        let shard_throughput_limit_bytes = shard_throughput_limit.as_u64();
        ScalingController {
            target_shard_throughput: ByteSize::b(shard_throughput_limit_bytes * 8 / 10),
            scale_up_shard_throughput_threshold: shard_throughput_limit,
            scale_down_shard_throughput_threshold: ByteSize::b(
                shard_throughput_limit_bytes * 4 / 10,
            ),
            last_shard_count_changes: HashMap::new(),
        }
    }

    /// Per source.
    fn is_scale_down_cooldown_expired(&self, source_uid: &SourceUid, now: Instant) -> bool {
        let Some(last_shard_count_change) = self.last_shard_count_changes.get(source_uid) else {
            return true;
        };
        now.duration_since(*last_shard_count_change) >= SCALE_DOWN_COOLDOWN
    }

    /// Any scaling operation resets the scaling cooldown.
    fn restart_scale_down_cooldown(&mut self, source_uid: &SourceUid, now: Instant) {
        self.last_shard_count_changes
            .insert(source_uid.clone(), now);
    }

    /// Receive a single ingester's update. Update the control plane model.
    pub(crate) fn handle_shards_update(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &IngestController,
        node_id: &str,
        generation_id: u64,
        shards_update: ShardsUpdate,
        model: &mut ControlPlaneModel,
    ) {
        if let Some(ingester) = ingest_controller.ingester_pool.get(node_id)
            && generation_id != ingester.generation_id.as_u64()
        {
            return;
        }
        for source_shard_infos in &shards_update.shard_infos_by_source {
            let SourceShardReport {
                source_uid,
                shard_infos,
            } = source_shard_infos.into();

            model.update_shards(&source_uid, &shard_infos);
        }
    }

    /// Reconcile shards is the entrypoint for the control plane loop. It first makes scaling
    /// decisions for each source, and then makes rebalancing decisions.
    /// It primarily differs from the legacy scaling controller in scaling up as a function of
    /// observed throughput rather than using ceilings or limits, and scales down more aggressively
    /// as well.
    ///
    /// While shard scale-up tries to strategically place shards, it can still violate global
    /// balance constraints, so rebalance follows, if needed.
    pub(crate) async fn reconcile_shards(
        &mut self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        // It's critical to only count shards from ingesters that can participate in ingestion,
        // to ensure there are enough shards to satisfy demands.
        let live_ingesters: FnvHashSet<NodeId> =
            ingest_controller.ingester_pool.keys().into_iter().collect();
        let source_uids: Vec<SourceUid> = model
            .source_configs()
            .map(|(source_uid, _source_config)| source_uid)
            .collect();

        for source_uid in source_uids {
            let scale_source_shards_result = self
                .scale_source_shards(
                    ingest_controller,
                    &source_uid,
                    &live_ingesters,
                    model,
                    progress,
                )
                .await;
            let Err(metastore_error) = scale_source_shards_result else {
                continue;
            };
            if !metastore_error.is_transaction_certainly_aborted() {
                return Err(metastore_error);
            }
            error!(error=?metastore_error, "failed to scale source shards");
        }
        ingest_controller
            .rebalance_shards(model, mailbox, progress)
            .await?;
        Ok(())
    }

    async fn scale_source_shards(
        &mut self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        source_uid: &SourceUid,
        live_ingesters: &FnvHashSet<NodeId>,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let Some(shard_throughput_stats) = model.shard_throughput_stats(source_uid, live_ingesters)
        else {
            return Ok(());
        };
        let Some(min_shards) = model
            .index_metadata(&source_uid.index_uid)
            .map(|metadata| metadata.index_config.ingest_settings.min_shards)
        else {
            return Ok(());
        };
        let cooldown_expired = self.is_scale_down_cooldown_expired(source_uid, Instant::now());
        let scaling_decision_opt =
            self.should_scale(shard_throughput_stats, min_shards, cooldown_expired);
        let Some(scaling_decision) = scaling_decision_opt else {
            return Ok(());
        };
        let num_open_shards = shard_throughput_stats.num_open_shards;

        match scaling_decision {
            ScalingDecision::ScaleUp {
                target_num_open_shards,
            } => {
                self.scale_up_shards(
                    ingest_controller,
                    source_uid,
                    num_open_shards,
                    target_num_open_shards,
                    model,
                    progress,
                )
                .await
            }
            ScalingDecision::ScaleDown {
                target_num_open_shards,
            } => {
                self.scale_down_shards(
                    ingest_controller,
                    source_uid,
                    num_open_shards,
                    target_num_open_shards,
                    live_ingesters,
                    model,
                    progress,
                )
                .await;
                Ok(())
            }
        }
    }

    async fn scale_up_shards(
        &mut self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        source_uid: &SourceUid,
        num_open_shards: usize,
        target_num_open_shards: usize,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let num_shards_to_open = target_num_open_shards - num_open_shards;
        let num_opened_shards = ingest_controller
            .open_shards_for_source(source_uid, num_shards_to_open, model, progress)
            .await?;

        if num_opened_shards == 0 {
            warn!(
                index_uid=%source_uid.index_uid,
                source_id=%source_uid.source_id,
                "failed to scale up number of shards from {num_open_shards} to {target_num_open_shards}"
            );
            return Ok(());
        }
        self.restart_scale_down_cooldown(source_uid, Instant::now());
        info!(
            index_uid=%source_uid.index_uid,
            source_id=%source_uid.source_id,
            "scaled up number of shards from {num_open_shards} by {num_opened_shards} (target {target_num_open_shards})"
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn scale_down_shards(
        &mut self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &IngestController,
        source_uid: &SourceUid,
        num_open_shards: usize,
        target_num_open_shards: usize,
        live_ingesters: &FnvHashSet<NodeId>,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) {
        if ingest_controller.is_rebalancing() {
            return;
        }
        let num_shards_to_close = num_open_shards - target_num_open_shards;
        let shards_to_close =
            find_scale_down_candidates(source_uid, num_shards_to_close, live_ingesters, model);
        let closed_shard_ids = ingest_controller
            .close_source_shards(source_uid, shards_to_close, model, progress)
            .await;

        if closed_shard_ids.is_empty() {
            warn!(
                index_uid=%source_uid.index_uid,
                source_id=%source_uid.source_id,
                "failed to scale down number of shards from {num_open_shards} to {target_num_open_shards}"
            );
            return;
        }
        self.restart_scale_down_cooldown(source_uid, Instant::now());
        info!(
            index_uid=%source_uid.index_uid,
            source_id=%source_uid.source_id,
            "scaled down number of shards from {num_open_shards} by {} (target {target_num_open_shards})",
            closed_shard_ids.len()
        );
    }

    /// Determine whether a shard scaling action should be taken. Derived from the observed shard
    /// throughput readings, and the target per-shard threshold.
    ///
    /// First, compute the target rate. This is purely a function of the ingestion rate (and
    /// min_shards). Then, it is a decision based on whether the given scale up/down throughput
    /// buffer has been exceeded.
    pub fn should_scale(
        &self,
        shard_throughput_stats: ShardStats,
        min_shards: NonZeroUsize,
        cooldown_expired: bool,
    ) -> Option<ScalingDecision> {
        let num_open_shards = shard_throughput_stats.num_open_shards;
        if num_open_shards == 0 {
            return None;
        }
        // Pick the higher of the short and long term rates, to be safe
        let ingestion_rate = shard_throughput_stats
            .total_short_term_ingestion_rate
            .max(shard_throughput_stats.total_long_term_ingestion_rate);
        let target_num_open_shards = self.target_num_open_shards(ingestion_rate, min_shards);

        if num_open_shards < min_shards.get() {
            let scale_up = ScalingDecision::ScaleUp {
                target_num_open_shards,
            };
            return Some(scale_up);
        }
        // This is the rate, for the number of shards open, above which a scale-up is needed.
        let scale_up_ingestion_rate =
            self.scale_up_shard_throughput_threshold * num_open_shards as u64;
        if ingestion_rate >= scale_up_ingestion_rate {
            let scale_up = ScalingDecision::ScaleUp {
                target_num_open_shards,
            };
            return Some(scale_up);
        }
        // Below this rate, a scale-down is permissible, as long as the cooldown has expired.
        let scale_down_ingestion_rate =
            self.scale_down_shard_throughput_threshold * num_open_shards as u64;
        if ingestion_rate < scale_down_ingestion_rate
            && cooldown_expired
            && target_num_open_shards < num_open_shards
        {
            let scale_down = ScalingDecision::ScaleDown {
                target_num_open_shards,
            };
            return Some(scale_down);
        }
        // Quiescent state - the throughput per shard is acceptable. Do nothing.
        None
    }

    fn target_num_open_shards(&self, ingestion_rate: ByteSize, min_shards: NonZeroUsize) -> usize {
        let num_shards_for_ingestion_rate = ingestion_rate
            .as_u64()
            .div_ceil(self.target_shard_throughput.as_u64())
            as usize;
        num_shards_for_ingestion_rate.max(min_shards.get())
    }
}

/// Pick individual shards to close, by taking shards off the ingesters that have the most open
/// shards. This is a candidate to eventually become an ingester-level decision rather than a
/// control plane one.
fn find_scale_down_candidates(
    source_uid: &SourceUid,
    num_shards_to_close: usize,
    live_ingesters: &FnvHashSet<NodeId>,
    model: &ControlPlaneModel,
) -> Vec<Shard> {
    let Some(source_shard_entries) = model.get_shards_for_source(source_uid) else {
        return Vec::new();
    };
    let mut num_open_shards_by_ingester_id: HashMap<&str, usize> = HashMap::new();

    for shard_entry in model.all_shards() {
        if shard_entry.is_open() {
            *num_open_shards_by_ingester_id
                .entry(shard_entry.ingester_id.as_str())
                .or_default() += 1;
        }
    }
    let mut source_open_shards_by_ingester_id: HashMap<&str, Vec<&Shard>> = HashMap::new();

    for shard_entry in source_shard_entries.values() {
        if shard_entry.is_open() && live_ingesters.contains(shard_entry.ingester_id.as_str()) {
            source_open_shards_by_ingester_id
                .entry(shard_entry.ingester_id.as_str())
                .or_default()
                .push(&shard_entry.shard);
        }
    }
    let mut shards_to_close: Vec<Shard> = Vec::with_capacity(num_shards_to_close);

    for _ in 0..num_shards_to_close {
        let most_loaded_ingester_id_opt = source_open_shards_by_ingester_id
            .iter()
            .filter(|(_ingester_id, shards)| !shards.is_empty())
            .map(|(ingester_id, _shards)| *ingester_id)
            .max_by_key(|ingester_id| num_open_shards_by_ingester_id[ingester_id]);

        let Some(most_loaded_ingester_id) = most_loaded_ingester_id_opt else {
            break;
        };
        let shard = source_open_shards_by_ingester_id
            .get_mut(most_loaded_ingester_id)
            .and_then(|shards| shards.pop())
            .expect("ingester should have an open shard for the source");
        *num_open_shards_by_ingester_id
            .get_mut(most_loaded_ingester_id)
            .expect("ingester should have open shards") -= 1;
        shards_to_close.push(shard.clone());
    }
    shards_to_close
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    #[test]
    fn test_scaling_decision_boundaries() {
        use ScalingDecision::{ScaleDown, ScaleUp};
        let controller = ScalingController::with_shard_throughput_limit(ByteSize::b(100));
        for (open, minimum, short, long, expired, expected) in [
            (0, 1, 1000, 1000, true, None),
            (
                1,
                3,
                0,
                0,
                false,
                Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            ),
            (2, 1, 199, 0, true, None),
            (
                2,
                1,
                200,
                0,
                false,
                Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            ),
            (
                2,
                1,
                0,
                201,
                false,
                Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            ),
            (2, 1, 0, 80, true, None),
            (
                2,
                1,
                79,
                0,
                true,
                Some(ScaleDown {
                    target_num_open_shards: 1,
                }),
            ),
            (2, 1, 79, 0, false, None),
            (2, 2, 0, 0, true, None),
            (
                10,
                1,
                0,
                0,
                true,
                Some(ScaleDown {
                    target_num_open_shards: 1,
                }),
            ),
            (
                1,
                1,
                801,
                0,
                false,
                Some(ScaleUp {
                    target_num_open_shards: 11,
                }),
            ),
            (
                1,
                1,
                80000,
                0,
                true,
                Some(ScaleUp {
                    target_num_open_shards: 1000,
                }),
            ),
        ] {
            let stats = ShardStats {
                num_open_shards: open,
                num_closed_shards: 0,
                total_short_term_ingestion_rate: ByteSize::b(short),
                total_long_term_ingestion_rate: ByteSize::b(long),
            };
            assert_eq!(
                controller.should_scale(stats, NonZeroUsize::new(minimum).unwrap(), expired),
                expected,
                "open={open}, minimum={minimum}, short={short}, long={long}, expired={expired}"
            );
        }
    }

    #[test]
    fn test_source_cooldown_expiry_and_restart() {
        let mut controller = ScalingController::with_shard_throughput_limit(ByteSize::b(100));
        let source = SourceUid {
            index_uid: IndexUid::for_test("index", 0),
            source_id: "a".to_string(),
        };
        let other = SourceUid {
            source_id: "b".to_string(),
            ..source.clone()
        };
        let now = Instant::now();
        assert!(controller.is_scale_down_cooldown_expired(&source, now));
        controller.restart_scale_down_cooldown(&source, now);
        assert!(!controller.is_scale_down_cooldown_expired(
            &source,
            now + SCALE_DOWN_COOLDOWN - Duration::from_nanos(1)
        ));
        assert!(controller.is_scale_down_cooldown_expired(&source, now + SCALE_DOWN_COOLDOWN));
        assert!(controller.is_scale_down_cooldown_expired(&other, now));
        controller.restart_scale_down_cooldown(&source, now + SCALE_DOWN_COOLDOWN);
        assert!(!controller.is_scale_down_cooldown_expired(&source, now + SCALE_DOWN_COOLDOWN));
        assert!(controller.is_scale_down_cooldown_expired(&source, now + SCALE_DOWN_COOLDOWN * 2));
    }

    use bytesize::ByteSize;
    use quickwit_actors::Universe;
    use quickwit_common::Progress;
    use quickwit_common::shared_consts::DEFAULT_SHARD_THROUGHPUT_LIMIT;
    use quickwit_config::SourceConfig;
    use quickwit_ingest::{IngesterPool, IngesterPoolEntry, ShardInfo, SourceShardReport};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::ShardsUpdate;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, InitShardSuccess, InitShardsResponse, MockIngesterService,
    };
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceUid};

    use super::*;
    use crate::model::ControlPlaneModel;

    fn scaling_model(count: u64) -> (ControlPlaneModel, SourceUid) {
        let (mut model, _) = shard_reports_for_test(1);
        let index_uid = model.all_shards().next().unwrap().index_uid().clone();
        let source = SourceUid {
            index_uid: index_uid.clone(),
            source_id: "source-a".to_string(),
        };
        for id in 2..=count {
            model.insert_shards(
                &index_uid,
                &source.source_id,
                vec![Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source.source_id.clone(),
                    shard_id: Some(ShardId::from(id)),
                    ingester_id: "ingester".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }],
            );
        }
        (model, source)
    }

    #[tokio::test(start_paused = true)]
    async fn test_scale_down_only_acknowledged_shards_start_cooldown() {
        for (outcome, closed) in [
            ("full", 2),
            ("partial", 1),
            ("empty", 0),
            ("error", 0),
            ("timeout", 0),
        ] {
            let (mut model, source) = scaling_model(3);
            let mut mock = MockIngesterService::new();
            mock.expect_close_shards()
                .times(if outcome == "timeout" { 0 } else { 1 })
                .returning(move |request| {
                    if outcome == "error" {
                        return Err(quickwit_proto::ingest::IngestV2Error::Internal(
                            "close failed".to_string(),
                        ));
                    }
                    Ok(quickwit_proto::ingest::ingester::CloseShardsResponse {
                        successes: request.shard_pkeys.into_iter().take(closed).collect(),
                    })
                });
            let client = if outcome == "timeout" {
                IngesterServiceClient::tower()
                    .stack_close_shards_layer(quickwit_common::tower::DelayLayer::new(
                        Duration::from_secs(60),
                    ))
                    .build_from_mock(mock)
            } else {
                IngesterServiceClient::from_mock(mock)
            };
            let pool = IngesterPool::default();
            pool.insert(
                NodeId::from_str("ingester"),
                IngesterPoolEntry::ready_with_client(client),
            );
            let controller = IngestController::new(MetastoreServiceClient::mocked(), pool);
            let mut scaling =
                ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
            scaling
                .scale_down_shards(
                    &controller,
                    &source,
                    3,
                    1,
                    &FnvHashSet::from_iter([NodeId::from_str("ingester")]),
                    &mut model,
                    &Progress::default(),
                )
                .await;
            assert_eq!(
                model
                    .get_shards_for_source(&source)
                    .unwrap()
                    .values()
                    .filter(|shard| shard.is_closed())
                    .count(),
                closed
            );
            assert_eq!(
                scaling.last_shard_count_changes.contains_key(&source),
                closed > 0
            );
        }
    }

    #[tokio::test]
    async fn test_scale_up_progress_controls_cooldown() {
        for (outcome, opened) in [
            ("full", 2),
            ("partial", 1),
            ("unavailable", 0),
            ("init-error", 0),
            ("metastore-error", 0),
        ] {
            let (mut model, source) = scaling_model(1);
            let mut metastore = MockMetastoreService::new();
            metastore
                .expect_open_shards()
                .times(if matches!(outcome, "unavailable" | "init-error") {
                    0
                } else {
                    1
                })
                .returning(move |request| {
                    if outcome == "metastore-error" {
                        return Err(MetastoreError::InvalidArgument {
                            message: "open failed".to_string(),
                        });
                    }
                    Ok(quickwit_proto::metastore::OpenShardsResponse {
                        subresponses: request
                            .subrequests
                            .into_iter()
                            .map(|request| quickwit_proto::metastore::OpenShardSubresponse {
                                subrequest_id: request.subrequest_id,
                                open_shard: Some(Shard {
                                    index_uid: request.index_uid,
                                    source_id: request.source_id,
                                    shard_id: request.shard_id,
                                    ingester_id: request.ingester_id,
                                    shard_state: ShardState::Open as i32,
                                    ..Default::default()
                                }),
                            })
                            .collect(),
                    })
                });
            let pool = IngesterPool::default();
            if outcome != "unavailable" {
                let mut ingester = MockIngesterService::new();
                ingester
                    .expect_init_shards()
                    .once()
                    .returning(move |request| {
                        if outcome == "init-error" {
                            return Err(quickwit_proto::ingest::IngestV2Error::Internal(
                                "init failed".to_string(),
                            ));
                        }
                        let count = if outcome == "partial" { 1 } else { 2 };
                        Ok(InitShardsResponse {
                            successes: request
                                .subrequests
                                .into_iter()
                                .take(count)
                                .map(|request| InitShardSuccess {
                                    subrequest_id: request.subrequest_id,
                                    shard: request.shard,
                                })
                                .collect(),
                            failures: Vec::new(),
                        })
                    });
                pool.insert(
                    NodeId::from_str("ingester"),
                    IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(
                        ingester,
                    )),
                );
            }
            let mut controller =
                IngestController::new(MetastoreServiceClient::from_mock(metastore), pool);
            let mut scaling =
                ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
            let result = scaling
                .scale_up_shards(
                    &mut controller,
                    &source,
                    1,
                    3,
                    &mut model,
                    &Progress::default(),
                )
                .await;
            assert_eq!(result.is_err(), outcome == "metastore-error");
            assert_eq!(
                model
                    .get_shards_for_source(&source)
                    .unwrap()
                    .values()
                    .filter(|shard| shard.is_open())
                    .count(),
                1 + opened
            );
            assert_eq!(
                scaling.last_shard_count_changes.contains_key(&source),
                opened > 0
            );
        }
    }

    #[test]
    fn test_candidates_reduce_global_imbalance_and_exclude_ineligible_shards() {
        let (mut model, source) = scaling_model(3);
        for (id, node, state) in [
            (4, "other", ShardState::Open),
            (5, "other", ShardState::Closed),
            (6, "departed", ShardState::Open),
            (7, "ingester", ShardState::Unavailable),
        ] {
            model.insert_shards(
                &source.index_uid,
                &source.source_id,
                vec![Shard {
                    index_uid: Some(source.index_uid.clone()),
                    source_id: source.source_id.clone(),
                    shard_id: Some(ShardId::from(id)),
                    ingester_id: node.to_string(),
                    shard_state: state as i32,
                    ..Default::default()
                }],
            );
        }
        let live = FnvHashSet::from_iter([NodeId::from_str("ingester"), NodeId::from_str("other")]);
        let candidates = find_scale_down_candidates(&source, 2, &live, &model);
        assert_eq!(candidates.len(), 2);
        assert!(
            candidates
                .iter()
                .all(|shard| shard.ingester_id == "ingester")
        );
        let candidates = find_scale_down_candidates(&source, 20, &live, &model);
        assert_eq!(candidates.len(), 4);
        assert!(
            candidates
                .iter()
                .all(|shard| shard.shard_state() == ShardState::Open
                    && live.contains(shard.ingester_id.as_str()))
        );
        assert!(
            find_scale_down_candidates(
                &SourceUid {
                    source_id: "missing".to_string(),
                    ..source
                },
                1,
                &live,
                &model
            )
            .is_empty()
        );
    }

    #[tokio::test]
    async fn test_deleted_and_departed_inputs_cannot_drive_scaling() {
        let (mut model, source) = scaling_model(1);
        let mut scaling =
            ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
        let pool = IngesterPool::default();
        let mut controller = IngestController::new(MetastoreServiceClient::mocked(), pool);
        model.update_shards(
            &source,
            &BTreeSet::from([ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(100),
                long_term_ingestion_rate: ByteSize::mib(100),
            }]),
        );
        scaling
            .scale_source_shards(
                &mut controller,
                &source,
                &FnvHashSet::default(),
                &mut model,
                &Progress::default(),
            )
            .await
            .unwrap();
        assert!(scaling.last_shard_count_changes.is_empty());
        assert_eq!(model.get_shards_for_source(&source).unwrap().len(), 1);
        model.delete_source(&source);
        scaling
            .scale_source_shards(
                &mut controller,
                &source,
                &FnvHashSet::default(),
                &mut model,
                &Progress::default(),
            )
            .await
            .unwrap();
        assert!(model.get_shards_for_source(&source).is_none());
        model.delete_index(&source.index_uid);
        scaling
            .scale_source_shards(
                &mut controller,
                &source,
                &FnvHashSet::default(),
                &mut model,
                &Progress::default(),
            )
            .await
            .unwrap();
        assert!(scaling.last_shard_count_changes.is_empty());
    }

    #[tokio::test]
    async fn test_rebalance_after_scaling_preserves_target_count() {
        let (mut model, source) = scaling_model(3);
        let mut ingester = MockIngesterService::new();
        ingester.expect_close_shards().once().returning(|request| {
            Ok(quickwit_proto::ingest::ingester::CloseShardsResponse {
                successes: request.shard_pkeys,
            })
        });
        let pool = IngesterPool::default();
        pool.insert(
            NodeId::from_str("ingester"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(ingester)),
        );
        let mut controller = IngestController::new(MetastoreServiceClient::mocked(), pool);
        let mut scaling =
            ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
        scaling
            .scale_down_shards(
                &controller,
                &source,
                3,
                1,
                &FnvHashSet::from_iter([NodeId::from_str("ingester")]),
                &mut model,
                &Progress::default(),
            )
            .await;
        let universe = Universe::new();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        controller
            .rebalance_shards(&mut model, &mailbox, &Progress::default())
            .await
            .unwrap();
        assert_eq!(
            model
                .get_shards_for_source(&source)
                .unwrap()
                .values()
                .filter(|shard| shard.is_open())
                .count(),
            1
        );
        universe.assert_quit().await;
    }

    fn shard_reports_for_test(min_shards: usize) -> (ControlPlaneModel, ShardsUpdate) {
        let mut model = ControlPlaneModel::default();
        let mut metadata = IndexMetadata::for_test("index", "ram:///index");
        metadata.index_config.ingest_settings.min_shards =
            std::num::NonZeroUsize::new(min_shards).unwrap();
        let index_uid = metadata.index_uid.clone();
        model.add_index(metadata);
        let mut update = ShardsUpdate::default();
        for source_id in ["source-a", "source-b"] {
            model
                .add_source(
                    &index_uid,
                    SourceConfig::for_test(source_id, quickwit_config::SourceParams::void()),
                )
                .unwrap();
            model.insert_shards(
                &index_uid,
                &source_id.to_string(),
                vec![Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.to_string(),
                    shard_id: Some(ShardId::from(1)),
                    ingester_id: "ingester".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }],
            );
            update.shard_infos_by_source.push(
                SourceShardReport {
                    source_uid: SourceUid {
                        index_uid: index_uid.clone(),
                        source_id: source_id.to_string(),
                    },
                    shard_infos: BTreeSet::from([ShardInfo {
                        shard_id: ShardId::from(1),
                        shard_state: ShardState::Open,
                        short_term_ingestion_rate: ByteSize::b(123),
                        long_term_ingestion_rate: ByteSize::b(456),
                    }]),
                }
                .into(),
            );
        }
        (model, update)
    }

    #[tokio::test]
    async fn test_shards_update_sources_and_generation() {
        let pool = IngesterPool::default();
        let ingester = IngesterPoolEntry::ready_with_client(IngesterServiceClient::mocked());
        let generation = ingester.generation_id.as_u64();
        pool.insert(NodeId::from_str("ingester"), ingester);
        let controller = IngestController::new(MetastoreServiceClient::mocked(), pool);
        let scaling_controller =
            ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
        let (mut model, update) = shard_reports_for_test(1);
        for wrong_generation in [generation - 1, generation + 1] {
            scaling_controller.handle_shards_update(
                &controller,
                "ingester",
                wrong_generation,
                update.clone(),
                &mut model,
            );
            assert!(
                model
                    .all_shards()
                    .all(|shard| shard.short_term_ingestion_rate == ByteSize::default())
            );
        }
        scaling_controller.handle_shards_update(
            &controller,
            "ingester",
            generation,
            update.clone(),
            &mut model,
        );
        assert_eq!(model.all_shards().count(), 2);
        assert!(
            model
                .all_shards()
                .all(|shard| shard.short_term_ingestion_rate == ByteSize::b(123)
                    && shard.long_term_ingestion_rate == ByteSize::b(456))
        );

        let (mut model, _) = shard_reports_for_test(1);
        scaling_controller.handle_shards_update(
            &controller,
            "joining-ingester",
            generation,
            update,
            &mut model,
        );
        assert!(
            model
                .all_shards()
                .all(|shard| shard.short_term_ingestion_rate == ByteSize::b(123))
        );
    }

    #[tokio::test]
    async fn test_reconciliation_continues_only_after_certainly_aborted_errors() {
        for certainly_aborted in [true, false] {
            let (mut model, update) = shard_reports_for_test(2);
            let universe = Universe::new();
            let (mailbox, _inbox) = universe.create_test_mailbox();
            let mut mock_metastore = MockMetastoreService::new();
            mock_metastore
                .expect_open_shards()
                .times(if certainly_aborted { 2 } else { 1 })
                .returning(move |_| {
                    if certainly_aborted {
                        Err(MetastoreError::InvalidArgument {
                            message: "aborted".to_string(),
                        })
                    } else {
                        Err(MetastoreError::Connection {
                            message: "uncertain".to_string(),
                        })
                    }
                });
            let mut mock_ingester = MockIngesterService::new();
            mock_ingester
                .expect_init_shards()
                .times(if certainly_aborted { 2 } else { 1 })
                .returning(|request| {
                    Ok(InitShardsResponse {
                        successes: request
                            .subrequests
                            .into_iter()
                            .map(|request| InitShardSuccess {
                                subrequest_id: request.subrequest_id,
                                shard: request.shard,
                            })
                            .collect(),
                        failures: Vec::new(),
                    })
                });
            let pool = IngesterPool::default();
            pool.insert(
                NodeId::from_str("ingester"),
                IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(
                    mock_ingester,
                )),
            );
            let mut controller =
                IngestController::new(MetastoreServiceClient::from_mock(mock_metastore), pool);
            let mut scaling_controller =
                ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT);
            ScalingController::with_shard_throughput_limit(DEFAULT_SHARD_THROUGHPUT_LIMIT)
                .handle_shards_update(&controller, "ingester", 1, update, &mut model);
            let result = scaling_controller
                .reconcile_shards(&mut controller, &mut model, &mailbox, &Progress::default())
                .await;
            assert_eq!(result.is_ok(), certainly_aborted);
            let second_source = model
                .all_shards()
                .find(|shard| shard.source_id == "source-b")
                .unwrap();
            assert_eq!(second_source.short_term_ingestion_rate, ByteSize::b(123));
            universe.assert_quit().await;
        }
    }
}
