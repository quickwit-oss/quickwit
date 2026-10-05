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
use crate::model::{ControlPlaneModel, ShardThroughputStats};

const SCALE_DOWN_COOLDOWN: Duration = Duration::from_mins(5);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ScalingDecision {
    ScaleUp { target_num_open_shards: usize },
    ScaleDown { target_num_open_shards: usize },
}

pub(crate) struct ScalingController {
    target_shard_throughput: ByteSize,
    scale_up_shard_throughput_threshold: ByteSize,
    scale_down_shard_throughput_threshold: ByteSize,
    last_shard_count_changes: HashMap<SourceUid, Instant>,
}

impl ScalingController {
    pub fn with_shard_throughput_limit(shard_throughput_limit: ByteSize) -> ScalingController {
        let shard_throughput_limit_bytes = shard_throughput_limit.as_u64();
        ScalingController {
            target_shard_throughput: ByteSize::b(shard_throughput_limit_bytes * 8 / 10),
            scale_up_shard_throughput_threshold: shard_throughput_limit,
            scale_down_shard_throughput_threshold: ByteSize::b(shard_throughput_limit_bytes / 2),
            last_shard_count_changes: HashMap::new(),
        }
    }

    fn is_scale_down_cooldown_expired(&self, source_uid: &SourceUid, now: Instant) -> bool {
        let Some(last_shard_count_change) = self.last_shard_count_changes.get(source_uid) else {
            return true;
        };
        now.duration_since(*last_shard_count_change) >= SCALE_DOWN_COOLDOWN
    }

    fn restart_scale_down_cooldown(&mut self, source_uid: &SourceUid, now: Instant) {
        self.last_shard_count_changes
            .insert(source_uid.clone(), now);
    }

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

    pub(crate) async fn reconcile_shards(
        &mut self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<()> {
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
        let min_shards = model
            .index_metadata(&source_uid.index_uid)
            .expect("index should exist")
            .index_config
            .ingest_settings
            .min_shards;
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

    pub fn should_scale(
        &self,
        shard_throughput_stats: ShardThroughputStats,
        min_shards: NonZeroUsize,
        cooldown_expired: bool,
    ) -> Option<ScalingDecision> {
        let num_open_shards = shard_throughput_stats.num_open_shards;
        if num_open_shards == 0 {
            return None;
        }
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
        let scale_up_ingestion_rate =
            self.scale_up_shard_throughput_threshold * num_open_shards as u64;
        if ingestion_rate > scale_up_ingestion_rate {
            let scale_up = ScalingDecision::ScaleUp {
                target_num_open_shards,
            };
            return Some(scale_up);
        }
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
    use quickwit_proto::types::{NodeId, ShardId, SourceUid};

    use super::{IngestController, ScalingController};
    use crate::ingest::LegacyScalingController;
    use crate::model::ControlPlaneModel;

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
            let scaling_controller =
                LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.5);
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
