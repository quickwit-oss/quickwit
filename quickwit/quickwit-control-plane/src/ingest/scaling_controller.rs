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
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use fnv::FnvHashSet;
use quickwit_actors::Mailbox;
use quickwit_common::Progress;
use quickwit_ingest::{ShardInfo, ShardInfos};
use quickwit_proto::control_plane::ShardsUpdate;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::{NodeId, SourceUid};
use tracing::{error, info, warn};

use super::ingest_controller::{find_scale_down_candidate, open_shards_by_ingester_id};
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
    ingest_controller: Arc<IngestController>,
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
    pub fn new(
        ingest_controller: Arc<IngestController>,
        shard_throughput_limit: ByteSize,
    ) -> ScalingController {
        let shard_throughput_limit_bytes = shard_throughput_limit.as_u64();
        ScalingController {
            ingest_controller,
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
        node_id: &str,
        generation_id: u64,
        shards_update: ShardsUpdate,
        model: &mut ControlPlaneModel,
    ) {
        if let Some(ingester) = self.ingest_controller.ingester_pool.get(node_id)
            && generation_id != ingester.generation_id.as_u64()
        {
            return;
        }
        for shard_infos_by_source in shards_update.shard_infos_by_source {
            let source_uid = SourceUid {
                index_uid: shard_infos_by_source.index_uid().clone(),
                source_id: shard_infos_by_source.source_id,
            };
            let shard_infos: ShardInfos = shard_infos_by_source
                .shard_infos
                .iter()
                .map(ShardInfo::from)
                .collect();
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
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        // It's critical to only count shards from ingesters that can participate in ingestion,
        // to ensure there are enough shards to satisfy demands.
        let live_ingesters = self.ingest_controller.live_ingesters();
        let source_uids: Vec<SourceUid> = model
            .source_configs()
            .map(|(source_uid, _source_config)| source_uid)
            .collect();

        for source_uid in source_uids {
            let scale_source_shards_result = self
                .scale_source_shards(&source_uid, &live_ingesters, model, progress)
                .await;
            let Err(metastore_error) = scale_source_shards_result else {
                continue;
            };
            if !metastore_error.is_transaction_certainly_aborted() {
                return Err(metastore_error);
            }
            error!(error=?metastore_error, "failed to scale source shards");
        }
        self.ingest_controller
            .rebalance_shards(model, mailbox, progress)
            .await?;
        Ok(())
    }

    async fn scale_source_shards(
        &mut self,
        source_uid: &SourceUid,
        live_ingesters: &FnvHashSet<NodeId>,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let Some(shard_stats) = model.shard_stats(source_uid, live_ingesters) else {
            return Ok(());
        };
        let Some(min_shards) = model
            .index_metadata(&source_uid.index_uid)
            .map(|metadata| metadata.index_config.ingest_settings.min_shards)
        else {
            return Ok(());
        };
        let cooldown_expired = self.is_scale_down_cooldown_expired(source_uid, Instant::now());
        let scaling_decision_opt = self.should_scale(shard_stats, min_shards, cooldown_expired);
        let Some(scaling_decision) = scaling_decision_opt else {
            return Ok(());
        };
        let num_open_shards = shard_stats.num_open_shards;

        match scaling_decision {
            ScalingDecision::ScaleUp {
                target_num_open_shards,
            } => {
                self.scale_up_shards(
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
        source_uid: &SourceUid,
        num_open_shards: usize,
        target_num_open_shards: usize,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let num_shards_to_open = target_num_open_shards - num_open_shards;
        let num_opened_shards = self
            .ingest_controller
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

    async fn scale_down_shards(
        &mut self,
        source_uid: &SourceUid,
        num_open_shards: usize,
        target_num_open_shards: usize,
        live_ingesters: &FnvHashSet<NodeId>,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) {
        if self.ingest_controller.is_rebalancing() {
            return;
        }
        let num_shards_to_close = num_open_shards - target_num_open_shards;
        // TODO: Eventually this should consider the size of the shard queue.
        let mut open_shards_by_ingester_id =
            open_shards_by_ingester_id(source_uid, live_ingesters, model);
        let mut shards_to_close: Vec<Shard> = Vec::with_capacity(num_shards_to_close);

        for _ in 0..num_shards_to_close {
            let Some(shard_entry) = find_scale_down_candidate(&mut open_shards_by_ingester_id)
            else {
                break;
            };
            shards_to_close.push(shard_entry.shard.clone());
        }
        let closed_shard_ids = self
            .ingest_controller
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
        shard_stats: ShardStats,
        min_shards: NonZeroUsize,
        cooldown_expired: bool,
    ) -> Option<ScalingDecision> {
        let num_open_shards = shard_stats.num_open_shards;
        if num_open_shards == 0 {
            return None;
        }
        // Pick the higher of the short and long term rates, to be safe
        let ingestion_rate = shard_stats
            .total_short_term_ingestion_rate
            .max(shard_stats.total_long_term_ingestion_rate);
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

#[cfg(test)]
mod tests {
    use quickwit_config::SourceConfig;
    use quickwit_ingest::{IngesterPool, IngesterPoolEntry};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::ShardInfosBySource;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::{
        CloseShardsResponse, IngesterServiceClient, InitShardSuccess, InitShardsResponse,
        MockIngesterService,
    };
    use quickwit_proto::metastore::{
        MetastoreServiceClient, MockMetastoreService, OpenShardSubresponse, OpenShardsResponse,
    };
    use quickwit_proto::types::{IndexUid, ShardId};

    use super::*;
    use crate::model::ShardEntry;

    fn scaling_controller_for_test(
        metastore: MetastoreServiceClient,
        ingester_pool: IngesterPool,
    ) -> ScalingController {
        let ingest_controller = Arc::new(IngestController::new(metastore, ingester_pool));
        ScalingController::new(ingest_controller, ByteSize::b(100))
    }

    fn model_for_test(shards: &[(u64, ShardState, &str)]) -> (ControlPlaneModel, SourceUid) {
        let mut model = ControlPlaneModel::default();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);
        let source_config = SourceConfig::ingest_v2();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_config.source_id.clone(),
        };
        model.add_source(&index_uid, source_config).unwrap();
        let shards: Vec<Shard> = shards
            .iter()
            .map(|(shard_id, shard_state, ingester_id)| Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                shard_id: Some(ShardId::from(*shard_id)),
                shard_state: *shard_state as i32,
                ingester_id: ingester_id.to_string(),
                ..Default::default()
            })
            .collect();
        model.insert_shards(&index_uid, &source_uid.source_id, shards);
        (model, source_uid)
    }

    fn num_shards_in_state(
        model: &ControlPlaneModel,
        source_uid: &SourceUid,
        shard_state: ShardState,
    ) -> usize {
        model
            .get_shards_for_source(source_uid)
            .unwrap()
            .values()
            .filter(|shard_entry| shard_entry.shard_state() == shard_state)
            .count()
    }

    #[test]
    fn test_should_scale() {
        use ScalingDecision::{ScaleDown, ScaleUp};

        struct TestCase {
            num_open_shards: usize,
            min_shards: usize,
            short_term_rate: u64,
            long_term_rate: u64,
            cooldown_expired: bool,
            expected: Option<ScalingDecision>,
        }
        // With a 100B/s limit: target 80B/s per shard, scale up at >= 100, scale down below 40.
        let test_cases = [
            TestCase {
                num_open_shards: 0,
                min_shards: 1,
                short_term_rate: 1000,
                long_term_rate: 1000,
                cooldown_expired: true,
                expected: None,
            },
            TestCase {
                num_open_shards: 1,
                min_shards: 3,
                short_term_rate: 0,
                long_term_rate: 0,
                cooldown_expired: false,
                expected: Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 199,
                long_term_rate: 0,
                cooldown_expired: true,
                expected: None,
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 200,
                long_term_rate: 0,
                cooldown_expired: false,
                expected: Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 0,
                long_term_rate: 201,
                cooldown_expired: false,
                expected: Some(ScaleUp {
                    target_num_open_shards: 3,
                }),
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 0,
                long_term_rate: 80,
                cooldown_expired: true,
                expected: None,
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 79,
                long_term_rate: 0,
                cooldown_expired: true,
                expected: Some(ScaleDown {
                    target_num_open_shards: 1,
                }),
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 1,
                short_term_rate: 79,
                long_term_rate: 0,
                cooldown_expired: false,
                expected: None,
            },
            TestCase {
                num_open_shards: 2,
                min_shards: 2,
                short_term_rate: 0,
                long_term_rate: 0,
                cooldown_expired: true,
                expected: None,
            },
            TestCase {
                num_open_shards: 10,
                min_shards: 1,
                short_term_rate: 0,
                long_term_rate: 0,
                cooldown_expired: true,
                expected: Some(ScaleDown {
                    target_num_open_shards: 1,
                }),
            },
            TestCase {
                num_open_shards: 1,
                min_shards: 1,
                short_term_rate: 801,
                long_term_rate: 0,
                cooldown_expired: false,
                expected: Some(ScaleUp {
                    target_num_open_shards: 11,
                }),
            },
        ];
        let scaling_controller =
            scaling_controller_for_test(MetastoreServiceClient::mocked(), IngesterPool::default());

        for test_case in test_cases {
            let shard_stats = ShardStats {
                num_open_shards: test_case.num_open_shards,
                total_short_term_ingestion_rate: ByteSize::b(test_case.short_term_rate),
                total_long_term_ingestion_rate: ByteSize::b(test_case.long_term_rate),
            };
            let min_shards = NonZeroUsize::new(test_case.min_shards).unwrap();
            assert_eq!(
                scaling_controller.should_scale(
                    shard_stats,
                    min_shards,
                    test_case.cooldown_expired
                ),
                test_case.expected,
                "num_open_shards={}, min_shards={}, short_term_rate={}, long_term_rate={}, \
                 cooldown_expired={}",
                test_case.num_open_shards,
                test_case.min_shards,
                test_case.short_term_rate,
                test_case.long_term_rate,
                test_case.cooldown_expired,
            );
        }
    }

    #[test]
    fn test_scale_down_cooldown() {
        let mut scaling_controller =
            scaling_controller_for_test(MetastoreServiceClient::mocked(), IngesterPool::default());
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "source-a".to_string(),
        };
        let other_source_uid = SourceUid {
            source_id: "source-b".to_string(),
            ..source_uid.clone()
        };
        let now = Instant::now();
        assert!(scaling_controller.is_scale_down_cooldown_expired(&source_uid, now));

        scaling_controller.restart_scale_down_cooldown(&source_uid, now);
        assert!(!scaling_controller.is_scale_down_cooldown_expired(
            &source_uid,
            now + SCALE_DOWN_COOLDOWN - Duration::from_nanos(1)
        ));
        assert!(
            scaling_controller
                .is_scale_down_cooldown_expired(&source_uid, now + SCALE_DOWN_COOLDOWN)
        );
        assert!(scaling_controller.is_scale_down_cooldown_expired(&other_source_uid, now));
    }

    #[test]
    fn test_handle_shards_update_checks_generation() {
        let ingester_pool = IngesterPool::default();
        let ingester = IngesterPoolEntry::mocked_ingester();
        let generation_id = ingester.generation_id.as_u64();
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);
        let scaling_controller =
            scaling_controller_for_test(MetastoreServiceClient::mocked(), ingester_pool);
        let (mut model, source_uid) = model_for_test(&[(1, ShardState::Open, "test-ingester")]);
        let shards_update = ShardsUpdate {
            shard_infos_by_source: vec![ShardInfosBySource {
                index_uid: Some(source_uid.index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                shard_infos: vec![quickwit_proto::control_plane::ShardInfo {
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    short_term_ingestion_rate_bytes_per_sec: 123,
                    long_term_ingestion_rate_bytes_per_sec: 456,
                }],
            }],
        };

        // Updates from another generation of the ingester are ignored.
        scaling_controller.handle_shards_update(
            "test-ingester",
            generation_id + 1,
            shards_update.clone(),
            &mut model,
        );
        let shard_entry = model.all_shards().next().unwrap();
        assert_eq!(shard_entry.short_term_ingestion_rate, ByteSize::b(0));

        scaling_controller.handle_shards_update(
            "test-ingester",
            generation_id,
            shards_update,
            &mut model,
        );
        let shard_entry = model.all_shards().next().unwrap();
        assert_eq!(shard_entry.short_term_ingestion_rate, ByteSize::b(123));
        assert_eq!(shard_entry.long_term_ingestion_rate, ByteSize::b(456));
    }

    #[tokio::test]
    async fn test_scale_up_shards() {
        let (mut model, source_uid) = model_for_test(&[(1, ShardState::Open, "test-ingester")]);

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                let subresponses = request
                    .subrequests
                    .into_iter()
                    .map(|subrequest| OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(Shard {
                            index_uid: subrequest.index_uid,
                            source_id: subrequest.source_id,
                            shard_id: subrequest.shard_id,
                            ingester_id: subrequest.ingester_id,
                            shard_state: ShardState::Open as i32,
                            ..Default::default()
                        }),
                    })
                    .collect();
                Ok(OpenShardsResponse { subresponses })
            });
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(|request| {
                let successes = request
                    .subrequests
                    .into_iter()
                    .map(|subrequest| InitShardSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        shard: subrequest.shard,
                    })
                    .collect();
                Ok(InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                })
            });
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester)),
        );
        let mut scaling_controller = scaling_controller_for_test(
            MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool,
        );

        scaling_controller
            .scale_up_shards(&source_uid, 1, 3, &mut model, &Progress::default())
            .await
            .unwrap();

        assert_eq!(
            num_shards_in_state(&model, &source_uid, ShardState::Open),
            3
        );
        assert!(!scaling_controller.is_scale_down_cooldown_expired(&source_uid, Instant::now()));
    }

    #[tokio::test]
    async fn test_scale_down_shards() {
        let (mut model, source_uid) = model_for_test(&[
            (1, ShardState::Open, "test-ingester"),
            (2, ShardState::Open, "test-ingester"),
            (3, ShardState::Open, "test-ingester"),
        ]);
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shard_pkeys.len(), 2);
                Ok(CloseShardsResponse {
                    successes: request.shard_pkeys,
                })
            });
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester)),
        );
        let mut scaling_controller =
            scaling_controller_for_test(MetastoreServiceClient::mocked(), ingester_pool);
        let live_ingesters = FnvHashSet::from_iter([NodeId::from_str("test-ingester")]);

        scaling_controller
            .scale_down_shards(
                &source_uid,
                3,
                1,
                &live_ingesters,
                &mut model,
                &Progress::default(),
            )
            .await;

        assert_eq!(
            num_shards_in_state(&model, &source_uid, ShardState::Open),
            1
        );
        assert_eq!(
            num_shards_in_state(&model, &source_uid, ShardState::Closed),
            2
        );
        assert!(!scaling_controller.is_scale_down_cooldown_expired(&source_uid, Instant::now()));
    }

    #[test]
    fn test_find_scale_down_candidates() {
        let (model, source_uid) = model_for_test(&[
            (1, ShardState::Open, "busy-ingester"),
            (2, ShardState::Open, "busy-ingester"),
            (3, ShardState::Open, "busy-ingester"),
            (4, ShardState::Open, "idle-ingester"),
            (5, ShardState::Closed, "idle-ingester"),
            (6, ShardState::Unavailable, "busy-ingester"),
            (7, ShardState::Open, "departed-ingester"),
        ]);
        let live_ingesters = FnvHashSet::from_iter([
            NodeId::from_str("busy-ingester"),
            NodeId::from_str("idle-ingester"),
        ]);

        let mut open_shards_by_ingester_id =
            open_shards_by_ingester_id(&source_uid, &live_ingesters, &model);
        let mut candidates: Vec<&ShardEntry> = Vec::new();
        while let Some(shard_entry) = find_scale_down_candidate(&mut open_shards_by_ingester_id) {
            candidates.push(shard_entry);
        }

        // Shards are taken off the ingester with the most open shards first.
        assert_eq!(candidates[0].ingester_id, "busy-ingester");
        assert_eq!(candidates[1].ingester_id, "busy-ingester");

        // Only open shards on live ingesters are candidates.
        let mut candidate_shard_ids: Vec<u64> = candidates
            .iter()
            .map(|shard_entry| shard_entry.shard_id().as_u64().unwrap())
            .collect();
        candidate_shard_ids.sort_unstable();
        assert_eq!(candidate_shard_ids, [1, 2, 3, 4]);
    }
}
