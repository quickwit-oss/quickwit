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

use bytesize::ByteSize;
use quickwit_actors::Mailbox;
use quickwit_common::Progress;
use quickwit_ingest::ShardInfos;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::{NodeId, SourceUid};
use rand::prelude::IndexedRandom;
use rand::{Rng, rng};
use tracing::{info, warn};

use super::legacy_scaling_arbiter::{LegacyScalingArbiter, ScalingMode};
use crate::control_plane::ControlPlane;
use crate::ingest::IngestController;
use crate::model::{ControlPlaneModel, ShardEntry, ShardStats};

pub(crate) struct LegacyScalingController {
    legacy_scaling_arbiter: LegacyScalingArbiter,
}

impl LegacyScalingController {
    pub fn new(max_shard_ingestion_throughput: ByteSize, shard_scale_up_factor: f32) -> Self {
        LegacyScalingController {
            legacy_scaling_arbiter: LegacyScalingArbiter::with_max_shard_ingestion_throughput(
                max_shard_ingestion_throughput,
                shard_scale_up_factor,
            ),
        }
    }

    /// This is the entrypoint for receiving a gossip update from an indexer. Historically, the
    /// control plane would scale shards on every update; this preserves that. The new scaling
    /// controller does so on an interval.
    pub(crate) async fn update_local_shards(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        source_uid: SourceUid,
        shard_infos: &ShardInfos,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        model.update_shards(&source_uid, shard_infos);
        self.scale_source_shards(ingest_controller, source_uid, model, progress)
            .await
    }

    pub(crate) async fn reconcile_shards(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        ingest_controller
            .rebalance_shards(model, mailbox, progress)
            .await?;
        Ok(())
    }

    async fn scale_source_shards(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        source_uid: SourceUid,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let Some(shard_stats) = model.legacy_shard_stats(&source_uid) else {
            return Ok(());
        };
        let Some(min_shards) = model
            .index_metadata(&source_uid.index_uid)
            .map(|index_metadata| index_metadata.index_config.ingest_settings.min_shards)
        else {
            warn!(
                index_uid=%source_uid.index_uid,
                "ignoring local shards update for a deleted index"
            );
            return Ok(());
        };

        let Some(scaling_mode) = self
            .legacy_scaling_arbiter
            .should_scale(shard_stats, min_shards)
        else {
            return Ok(());
        };
        match scaling_mode {
            ScalingMode::Up(num_shards) => {
                self.try_scale_up_shards(
                    ingest_controller,
                    source_uid,
                    shard_stats,
                    model,
                    progress,
                    num_shards,
                )
                .await?;
            }
            ScalingMode::Down => {
                self.try_scale_down_shards(
                    ingest_controller,
                    source_uid,
                    shard_stats,
                    min_shards,
                    model,
                    progress,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Attempts to increase the number of shards. This operation is rate limited to avoid creating
    /// to many shards in a short period of time. As a result, this method may not create any
    /// shard.
    async fn try_scale_up_shards(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &mut IngestController,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        model: &mut ControlPlaneModel,
        progress: &Progress,
        num_shards_to_open: usize,
    ) -> MetastoreResult<()> {
        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let new_num_open_shards = shard_stats.num_open_shards + num_shards_to_open;
        let open_shards_result = ingest_controller
            .open_shards_for_source(&source_uid, num_shards_to_open, model, progress)
            .await;

        match open_shards_result {
            Ok(num_opened_shards) => {
                if num_opened_shards == 0 {
                    // We did not manage to create the shard.
                    // We can release our permit.
                    model.release_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open));
                    warn!(
                        index_uid=%source_uid.index_uid,
                        source_id=%source_uid.source_id,
                        "scaling up number of shards to {new_num_open_shards} failed: shard initialization failure"
                    );
                } else {
                    info!(
                        index_id=%source_uid.index_uid.index_id,
                        source_id=%source_uid.source_id,
                        "successfully scaled up number of shards to {new_num_open_shards}"
                    );
                }
                Ok(())
            }
            Err(metastore_error) => {
                // We did not manage to create the shard.
                // We can release our permit, but we also need to return the error to the caller, in
                // order to restart the control plane actor if necessary.
                warn!(
                    index_id=%source_uid.index_uid.index_id,
                    source_id=%source_uid.source_id,
                    "scaling up number of shards to {new_num_open_shards} failed: {metastore_error:?}"
                );
                model.release_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open));
                Err(metastore_error)
            }
        }
    }

    /// Attempts to decrease the number of shards. This operation is rate limited to avoid closing
    /// shards too aggressively. As a result, this method may not close any shard.
    async fn try_scale_down_shards(
        &self,
        // TODO: hold the ingest controller on the struct instead of passing it in.
        ingest_controller: &IngestController,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        min_shards: NonZeroUsize,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        // The scaling arbiter should not suggest scaling down if the number of shards is already
        // below the minimum, but we're just being defensive here.
        if ingest_controller.is_rebalancing() || shard_stats.num_open_shards <= min_shards.get() {
            return Ok(());
        }
        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Down)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let new_num_open_shards = shard_stats.num_open_shards - 1;

        info!(
            index_id=%source_uid.index_uid.index_id,
            source_id=%source_uid.source_id,
            "scaling down number of shards to {new_num_open_shards}"
        );
        let Some(shard) = find_scale_down_candidate(&source_uid, model) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
            return Ok(());
        };
        info!(
            "scaling down shard {} from {}",
            shard.shard_id(),
            shard.ingester_id
        );
        let closed_shard_ids = ingest_controller
            .close_source_shards(&source_uid, vec![shard], model, progress)
            .await;

        if closed_shard_ids.is_empty() {
            warn!("failed to scale down number of shards");
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
        }
        Ok(())
    }
}

/// Finds a shard on the ingester with the highest number of open
/// shards for this source.
///
/// If multiple shards are hosted on that ingester, the shard with the lowest (oldest)
/// shard ID is chosen.
fn find_scale_down_candidate(source_uid: &SourceUid, model: &ControlPlaneModel) -> Option<Shard> {
    let mut shard_entries_by_ingester_id: HashMap<NodeId, Vec<&ShardEntry>> = HashMap::new();
    let mut rng = rng();

    for shard in model.get_shards_for_source(source_uid)?.values() {
        if shard.is_open() {
            shard_entries_by_ingester_id
                .entry(NodeId::from_str(&shard.ingester_id))
                .or_default()
                .push(shard);
        }
    }
    shard_entries_by_ingester_id
        .into_iter()
        // We use a random number to break ties... The HashMap is randomly seeded so this is
        // should not make much difference, but we might want to be as explicit as possible.
        .max_by_key(|(_ingester_id, shard_entries)| (shard_entries.len(), rng.next_u32()))
        .map(|(_ingester_id, shard_entries)| shard_entries.choose(&mut rng).unwrap().shard.clone())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::str::FromStr;

    use bytesize::ByteSize;
    use quickwit_common::Progress;
    use quickwit_common::shared_consts::DEFAULT_SHARD_THROUGHPUT_LIMIT;
    use quickwit_config::{INGEST_V2_SOURCE_ID, SourceConfig};
    use quickwit_ingest::{IngesterPool, IngesterPoolEntry, ShardInfo};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::ingest::ingester::{
        CloseShardsResponse, IngesterServiceClient, InitShardSubrequest, InitShardSuccess,
        InitShardsRequest, InitShardsResponse, MockIngesterService,
    };
    use quickwit_proto::ingest::{IngestV2Error, Shard, ShardState};
    use quickwit_proto::metastore;
    use quickwit_proto::metastore::{
        MetastoreError, MetastoreServiceClient, MockMetastoreService, OpenShardSubrequest,
        OpenShardSubresponse, OpenShardsResponse,
    };
    use quickwit_proto::types::{IndexUid, NodeId, Position, ShardId, SourceId, SourceUid};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_unacknowledged_closes_leave_permits_reusable() {
        for outcome in ["empty", "timeout", "acknowledged"] {
            let index = IndexUid::for_test("index", 0);
            let source = SourceUid {
                index_uid: index.clone(),
                source_id: "source".to_string(),
            };
            let mut model = ControlPlaneModel::default();
            model.insert_shards(
                &index,
                &source.source_id,
                (1..=2)
                    .map(|id| Shard {
                        index_uid: Some(index.clone()),
                        source_id: source.source_id.clone(),
                        shard_id: Some(ShardId::from(id)),
                        ingester_id: "ingester".to_string(),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    })
                    .collect(),
            );
            let mut mock = MockIngesterService::new();
            mock.expect_close_shards()
                .times(if outcome == "timeout" { 0 } else { 1 })
                .returning(move |request| {
                    Ok(CloseShardsResponse {
                        successes: if outcome == "acknowledged" {
                            request.shard_pkeys
                        } else {
                            Vec::new()
                        },
                    })
                });
            let client = if outcome == "timeout" {
                IngesterServiceClient::tower()
                    .stack_close_shards_layer(quickwit_common::tower::DelayLayer::new(
                        std::time::Duration::from_secs(60),
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
            let scaling = LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.5);
            scaling
                .try_scale_down_shards(
                    &controller,
                    source.clone(),
                    ShardStats {
                        num_open_shards: 2,
                        ..Default::default()
                    },
                    NonZeroUsize::MIN,
                    &mut model,
                    &Progress::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                model.all_shards().filter(|shard| shard.is_closed()).count(),
                usize::from(outcome == "acknowledged")
            );
            assert_eq!(
                model.acquire_scaling_permits(&source, ScalingMode::Down),
                Some(outcome != "acknowledged")
            );
        }
    }

    #[tokio::test]
    async fn test_handle_shards_update_for_deleted_index() {
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let ingester_pool = IngesterPool::default();
        let mut controller = IngestController::new(metastore, ingester_pool);
        let scaling_controller =
            LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.001);
        let mut model = ControlPlaneModel::default();
        let source_shard_report = quickwit_ingest::SourceShardReport {
            source_uid: SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            shard_infos: BTreeSet::new(),
        };
        scaling_controller
            .update_local_shards(
                &mut controller,
                source_shard_report.source_uid,
                &source_shard_report.shard_infos,
                &mut model,
                &Progress::default(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingest_controller_update_source_shards() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];

                assert_eq!(subrequest.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.ingester_id, "test-ingester");

                Err(MetastoreError::InvalidArgument {
                    message: "failed to open shards".to_string(),
                })
            });
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest: &OpenShardSubrequest = &request.subrequests[0];

                assert_eq!(subrequest.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.ingester_id, "test-ingester");

                let shard = Shard {
                    index_uid: subrequest.index_uid.clone(),
                    source_id: subrequest.source_id.clone(),
                    shard_id: subrequest.shard_id.clone(),
                    shard_state: ShardState::Open as i32,
                    ingester_id: subrequest.ingester_id.clone(),
                    doc_mapping_uid: subrequest.doc_mapping_uid,
                    publish_position_inclusive: Some(Position::Beginning),
                    publish_token: None,
                    update_timestamp: 1724158996,
                };
                let response = OpenShardsResponse {
                    subresponses: vec![OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(shard),
                    }],
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();

        let mut controller = IngestController::new(metastore, ingester_pool.clone());
        let scaling_controller =
            LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.001);

        let index_uid = IndexUid::for_test("test-index", 0);
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let source_id: SourceId = "test-source".to_string();
        index_metadata.sources.insert(
            source_id.clone(),
            SourceConfig::for_test(&source_id, quickwit_config::SourceParams::void()),
        );

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);
        let progress = Progress::default();

        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            ingester_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);
        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();

        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].short_term_ingestion_rate, ByteSize::mib(0));

        // Test update shard ingestion rate but no scale down because num open shards is 1.
        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: ByteSize::mib(1),
            long_term_ingestion_rate: ByteSize::mib(1),
        }]);

        scaling_controller
            .update_local_shards(
                &mut controller,
                source_uid.clone(),
                &shard_infos,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();
        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].short_term_ingestion_rate, ByteSize::mib(1));

        // Test update shard ingestion rate with failing scale down.
        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            ingester_id: "test-ingester".to_string(),
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();
        assert_eq!(shard_entries.len(), 2);

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester.expect_init_shards().returning(
            move |init_shard_request: InitShardsRequest| {
                assert_eq!(init_shard_request.subrequests.len(), 1);
                let init_shard_subrequest: &InitShardSubrequest =
                    &init_shard_request.subrequests[0];
                assert!(init_shard_subrequest.validate_docs);
                Ok(InitShardsResponse {
                    successes: vec![InitShardSuccess {
                        subrequest_id: init_shard_subrequest.subrequest_id,
                        shard: init_shard_subrequest.shard.clone(),
                    }],
                    failures: Vec::new(),
                })
            },
        );
        mock_ingester
            .expect_close_shards()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                Err(IngestV2Error::Internal(
                    "failed to close shards".to_string(),
                ))
            });
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(1),
                long_term_ingestion_rate: ByteSize::mib(1),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(1),
                long_term_ingestion_rate: ByteSize::mib(1),
            },
        ]);
        scaling_controller
            .update_local_shards(
                &mut controller,
                source_uid.clone(),
                &shard_infos,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        // Test update shard ingestion rate with failing scale up.
        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(4),
                long_term_ingestion_rate: ByteSize::mib(4),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(4),
                long_term_ingestion_rate: ByteSize::mib(4),
            },
        ]);

        // The first request fails due to an error on the metastore.
        let MetastoreError::InvalidArgument { .. } = scaling_controller
            .update_local_shards(
                &mut controller,
                source_uid.clone(),
                &shard_infos,
                &mut model,
                &progress,
            )
            .await
            .unwrap_err()
        else {
            panic!();
        };

        // The second request works!
        scaling_controller
            .update_local_shards(
                &mut controller,
                source_uid.clone(),
                &shard_infos,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingest_controller_disable_validation_when_vrl() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                let subrequest: &OpenShardSubrequest = &request.subrequests[0];
                let shard = Shard {
                    index_uid: subrequest.index_uid.clone(),
                    source_id: subrequest.source_id.clone(),
                    shard_id: subrequest.shard_id.clone(),
                    shard_state: ShardState::Open as i32,
                    ingester_id: subrequest.ingester_id.clone(),
                    doc_mapping_uid: subrequest.doc_mapping_uid,
                    publish_position_inclusive: Some(Position::Beginning),
                    publish_token: None,
                    update_timestamp: 1724158996,
                };
                let response = OpenShardsResponse {
                    subresponses: vec![OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(shard),
                    }],
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();

        let mut controller = IngestController::new(metastore, ingester_pool.clone());
        let scaling_controller =
            LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.001);

        let index_uid = IndexUid::for_test("test-index", 0);
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let source_id: SourceId = "test-source".to_string();
        let mut source_config =
            SourceConfig::for_test(&source_id, quickwit_config::SourceParams::void());
        // set a vrl script
        source_config.transform_config =
            Some(quickwit_config::TransformConfig::new("".to_string(), None));
        index_metadata
            .sources
            .insert(source_id.clone(), source_config);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);
        let progress = Progress::default();

        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            ingester_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let mut mock_ingester = MockIngesterService::new();

        mock_ingester.expect_init_shards().returning(
            move |init_shard_request: InitShardsRequest| {
                assert_eq!(init_shard_request.subrequests.len(), 1);
                let init_shard_subrequest: &InitShardSubrequest =
                    &init_shard_request.subrequests[0];
                // we have vrl, so no validation
                assert!(!init_shard_subrequest.validate_docs);
                Ok(InitShardsResponse {
                    successes: vec![InitShardSuccess {
                        subrequest_id: init_shard_subrequest.subrequest_id,
                        shard: init_shard_subrequest.shard.clone(),
                    }],
                    failures: Vec::new(),
                })
            },
        );

        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: ByteSize::mib(4),
            long_term_ingestion_rate: ByteSize::mib(4),
        }]);

        scaling_controller
            .update_local_shards(
                &mut controller,
                source_uid.clone(),
                &shard_infos,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_up_shards() {
        let mut mock_metastore = MockMetastoreService::new();

        let index_uid = IndexUid::from_str("test-index:00000000000000000000000000").unwrap();
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid(), &index_uid_clone);
                assert_eq!(request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(request.subrequests[0].ingester_id, "test-ingester");

                Err(MetastoreError::InvalidArgument {
                    message: "failed to open shards".to_string(),
                })
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_open_shards()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid(), &index_uid_clone);
                assert_eq!(request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(request.subrequests[0].ingester_id, "test-ingester");

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        ingester_id: "test-ingester".to_string(),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    }),
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let ingester_pool = IngesterPool::default();

        let mut controller = IngestController::new(metastore, ingester_pool.clone());
        let scaling_controller =
            LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.001);

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = INGEST_V2_SOURCE_ID.to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let mut model = ControlPlaneModel::default();
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram://indexes/test-index:0");
        model.add_index(index_metadata);

        let source_config = SourceConfig::ingest_v2();
        model.add_source(&index_uid, source_config).unwrap();

        let progress = Progress::default();

        // Test could not find ingester because no ingester in pool
        scaling_controller
            .try_scale_up_shards(
                &mut controller,
                source_uid.clone(),
                shard_stats,
                &mut model,
                &progress,
                1,
            )
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                let shard = request.subrequests[0].shard();
                assert_eq!(shard.index_uid(), &index_uid_clone);
                assert_eq!(shard.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(shard.ingester_id, "test-ingester");

                Err(IngestV2Error::Internal("failed to init shards".to_string()))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_init_shards()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                let shard = subrequest.shard();
                assert_eq!(shard.index_uid(), &index_uid_clone);
                assert_eq!(shard.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(shard.ingester_id, "test-ingester");

                let successes = vec![InitShardSuccess {
                    subrequest_id: request.subrequests[0].subrequest_id,
                    shard: Some(shard.clone()),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        // Test failed to open shards.
        scaling_controller
            .try_scale_up_shards(
                &mut controller,
                source_uid.clone(),
                shard_stats,
                &mut model,
                &progress,
                1,
            )
            .await
            .unwrap();
        assert_eq!(model.all_shards().count(), 0);

        // Test failed to init shards.
        scaling_controller
            .try_scale_up_shards(
                &mut controller,
                source_uid.clone(),
                shard_stats,
                &mut model,
                &progress,
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(model.all_shards().count(), 0);

        // Test successfully opened shard.
        scaling_controller
            .try_scale_up_shards(
                &mut controller,
                source_uid.clone(),
                shard_stats,
                &mut model,
                &progress,
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            model.all_shards().filter(|shard| shard.is_open()).count(),
            1
        );
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_down_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();

        let controller = IngestController::new(metastore, ingester_pool.clone());
        let scaling_controller =
            LegacyScalingController::new(DEFAULT_SHARD_THROUGHPUT_LIMIT, 1.001);

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let min_shards = NonZeroUsize::MIN;
        let mut model = ControlPlaneModel::default();
        let progress = Progress::default();

        model.insert_shards(&index_uid, &source_id, Vec::new());
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                NonZeroUsize::new(2).unwrap(),
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert_eq!(
            model.acquire_scaling_permits(&source_uid, ScalingMode::Down),
            Some(true)
        );
        model.release_scaling_permits(&source_uid, ScalingMode::Down);

        // Test could not find a scale down candidate.
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(1)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            ingester_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        // Test ingester is unavailable.
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_close_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                assert_eq!(request.shard_pkeys[0].shard_id(), ShardId::from(1));

                Err(IngestV2Error::Internal(
                    "failed to close shards".to_string(),
                ))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_close_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                assert_eq!(request.shard_pkeys[0].shard_id(), ShardId::from(1));

                let response = CloseShardsResponse {
                    successes: request.shard_pkeys,
                };
                Ok(response)
            });
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        // Test failed to close shard.
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().all(|shard| shard.is_open()));

        // Test successfully closed shard.
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().all(|shard| shard.is_closed()));

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(2)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            ingester_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        // Test rate limited.
        scaling_controller
            .try_scale_down_shards(
                &controller,
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().any(|shard| shard.is_open()));
    }

    #[test]
    fn test_find_scale_down_candidate() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();

        assert!(find_scale_down_candidate(&source_uid, &model).is_none());

        let shards = vec![
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Closed as i32, //< this one is closed
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(4)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(5)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(6)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
        ];
        // That's 3 open shards on indexer-1, 2 open shard and one closed shard on indexer-0..
        model.insert_shards(&index_uid, &source_id, shards);

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(1),
                long_term_ingestion_rate: ByteSize::mib(1),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(2),
                long_term_ingestion_rate: ByteSize::mib(2),
            },
            ShardInfo {
                shard_id: ShardId::from(3),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(3),
                long_term_ingestion_rate: ByteSize::mib(3),
            },
            ShardInfo {
                shard_id: ShardId::from(4),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(4),
                long_term_ingestion_rate: ByteSize::mib(4),
            },
            ShardInfo {
                shard_id: ShardId::from(5),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(5),
                long_term_ingestion_rate: ByteSize::mib(5),
            },
            ShardInfo {
                shard_id: ShardId::from(6),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: ByteSize::mib(6),
                long_term_ingestion_rate: ByteSize::mib(6),
            },
        ]);
        model.update_shards(&source_uid, &shard_infos);

        let shard = find_scale_down_candidate(&source_uid, &model).unwrap();
        // We pick ingester 1 has it has more open shard
        assert_eq!(shard.ingester_id, "test-ingester-1");
    }
}
