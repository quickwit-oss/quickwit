// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, GetOpenShardsRequest, GetOpenShardsSubrequest,
};
use quickwit_proto::ingest::ingester::{IngesterService, PersistRequest, PersistSubrequest};
use quickwit_proto::ingest::router::{IngestRequestV2, IngestResponseV2, IngestRouterService};
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::types::NodeId;
use quickwit_proto::IndexUid;
use tokio::sync::RwLock;

use super::shard_table::ShardTable;
use super::{DocBatchBuilderV2, IngesterPool};

type LeaderId = String;

#[derive(Clone)]
pub struct IngestRouter {
    self_node_id: NodeId,
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
    state: Arc<RwLock<RouterState>>,
    replication_factor: usize,
}

struct RouterState {
    shard_table: ShardTable,
}

impl fmt::Debug for IngestRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestRouter")
            .field("self_node_id", &self.self_node_id)
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

impl IngestRouter {
    pub fn new(
        self_node_id: NodeId,
        control_plane: ControlPlaneServiceClient,
        ingester_pool: IngesterPool,
        replication_factor: usize,
    ) -> Self {
        let state = RouterState {
            shard_table: ShardTable::default(),
        };
        Self {
            self_node_id,
            control_plane,
            ingester_pool,
            state: Arc::new(RwLock::new(state)),
            replication_factor,
        }
    }

    async fn refresh_shard_table(
        &mut self,
        ingest_request: &IngestRequestV2,
    ) -> IngestV2Result<()> {
        let state_guard = self.state.read().await;

        let shard_table = &state_guard.shard_table;
        let mut get_open_shards_subrequests = Vec::new();

        for ingest_subrequest in &ingest_request.subrequests {
            if !shard_table
                .contains_entry(&*ingest_subrequest.index_id, &ingest_subrequest.source_id)
            {
                let subrequest = GetOpenShardsSubrequest {
                    index_id: ingest_subrequest.index_id.clone(),
                    source_id: ingest_subrequest.source_id.clone(),
                };
                get_open_shards_subrequests.push(subrequest);
            }
        }
        if get_open_shards_subrequests.is_empty() {
            return Ok(());
        }
        drop(state_guard);

        let request = GetOpenShardsRequest {
            subrequests: get_open_shards_subrequests,
            unavailable_ingesters: Vec::new(),
        };
        let response = self.control_plane.get_open_shards(request).await?;

        let mut state_guard = self.state.write().await;

        for subresponse in response.subresponses {
            let index_uid: IndexUid = subresponse.index_uid.into();
            let index_id = index_uid.index_id().to_string();
            state_guard.shard_table.update_entry(
                index_id,
                subresponse.source_id,
                subresponse.open_shards,
            );
        }
        Ok(())
    }
}

#[async_trait]
impl IngestRouterService for IngestRouter {
    async fn ingest(
        &mut self,
        ingest_request: IngestRequestV2,
    ) -> IngestV2Result<IngestResponseV2> {
        self.refresh_shard_table(&ingest_request).await?;

        let mut doc_batch_builders: Vec<DocBatchBuilderV2> = Vec::new();
        let mut persist_subrequests: HashMap<&LeaderId, Vec<PersistSubrequest>> = HashMap::new();

        let state_guard = self.state.read().await;

        // TODO: Here would be the most optimal place to split the body of the HTTP request into
        // lines, validate, transform and then pack the docs into compressed batches routed
        // to the right shards.
        for ingest_subrequest in ingest_request.subrequests {
            let table_entry = state_guard
                .shard_table
                .find_entry(&*ingest_subrequest.index_id, &ingest_subrequest.source_id)
                .expect("TODO");

            if table_entry.len() == 1 {
                let shard = &table_entry.shards()[0];
                let persist_subrequest = PersistSubrequest {
                    index_uid: shard.index_uid.clone(),
                    source_id: ingest_subrequest.source_id,
                    shard_id: shard.shard_id,
                    follower_id: shard.follower_id.clone(),
                    doc_batch: ingest_subrequest.doc_batch,
                };
                persist_subrequests
                    .entry(&shard.leader_id)
                    .or_default()
                    .push(persist_subrequest);
                continue;
            }
            doc_batch_builders.resize_with(table_entry.len(), DocBatchBuilderV2::default);

            for (i, doc) in ingest_subrequest.docs().enumerate() {
                let shard_idx = i % table_entry.len();
                doc_batch_builders[shard_idx].add_doc(doc.borrow());
            }
            for (shard, doc_batch_builder) in table_entry
                .shards()
                .iter()
                .zip(doc_batch_builders.drain(..))
            {
                if !doc_batch_builder.is_empty() {
                    let doc_batch = doc_batch_builder.build();
                    let persist_subrequest = PersistSubrequest {
                        index_uid: shard.index_uid.clone(),
                        source_id: ingest_subrequest.source_id.clone(),
                        shard_id: shard.shard_id,
                        follower_id: shard.follower_id.clone(),
                        doc_batch: Some(doc_batch),
                    };
                    persist_subrequests
                        .entry(&shard.leader_id)
                        .or_default()
                        .push(persist_subrequest);
                }
            }
        }
        let mut persist_futures = FuturesUnordered::new();

        for (leader_id, subrequests) in persist_subrequests {
            let leader_id: NodeId = leader_id.clone().into();
            let mut ingester = self.ingester_pool.get(&leader_id).await.expect("TODO");

            let persist_request = PersistRequest {
                leader_id: leader_id.into(),
                subrequests,
                commit_type: ingest_request.commit_type,
            };
            let persist_future = async move { ingester.persist(persist_request).await };
            persist_futures.push(persist_future);
        }
        drop(state_guard);

        while let Some(persist_result) = persist_futures.next().await {
            // TODO: Handle errors.
            persist_result?;
        }
        Ok(IngestResponseV2 {
            successes: Vec::new(), // TODO
            failures: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use quickwit_proto::control_plane::{GetOpenShardsResponse, GetOpenShardsSubresponse};
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, PersistResponse};
    use quickwit_proto::ingest::router::IngestSubrequest;
    use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2, Shard};

    use super::*;

    #[tokio::test]
    async fn test_router_refresh_shard_table() {
        let self_node_id = "test-router".into();

        let mut control_plane_mock = ControlPlaneServiceClient::mock();
        control_plane_mock
            .expect_get_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 2);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_id, "test-index-0");
                assert_eq!(subrequest.source_id, "test-source");

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_id, "test-index-1");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOpenShardsResponse {
                    subresponses: vec![GetOpenShardsSubresponse {
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        open_shards: vec![Shard {
                            shard_id: 0,
                            ..Default::default()
                        }],
                    }],
                };
                Ok(response)
            });
        let control_plane: ControlPlaneServiceClient = control_plane_mock.into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );

        let ingest_request = IngestRequestV2 {
            subrequests: Vec::new(),
            commit_type: CommitTypeV2::Auto as i32,
        };
        router.refresh_shard_table(&ingest_request).await.unwrap();
        assert!(router.state.read().await.shard_table.is_empty());

        let ingest_request = IngestRequestV2 {
            subrequests: vec![
                IngestSubrequest {
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
                IngestSubrequest {
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
            ],
            commit_type: CommitTypeV2::Auto as i32,
        };
        router.refresh_shard_table(&ingest_request).await.unwrap();

        let state_guard = router.state.read().await;
        let shard_table = &state_guard.shard_table;
        assert_eq!(shard_table.len(), 1);

        let routing_entry_0 = shard_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.shards()[0].shard_id, 0);
        drop(state_guard);

        let mut control_plane_mock = ControlPlaneServiceClient::mock();
        control_plane_mock
            .expect_get_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_id, "test-index-1");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOpenShardsResponse {
                    subresponses: vec![GetOpenShardsSubresponse {
                        index_uid: "test-index-1:1".to_string(),
                        source_id: "test-source".to_string(),
                        open_shards: vec![
                            Shard {
                                shard_id: 0,
                                ..Default::default()
                            },
                            Shard {
                                shard_id: 1,
                                ..Default::default()
                            },
                        ],
                    }],
                };
                Ok(response)
            });
        let control_plane: ControlPlaneServiceClient = control_plane_mock.into();
        router.control_plane = control_plane;

        let ingest_request = IngestRequestV2 {
            subrequests: vec![
                IngestSubrequest {
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
                IngestSubrequest {
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                    ..Default::default()
                },
            ],
            commit_type: CommitTypeV2::Auto as i32,
        };
        router.refresh_shard_table(&ingest_request).await.unwrap();

        let state_guard = router.state.read().await;
        let shard_table = &state_guard.shard_table;
        assert_eq!(shard_table.len(), 2);

        let routing_entry_0 = shard_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.shards()[0].shard_id, 0);

        let routing_entry_1 = shard_table
            .find_entry("test-index-1", "test-source")
            .unwrap();
        assert_eq!(routing_entry_1.len(), 2);
        assert_eq!(routing_entry_1.shards()[0].shard_id, 0);
        assert_eq!(routing_entry_1.shards()[1].shard_id, 1);
    }

    #[tokio::test]
    async fn test_router_ingest() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );

        let mut state_guard = router.state.write().await;
        state_guard.shard_table.update_entry(
            "test-index-0",
            "test-source",
            vec![Shard {
                index_uid: "test-index-0:0".to_string(),
                shard_id: 0,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        state_guard.shard_table.update_entry(
            "test-index-1",
            "test-source",
            vec![
                Shard {
                    index_uid: "test-index-1:1".to_string(),
                    shard_id: 0,
                    leader_id: "test-ingester-0".to_string(),
                    follower_id: Some("test-ingester-1".to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index-1:1".to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester-1".to_string(),
                    follower_id: Some("test-ingester-2".to_string()),
                    ..Default::default()
                },
            ],
        );
        drop(state_guard);

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|mut request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 2);
                assert_eq!(request.commit_type, CommitTypeV2::Auto as i32);

                request
                    .subrequests
                    .sort_unstable_by(|left, right| left.index_uid.cmp(&right.index_uid));

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index-0:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 0);
                assert_eq!(subrequest.follower_id(), "");
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-000test-doc-001"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [12, 12]);

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_uid, "test-index-1:1");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 0);
                assert_eq!(subrequest.follower_id(), "test-ingester-1");
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-100test-doc-102"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [12, 12]);

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
        ingester_pool
            .insert("test-ingester-0".into(), ingester_0.clone())
            .await;

        let mut ingester_mock_1 = IngesterServiceClient::mock();
        ingester_mock_1
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-1");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type, CommitTypeV2::Auto as i32);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index-1:1");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert_eq!(subrequest.follower_id(), "test-ingester-2");
                assert_eq!(
                    subrequest.doc_batch.as_ref().unwrap().doc_buffer,
                    "test-doc-111test-doc-113"
                );
                assert_eq!(subrequest.doc_batch.as_ref().unwrap().doc_lengths, [12, 12]);

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_1: IngesterServiceClient = ingester_mock_1.into();
        ingester_pool
            .insert("test-ingester-1".into(), ingester_1)
            .await;

        let ingest_request = IngestRequestV2 {
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                IngestSubrequest {
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-000test-doc-001"),
                        doc_lengths: vec![12, 12],
                    }),
                },
                IngestSubrequest {
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(
                            b"test-doc-100test-doc-111test-doc-102test-doc-113",
                        ),
                        doc_lengths: vec![12, 12, 12, 12],
                    }),
                },
            ],
        };
        router.ingest(ingest_request).await.unwrap();
    }
}
