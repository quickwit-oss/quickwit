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

use std::num::NonZeroUsize;
use std::time::Duration;

use chitchat::transport::ChannelTransport;
use fnv::FnvHashMap;
use futures::{Stream, StreamExt};
use quickwit_actors::{Inbox, Mailbox, Observe, Universe};
use quickwit_cluster::{create_cluster_for_test, Cluster, ClusterChange};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_common::tower::{Change, Pool};
use quickwit_config::service::QuickwitService;
use quickwit_config::{KafkaSourceParams, SourceConfig, SourceInputFormat, SourceParams};
use quickwit_indexing::IndexingService;
use quickwit_metastore::{IndexMetadata, ListIndexesMetadataResponseExt};
use quickwit_proto::indexing::{ApplyIndexingPlanRequest, IndexingServiceClient};
use quickwit_proto::metastore::{
    ListIndexesMetadataResponse, ListShardsResponse, MetastoreServiceClient,
};
use quickwit_proto::types::NodeId;
use serde_json::json;

use crate::control_plane::{ControlPlane, CONTROL_PLAN_LOOP_INTERVAL};
use crate::indexing_scheduler::MIN_DURATION_BETWEEN_SCHEDULING;
use crate::IndexerNodeInfo;

fn index_metadata_for_test(
    index_id: &str,
    source_id: &str,
    desired_num_pipelines: usize,
    max_num_pipelines_per_indexer: usize,
) -> IndexMetadata {
    let mut index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/test-index");
    let source_config = SourceConfig {
        enabled: true,
        source_id: source_id.to_string(),
        max_num_pipelines_per_indexer: NonZeroUsize::new(max_num_pipelines_per_indexer).unwrap(),
        desired_num_pipelines: NonZeroUsize::new(desired_num_pipelines).unwrap(),
        source_params: SourceParams::Kafka(KafkaSourceParams {
            topic: "topic".to_string(),
            client_log_level: None,
            client_params: json!({
            "bootstrap.servers": "localhost:9092",
            }),
            enable_backfill_mode: true,
        }),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    index_metadata
        .sources
        .insert(source_id.to_string(), source_config);
    index_metadata
}

pub fn test_indexer_change_stream(
    cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    indexing_clients: FnvHashMap<String, Mailbox<IndexingService>>,
) -> impl Stream<Item = Change<String, IndexerNodeInfo>> + Send + 'static {
    cluster_change_stream.filter_map(move |cluster_change| {
        let indexing_clients = indexing_clients.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node)
                    if node.enabled_services().contains(&QuickwitService::Indexer) =>
                {
                    let node_id = node.node_id().to_string();
                    let indexing_tasks = node.indexing_tasks().to_vec();
                    let client_mailbox = indexing_clients.get(&node_id).unwrap().clone();
                    let client = IndexingServiceClient::from_mailbox(client_mailbox);
                    Some(Change::Insert(
                        node_id,
                        IndexerNodeInfo {
                            client,
                            indexing_tasks,
                        },
                    ))
                }
                ClusterChange::Remove(node) => Some(Change::Remove(node.node_id().to_string())),
                _ => None,
            }
        })
    })
}

async fn start_control_plane(
    cluster: Cluster,
    indexers: &[&Cluster],
    universe: &Universe,
) -> (Vec<Inbox<IndexingService>>, Mailbox<ControlPlane>) {
    let index_1 = "test-indexing-plan-1";
    let source_1 = "source-1";
    let index_2 = "test-indexing-plan-2";
    let source_2 = "source-2";
    let index_metadata_1 = index_metadata_for_test(index_1, source_1, 2, 2);
    let mut index_metadata_2 = index_metadata_for_test(index_2, source_2, 1, 1);
    index_metadata_2.create_timestamp = index_metadata_1.create_timestamp + 1;
    let mut metastore = MetastoreServiceClient::mock();
    metastore.expect_list_indexes_metadata().returning(
        move |_list_indexes_request: quickwit_proto::metastore::ListIndexesMetadataRequest| {
            let indexes_metadata = vec![index_metadata_2.clone(), index_metadata_1.clone()];
            Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata).unwrap())
        },
    );
    metastore.expect_list_shards().returning(|_| {
        Ok(ListShardsResponse {
            subresponses: Vec::new(),
        })
    });
    let mut indexer_inboxes = Vec::new();

    let indexer_pool = Pool::default();
    let ingester_pool = Pool::default();
    let change_stream = cluster.ready_nodes_change_stream().await;
    let mut indexing_clients = FnvHashMap::default();

    for indexer in indexers {
        let (indexing_service_mailbox, indexing_service_inbox) = universe.create_test_mailbox();
        indexing_clients.insert(indexer.self_node_id().to_string(), indexing_service_mailbox);
        indexer_inboxes.push(indexing_service_inbox);
    }
    let indexer_change_stream = test_indexer_change_stream(change_stream, indexing_clients);
    indexer_pool.listen_for_changes(indexer_change_stream);

    let self_node_id: NodeId = cluster.self_node_id().to_string().into();
    let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
        universe,
        cluster.cluster_id().to_string(),
        self_node_id,
        indexer_pool,
        ingester_pool,
        MetastoreServiceClient::from(metastore),
        1,
    );

    (indexer_inboxes, control_plane_mailbox)
}

#[tokio::test]
async fn test_scheduler_scheduling_and_control_loop_apply_plan_again() {
    let transport = ChannelTransport::default();
    let cluster =
        create_cluster_for_test(Vec::new(), &["indexer", "control_plane"], &transport, true)
            .await
            .unwrap();
    cluster
        .wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
        .await
        .unwrap();
    let universe = Universe::with_accelerated_time();
    let (indexing_service_inboxes, control_plane_mailbox) =
        start_control_plane(cluster.clone(), &[&cluster.clone()], &universe).await;
    let indexing_service_inbox = indexing_service_inboxes[0].clone();
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    let indexing_service_inbox_messages =
        indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
    assert!(scheduler_state.last_applied_physical_plan.is_some());
    assert_eq!(indexing_service_inbox_messages.len(), 1);

    // After a CONTROL_PLAN_LOOP_INTERVAL, the control loop will check if the desired plan is
    // running on the indexer. As chitchat state of the indexer is not updated (we did
    // not instantiate a indexing service for that), the control loop will apply again
    // the same plan.
    // Check first the plan is not updated before `MIN_DURATION_BETWEEN_SCHEDULING`.
    tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(0.5)).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);

    // After `MIN_DURATION_BETWEEN_SCHEDULING`, we should see a plan update.
    tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(0.7)).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    let indexing_service_inbox_messages =
        indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 2);
    assert_eq!(indexing_service_inbox_messages.len(), 1);
    let indexing_tasks = indexing_service_inbox_messages
        .first()
        .unwrap()
        .indexing_tasks
        .clone();

    // Update the indexer state and check that the indexer does not receive any new
    // `ApplyIndexingPlanRequest`.
    cluster
        .update_self_node_indexing_tasks(&indexing_tasks)
        .await
        .unwrap();
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 2);
    let indexing_service_inbox_messages =
        indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(indexing_service_inbox_messages.len(), 0);

    // Update the indexer state with a different plan and check that the indexer does now
    // receive a new `ApplyIndexingPlanRequest`.
    cluster
        .update_self_node_indexing_tasks(&[indexing_tasks[0].clone()])
        .await
        .unwrap();
    tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(1.2)).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 3);
    let indexing_service_inbox_messages =
        indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(indexing_service_inbox_messages.len(), 1);
    universe.assert_quit().await;
}

#[tokio::test]
async fn test_scheduler_scheduling_no_indexer() {
    let transport = ChannelTransport::default();
    let cluster = create_cluster_for_test(Vec::new(), &["control_plane"], &transport, true)
        .await
        .unwrap();
    let universe = Universe::with_accelerated_time();
    let (indexing_service_inboxes, control_plane_mailbox) =
        start_control_plane(cluster.clone(), &[], &universe).await;
    assert_eq!(indexing_service_inboxes.len(), 0);

    // No indexer.
    universe.sleep(CONTROL_PLAN_LOOP_INTERVAL).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
    assert!(scheduler_state.last_applied_physical_plan.is_none());

    // There is no indexer, we should observe no
    // scheduling.
    universe.sleep(Duration::from_secs(60)).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
    assert!(scheduler_state.last_applied_physical_plan.is_none());
    universe.assert_quit().await;
}

#[tokio::test]
async fn test_scheduler_scheduling_multiple_indexers() {
    let transport = ChannelTransport::default();
    let cluster = create_cluster_for_test(Vec::new(), &["control_plane"], &transport, true)
        .await
        .unwrap();
    let cluster_indexer_1 = create_cluster_for_test(
        vec![cluster.gossip_advertise_addr().to_string()],
        &["indexer"],
        &transport,
        true,
    )
    .await
    .unwrap();
    let cluster_indexer_2 = create_cluster_for_test(
        vec![cluster.gossip_advertise_addr().to_string()],
        &["indexer"],
        &transport,
        true,
    )
    .await
    .unwrap();
    let universe = Universe::new();
    let (indexing_service_inboxes, control_plane_mailbox) = start_control_plane(
        cluster.clone(),
        &[&cluster_indexer_1, &cluster_indexer_2],
        &universe,
    )
    .await;
    let indexing_service_inbox_1 = indexing_service_inboxes[0].clone();
    let indexing_service_inbox_2 = indexing_service_inboxes[1].clone();

    // No indexer.
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    let indexing_service_inbox_messages =
        indexing_service_inbox_1.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
    assert!(scheduler_state.last_applied_physical_plan.is_none());
    assert_eq!(indexing_service_inbox_messages.len(), 0);

    cluster
        .wait_for_ready_members(
            |members| {
                members
                    .iter()
                    .any(|member| member.enabled_services.contains(&QuickwitService::Indexer))
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    // Wait for chitchat update, sheduler will detect new indexers and schedule a plan.
    wait_until_predicate(
        || {
            let control_plane_mailbox_clone = control_plane_mailbox.clone();
            async move {
                let scheduler_state = control_plane_mailbox_clone
                    .ask(Observe)
                    .await
                    .unwrap()
                    .indexing_scheduler;
                scheduler_state.num_schedule_indexing_plan == 1
            }
        },
        CONTROL_PLAN_LOOP_INTERVAL * 4,
        Duration::from_millis(100),
    )
    .await
    .unwrap();
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);
    let indexing_service_inbox_messages_1 =
        indexing_service_inbox_1.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    let indexing_service_inbox_messages_2 =
        indexing_service_inbox_2.drain_for_test_typed::<ApplyIndexingPlanRequest>();
    assert_eq!(indexing_service_inbox_messages_1.len(), 1);
    assert_eq!(indexing_service_inbox_messages_2.len(), 1);
    cluster_indexer_1
        .update_self_node_indexing_tasks(&indexing_service_inbox_messages_1[0].indexing_tasks)
        .await
        .unwrap();
    cluster_indexer_2
        .update_self_node_indexing_tasks(&indexing_service_inbox_messages_2[0].indexing_tasks)
        .await
        .unwrap();

    // Wait 2 CONTROL_PLAN_LOOP_INTERVAL again and check the scheduler will not apply the plan
    // several times.
    universe.sleep(CONTROL_PLAN_LOOP_INTERVAL * 2).await;
    let scheduler_state = control_plane_mailbox
        .ask(Observe)
        .await
        .unwrap()
        .indexing_scheduler;
    assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);

    // Shutdown cluster and wait until the new scheduling.
    cluster_indexer_2.shutdown().await;

    cluster
        .wait_for_ready_members(
            |members| {
                members
                    .iter()
                    .filter(|member| member.enabled_services.contains(&QuickwitService::Indexer))
                    .count()
                    == 1
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    wait_until_predicate(
        || {
            let scheduler_handler_mailbox_clone = control_plane_mailbox.clone();
            async move {
                let scheduler_state = scheduler_handler_mailbox_clone
                    .ask(Observe)
                    .await
                    .unwrap()
                    .indexing_scheduler;
                scheduler_state.num_schedule_indexing_plan == 2
            }
        },
        CONTROL_PLAN_LOOP_INTERVAL * 10,
        Duration::from_millis(100),
    )
    .await
    .unwrap();

    universe.assert_quit().await;
}
