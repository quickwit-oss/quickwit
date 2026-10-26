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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::StreamExt;
use quickwit_cluster::{Cluster, ClusterNode};
use quickwit_config::{INGEST_V2_SOURCE_ID, build_doc_mapper};
use quickwit_ingest::find_ingesters_with_open_shards;
use quickwit_proto::live_tail::{
    LiveTailError, LiveTailHits, LiveTailResult, LiveTailService, LiveTailServiceClient,
    OpenLiveTailRequest, live_tail_message,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, NodeId};
use quickwit_query::query_ast::query_ast_from_user_text;
use quickwit_search::{SearchError, resolve_index_patterns};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::debug;

use super::{LiveTailContext, LiveTailEvent};

/// Interval at which the set of ingesters hosting open shards of the tailed indexes is refreshed.
/// It also bounds the time it takes to reconnect to an ingester after a stream failure.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Everything needed to tail one index. Plans are built, and the query validated, before the
/// event stream is opened so that invalid requests get a proper HTTP error status.
#[derive(Debug, Clone)]
pub(crate) struct IndexTailPlan {
    pub index_uid: IndexUid,
    /// JSON-serialized `QueryAst`, resolved with the index default search fields.
    pub query_ast_json: String,
}

pub(crate) async fn plan_live_tail(
    metastore: &MetastoreServiceClient,
    index_id_patterns: &[String],
    user_query: &str,
) -> Result<Vec<IndexTailPlan>, SearchError> {
    let indexes_metadata = resolve_index_patterns(index_id_patterns, metastore).await?;
    let mut plans = Vec::with_capacity(indexes_metadata.len());

    for index_metadata in indexes_metadata {
        let index_config = &index_metadata.index_config;

        // Live tail streams the documents persisted by the ingesters, before the VRL transform
        // is applied by the indexing pipeline: filtering them with the doc mapping would not
        // match what is indexed.
        let has_transform = matches!(
            index_metadata.sources.get(INGEST_V2_SOURCE_ID),
            Some(source_config) if source_config.transform_config.is_some()
        );
        if has_transform {
            return Err(SearchError::InvalidArgument(format!(
                "live tail does not support index `{}`: its source `{INGEST_V2_SOURCE_ID}` has a \
                 VRL transform",
                index_config.index_id
            )));
        }
        let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
            .map_err(|error| {
                SearchError::Internal(format!("failed to build doc mapper: {error}"))
            })?;
        let query_ast = query_ast_from_user_text(user_query, None)
            .parse_user_query(doc_mapper.default_search_fields())
            .map_err(|error| SearchError::InvalidQuery(error.to_string()))?;
        doc_mapper
            .query(doc_mapper.schema(), query_ast.clone(), true, None)
            .map_err(|error| SearchError::InvalidQuery(error.to_string()))?;

        let query_ast_json = serde_json::to_string(&query_ast)
            .map_err(|error| SearchError::Internal(error.to_string()))?;
        plans.push(IndexTailPlan {
            index_uid: index_metadata.index_uid.clone(),
            query_ast_json,
        });
    }
    Ok(plans)
}

/// An ingester node tailed for a given index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TailTarget {
    node_id: NodeId,
    index_uid: IndexUid,
}

/// Tails the indexes of `plans` on all the ingesters hosting open shards for them, and sends the
/// resulting events to `event_tx` until the receiver is dropped.
///
/// Each target is served by one forwarder task, which sends its events directly to `event_tx`.
/// The forwarders are only spawned and aborted by this function, which owns their handles: a
/// forwarder whose stream ended is respawned at the next refresh.
pub(crate) async fn run_live_tail(
    context: LiveTailContext,
    plans: Vec<IndexTailPlan>,
    max_docs_per_sec: u32,
    event_tx: mpsc::Sender<LiveTailEvent>,
) {
    let session_id = quickwit_common::new_coolid("live-tail");
    debug!(%session_id, "live tail started");
    let mut forwarders: HashMap<TailTarget, JoinHandle<()>> = HashMap::new();
    let mut refresh_interval = tokio::time::interval(REFRESH_INTERVAL);
    refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = event_tx.closed() => break,
            _ = refresh_interval.tick() => {}
        }
        let ready_nodes = context.cluster.ready_nodes().await;
        let ingesters_per_index = find_ingesters_per_index(&context.cluster, &plans).await;
        let targets = tail_targets(&plans, &ready_nodes, &ingesters_per_index);

        forwarders.retain(|target, forwarder| {
            let keep = targets.contains_key(target) && !forwarder.is_finished();
            if !keep {
                forwarder.abort();
            }
            keep
        });
        for (target, (node, plan)) in targets {
            if forwarders.contains_key(&target) {
                continue;
            }
            debug!(%session_id, node_id=%target.node_id, index_id=%plan.index_uid.index_id, "opening live tail stream");
            let request = OpenLiveTailRequest {
                session_id: session_id.clone(),
                index_uid: Some(plan.index_uid.clone()),
                query_ast_json: plan.query_ast_json.clone(),
                max_docs_per_sec,
            };
            let forwarder = quickwit_common::spawn_named_task(
                forward_target(
                    live_tail_client(&context, node),
                    request,
                    target.node_id.clone(),
                    event_tx.clone(),
                ),
                "live_tail_forwarder",
            );
            forwarders.insert(target, forwarder);
        }
    }
    for forwarder in forwarders.values() {
        forwarder.abort();
    }
    debug!(%session_id, "live tail ended");
}

async fn find_ingesters_per_index(
    cluster: &Cluster,
    plans: &[IndexTailPlan],
) -> HashMap<IndexUid, HashSet<NodeId>> {
    let mut ingesters_per_index = HashMap::with_capacity(plans.len());

    for plan in plans {
        let ingester_ids = find_ingesters_with_open_shards(cluster, &plan.index_uid).await;
        ingesters_per_index.insert(plan.index_uid.clone(), ingester_ids);
    }
    ingesters_per_index
}

/// Returns the ready ingesters hosting open shards of the tailed indexes.
///
/// Ingesters only persist documents in open shards: the other ingesters have nothing to stream.
fn tail_targets<'a>(
    plans: &'a [IndexTailPlan],
    ready_nodes: &'a [ClusterNode],
    ingesters_per_index: &HashMap<IndexUid, HashSet<NodeId>>,
) -> HashMap<TailTarget, (&'a ClusterNode, &'a IndexTailPlan)> {
    let mut targets = HashMap::new();

    for node in ready_nodes {
        if !node.is_ingester() {
            continue;
        }
        let node_id = &node.member().node_id;

        for plan in plans {
            let Some(ingester_ids) = ingesters_per_index.get(&plan.index_uid) else {
                continue;
            };
            if ingester_ids.contains(node_id) {
                let target = TailTarget {
                    node_id: node_id.clone(),
                    index_uid: plan.index_uid.clone(),
                };
                targets.insert(target, (node, plan));
            }
        }
    }
    targets
}

fn live_tail_client(context: &LiveTailContext, node: &ClusterNode) -> LiveTailServiceClient {
    if node.is_self_node()
        && let Some(local_live_tail_service) = &context.local_live_tail_service_opt
    {
        return local_live_tail_service.clone();
    }
    LiveTailServiceClient::from_channel(
        node.member().grpc_advertise_addr,
        node.channel(),
        context.grpc_max_message_size,
        None,
    )
}

/// Forwards the live tail stream of one target to the client until the stream ends or the client
/// leaves. A stream that ends while the client is still there is reported as an `error` event.
async fn forward_target(
    client: LiveTailServiceClient,
    request: OpenLiveTailRequest,
    node_id: NodeId,
    event_tx: mpsc::Sender<LiveTailEvent>,
) {
    let index_id = request.index_uid().index_id.clone();
    let Err(error) = forward_stream(client, request, &index_id, &event_tx).await else {
        return;
    };
    let message = format!(
        "lost live tail stream of index `{index_id}` on ingester `{node_id}` ({error}): retrying \
         in up to {}s",
        REFRESH_INTERVAL.as_secs()
    );
    let _ = event_tx.send(LiveTailEvent::Error { message }).await;
}

/// Returns `Ok` when the client left.
async fn forward_stream(
    client: LiveTailServiceClient,
    request: OpenLiveTailRequest,
    index_id: &str,
    event_tx: &mpsc::Sender<LiveTailEvent>,
) -> LiveTailResult<()> {
    let mut stream = client.open_live_tail(request).await?;

    while let Some(message_result) = stream.next().await {
        let event = match message_result?.message {
            Some(live_tail_message::Message::Hits(hits)) => hits_event(index_id, hits)?,
            Some(live_tail_message::Message::Dropped(dropped)) => LiveTailEvent::Dropped {
                reason: dropped.reason(),
                num_dropped: dropped.num_dropped,
            },
            None => continue,
        };
        if event_tx.send(event).await.is_err() {
            return Ok(());
        }
    }
    Err(LiveTailError::Unavailable(
        "stream closed by the ingester".to_string(),
    ))
}

fn hits_event(index_id: &str, hits: LiveTailHits) -> LiveTailResult<LiveTailEvent> {
    let mut docs: Vec<JsonValue> = Vec::new();

    for line in hits.docs_ndjson.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        // Ingesters serialize valid JSON objects: this is a violated invariant, not a client error.
        let doc = serde_json::from_slice(line).map_err(|error| {
            LiveTailError::Internal(format!("invalid JSON document from ingester: {error}"))
        })?;
        docs.push(doc);
    }
    Ok(LiveTailEvent::Hits {
        index_id: index_id.to_string(),
        docs,
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytesize::ByteSize;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_common::ServiceStream;
    use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
    use quickwit_config::{SourceConfig, TransformConfig};
    use quickwit_ingest::{RateMibPerSec, ShardInfo};
    use quickwit_metastore::{IndexMetadata, ListIndexesMetadataResponseExt};
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::live_tail::{LiveTailMessage, MockLiveTailService};
    use quickwit_proto::metastore::{ListIndexesMetadataResponse, MockMetastoreService};
    use quickwit_proto::types::ShardId;
    use serde_json::json;

    use super::*;

    async fn ingester(node_id: &str, port: u16, enabled_services: &[&str]) -> ClusterNode {
        ClusterNode::for_test(
            node_id,
            port,
            false,
            enabled_services,
            &[],
            IngesterStatus::Ready,
        )
        .await
    }

    fn metastore_for_test(index_metadata: IndexMetadata) -> MetastoreServiceClient {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata]))
            });
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    #[tokio::test]
    async fn test_plan_live_tail() {
        let index_metadata =
            IndexMetadata::for_test("test-index", "file:///qwdata/indexes/test-index");
        let metastore = metastore_for_test(index_metadata);
        let plans = plan_live_tail(&metastore, &["test-index".to_string()], "body:hello")
            .await
            .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].index_uid, IndexUid::for_test("test-index", 0));
    }

    #[tokio::test]
    async fn test_plan_live_tail_rejects_indexes_with_transform() {
        let mut index_metadata =
            IndexMetadata::for_test("test-index", "file:///qwdata/indexes/test-index");
        let mut source_config = SourceConfig::ingest_v2();
        source_config.transform_config = Some(TransformConfig::new(
            ".body = upcase(string!(.body))".to_string(),
            None,
        ));
        index_metadata.add_source(source_config).unwrap();
        let metastore = metastore_for_test(index_metadata);

        let error = plan_live_tail(&metastore, &["test-index".to_string()], "*")
            .await
            .unwrap_err();
        let SearchError::InvalidArgument(message) = error else {
            panic!("expected invalid argument error, got {error:?}");
        };
        assert!(message.contains("VRL transform"));
    }

    #[tokio::test]
    async fn test_tail_targets() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let plans = vec![IndexTailPlan {
            index_uid: index_uid.clone(),
            query_ast_json: r#"{"type": "match_all"}"#.to_string(),
        }];
        let ready_nodes = vec![
            ingester("ingester-1", 1337, &["indexer"]).await,
            // Does not host open shards for the index.
            ingester("ingester-2", 1339, &["indexer"]).await,
            // Not an ingester.
            ingester("searcher-1", 1341, &["searcher"]).await,
        ];
        let ingesters_per_index = HashMap::from([(
            index_uid.clone(),
            HashSet::from([
                NodeId::from_str("ingester-1"),
                NodeId::from_str("searcher-1"),
            ]),
        )]);
        let targets = tail_targets(&plans, &ready_nodes, &ingesters_per_index);
        assert_eq!(targets.len(), 1);
        assert!(targets.contains_key(&TailTarget {
            node_id: NodeId::from_str("ingester-1"),
            index_uid,
        }));
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_live_tail_forwards_hits_and_reconnects() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let (message_tx, message_stream) = ServiceStream::new_bounded(1);
        let (_message_tx_2, message_stream_2) = ServiceStream::new_bounded(1);

        let mut mock_live_tail_service = MockLiveTailService::new();
        let mut message_streams = vec![message_stream_2, message_stream];
        mock_live_tail_service
            .expect_open_live_tail()
            .times(2)
            .returning(move |request| {
                assert_eq!(request.max_docs_per_sec, 100);
                Ok(message_streams.pop().unwrap())
            });
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        // The ingester of this node hosts an open shard of the index.
        let shard_infos = vec![ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(0),
            long_term_ingestion_rate: RateMibPerSec(0),
        }];
        cluster
            .set_self_key_value(
                format!("{INGESTER_SHARDS_PREFIX}{index_uid}:{INGEST_V2_SOURCE_ID}"),
                serde_json::to_string(&shard_infos).unwrap(),
            )
            .await;
        let context = LiveTailContext {
            cluster,
            metastore: MetastoreServiceClient::mocked(),
            local_live_tail_service_opt: Some(LiveTailServiceClient::from_mock(
                mock_live_tail_service,
            )),
            grpc_max_message_size: ByteSize::mib(20),
        };
        let plans = vec![IndexTailPlan {
            index_uid,
            query_ast_json: r#"{"type": "match_all"}"#.to_string(),
        }];
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let live_tail_handle = tokio::spawn(run_live_tail(context, plans, 100, event_tx));

        let live_tail_message = LiveTailMessage {
            message: Some(live_tail_message::Message::Hits(LiveTailHits {
                docs_ndjson: Bytes::from_static(b"{\"body\":\"hello\"}\n"),
            })),
        };
        message_tx.send(Ok(live_tail_message)).await.unwrap();
        assert_eq!(
            event_rx.recv().await.unwrap(),
            LiveTailEvent::Hits {
                index_id: "test-index".to_string(),
                docs: vec![json!({"body": "hello"})],
            }
        );
        // The ingester closes the stream: an error is reported and the stream is reopened.
        drop(message_tx);
        let LiveTailEvent::Error { message } = event_rx.recv().await.unwrap() else {
            panic!("expected error event");
        };
        assert!(message.contains("stream closed by the ingester"));
        tokio::time::sleep(REFRESH_INTERVAL * 2).await;

        // The client leaves: the tail ends.
        drop(event_rx);
        live_tail_handle.await.unwrap();
    }
}
