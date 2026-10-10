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
use std::time::Duration;

use futures::StreamExt;
use quickwit_cluster::ClusterNode;
use quickwit_config::build_doc_mapper;
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

/// Interval at which the set of indexers running pipelines for the tailed indexes is refreshed.
/// It also bounds the time it takes to reconnect to an indexer after a stream failure.
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

/// An indexer node tailed for a given index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TailTarget {
    node_id: NodeId,
    index_uid: IndexUid,
}

/// Tails the indexes of `plans` on all the indexers running pipelines for them, and sends the
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
        let targets = tail_targets(&plans, &ready_nodes);

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

/// Returns the indexers running pipelines for the tailed indexes, according to the indexing tasks
/// they advertise in the cluster state.
fn tail_targets<'a>(
    plans: &'a [IndexTailPlan],
    ready_nodes: &'a [ClusterNode],
) -> HashMap<TailTarget, (&'a ClusterNode, &'a IndexTailPlan)> {
    let mut targets = HashMap::new();

    for node in ready_nodes {
        if !node.is_indexer() {
            continue;
        }
        for plan in plans {
            let runs_pipeline_for_index = node
                .member()
                .indexing_tasks
                .iter()
                .any(|indexing_task| indexing_task.index_uid.as_ref() == Some(&plan.index_uid));
            if runs_pipeline_for_index {
                let target = TailTarget {
                    node_id: node.member().node_id.clone(),
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
        "lost live tail stream of index `{index_id}` on indexer `{node_id}` ({error}): retrying \
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
        "stream closed by the indexer".to_string(),
    ))
}

fn hits_event(index_id: &str, hits: LiveTailHits) -> LiveTailResult<LiveTailEvent> {
    let mut docs: Vec<JsonValue> = Vec::new();

    for line in hits.docs_ndjson.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        // Indexers serialize valid JSON objects: this is a violated invariant, not a client error.
        let doc = serde_json::from_slice(line).map_err(|error| {
            LiveTailError::Internal(format!("invalid JSON document from indexer: {error}"))
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
    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::live_tail::{LiveTailMessage, MockLiveTailService};
    use quickwit_proto::types::PipelineUid;
    use serde_json::json;

    use super::*;

    async fn indexer(node_id: &str, port: u16, index_uid_opt: Option<&IndexUid>) -> ClusterNode {
        let indexing_tasks: Vec<IndexingTask> = index_uid_opt
            .into_iter()
            .map(|index_uid| IndexingTask {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                pipeline_uid: Some(PipelineUid::for_test(0)),
                ..Default::default()
            })
            .collect();
        let is_self_node = node_id == "indexer-1";
        ClusterNode::for_test(
            node_id,
            port,
            is_self_node,
            &["indexer"],
            &indexing_tasks,
            IngesterStatus::Ready,
        )
        .await
    }

    #[tokio::test]
    async fn test_tail_targets() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let plans = vec![IndexTailPlan {
            index_uid: index_uid.clone(),
            query_ast_json: r#"{"type": "match_all"}"#.to_string(),
        }];
        let ready_nodes = vec![
            indexer("indexer-1", 1337, Some(&index_uid)).await,
            // Does not run any pipeline for the index.
            indexer("indexer-2", 1339, None).await,
        ];
        let targets = tail_targets(&plans, &ready_nodes);
        assert_eq!(targets.len(), 1);
        assert!(targets.contains_key(&TailTarget {
            node_id: NodeId::from_str("indexer-1"),
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
        cluster
            .update_self_node_indexing_tasks(&[IndexingTask {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                pipeline_uid: Some(PipelineUid::for_test(0)),
                ..Default::default()
            }])
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
        // The indexer closes the stream: an error is reported and the stream is reopened.
        drop(message_tx);
        let LiveTailEvent::Error { message } = event_rx.recv().await.unwrap() else {
            panic!("expected error event");
        };
        assert!(message.contains("stream closed by the indexer"));
        tokio::time::sleep(REFRESH_INTERVAL * 2).await;

        // The client leaves: the tail ends.
        drop(event_rx);
        live_tail_handle.await.unwrap();
    }
}
