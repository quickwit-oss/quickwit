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
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use quickwit_common::ServiceStream;
use quickwit_proto::live_tail::{
    LiveTailError, LiveTailMessage, LiveTailResult, LiveTailService, LiveTailServiceStream,
    OpenLiveTailRequest,
};
use quickwit_query::query_ast::QueryAst;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::LiveTailHub;
use crate::rate_limiter::DocRateLimiter;
use crate::session::{LiveTailSession, max_hits_payload_num_bytes};

/// Implementation of the live tail service hosted by ingester nodes.
#[derive(Debug, Clone)]
pub struct LiveTailServiceImpl {
    hub: LiveTailHub,
    session_permits: Arc<Semaphore>,
    /// Rates requested by clients above this limit are capped.
    max_docs_per_sec: u32,
    max_hits_payload_num_bytes: usize,
    /// Rate limiters of the client sessions, by session ID. A client session tailing several
    /// indexes opens one session per index on this node: they share the same rate limiter, so
    /// that `max_docs_per_sec` applies to the node, not to each index.
    rate_limiters: Arc<Mutex<HashMap<String, Weak<Mutex<DocRateLimiter>>>>>,
}

impl LiveTailServiceImpl {
    pub fn new(
        hub: LiveTailHub,
        max_sessions: usize,
        max_docs_per_sec: u32,
        grpc_max_message_num_bytes: usize,
    ) -> Self {
        Self {
            hub,
            session_permits: Arc::new(Semaphore::new(max_sessions)),
            max_docs_per_sec,
            max_hits_payload_num_bytes: max_hits_payload_num_bytes(grpc_max_message_num_bytes),
            rate_limiters: Arc::default(),
        }
    }

    /// Returns the rate limiter of the client session `session_id`, creating it if this is the
    /// first session opened on this node for this client session.
    ///
    /// The sessions of a client session request the same `max_docs_per_sec`: the first one
    /// determines the rate.
    fn rate_limiter(&self, session_id: &str, max_docs_per_sec: u32) -> Arc<Mutex<DocRateLimiter>> {
        let mut rate_limiters = self
            .rate_limiters
            .lock()
            .expect("lock should not be poisoned");
        rate_limiters.retain(|_, rate_limiter| rate_limiter.strong_count() > 0);

        if let Some(rate_limiter) = rate_limiters.get(session_id).and_then(Weak::upgrade) {
            return rate_limiter;
        }
        let rate_limiter = Arc::new(Mutex::new(DocRateLimiter::new(
            max_docs_per_sec.min(self.max_docs_per_sec),
            Instant::now(),
        )));
        rate_limiters.insert(session_id.to_string(), Arc::downgrade(&rate_limiter));
        rate_limiter
    }
}

#[async_trait]
impl LiveTailService for LiveTailServiceImpl {
    async fn open_live_tail(
        &self,
        request: OpenLiveTailRequest,
    ) -> LiveTailResult<LiveTailServiceStream<LiveTailMessage>> {
        let Some(index_uid) = request.index_uid.clone() else {
            return Err(LiveTailError::InvalidArgument(
                "missing index UID".to_string(),
            ));
        };
        if request.max_docs_per_sec == 0 {
            return Err(LiveTailError::InvalidArgument(
                "`max_docs_per_sec` must be positive".to_string(),
            ));
        }
        // The query was validated against the index doc mapping by the coordinator.
        let query_ast: QueryAst =
            serde_json::from_str(&request.query_ast_json).map_err(|error| {
                LiveTailError::InvalidArgument(format!("invalid query AST: {error}"))
            })?;
        let session_permit = self
            .session_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| LiveTailError::TooManyRequests)?;

        // Subscribe before returning so that no document persisted after this call is missed.
        let batch_rx = self.hub.subscribe(&index_uid);
        let (message_tx, message_stream) = ServiceStream::new_bounded(3);
        let rate_limiter = self.rate_limiter(&request.session_id, request.max_docs_per_sec);
        let session = LiveTailSession::new(
            request.session_id,
            query_ast,
            batch_rx,
            message_tx,
            rate_limiter,
            self.max_hits_payload_num_bytes,
            session_permit,
        );
        quickwit_common::spawn_named_task(session.run(), "live_tail_session");
        Ok(message_stream)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use quickwit_doc_mapper::DocMapper;
    use quickwit_proto::live_tail::{LiveTailDropReason, live_tail_message};
    use quickwit_proto::types::IndexUid;
    use quickwit_query::query_ast::query_ast_from_user_text;
    use serde_json::json;

    use super::*;
    use crate::hub::doc_batch_for_test;

    const GRPC_MAX_MESSAGE_NUM_BYTES: usize = 20 * 1024 * 1024;

    const DOC_MAPPING_JSON: &str = r#"{
        "field_mappings": [
            {"name": "severity_text", "type": "text", "tokenizer": "raw"},
            {"name": "body", "type": "text"}
        ],
        "mode": "dynamic"
    }"#;

    fn publish(hub: &LiveTailHub, docs: Vec<serde_json::Value>) {
        publish_to(hub, &IndexUid::for_test("test-index", 0), docs);
    }

    fn publish_to(hub: &LiveTailHub, index_uid: &IndexUid, docs: Vec<serde_json::Value>) {
        let doc_mapper: DocMapper = serde_json::from_str(DOC_MAPPING_JSON).unwrap();
        hub.publish(index_uid, Arc::new(doc_mapper), doc_batch_for_test(&docs));
    }

    fn open_live_tail_request(user_query: &str, max_docs_per_sec: u32) -> OpenLiveTailRequest {
        let query_ast = query_ast_from_user_text(user_query, None)
            .parse_user_query(&["body".to_string()])
            .unwrap();
        OpenLiveTailRequest {
            session_id: "test-session".to_string(),
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            query_ast_json: serde_json::to_string(&query_ast).unwrap(),
            max_docs_per_sec,
        }
    }

    fn tail_doc(severity_text: &str, body: &str) -> serde_json::Value {
        json!({"severity_text": severity_text, "body": body})
    }

    async fn next_message(
        stream: &mut LiveTailServiceStream<LiveTailMessage>,
    ) -> live_tail_message::Message {
        stream.next().await.unwrap().unwrap().message.unwrap()
    }

    #[tokio::test]
    async fn test_live_tail_service_streams_matching_docs() {
        let hub = LiveTailHub::default();
        let live_tail_service =
            LiveTailServiceImpl::new(hub.clone(), 1, 100, GRPC_MAX_MESSAGE_NUM_BYTES);

        let mut stream = live_tail_service
            .open_live_tail(open_live_tail_request("severity_text:ERROR", 100))
            .await
            .unwrap();
        assert!(hub.is_active(&IndexUid::for_test("test-index", 0)));

        publish(
            &hub,
            vec![
                tail_doc("INFO", "all good"),
                tail_doc("ERROR", "connection refused"),
                tail_doc("ERROR", "disk full"),
            ],
        );
        let live_tail_message::Message::Hits(hits) = next_message(&mut stream).await else {
            panic!("expected hits");
        };
        let docs: Vec<serde_json::Value> = hits
            .docs_ndjson
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(
            docs,
            vec![
                json!({"severity_text": "ERROR", "body": "connection refused"}),
                json!({"severity_text": "ERROR", "body": "disk full"}),
            ]
        );

        // The session permit is released when the client drops the stream.
        let error = live_tail_service
            .open_live_tail(open_live_tail_request("*", 100))
            .await
            .unwrap_err();
        assert_eq!(error, LiveTailError::TooManyRequests);
        drop(stream);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while live_tail_service.session_permits.available_permits() == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!hub.is_active(&IndexUid::for_test("test-index", 0)));
    }

    #[tokio::test]
    async fn test_live_tail_service_caps_requested_rate() {
        let hub = LiveTailHub::default();
        let live_tail_service =
            LiveTailServiceImpl::new(hub.clone(), 1, 2, GRPC_MAX_MESSAGE_NUM_BYTES);

        let mut stream = live_tail_service
            .open_live_tail(open_live_tail_request("*", 100))
            .await
            .unwrap();
        publish(
            &hub,
            vec![
                tail_doc("INFO", "one"),
                tail_doc("INFO", "two"),
                tail_doc("INFO", "three"),
            ],
        );
        let live_tail_message::Message::Hits(hits) = next_message(&mut stream).await else {
            panic!("expected hits");
        };
        assert_eq!(hits.docs_ndjson.split(|byte| *byte == b'\n').count(), 3);

        let live_tail_message::Message::Dropped(dropped) = next_message(&mut stream).await else {
            panic!("expected dropped");
        };
        assert_eq!(dropped.reason(), LiveTailDropReason::RateLimited);
        assert_eq!(dropped.num_dropped, 1);
    }

    #[tokio::test]
    async fn test_live_tail_service_shares_rate_limit_across_indexes() {
        let hub = LiveTailHub::default();
        let live_tail_service =
            LiveTailServiceImpl::new(hub.clone(), 2, 1, GRPC_MAX_MESSAGE_NUM_BYTES);
        let index_uid_a = IndexUid::for_test("test-index-a", 0);
        let index_uid_b = IndexUid::for_test("test-index-b", 0);

        // The same client session tails two indexes on this node.
        let mut request = open_live_tail_request("*", 100);
        request.index_uid = Some(index_uid_a.clone());
        let mut stream_a = live_tail_service
            .open_live_tail(request.clone())
            .await
            .unwrap();
        request.index_uid = Some(index_uid_b.clone());
        let mut stream_b = live_tail_service.open_live_tail(request).await.unwrap();

        publish_to(&hub, &index_uid_a, vec![tail_doc("INFO", "a")]);
        let live_tail_message::Message::Hits(_) = next_message(&mut stream_a).await else {
            panic!("expected hits");
        };
        // The budget of 1 document per second was spent by the session of the other index.
        publish_to(&hub, &index_uid_b, vec![tail_doc("INFO", "b")]);
        let live_tail_message::Message::Dropped(dropped) = next_message(&mut stream_b).await else {
            panic!("expected dropped");
        };
        assert_eq!(dropped.reason(), LiveTailDropReason::RateLimited);
        assert_eq!(dropped.num_dropped, 1);
    }

    #[tokio::test]
    async fn test_live_tail_service_drops_docs_larger_than_grpc_messages() {
        let hub = LiveTailHub::default();
        // Hits payloads are limited to 2,048 - 1,024 bytes.
        let live_tail_service = LiveTailServiceImpl::new(hub.clone(), 1, 100, 2_048);
        let mut stream = live_tail_service
            .open_live_tail(open_live_tail_request("*", 100))
            .await
            .unwrap();

        publish(
            &hub,
            vec![
                tail_doc("INFO", &"x".repeat(2_000)),
                tail_doc("INFO", &"y".repeat(600)),
                tail_doc("INFO", &"z".repeat(600)),
            ],
        );
        // The two small documents do not fit together in a single message.
        for expected_body_char in ['y', 'z'] {
            let live_tail_message::Message::Hits(hits) = next_message(&mut stream).await else {
                panic!("expected hits");
            };
            assert!(hits.docs_ndjson.len() <= 1_024);
            let doc: serde_json::Value =
                serde_json::from_slice(hits.docs_ndjson.trim_ascii_end()).unwrap();
            assert!(
                doc["body"]
                    .as_str()
                    .unwrap()
                    .starts_with(expected_body_char)
            );
        }
        let live_tail_message::Message::Dropped(dropped) = next_message(&mut stream).await else {
            panic!("expected dropped");
        };
        assert_eq!(dropped.reason(), LiveTailDropReason::TooLarge);
        assert_eq!(dropped.num_dropped, 1);
    }

    #[tokio::test]
    async fn test_live_tail_service_rejects_invalid_requests() {
        let live_tail_service =
            LiveTailServiceImpl::new(LiveTailHub::default(), 1, 100, GRPC_MAX_MESSAGE_NUM_BYTES);

        let error = live_tail_service
            .open_live_tail(open_live_tail_request("*", 0))
            .await
            .unwrap_err();
        assert!(matches!(error, LiveTailError::InvalidArgument(_)));

        let mut request = open_live_tail_request("*", 10);
        request.query_ast_json = "not json".to_string();
        let error = live_tail_service.open_live_tail(request).await.unwrap_err();
        assert!(matches!(error, LiveTailError::InvalidArgument(_)));

        // Invalid requests do not consume a session permit.
        assert_eq!(live_tail_service.session_permits.available_permits(), 1);
    }
}
