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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::ServiceStream;
use quickwit_proto::live_tail::{
    LiveTailError, LiveTailMessage, LiveTailResult, LiveTailService, LiveTailServiceStream,
    OpenLiveTailRequest,
};
use quickwit_query::query_ast::QueryAst;
use tokio::sync::Semaphore;

use crate::LiveTailHub;
use crate::session::LiveTailSession;

/// Implementation of the live tail service hosted by indexer nodes.
#[derive(Debug, Clone)]
pub struct LiveTailServiceImpl {
    hub: LiveTailHub,
    session_permits: Arc<Semaphore>,
    /// Rates requested by clients above this limit are capped.
    max_docs_per_sec: u32,
}

impl LiveTailServiceImpl {
    pub fn new(hub: LiveTailHub, max_sessions: usize, max_docs_per_sec: u32) -> Self {
        Self {
            hub,
            session_permits: Arc::new(Semaphore::new(max_sessions)),
            max_docs_per_sec,
        }
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

        // Subscribe before returning so that no document processed after this call is missed.
        let batch_rx = self.hub.subscribe(&index_uid);
        let (message_tx, message_stream) = ServiceStream::new_bounded(3);
        let session = LiveTailSession::new(
            request.session_id,
            query_ast,
            batch_rx,
            message_tx,
            request.max_docs_per_sec.min(self.max_docs_per_sec),
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
    use crate::{LiveTailTap, TailDoc};

    const DOC_MAPPING_JSON: &str = r#"{
        "field_mappings": [
            {"name": "severity_text", "type": "text", "tokenizer": "raw"},
            {"name": "body", "type": "text"}
        ],
        "mode": "dynamic"
    }"#;

    fn publish(tap: &LiveTailTap, docs: Vec<TailDoc>) {
        let doc_mapper: DocMapper = serde_json::from_str(DOC_MAPPING_JSON).unwrap();
        tap.publish(Arc::new(doc_mapper), docs);
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

    fn tail_doc(severity_text: &str, body: &str) -> TailDoc {
        let serde_json::Value::Object(json_obj) =
            json!({"severity_text": severity_text, "body": body})
        else {
            unreachable!()
        };
        TailDoc {
            json_obj,
            num_bytes: 64,
        }
    }

    async fn next_message(
        stream: &mut LiveTailServiceStream<LiveTailMessage>,
    ) -> live_tail_message::Message {
        stream.next().await.unwrap().unwrap().message.unwrap()
    }

    #[tokio::test]
    async fn test_live_tail_service_streams_matching_docs() {
        let hub = LiveTailHub::default();
        let tap = hub.tap(IndexUid::for_test("test-index", 0));
        let live_tail_service = LiveTailServiceImpl::new(hub, 1, 100);

        let mut stream = live_tail_service
            .open_live_tail(open_live_tail_request("severity_text:ERROR", 100))
            .await
            .unwrap();
        assert!(tap.is_active());

        publish(
            &tap,
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
        assert!(!tap.is_active());
    }

    #[tokio::test]
    async fn test_live_tail_service_caps_requested_rate() {
        let hub = LiveTailHub::default();
        let tap = hub.tap(IndexUid::for_test("test-index", 0));
        let live_tail_service = LiveTailServiceImpl::new(hub, 1, 2);

        let mut stream = live_tail_service
            .open_live_tail(open_live_tail_request("*", 100))
            .await
            .unwrap();
        publish(
            &tap,
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
    async fn test_live_tail_service_rejects_invalid_requests() {
        let live_tail_service = LiveTailServiceImpl::new(LiveTailHub::default(), 1, 100);

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
