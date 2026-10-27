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

//! Forwards the live tail streams of the ingesters to the client, as events.

use std::time::Duration;

use futures::StreamExt;
use quickwit_proto::live_tail::{
    LiveTailDropReason, LiveTailDropped, LiveTailError, LiveTailHits, LiveTailMessage,
    LiveTailResult, LiveTailService, LiveTailServiceClient, LiveTailServiceStream,
    OpenLiveTailRequest, live_tail_message,
};
use quickwit_proto::types::NodeId;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use super::LiveTailEvent;
use super::coordinator::REFRESH_INTERVAL;

/// Maximum time to wait for an ingester to open a stream.
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) async fn open_stream(
    client: LiveTailServiceClient,
    request: OpenLiveTailRequest,
) -> LiveTailResult<LiveTailServiceStream<LiveTailMessage>> {
    tokio::time::timeout(OPEN_STREAM_TIMEOUT, client.open_live_tail(request))
        .await
        .map_err(|_| LiveTailError::Timeout("failed to open stream in time".to_string()))?
}

/// Forwards the live tail stream of one target to the client until the stream ends or the client
/// leaves. A stream that failed to open or ended while the client is still there is reported as
/// an `error` event.
pub(super) async fn forward_target(
    stream_result: LiveTailResult<LiveTailServiceStream<LiveTailMessage>>,
    index_id: String,
    node_id: NodeId,
    event_tx: mpsc::Sender<LiveTailEvent>,
) {
    let forward_result = match stream_result {
        Ok(stream) => forward_stream(stream, &index_id, &event_tx).await,
        Err(error) => Err(error),
    };
    let Err(error) = forward_result else {
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
    mut stream: LiveTailServiceStream<LiveTailMessage>,
    index_id: &str,
    event_tx: &mpsc::Sender<LiveTailEvent>,
) -> LiveTailResult<()> {
    while let Some(message_result) = stream.next().await {
        let event = match message_result?.message {
            Some(live_tail_message::Message::Hits(hits)) => hits_event(index_id, hits)?,
            Some(live_tail_message::Message::Dropped(dropped)) => dropped_event(dropped)?,
            // Ingesters always set the message: this is a violated invariant.
            None => {
                return Err(LiveTailError::Internal(
                    "empty live tail message from ingester".to_string(),
                ));
            }
        };
        if event_tx.send(event).await.is_err() {
            return Ok(());
        }
    }
    Err(LiveTailError::Unavailable(
        "stream closed by the ingester".to_string(),
    ))
}

fn dropped_event(dropped: LiveTailDropped) -> LiveTailResult<LiveTailEvent> {
    let reason = dropped.reason();
    // `reason()` maps the unknown values to `Unspecified`, which ingesters never send.
    if reason == LiveTailDropReason::Unspecified {
        return Err(LiveTailError::Internal(format!(
            "invalid live tail drop reason from ingester: `{}`",
            dropped.reason
        )));
    }
    Ok(LiveTailEvent::Dropped {
        reason,
        num_dropped: dropped.num_dropped,
    })
}

fn hits_event(index_id: &str, hits: LiveTailHits) -> LiveTailResult<LiveTailEvent> {
    let mut docs: Vec<JsonValue> = Vec::new();

    for line in hits.docs_ndjson.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        // Ingesters serialize valid JSON objects: this is a violated invariant, not a client error.
        let doc: JsonValue = serde_json::from_slice(line).map_err(|error| {
            LiveTailError::Internal(format!("invalid JSON document from ingester: {error}"))
        })?;
        if !doc.is_object() {
            return Err(LiveTailError::Internal(
                "JSON document from ingester is not an object".to_string(),
            ));
        }
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
    use serde_json::json;

    use super::*;

    #[test]
    fn test_hits_event_rejects_non_object_docs() {
        let hits = LiveTailHits {
            docs_ndjson: Bytes::from_static(b"{\"body\":\"hello\"}\n"),
        };
        let LiveTailEvent::Hits { docs, .. } = hits_event("test-index", hits).unwrap() else {
            panic!("expected hits event");
        };
        assert_eq!(docs, vec![json!({"body": "hello"})]);

        for docs_ndjson in [
            &b"42\n"[..],
            b"[1, 2]\n",
            b"{\"body\":\"hello\"}\n\"hello\"\n",
        ] {
            let hits = LiveTailHits {
                docs_ndjson: Bytes::copy_from_slice(docs_ndjson),
            };
            hits_event("test-index", hits).unwrap_err();
        }
    }

    #[test]
    fn test_dropped_event_rejects_unspecified_reason() {
        let dropped = LiveTailDropped {
            reason: LiveTailDropReason::RateLimited as i32,
            num_dropped: 3,
        };
        assert_eq!(
            dropped_event(dropped).unwrap(),
            LiveTailEvent::Dropped {
                reason: LiveTailDropReason::RateLimited,
                num_dropped: 3,
            }
        );
        for reason in [LiveTailDropReason::Unspecified as i32, 42] {
            let dropped = LiveTailDropped {
                reason,
                num_dropped: 3,
            };
            dropped_event(dropped).unwrap_err();
        }
    }
}
