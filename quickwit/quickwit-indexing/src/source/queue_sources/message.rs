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

use core::fmt;
use std::io::read_to_string;
use std::str::FromStr;
use std::time::Instant;

use anyhow::Context;
use quickwit_common::rate_limited_warn;
use quickwit_common::uri::Uri;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::types::Position;
use quickwit_storage::{OwnedBytes, StorageResolver};
use serde_json::Value;
use thiserror::Error;
use tracing::info;

use super::visibility::VisibilityTaskHandle;
use crate::source::doc_file_reader::ObjectUriBatchReader;

#[derive(Debug, Clone, Copy)]
pub enum MessageType {
    S3Notification,
    // GcsNotification,
    RawUri,
    // RawData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMetadata {
    /// The handle that should be used to acknowledge the message or change its visibility deadline
    pub ack_id: String,

    /// The unique message id assigned by the queue
    pub message_id: String,

    /// The approximate number of times the message was delivered. 1 means it is
    /// the first time this message is being delivered.
    pub delivery_attempts: usize,

    /// The first deadline when the message is received. It can be extended later using the ack_id.
    pub initial_deadline: Instant,
}

/// The raw messages as received from the queue abstraction
pub struct RawMessage {
    pub metadata: MessageMetadata,
    pub payload: OwnedBytes,
}

impl fmt::Debug for RawMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawMessage")
            .field("metadata", &self.metadata)
            .field("payload", &"<bytes>")
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum PreProcessingError {
    #[error("message can be acknowledged without processing")]
    Discardable { ack_id: String },
    #[error("unexpected message format: {0}")]
    UnexpectedFormat(#[from] anyhow::Error),
}

impl RawMessage {
    pub fn pre_process(
        self,
        message_type: MessageType,
    ) -> Result<PreProcessedMessage, PreProcessingError> {
        let payload = match message_type {
            MessageType::S3Notification => PreProcessedPayload::ObjectUri(
                uri_from_s3_notification(&self.payload, &self.metadata.ack_id)?,
            ),
            MessageType::RawUri => {
                let payload_str = read_to_string(self.payload).context("failed to read payload")?;
                PreProcessedPayload::ObjectUri(Uri::from_str(&payload_str)?)
            }
        };
        Ok(PreProcessedMessage {
            metadata: self.metadata,
            payload,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PreProcessedPayload {
    /// The message contains an object URI
    ObjectUri(Uri),
    // /// The message contains the raw JSON data
    // RawData(OwnedBytes),
}

impl PreProcessedPayload {
    pub fn partition_id(&self) -> PartitionId {
        match &self {
            Self::ObjectUri(uri) => PartitionId::from(uri.as_str()),
        }
    }
}

/// A message that went through the minimal transformation to discover its
/// partition id. Indeed, the message might be discarded if the partition was
/// already processed, so it's better to avoid doing unnecessary work at this
/// stage.
#[derive(Debug, PartialEq, Eq)]
pub struct PreProcessedMessage {
    pub metadata: MessageMetadata,
    pub payload: PreProcessedPayload,
}

impl PreProcessedMessage {
    pub fn partition_id(&self) -> PartitionId {
        self.payload.partition_id()
    }
}

fn uri_from_s3_notification(message: &[u8], ack_id: &str) -> Result<Uri, PreProcessingError> {
    let value: Value = serde_json::from_slice(message).context("invalid JSON message")?;
    if matches!(value["Event"].as_str(), Some("s3:TestEvent")) {
        info!("discarding S3 test event");
        return Err(PreProcessingError::Discardable {
            ack_id: ack_id.to_string(),
        });
    }
    let event_name = value["Records"][0]["eventName"]
        .as_str()
        .context("invalid S3 notification: Records[0].eventName not found")?;
    if !event_name.starts_with("ObjectCreated:") {
        rate_limited_warn!(
            limit_per_min = 5,
            event = event_name,
            "only s3:ObjectCreated:* events are supported"
        );
        return Err(PreProcessingError::Discardable {
            ack_id: ack_id.to_string(),
        });
    }
    let key = value["Records"][0]["s3"]["object"]["key"]
        .as_str()
        .context("invalid S3 notification: Records[0].s3.object.key not found")?;
    let bucket = value["Records"][0]["s3"]["bucket"]["name"]
        .as_str()
        .context("invalid S3 notification: Records[0].s3.bucket.name not found")?;
    let encoded_key = percent_encoding::percent_decode(key.as_bytes())
        .decode_utf8()
        .context("invalid S3 notification: Records[0].s3.object.key could not be url decoded")?;
    Uri::from_str(&format!("s3://{bucket}/{encoded_key}")).map_err(|e| e.into())
}

/// A message for which we know as much of the global processing status as
/// possible and that is now ready to be processed.
pub struct ReadyMessage {
    pub position: Position,
    pub content: PreProcessedMessage,
    pub visibility_handle: VisibilityTaskHandle,
}

impl ReadyMessage {
    pub async fn start_processing(
        self,
        storage_resolver: &StorageResolver,
    ) -> anyhow::Result<Option<InProgressMessage>> {
        let partition_id = self.partition_id();
        match self.content.payload {
            PreProcessedPayload::ObjectUri(uri) => {
                let batch_reader = ObjectUriBatchReader::try_new(
                    storage_resolver,
                    partition_id.clone(),
                    &uri,
                    self.position,
                )
                .await?;
                if batch_reader.is_eof() {
                    Ok(None)
                } else {
                    Ok(Some(InProgressMessage {
                        batch_reader,
                        partition_id,
                        visibility_handle: self.visibility_handle,
                    }))
                }
            }
        }
    }

    pub fn partition_id(&self) -> PartitionId {
        self.content.partition_id()
    }
}

/// A message that is actively being read
pub struct InProgressMessage {
    pub partition_id: PartitionId,
    pub visibility_handle: VisibilityTaskHandle,
    pub batch_reader: ObjectUriBatchReader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_from_s3_notification_valid() {
        let test_message = r#"
        {
            "Records": [
                {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-west-2",
                "eventTime": "2021-05-22T09:22:41.789Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AIDAJDPLRKLG7UEXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "C3D13FE58DE4C810",
                    "x-amz-id-2": "FMyUVURIx7Zv2cPi/IZb9Fk1/U4QfTaVK5fahHPj/"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "mybucket",
                        "ownerIdentity": {
                            "principalId": "A3NL1KOZZKExample"
                        },
                        "arn": "arn:aws:s3:::mybucket"
                    },
                    "object": {
                        "key": "logs.json",
                        "size": 1024,
                        "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                        "versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko",
                        "sequencer": "0055AED6DCD90281E5"
                    }
                }
                }
            ]
        }"#;
        let actual_uri = uri_from_s3_notification(test_message.as_bytes(), "myackid").unwrap();
        let expected_uri = Uri::from_str("s3://mybucket/logs.json").unwrap();
        assert_eq!(actual_uri, expected_uri);
    }

    #[test]
    fn test_uri_from_s3_notification_invalid() {
        let invalid_message = r#"{
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": "test_key"
                        }
                    }
                }
            ]
        }"#;
        let result =
            uri_from_s3_notification(&OwnedBytes::new(invalid_message.as_bytes()), "myackid");
        assert!(matches!(
            result,
            Err(PreProcessingError::UnexpectedFormat(_))
        ));
    }

    #[test]
    fn test_uri_from_s3_bad_event_type() {
        let invalid_message = r#"{
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "2024-07-29T12:47:14.577Z",
                    "eventName": "ObjectRemoved:Delete",
                    "userIdentity": {
                        "principalId": "AWS:ARGHGOHSDGOKGHOGHMCC4:user"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "1.1.1.1"
                    },
                    "responseElements": {
                        "x-amz-request-id": "GHGSH",
                        "x-amz-id-2": "gndflghndflhmnrflsh+gLLKU6X0PvD6ANdVY1+/hspflhjladgfkelagfkndl"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "hello",
                        "bucket": {
                            "name": "mybucket",
                            "ownerIdentity": {
                                "principalId": "KMGP12GHKKH"
                            },
                            "arn": "arn:aws:s3:::mybucket"
                        },
                        "object": {
                            "key": "my_deleted_file",
                            "sequencer": "GKHOFLGKHSALFK0"
                        }
                    }
                }
            ]
        }"#;
        let result =
            uri_from_s3_notification(&OwnedBytes::new(invalid_message.as_bytes()), "myackid");
        assert!(matches!(
            result,
            Err(PreProcessingError::Discardable { .. })
        ));
    }

    #[test]
    fn test_uri_from_s3_notification_discardable() {
        let invalid_message = r#"{
            "Service":"Amazon S3",
            "Event":"s3:TestEvent",
            "Time":"2014-10-13T15:57:02.089Z",
            "Bucket":"bucketname",
            "RequestId":"5582815E1AEA5ADF",
            "HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"
        }"#;
        let result =
            uri_from_s3_notification(&OwnedBytes::new(invalid_message.as_bytes()), "myackid");
        if let Err(PreProcessingError::Discardable { ack_id }) = result {
            assert_eq!(ack_id, "myackid");
        } else {
            panic!("Expected skippable error");
        }
    }

    #[test]
    fn test_uri_from_s3_notification_url_decode() {
        let test_message = r#"
        {
            "Records": [
                {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-west-2",
                "eventTime": "2021-05-22T09:22:41.789Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AIDAJDPLRKLG7UEXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "C3D13FE58DE4C810",
                    "x-amz-id-2": "FMyUVURIx7Zv2cPi/IZb9Fk1/U4QfTaVK5fahHPj/"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "mybucket",
                        "ownerIdentity": {
                            "principalId": "A3NL1KOZZKExample"
                        },
                        "arn": "arn:aws:s3:::mybucket"
                    },
                    "object": {
                        "key": "hello%3A%3Aworld%3A%3Alogs.json",
                        "size": 1024,
                        "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                        "versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko",
                        "sequencer": "0055AED6DCD90281E5"
                    }
                }
                }
            ]
        }"#;
        let actual_uri = uri_from_s3_notification(test_message.as_bytes(), "myackid").unwrap();
        let expected_uri = Uri::from_str("s3://mybucket/hello::world::logs.json").unwrap();
        assert_eq!(actual_uri, expected_uri);
    }
}
