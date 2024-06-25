// Copyright (C) 2024 Quickwit, Inc.
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

use std::io::read_to_string;
use std::str::FromStr;
use std::time::Instant;

use anyhow::Context;
use quickwit_actors::Mailbox;
use quickwit_common::uri::Uri;
use quickwit_config::QueueMessageType;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use quickwit_storage::{OwnedBytes, StorageResolver};
use serde_json::Value;

use super::visibility::VisibilityTaskHandle;
use crate::actors::DocProcessor;
use crate::source::doc_file_reader::DocFileReader;
use crate::source::{BatchBuilder, SourceContext, BATCH_NUM_BYTES_LIMIT};

#[derive(Debug, Clone)]
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
#[derive(Clone)]
pub struct RawMessage {
    pub metadata: MessageMetadata,
    pub payload: OwnedBytes,
}

impl RawMessage {
    pub fn pre_process(
        self,
        message_type: QueueMessageType,
    ) -> anyhow::Result<PreProcessedMessage> {
        let payload = match message_type {
            QueueMessageType::S3Notification => {
                PreProcessedPayload::ObjectUri(uri_from_s3_notification(&self.payload)?)
            }
            QueueMessageType::RawUri => {
                PreProcessedPayload::ObjectUri(Uri::from_str(&read_to_string(self.payload)?)?)
            }
        };
        Ok(PreProcessedMessage {
            metadata: self.metadata,
            payload,
        })
    }
}

pub enum PreProcessedPayload {
    ObjectUri(Uri),
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
pub struct PreProcessedMessage {
    pub metadata: MessageMetadata,
    pub payload: PreProcessedPayload,
}

impl PreProcessedMessage {
    pub fn partition_id(&self) -> PartitionId {
        self.payload.partition_id()
    }
}

fn uri_from_s3_notification(message: &OwnedBytes) -> anyhow::Result<Uri> {
    let value: Value = serde_json::from_slice(message.as_slice())?;
    let key = value["Records"][0]["s3"]["object"]["key"]
        .as_str()
        .context("Invalid S3 notification")?;
    let bucket = value["Records"][0]["s3"]["bucket"]["name"]
        .as_str()
        .context("Invalid S3 notification")?;
    Uri::from_str(&format!("s3://{}/{}", bucket, key))
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
        source_type: SourceType,
    ) -> anyhow::Result<Option<InProgressMessage>> {
        let partition_id = self.partition_id();
        match self.content.payload {
            PreProcessedPayload::ObjectUri(uri) => {
                let current_offset = match self.position {
                    Position::Beginning => 0,
                    Position::Offset(offset) => offset
                        .as_usize()
                        .context("file offset should be stored as usize")?,
                    Position::Eof(_) => return Ok(None),
                };
                let reader =
                    DocFileReader::from_uri(storage_resolver, &uri, current_offset).await?;
                Ok(Some(InProgressMessage {
                    progress_tracker: ProgressTracker::ObjectUri(ObjectUriInProgress {
                        reader,
                        current_offset,

                        source_type,
                        is_eof: false,
                    }),
                    partition_id,
                    visibility_handle: self.visibility_handle,
                }))
            }
        }
    }

    pub fn partition_id(&self) -> PartitionId {
        self.content.partition_id()
    }
}

/// A message that is actively being read
pub struct InProgressMessage {
    partition_id: PartitionId,
    visibility_handle: VisibilityTaskHandle,
    progress_tracker: ProgressTracker,
}

pub enum ProgressTracker {
    ObjectUri(ObjectUriInProgress),
}

impl InProgressMessage {
    pub async fn process_batch(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        source_ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        match &mut self.progress_tracker {
            ProgressTracker::ObjectUri(in_progress) => {
                in_progress
                    .process_uri_batch(doc_processor_mailbox, source_ctx, self.partition_id.clone())
                    .await
            }
        }
    }

    pub fn is_eof(&self) -> bool {
        match &self.progress_tracker {
            ProgressTracker::ObjectUri(in_progress) => in_progress.is_eof,
        }
    }

    pub fn partition_id(&self) -> &PartitionId {
        &self.partition_id
    }

    pub fn ack_id(&self) -> &str {
        self.visibility_handle.ack_id()
    }
}

pub struct ObjectUriInProgress {
    reader: DocFileReader,
    current_offset: usize,
    source_type: SourceType,
    is_eof: bool,
}

impl ObjectUriInProgress {
    async fn process_uri_batch(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        source_ctx: &SourceContext,
        partition_id: PartitionId,
    ) -> anyhow::Result<()> {
        let limit_num_bytes = self.current_offset + BATCH_NUM_BYTES_LIMIT as usize;
        let mut new_offset = self.current_offset;
        let mut batch_builder = BatchBuilder::new(self.source_type);
        while new_offset < limit_num_bytes {
            if let Some(record) = source_ctx.protect_future(self.reader.next_record()).await? {
                new_offset = record.next_offset as usize;
                batch_builder.add_doc(record.doc);
            } else {
                self.is_eof = true;
                break;
            }
        }
        if new_offset > self.current_offset {
            let to_position = if self.is_eof {
                Position::eof(new_offset)
            } else {
                Position::offset(new_offset)
            };
            batch_builder
                .checkpoint_delta
                .record_partition_delta(
                    partition_id,
                    Position::offset(self.current_offset),
                    to_position,
                )
                .unwrap();
            doc_processor_mailbox
                .send_message(batch_builder.build())
                .await?;
            self.current_offset = new_offset;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_from_s3_notification() {
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
        let actual_uri =
            uri_from_s3_notification(&OwnedBytes::new(test_message.as_bytes())).unwrap();
        let expected_uri = Uri::from_str("s3://mybucket/logs.json").unwrap();
        assert_eq!(actual_uri, expected_uri);

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
        let result = uri_from_s3_notification(&OwnedBytes::new(invalid_message.as_bytes()));
        assert!(result.is_err());
    }
}
