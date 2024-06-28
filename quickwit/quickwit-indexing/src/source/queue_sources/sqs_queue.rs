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

use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_sqs::config::{BehaviorVersion, Builder, Region, SharedAsyncSleep};
// use aws_sdk_sqs::error::{DisplayErrorContext, SdkError};
// use aws_sdk_sqs::operation::change_message_visibility::ChangeMessageVisibilityError;
// use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError;
// use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;
use aws_sdk_sqs::types::{DeleteMessageBatchRequestEntry, MessageSystemAttributeName};
use aws_sdk_sqs::{Client, Config};
use itertools::Itertools;
use quickwit_aws::{get_aws_config, DEFAULT_AWS_REGION};
use quickwit_storage::OwnedBytes;
use regex::Regex;

use super::message::MessageMetadata;
use super::{Queue, RawMessage};

#[derive(Debug)]
pub struct SqsQueue {
    sqs_client: Client,
    queue_url: String,
    wait_time_seconds: u8,
}

impl SqsQueue {
    pub async fn try_new(queue_url: String, wait_time_seconds: u8) -> anyhow::Result<Self> {
        let sqs_client = get_sqs_client(&queue_url).await?;
        Ok(SqsQueue {
            sqs_client,
            queue_url,
            wait_time_seconds,
        })
    }
}

#[async_trait]
impl Queue for SqsQueue {
    async fn receive(&self, max_messages: usize) -> anyhow::Result<Vec<RawMessage>> {
        let visibility_timeout_sec = 120;
        // TODO: We estimate the message deadline using the start of the
        // ReceiveMessage request. This might be overly pessimistic: the docs
        // state that it starts when the message is returned.
        let initial_deadline = Instant::now() + Duration::from_secs(visibility_timeout_sec as u64);
        let res = self
            .sqs_client
            .receive_message()
            .queue_url(&self.queue_url)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .wait_time_seconds(self.wait_time_seconds as i32)
            .set_max_number_of_messages(Some(max_messages as i32))
            .visibility_timeout(visibility_timeout_sec)
            .send()
            .await?;

        res.messages
            .unwrap_or_default()
            .into_iter()
            .map(|msg| {
                let delivery_attempts: usize = msg
                    .attributes
                    .as_ref()
                    .and_then(|attrs| {
                        attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount)
                    })
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let ack_id = msg
                    .receipt_handle
                    .context("missing receipt_handle in received message")?;
                let message_id = msg
                    .message_id
                    .context("missing message_id in received message")?;
                Ok(RawMessage {
                    metadata: MessageMetadata {
                        ack_id,
                        message_id,
                        initial_deadline,
                        delivery_attempts,
                    },
                    payload: OwnedBytes::new(msg.body.unwrap_or_default().into_bytes()),
                })
            })
            .collect::<anyhow::Result<_>>()
    }

    async fn acknowledge(&self, ack_ids: &[String]) -> anyhow::Result<()> {
        let entry_batches: Vec<_> = ack_ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                DeleteMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(id.to_string())
                    .build()
                    .unwrap()
            })
            .chunks(10)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect();

        // TODO: retries, partial success and parallelization
        let mut errors = 0;
        let num_batches = entry_batches.len();
        for batch in entry_batches {
            let res = self
                .sqs_client
                .delete_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(batch))
                .send()
                .await;
            if res.is_err() {
                errors += 1;
                if errors == num_batches {
                    // fail when all batches fail and return last err
                    res?;
                }
            }
        }

        Ok(())
    }

    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant> {
        let visibility_timeout = std::cmp::min(suggested_deadline.as_secs() as i32, 43200);
        let new_deadline = Instant::now() + suggested_deadline;
        // TODO: retry if transient
        self.sqs_client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .visibility_timeout(visibility_timeout)
            .receipt_handle(ack_id)
            .send()
            .await?;
        Ok(new_deadline)
    }
}

async fn preconfigured_builder() -> anyhow::Result<Builder> {
    let aws_config = get_aws_config().await;

    let mut sqs_config = Config::builder().behavior_version(BehaviorVersion::v2024_03_28());
    sqs_config.set_retry_config(aws_config.retry_config().cloned());
    sqs_config.set_credentials_provider(aws_config.credentials_provider());
    sqs_config.set_http_client(aws_config.http_client());
    sqs_config.set_timeout_config(aws_config.timeout_config().cloned());

    if let Some(identity_cache) = aws_config.identity_cache() {
        sqs_config.set_identity_cache(identity_cache);
    }
    sqs_config.set_sleep_impl(Some(SharedAsyncSleep::new(
        quickwit_aws::TokioSleep::default(),
    )));

    Ok(sqs_config)
}

fn queue_url_region(queue_url: &str) -> Option<Region> {
    let re = Regex::new(r"^https?://sqs\.(.*?)\.amazonaws\.com").unwrap();
    let caps = re.captures(queue_url)?;
    let region_str = caps.get(1)?.as_str();
    Some(Region::new(region_str.to_string()))
}

fn queue_url_endpoint(queue_url: &str) -> anyhow::Result<String> {
    let re = Regex::new(r"(^https?://[^/]+)").unwrap();
    let caps = re.captures(queue_url).context("Invalid queue URL")?;
    let endpoint_str = caps.get(1).context("Invalid queue URL")?.as_str();
    Ok(endpoint_str.to_string())
}

pub async fn get_sqs_client(queue_url: &str) -> anyhow::Result<Client> {
    let mut sqs_config = preconfigured_builder().await?;
    // region is required by the SDK to work
    let inferred_region = queue_url_region(queue_url).unwrap_or(DEFAULT_AWS_REGION);
    let inferred_endpoint = queue_url_endpoint(queue_url)?;
    sqs_config.set_region(Some(inferred_region));
    sqs_config.set_endpoint_url(Some(inferred_endpoint));
    Ok(Client::from_conf(sqs_config.build()))
}

/// Checks whether we can establish a connection to the SQS service and we can
/// access the provided queue_url
pub(crate) async fn check_connectivity(queue_url: &str) -> anyhow::Result<()> {
    let client = get_sqs_client(queue_url).await?;
    client
        .get_queue_attributes()
        .queue_url(queue_url)
        .send()
        .await?;

    Ok(())
}

#[cfg(test)]
pub mod test_helpers {
    use ulid::Ulid;

    use super::*;

    pub async fn get_localstack_sqs_client() -> anyhow::Result<Client> {
        let mut sqs_config = preconfigured_builder().await?;
        sqs_config.set_endpoint_url(Some("http://localhost:4566".to_string()));
        sqs_config.set_region(Some(DEFAULT_AWS_REGION));
        Ok(Client::from_conf(sqs_config.build()))
    }

    pub(crate) async fn create_queue(sqs_client: &Client, queue_name_prefix: &str) -> String {
        let queue_name = format!("{}-{}", queue_name_prefix, Ulid::new());
        sqs_client
            .create_queue()
            .queue_name(queue_name)
            .send()
            .await
            .unwrap()
            .queue_url
            .unwrap()
    }

    pub(crate) async fn send_message(sqs_client: &Client, queue_url: &str, payload: &str) {
        sqs_client
            .send_message()
            .queue_url(queue_url)
            .message_body(payload.to_string())
            .send()
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_url_region() {
        let url = "https://sqs.eu-west-2.amazonaws.com/12345678910/test";
        let region = queue_url_region(url);
        assert_eq!(region, Some(Region::from_static("eu-west-2")));

        let url = "https://sqs.ap-south-1.amazonaws.com/12345678910/test";
        let region = queue_url_region(url);
        assert_eq!(region, Some(Region::from_static("ap-south-1")));

        let url = "http://localhost:4566/000000000000/test-queue";
        let region = queue_url_region(url);
        assert_eq!(region, None);
    }

    #[test]
    fn test_queue_url_endpoint() {
        let url = "https://sqs.eu-west-2.amazonaws.com/12345678910/test";
        let endpoint = queue_url_endpoint(url).unwrap();
        assert_eq!(endpoint, "https://sqs.eu-west-2.amazonaws.com");

        let url = "https://sqs.ap-south-1.amazonaws.com/12345678910/test";
        let endpoint = queue_url_endpoint(url).unwrap();
        assert_eq!(endpoint, "https://sqs.ap-south-1.amazonaws.com");

        let url = "http://localhost:4566/000000000000/test-queue";
        let endpoint = queue_url_endpoint(url).unwrap();
        assert_eq!(endpoint, "http://localhost:4566");

        let url = "http://localhost:4566/000000000000/test-queue";
        let endpoint = queue_url_endpoint(url).unwrap();
        assert_eq!(endpoint, "http://localhost:4566");
    }
}

#[cfg(all(test, feature = "sqs-localstack-tests"))]
mod localstack_tests {
    use super::*;
    use crate::source::queue_sources::sqs_queue::test_helpers::{
        create_queue, get_localstack_sqs_client,
    };

    #[tokio::test]
    async fn test_check_connectivity() {
        let sqs_client = get_localstack_sqs_client().await.unwrap();
        let queue_url = create_queue(&sqs_client, "check-connectivity").await;
        check_connectivity(&queue_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_receive_existing_msg_quickly() {
        let client = test_helpers::get_localstack_sqs_client().await.unwrap();
        let queue_url = test_helpers::create_queue(&client, "test-receive-existing-msg").await;
        let message = "hello world";
        test_helpers::send_message(&client, &queue_url, message).await;

        let queue = SqsQueue::try_new(queue_url, 20).await.unwrap();
        let messages = tokio::time::timeout(Duration::from_millis(500), queue.receive(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload.as_slice(), message.as_bytes());

        // just assess that there are no errors for now
        queue
            .modify_deadlines(&messages[0].metadata.ack_id, Duration::from_secs(10))
            .await
            .unwrap();
        queue
            .acknowledge(&[messages[0].metadata.ack_id.clone()])
            .await
            .unwrap();
    }
}
