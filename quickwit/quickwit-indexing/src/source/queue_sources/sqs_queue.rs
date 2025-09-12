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
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use async_trait::async_trait;
use aws_sdk_sqs::config::{Builder, Region, SharedAsyncSleep};
use aws_sdk_sqs::types::{DeleteMessageBatchRequestEntry, MessageSystemAttributeName};
use aws_sdk_sqs::{Client, Config};
use itertools::Itertools;
use quickwit_aws::retry::{AwsRetryable, aws_retry};
use quickwit_aws::{DEFAULT_AWS_REGION, aws_behavior_version, get_aws_config};
use quickwit_common::rate_limited_error;
use quickwit_common::retry::RetryParams;
use quickwit_storage::OwnedBytes;
use regex::Regex;

use super::message::MessageMetadata;
use super::{Queue, RawMessage};

#[derive(Debug)]
pub struct SqsQueue {
    sqs_client: Client,
    queue_url: String,
    receive_retries: RetryParams,
    acknowledge_retries: RetryParams,
    modify_deadline_retries: RetryParams,
}

impl SqsQueue {
    pub async fn try_new(queue_url: String) -> anyhow::Result<Self> {
        let sqs_client = get_sqs_client(&queue_url).await?;
        Ok(SqsQueue {
            sqs_client,
            queue_url,
            receive_retries: RetryParams::standard(),
            // Acknowledgment is retried when the message is received again
            acknowledge_retries: RetryParams::no_retries(),
            // Retry aggressively to avoid loosing the ownership of the message
            modify_deadline_retries: RetryParams::aggressive(),
        })
    }
}

#[async_trait]
impl Queue for SqsQueue {
    async fn receive(
        self: Arc<Self>,
        max_messages: usize,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Vec<RawMessage>> {
        // TODO: We estimate the message deadline using the start of the
        // ReceiveMessage request. This might be overly pessimistic: the docs
        // state that it starts when the message is returned.
        let initial_deadline = Instant::now() + suggested_deadline;
        let clamped_max_messages = std::cmp::min(max_messages, 10) as i32;
        let receive_output = aws_retry(&self.receive_retries, || async {
            self.sqs_client
                .receive_message()
                .queue_url(&self.queue_url)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .wait_time_seconds(20)
                .set_max_number_of_messages(Some(clamped_max_messages))
                .visibility_timeout(suggested_deadline.as_secs() as i32)
                .send()
                .await
        })
        .await?;

        let received_messages = receive_output.messages.unwrap_or_default();
        let mut resulting_raw_messages = Vec::with_capacity(received_messages.len());
        for received_message in received_messages {
            let delivery_attempts: usize = received_message
                .attributes
                .as_ref()
                .and_then(|attrs| attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let ack_id = received_message
                .receipt_handle
                .context("missing receipt_handle in received message")?;
            let message_id = received_message
                .message_id
                .context("missing message_id in received message")?;
            let raw_message = RawMessage {
                metadata: MessageMetadata {
                    ack_id,
                    message_id,
                    initial_deadline,
                    delivery_attempts,
                },
                payload: OwnedBytes::new(received_message.body.unwrap_or_default().into_bytes()),
            };
            resulting_raw_messages.push(raw_message);
        }
        Ok(resulting_raw_messages)
    }

    async fn acknowledge(&self, ack_ids: &[String]) -> anyhow::Result<()> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        let entry_batches: Vec<Vec<_>> = ack_ids
            .iter()
            .dedup()
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

        // TODO: parallelization
        let mut batch_errors = Vec::new();
        let mut message_errors = Vec::new();
        for batch in entry_batches {
            let res = aws_retry(&self.acknowledge_retries, || {
                self.sqs_client
                    .delete_message_batch()
                    .queue_url(&self.queue_url)
                    .set_entries(Some(batch.clone()))
                    .send()
            })
            .await;
            match res {
                Ok(res) => {
                    message_errors.extend(res.failed.into_iter());
                }
                Err(err) => {
                    batch_errors.push(err);
                }
            }
        }
        if batch_errors.iter().any(|err| !err.is_retryable()) {
            let fatal_error = batch_errors
                .into_iter()
                .find(|err| !err.is_retryable())
                .unwrap();
            bail!(fatal_error);
        } else if !batch_errors.is_empty() {
            rate_limited_error!(
                limit_per_min = 10,
                count = batch_errors.len(),
                first_err = ?batch_errors.into_iter().next().unwrap(),
                "failed to acknowledge some message batches",
            );
        }
        // The documentation is unclear about these partial failures. We assume
        // it is either:
        // - a transient failure
        // - the message is already acknowledged
        // - the message is expired
        if !message_errors.is_empty() {
            rate_limited_error!(
                limit_per_min = 10,
                count = message_errors.len(),
                first_err = ?message_errors.into_iter().next().unwrap(),
                "failed to acknowledge individual messages",
            );
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
        aws_retry(&self.modify_deadline_retries, || {
            self.sqs_client
                .change_message_visibility()
                .queue_url(&self.queue_url)
                .visibility_timeout(visibility_timeout)
                .receipt_handle(ack_id)
                .send()
        })
        .await?;
        Ok(new_deadline)
    }
}

async fn preconfigured_builder() -> anyhow::Result<Builder> {
    let aws_config = get_aws_config().await;

    let mut sqs_config = Config::builder().behavior_version(aws_behavior_version());
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

#[cfg(feature = "sqs-test-helpers")]
pub mod test_helpers {
    use aws_sdk_sqs::types::QueueAttributeName;
    use ulid::Ulid;
    use warp::Filter;

    use super::*;

    pub async fn get_localstack_sqs_client() -> anyhow::Result<Client> {
        let mut sqs_config = preconfigured_builder().await?;
        sqs_config.set_endpoint_url(Some("http://localhost:4566".to_string()));
        sqs_config.set_region(Some(DEFAULT_AWS_REGION));
        Ok(Client::from_conf(sqs_config.build()))
    }

    pub async fn create_queue(sqs_client: &Client, queue_name_prefix: &str) -> String {
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

    pub async fn send_message(sqs_client: &Client, queue_url: &str, payload: &str) {
        sqs_client
            .send_message()
            .queue_url(queue_url)
            .message_body(payload.to_string())
            .send()
            .await
            .unwrap();
    }

    pub async fn get_queue_attribute(
        sqs_client: &Client,
        queue_url: &str,
        attribute: QueueAttributeName,
    ) -> String {
        let queue_attributes = sqs_client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(attribute.clone())
            .send()
            .await
            .unwrap();
        queue_attributes
            .attributes
            .unwrap()
            .get(&attribute)
            .unwrap()
            .to_string()
    }

    /// Runs a mock SQS GetQueueAttributes endpoint to enable creating SQS
    /// sources that pass the connectivity check
    ///
    /// Returns the queue URL to use for the source and a guard for the
    /// temporary mock server
    pub async fn start_mock_sqs_get_queue_attributes_endpoint() -> (String, oneshot::Sender<()>) {
        let hello = warp::path!().map(|| "{}");
        let (tx, rx) = oneshot::channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().unwrap();

        let server = warp::serve(hello).incoming(listener).graceful(async {
            rx.await.ok();
        });
        tokio::spawn(server.run());

        let queue_url = format!("http://{}:{}/", addr.ip(), addr.port());
        (queue_url, tx)
    }

    #[tokio::test]
    async fn test_mock_sqs_get_queue_attributes_endpoint() {
        let (queue_url, _shutdown) = start_mock_sqs_get_queue_attributes_endpoint().await;
        check_connectivity(&queue_url).await.unwrap();
        drop(_shutdown);
        check_connectivity(&queue_url).await.unwrap_err();
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
    use aws_sdk_sqs::types::QueueAttributeName;

    use super::*;
    use crate::source::queue_sources::helpers::QueueReceiver;
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

        let queue = Arc::new(SqsQueue::try_new(queue_url).await.unwrap());
        let messages = tokio::time::timeout(
            Duration::from_millis(500),
            queue.clone().receive(5, Duration::from_secs(60)),
        )
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

    #[tokio::test]
    async fn test_acknowledge_larger_batch() {
        let client = test_helpers::get_localstack_sqs_client().await.unwrap();
        let queue_url = test_helpers::create_queue(&client, "test-ack-large").await;
        let message = "hello world";
        for _ in 0..20 {
            test_helpers::send_message(&client, &queue_url, message).await;
        }

        let queue: Arc<SqsQueue> = Arc::new(SqsQueue::try_new(queue_url.clone()).await.unwrap());
        let mut queue_receiver = QueueReceiver::new(queue.clone(), Duration::from_millis(200));
        let mut messages = Vec::new();
        for _ in 0..5 {
            let new_messages = queue_receiver
                .receive(20, Duration::from_secs(60))
                .await
                .unwrap();
            messages.extend(new_messages.into_iter());
        }
        assert_eq!(messages.len(), 20);
        let in_flight_count: usize = test_helpers::get_queue_attribute(
            &client,
            &queue_url,
            QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
        )
        .await
        .parse()
        .unwrap();
        assert_eq!(in_flight_count, 20);

        let ack_ids = messages
            .iter()
            .map(|msg| msg.metadata.ack_id.clone())
            .collect::<Vec<_>>();

        queue.acknowledge(&ack_ids).await.unwrap();

        let in_flight_count: usize = test_helpers::get_queue_attribute(
            &client,
            &queue_url,
            QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
        )
        .await
        .parse()
        .unwrap();
        assert_eq!(in_flight_count, 0);
    }

    #[tokio::test]
    async fn test_receive_wrong_queue() {
        let client = test_helpers::get_localstack_sqs_client().await.unwrap();
        let queue_url = test_helpers::create_queue(&client, "test-receive-existing-msg").await;
        let bad_queue_url = format!("{queue_url}wrong");
        let queue = Arc::new(SqsQueue::try_new(bad_queue_url).await.unwrap());
        tokio::time::timeout(
            Duration::from_millis(500),
            queue.clone().receive(5, Duration::from_secs(60)),
        )
        .await
        .unwrap()
        .unwrap_err();
    }
}
