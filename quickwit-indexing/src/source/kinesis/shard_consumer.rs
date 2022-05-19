// Copyright (C) 2021 Quickwit, Inc.
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

// TODO: Remove when `KinesisSource` is fully implemented.
#![allow(dead_code)]

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Mailbox};
use quickwit_aws::retry::RetryParams;
use rusoto_kinesis::{KinesisClient, Record};
use serde_json::json;
use tokio::sync::mpsc;

use crate::source::kinesis::api::{get_records, get_shard_iterator};
use crate::source::SourceContext;

use super::retry::RetryPolicyParams;

#[derive(Debug)]
pub(super) enum ShardConsumerMessage {
    /// The shard was the subject of a merge or a split and points to one (merge) or two (split)
    /// children.
    ChildShards(Vec<String>),
    Records {
        shard_id: String,
        records: Vec<Record>,
        lag_millis: Option<i64>,
    },
    /// The shard is closed after a merge or a split. There are no new records available.
    ShardClosed(String),
    /// The consumer has reached the latest record in the shard and stops if
    /// `shutdown_at_shard_eof` is set to true.
    ShardEOF(String),
}

#[derive(Default)]
pub(super) struct ShardConsumerState {
    /// The sequence number of the last record processed.
    current_sequence_number: Option<String>,
    /// The number of milliseconds the last `GetRecords` response is from the tip of the stream.
    lag_millis: Option<i64>,
    /// Number of bytes processed by the consumer.
    num_bytes_processed: u64,
    /// Number of records processed by the consumer.
    num_records_processed: u64,
    /// The shard iterator value that will be used for the next call to `GetRecords`.
    next_shard_iterator: Option<String>,
}

pub(super) struct ShardConsumer {
    stream_name: String,
    shard_id: String,
    /// Sequence number of the last record processed. Consumption of the shard is resumed right
    /// after this sequence number.
    from_sequence_number_exclusive: Option<String>,
    /// When this value is set to true, the consumer shuts down after reaching the last (most
    /// recent) record in the shard.
    shutdown_at_shard_eof: bool,
    state: ShardConsumerState,
    kinesis_client: KinesisClient,
    sink: mpsc::Sender<ShardConsumerMessage>,
    retry_params: RetryParams,
}

impl fmt::Debug for ShardConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KinesisShardConsumer {{ stream_name: {}, shard_id: {} }}",
            self.stream_name, self.shard_id
        )
    }
}

impl ShardConsumer {
    pub fn new(
        stream_name: String,
        shard_id: String,
        from_sequence_number_exclusive: Option<String>,
        shutdown_at_shard_eof: bool,
        kinesis_client: KinesisClient,
        sink: mpsc::Sender<ShardConsumerMessage>,
        retry_params: RetryParams,
    ) -> Self {
        Self {
            stream_name,
            shard_id,
            from_sequence_number_exclusive,
            state: Default::default(),
            shutdown_at_shard_eof,
            kinesis_client,
            sink,
            retry_params,
        }
    }

    pub fn spawn(self, ctx: &SourceContext) -> ShardConsumerHandle {
        let shard_id = self.shard_id.clone();
        let (mailbox, actor_handle) = ctx.spawn_actor(self).spawn();
        ShardConsumerHandle {
            shard_id,
            mailbox,
            actor_handle,
        }
    }

    async fn send_message(
        &self,
        ctx: &ActorContext<Self>,
        message: ShardConsumerMessage,
    ) -> anyhow::Result<()> {
        let _guard = ctx.protect_zone();
        self.sink.send(message).await?;
        Ok(())
    }
}

pub(super) struct ShardConsumerHandle {
    shard_id: String,
    mailbox: Mailbox<ShardConsumer>,
    actor_handle: ActorHandle<ShardConsumer>,
}

#[derive(Debug)]
pub(super) struct Loop;

#[async_trait]
impl Actor for ShardConsumer {
    type ObservableState = serde_json::Value;

    fn name(&self) -> String {
        "KinesisShardConsumer".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.state.next_shard_iterator = get_shard_iterator(
            &self.kinesis_client,
            &self.retry_params,
            &self.stream_name,
            &self.shard_id,
            self.from_sequence_number_exclusive.clone(),
            &self.retry_policy,
        )
        .await?;
        ctx.send_self_message(Loop).await?;
        Ok(())
    }

    fn observable_state(&self) -> Self::ObservableState {
        json!({
            "stream_name": self.stream_name,
            "shard_id": self.shard_id,
            "current_sequence_number": self.state.current_sequence_number,
            "lag_millis": self.state.lag_millis,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_records_processed": self.state.num_records_processed,
        })
    }
}

#[async_trait]
impl Handler<Loop> for ShardConsumer {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(shard_iterator) = self.state.next_shard_iterator.take() {
            let response =
                get_records(&self.kinesis_client, &self.retry_params, shard_iterator).await?;
            self.state.lag_millis = response.millis_behind_latest.clone();
            self.state.next_shard_iterator = response.next_shard_iterator;

            if !response.records.is_empty() {
                self.state.current_sequence_number = response
                    .records
                    .last()
                    .map(|record| record.sequence_number.clone());
                self.state.num_bytes_processed += response
                    .records
                    .iter()
                    .map(|record| record.data.len() as u64)
                    .sum::<u64>();
                self.state.num_records_processed += response.records.len() as u64;

                let message = ShardConsumerMessage::Records {
                    shard_id: self.shard_id.clone(),
                    records: response.records,
                    lag_millis: response.millis_behind_latest,
                };
                self.send_message(ctx, message).await?;
            }
            if let Some(children) = response.child_shards {
                let shard_ids: Vec<String> = children
                    .into_iter()
                    // Filter out duplicate message when two shards are merged.
                    .filter(|child| child.parent_shards.first() == Some(&self.shard_id))
                    .map(|child| child.shard_id)
                    .collect();
                if !shard_ids.is_empty() {
                    let message = ShardConsumerMessage::ChildShards(shard_ids);
                    self.send_message(ctx, message).await?;
                }
            }
            if self.shutdown_at_shard_eof && response.millis_behind_latest == Some(0) {
                let message = ShardConsumerMessage::ShardEOF(self.shard_id.clone());
                self.send_message(ctx, message).await?;
                return Err(ActorExitStatus::Success);
            };
            // The `GetRecords` API has a limit of 5 transactions per second. 1s / 5 + ε = 205ms.
            let interval = Duration::from_millis(205);
            ctx.schedule_self_msg(interval, Loop).await;
            return Ok(());
        }
        let message = ShardConsumerMessage::ShardClosed(self.shard_id.clone());
        self.send_message(ctx, message).await?;
        Err(ActorExitStatus::Success)
    }
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
mod tests {
    use quickwit_actors::Universe;

    use super::*;
    use crate::source::kinesis::api::tests::{merge_shards, split_shard};
    use crate::source::kinesis::helpers::tests::{
        make_shard_id, put_records_into_shards, setup, teardown, DEFAULT_RETRY_PARAMS,
    };

    async fn drain_messages(
        sink_rx: &mut mpsc::Receiver<ShardConsumerMessage>,
    ) -> Vec<ShardConsumerMessage> {
        let mut messages = Vec::new();
        while let Ok(message) = sink_rx.try_recv() {
            messages.push(message);
        }
        messages
    }

    #[tokio::test]
    async fn test_shard_eof() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sink_tx, mut sink_rx) = mpsc::channel(100);
        let (kinesis_client, stream_name) = setup("test-shard-eof", 1).await?;
        let shard_id_0 = make_shard_id(0);
        let shard_consumer = ShardConsumer::new(
            stream_name.clone(),
            shard_id_0.clone(),
            None,
            true,
            kinesis_client.clone(),
            sink_tx,
            DEFAULT_RETRY_PARAMS.clone(),
        );
        let (_mailbox, handle) = universe.spawn_actor(shard_consumer).spawn();
        let (exit_status, exit_state) = handle.join().await;
        assert!(exit_status.is_success());

        let messages = drain_messages(&mut sink_rx).await;
        assert_eq!(messages.len(), 1);

        assert!(matches!(
            &messages[0],
            ShardConsumerMessage::ShardEOF(shard_id) if *shard_id == shard_id_0
        ));
        let expected_state = json!({
            "stream_name": stream_name,
            "shard_id": shard_id_0,
            "current_sequence_number": serde_json::Value::Null,
            "lag_millis": 0,
            "num_bytes_processed": 0,
            "num_records_processed": 0,
        });
        assert_eq!(exit_state, expected_state);

        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_start_at_horizon() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sink_tx, mut sink_rx) = mpsc::channel(100);
        let (kinesis_client, stream_name) = setup("test-start-at-horizon", 1).await?;
        let sequence_numbers = put_records_into_shards(
            &kinesis_client,
            &stream_name,
            [(0, "Record #00"), (0, "Record #01")],
        )
        .await?;
        let shard_id_0 = make_shard_id(0);
        let shard_consumer = ShardConsumer::new(
            stream_name.clone(),
            shard_id_0.clone(),
            None,
            true,
            kinesis_client.clone(),
            sink_tx,
            DEFAULT_RETRY_PARAMS.clone(),
        );
        let (_mailbox, handle) = universe.spawn_actor(shard_consumer).spawn();
        let (exit_status, exit_state) = handle.join().await;
        assert!(exit_status.is_success());

        let messages = drain_messages(&mut sink_rx).await;
        assert_eq!(messages.len(), 2);

        assert!(matches!(
            &messages[0],
            ShardConsumerMessage::Records { shard_id, records, lag_millis: _ } if *shard_id == shard_id_0 && records.len() == 2
        ));
        assert!(matches!(
            &messages[1],
            ShardConsumerMessage::ShardEOF(shard_id) if *shard_id == shard_id_0
        ));
        let current_sequence_number = sequence_numbers
            .get(&0)
            .and_then(|per_shard_sequence_numbers| per_shard_sequence_numbers.last())
            .cloned();
        let expected_state = json!({
            "stream_name": stream_name,
            "shard_id": shard_id_0,
            "current_sequence_number": current_sequence_number,
            "lag_millis": 0,
            "num_bytes_processed": 20,
            "num_records_processed": 2,
        });
        assert_eq!(exit_state, expected_state);

        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_start_after_sequence_number() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sink_tx, mut sink_rx) = mpsc::channel(100);
        let (kinesis_client, stream_name) = setup("test-start-after-sequence-number", 1).await?;
        let sequence_numbers = put_records_into_shards(
            &kinesis_client,
            &stream_name,
            [(0, "Record #00"), (0, "Record #01")],
        )
        .await?;
        let shard_id_0 = make_shard_id(0);
        let from_sequence_number_exclusive = sequence_numbers
            .get(&0)
            .and_then(|sequence_numbers| sequence_numbers.first())
            .cloned();
        let shard_consumer = ShardConsumer::new(
            stream_name.clone(),
            shard_id_0.clone(),
            from_sequence_number_exclusive,
            true,
            kinesis_client.clone(),
            sink_tx,
            DEFAULT_RETRY_PARAMS.clone(),
        );
        let (_mailbox, handle) = universe.spawn_actor(shard_consumer).spawn();
        let (exit_status, exit_state) = handle.join().await;
        assert!(exit_status.is_success());

        let messages = drain_messages(&mut sink_rx).await;
        assert_eq!(messages.len(), 2);

        assert!(matches!(
            &messages[0],
            ShardConsumerMessage::Records { shard_id, records, lag_millis: _ } if *shard_id == shard_id_0 && records.len() == 1
        ));
        assert!(matches!(
            &messages[1],
            ShardConsumerMessage::ShardEOF(shard_id) if *shard_id == shard_id_0
        ));
        let current_sequence_number = sequence_numbers
            .get(&0)
            .and_then(|per_shard_sequence_numbers| per_shard_sequence_numbers.last())
            .cloned();
        let expected_state = json!({
            "stream_name": stream_name,
            "shard_id": shard_id_0,
            "current_sequence_number": current_sequence_number,
            "lag_millis": 0,
            "num_bytes_processed": 10,
            "num_records_processed": 1,
        });
        assert_eq!(exit_state, expected_state);

        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    // This test fails when run against the Localstack Kinesis providers `kinesis-mock` or
    // `kinesalite` since they do not properly implement the `ChildShards` API.
    #[ignore]
    #[tokio::test]
    async fn test_merge_shards() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sink_tx, mut sink_rx) = mpsc::channel(100);
        let (kinesis_client, stream_name) = setup("test-merge-shards", 2).await?;
        let shard_id_0 = make_shard_id(0);
        let shard_id_1 = make_shard_id(1);
        merge_shards(&kinesis_client, &stream_name, &shard_id_0, &shard_id_1).await?;
        {
            let shard_consumer_0 = ShardConsumer::new(
                stream_name.clone(),
                shard_id_0.clone(),
                None,
                false,
                kinesis_client.clone(),
                sink_tx.clone(),
                DEFAULT_RETRY_PARAMS.clone(),
            );
            let (_mailbox, handle) = universe.spawn_actor(shard_consumer_0).spawn();
            let (exit_status, _exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = drain_messages(&mut sink_rx).await;
            assert_eq!(messages.len(), 2);

            assert!(matches!(
                &messages[0],
                ShardConsumerMessage::ChildShards(shard_ids) if *shard_ids == vec![make_shard_id(2)]
            ));
            assert!(matches!(
                &messages[1],
                ShardConsumerMessage::ShardClosed(shard_id) if *shard_id == shard_id_0
            ));
        }
        {
            let shard_consumer_1 = ShardConsumer::new(
                stream_name.clone(),
                shard_id_1.clone(),
                None,
                false,
                kinesis_client.clone(),
                sink_tx,
                DEFAULT_RETRY_PARAMS.clone(),
            );
            let (_mailbox, handle) = universe.spawn_actor(shard_consumer_1).spawn();
            let (exit_status, _exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = drain_messages(&mut sink_rx).await;
            assert_eq!(messages.len(), 1);

            assert!(matches!(
                &messages[0],
                ShardConsumerMessage::ShardClosed(shard_id) if *shard_id == shard_id_1
            ));
        }
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    // This test fails when run against the Localstack Kinesis providers `kinesis-mock` or
    // `kinesalite` since they do not properly implement the `ChildShards` API.
    #[ignore]
    #[tokio::test]
    async fn test_split_shard() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sink_tx, mut sink_rx) = mpsc::channel(100);
        let (kinesis_client, stream_name) = setup("test-split-shard", 1).await?;
        let shard_id_0 = make_shard_id(0);
        split_shard(&kinesis_client, &stream_name, &shard_id_0, "42").await?;

        let shard_consumer = ShardConsumer::new(
            stream_name.clone(),
            shard_id_0.clone(),
            None,
            false,
            kinesis_client.clone(),
            sink_tx,
            DEFAULT_RETRY_PARAMS.clone(),
        );
        let (_mailbox, handle) = universe.spawn_actor(shard_consumer).spawn();
        let (exit_status, _exit_state) = handle.join().await;
        assert!(exit_status.is_success());

        let messages = drain_messages(&mut sink_rx).await;
        assert_eq!(messages.len(), 2);

        assert!(matches!(
            &messages[0],
            ShardConsumerMessage::ChildShards(shard_ids) if *shard_ids == vec![make_shard_id(1), make_shard_id(2)]
        ));
        assert!(matches!(
            &messages[1],
            ShardConsumerMessage::ShardClosed(shard_id) if *shard_id == shard_id_0
        ));
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }
}
