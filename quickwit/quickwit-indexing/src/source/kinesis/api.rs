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

use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use quickwit_aws::retry::aws_retry;
use quickwit_common::retry::RetryParams;

/// Gets records from a Kinesis data stream's shard.
/// <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html>
pub(crate) async fn get_records(
    kinesis_client: &KinesisClient,
    retry_params: &RetryParams,
    shard_iterator: String,
) -> anyhow::Result<GetRecordsOutput> {
    // TODO: Return an error other than `anyhow::Error` so that expired shard iterators can be
    // handled properly.
    let response = aws_retry(retry_params, || async {
        kinesis_client
            .get_records()
            .shard_iterator(shard_iterator.clone())
            .send()
            .await
    })
    .await?;

    Ok(response)
}

/// Gets a Kinesis shard iterator. A shard iterator expires 5 minutes after it is returned
/// to the requester.
/// <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html>
///
/// The returned shard iterator points to the record positioned right after
/// `from_sequence_number_exclusive` if a value is provided. Otherwise, it points to the first
/// (oldest) record in the shard.
pub(crate) async fn get_shard_iterator(
    kinesis_client: &KinesisClient,
    retry_params: &RetryParams,
    stream_name: &str,
    shard_id: &str,
    from_sequence_number_exclusive: Option<String>,
) -> anyhow::Result<Option<String>> {
    let shard_iterator_type = if from_sequence_number_exclusive.is_some() {
        ShardIteratorType::AfterSequenceNumber
    } else {
        ShardIteratorType::TrimHorizon
    };

    let response = aws_retry(retry_params, || async {
        kinesis_client
            .get_shard_iterator()
            .stream_name(stream_name)
            .shard_id(shard_id.to_string())
            .shard_iterator_type(shard_iterator_type.clone())
            .set_starting_sequence_number(from_sequence_number_exclusive.clone())
            .send()
            .await
    })
    .await?;
    Ok(response.shard_iterator)
}

/// Lists the shards in a stream and provides information about each shard. This operation has a
/// limit of 1000 transactions per second per data stream.
/// <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html>
pub(crate) async fn list_shards(
    kinesis_client: &KinesisClient,
    retry_params: &RetryParams,
    stream_name: &str,
    limit_per_request: Option<usize>,
) -> anyhow::Result<Vec<Shard>> {
    let mut shards = Vec::new();
    let mut next_token = None;

    loop {
        // `stream_name` and `next_token` cannot be set simultaneously.
        let stream_name = if next_token.is_none() {
            Some(stream_name.to_string())
        } else {
            None
        };
        let limit_per_request = limit_per_request.map(|limit| limit as i32);
        let response = aws_retry(retry_params, || async {
            kinesis_client
                .list_shards()
                .set_stream_name(stream_name.clone())
                .set_next_token(next_token.clone())
                .set_max_results(limit_per_request)
                .send()
                .await
        })
        .await?;

        if let Some(shrds) = response.shards {
            shards.extend_from_slice(&shrds);
        }
        if response.next_token.is_none() {
            return Ok(shards);
        }
        next_token = response.next_token;
    }
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
pub(crate) mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use anyhow::{Context, anyhow};
    use aws_sdk_kinesis::types::StreamDescription;

    use super::*;
    use crate::source::kinesis::helpers::tests::DEFAULT_RETRY_PARAMS;

    /// Creates a Kinesis data stream.
    /// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html
    pub(crate) async fn create_stream(
        kinesis_client: &KinesisClient,
        stream_name: &str,
        num_shards: usize,
    ) -> anyhow::Result<()> {
        aws_retry(&DEFAULT_RETRY_PARAMS, || async {
            kinesis_client
                .create_stream()
                .stream_name(stream_name)
                .shard_count(num_shards as i32)
                .send()
                .await
        })
        .await
        .with_context(|| format!("failed to create Kinesis data stream `{stream_name}`"))?;
        Ok(())
    }

    /// Deletes a Kinesis data stream. Only streams in `ACTIVE` state can be deleted.
    /// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeleteStream.html
    pub(crate) async fn delete_stream(
        kinesis_client: &KinesisClient,
        stream_name: &str,
    ) -> anyhow::Result<()> {
        aws_retry(&DEFAULT_RETRY_PARAMS, || async {
            kinesis_client
                .delete_stream()
                .stream_name(stream_name.to_string())
                .send()
                .await
        })
        .await
        .with_context(|| format!("failed to delete Kinesis data stream `{stream_name}`"))?;
        Ok(())
    }

    /// Provides a summarized description of the specified Kinesis data stream without the shard
    /// list. https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html
    pub(crate) async fn describe_stream(
        kinesis_client: &KinesisClient,
        stream_name: &str,
    ) -> anyhow::Result<StreamDescription> {
        let response = aws_retry(&DEFAULT_RETRY_PARAMS, || async {
            kinesis_client
                .describe_stream()
                .stream_name(stream_name.to_string())
                .send()
                .await
        })
        .await?;

        response
            .stream_description
            .ok_or_else(|| anyhow!("no stream summary was returned from AWS"))
    }
    /// Lists the Kinesis data streams.
    /// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListStreams.html
    pub(crate) async fn list_streams(
        kinesis_client: &KinesisClient,
        mut exclusive_start_stream_name: Option<String>,
        limit_per_request: Option<usize>,
    ) -> anyhow::Result<BTreeSet<String>> {
        let mut stream_names = BTreeSet::new();
        let mut has_more_streams = true;
        let limit_per_request = limit_per_request.map(|limit| limit as i32);
        while has_more_streams {
            let response = aws_retry(&DEFAULT_RETRY_PARAMS, || async {
                kinesis_client
                    .list_streams()
                    .set_exclusive_start_stream_name(exclusive_start_stream_name.clone())
                    .set_limit(limit_per_request)
                    .send()
                    .await
            })
            .await?;
            exclusive_start_stream_name = response.stream_names.last().cloned();
            has_more_streams = response.has_more_streams;
            stream_names.extend(response.stream_names);
        }
        Ok(stream_names)
    }

    /// Merges two adjacent shards in a Kinesis data stream and combines them into a single shard.
    /// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_MergeShards.html
    #[cfg(test)]
    pub(crate) async fn merge_shards(
        kinesis_client: &KinesisClient,
        stream_name: &str,
        shard_id: &str,
        adjacent_shard_id: &str,
    ) -> anyhow::Result<()> {
        aws_retry(&DEFAULT_RETRY_PARAMS, || async {
            kinesis_client
                .merge_shards()
                .stream_name(stream_name)
                .shard_to_merge(shard_id)
                .adjacent_shard_to_merge(adjacent_shard_id)
                .send()
                .await
        })
        .await?;
        Ok(())
    }

    /// Splits a shard into two new shards in the Kinesis data stream.
    /// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SplitShard.html
    #[cfg(test)]
    pub(crate) async fn split_shard(
        kinesis_client: &KinesisClient,
        stream_name: &str,
        shard_id: &str,
        starting_hash_key: &str,
    ) -> anyhow::Result<()> {
        aws_retry(&DEFAULT_RETRY_PARAMS, || async {
            kinesis_client
                .split_shard()
                .stream_name(stream_name)
                .shard_to_split(shard_id)
                .new_starting_hash_key(starting_hash_key)
                .send()
                .await
        })
        .await?;
        Ok(())
    }

    /// Waits for a Kinesis data stream's status to satisfy the specified predicate. This is done
    /// through periodically polling the `[describe_stream]` API for the stream. Returns an error
    /// after the specified `timeout` duration has passed.
    #[cfg(test)]
    pub(crate) async fn wait_for_stream_status<P>(
        kinesis_client: &KinesisClient,
        stream_name: &str,
        stream_status_predicate: P,
        timeout: Duration,
    ) -> Result<anyhow::Result<()>, tokio::time::error::Elapsed>
    where
        P: Fn(aws_sdk_kinesis::types::StreamStatus) -> bool,
    {
        tokio::time::timeout(timeout, async {
            let period = Duration::from_millis(if cfg!(test) { 100 } else { 5000 });
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                let stream_status = describe_stream(kinesis_client, stream_name)
                    .await?
                    .stream_status;

                if stream_status_predicate(stream_status) {
                    return Ok(());
                }
            }
        })
        .await
    }
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
mod kinesis_localstack_tests {
    use std::time::Duration;

    use aws_sdk_kinesis::types::StreamStatus;
    use quickwit_common::rand::append_random_suffix;

    use super::*;
    use crate::source::kinesis::api::tests::{
        create_stream, delete_stream, describe_stream, list_streams, wait_for_stream_status,
    };
    use crate::source::kinesis::helpers::tests::{
        DEFAULT_RETRY_PARAMS, get_localstack_client, make_shard_id, put_records_into_shards, setup,
        teardown, wait_for_active_stream,
    };

    #[ignore]
    #[tokio::test]
    async fn test_create_stream() -> anyhow::Result<()> {
        let stream_name = append_random_suffix("test-create-stream");
        let kinesis_client = get_localstack_client().await?;
        create_stream(&kinesis_client, &stream_name, 1).await?;
        wait_for_active_stream(&kinesis_client, &stream_name).await??;
        let description_summary = describe_stream(&kinesis_client, &stream_name).await?;
        assert_eq!(description_summary.stream_name, stream_name);
        assert_eq!(description_summary.stream_status, StreamStatus::Active,);
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_delete_stream() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-delete-stream", 1).await?;
        delete_stream(&kinesis_client, &stream_name).await?;
        let _ = wait_for_stream_status(
            &kinesis_client,
            &stream_name,
            |stream_status| stream_status != StreamStatus::Deleting,
            Duration::from_secs(1),
        )
        .await;
        assert!(
            !list_streams(&kinesis_client, None, None,)
                .await?
                .contains(&stream_name)
        );
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_records() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-get-records", 2).await?;
        let _sequence_numbers = put_records_into_shards(
            &kinesis_client,
            &stream_name,
            [(0, "Record #00"), (0, "Record #01"), (1, "Record #10")],
        )
        .await?;
        let shard_id = make_shard_id(0);
        let shard_iterator = get_shard_iterator(
            &kinesis_client,
            &DEFAULT_RETRY_PARAMS,
            &stream_name,
            &shard_id,
            None,
        )
        .await?;

        let get_records_output = get_records(
            &kinesis_client,
            &DEFAULT_RETRY_PARAMS,
            shard_iterator.unwrap(),
        )
        .await?;

        let records = get_records_output.records;
        assert_eq!(records.len(), 2);
        assert_eq!(std::str::from_utf8(records[0].data.as_ref())?, "Record #00");
        assert_eq!(std::str::from_utf8(records[1].data.as_ref())?, "Record #01");
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    // Ignoring this test because the localstack implementation of Kinesis is bogus.
    #[ignore]
    #[tokio::test]
    async fn test_get_shard_iterator() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-get-shard-iterator", 2).await?;
        let sequence_numbers = put_records_into_shards(
            &kinesis_client,
            &stream_name,
            [(0, "Record #00"), (1, "Record #10")],
        )
        .await?;
        let shard_id = make_shard_id(0);
        {
            let shard_iterator = get_shard_iterator(
                &kinesis_client,
                &DEFAULT_RETRY_PARAMS,
                &stream_name,
                &shard_id,
                None,
            )
            .await?;
            assert!(shard_iterator.is_some());

            let get_records_output = get_records(
                &kinesis_client,
                &DEFAULT_RETRY_PARAMS,
                shard_iterator.unwrap(),
            )
            .await?;
            assert_eq!(get_records_output.records.len(), 1);
        }
        {
            let starting_sequence_number = sequence_numbers.get(&0).unwrap().first().cloned();
            let shard_iterator = get_shard_iterator(
                &kinesis_client,
                &DEFAULT_RETRY_PARAMS,
                &stream_name,
                &shard_id,
                starting_sequence_number,
            )
            .await?;
            assert!(shard_iterator.is_some());

            let get_records_output = get_records(
                &kinesis_client,
                &DEFAULT_RETRY_PARAMS,
                shard_iterator.unwrap(),
            )
            .await?;
            assert_eq!(get_records_output.records.len(), 0)
        }
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_list_shards() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-list-shards", 2).await?;
        let shards = list_shards(
            &kinesis_client,
            &DEFAULT_RETRY_PARAMS,
            &stream_name,
            Some(1),
        )
        .await?;
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].shard_id, make_shard_id(0));
        assert_eq!(shards[1].shard_id, make_shard_id(1));
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    // Ignoring this test because the localstack implementation of Kinesis is bogus.
    #[ignore]
    #[tokio::test]
    async fn test_list_streams() -> anyhow::Result<()> {
        let kinesis_client = get_localstack_client().await?;
        let mut stream_names = Vec::new();

        for stream_name_suffix in ["foo", "bar"] {
            let (_kinesis_client, stream_name) =
                setup(format!("test-list-streams-{stream_name_suffix}"), 1).await?;
            stream_names.push(stream_name);
        }
        {
            let streams = list_streams(&kinesis_client, None, Some(1)).await?;
            assert!(streams.contains(&stream_names[0]));
            assert!(streams.contains(&stream_names[1]));
        }
        {
            let streams = list_streams(
                &kinesis_client,
                Some("test-list-streams-foo".to_string()),
                Some(1),
            )
            .await?;
            assert!(streams.contains(&stream_names[0]));
            assert!(!streams.contains(&stream_names[1]));
        }
        for stream_name in stream_names {
            teardown(&kinesis_client, &stream_name).await;
        }
        Ok(())
    }
}
