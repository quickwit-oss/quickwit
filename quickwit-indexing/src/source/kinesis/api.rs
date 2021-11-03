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

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamSummaryInput, GetRecordsInput,
    GetRecordsOutput, GetShardIteratorInput, Kinesis, ListShardsInput, ListStreamsInput, Shard,
    StreamDescriptionSummary,
};

/// Creates a Kinesis data stream.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html
async fn create_stream(
    kinesis_client: &dyn Kinesis,
    stream_name: &str,
    num_shards: usize,
) -> anyhow::Result<()> {
    let request = CreateStreamInput {
        stream_name: stream_name.to_string(),
        shard_count: num_shards as i64,
    };
    // TODO: Implement retry.
    kinesis_client
        .create_stream(request)
        .await
        .with_context(|| format!("Failed to create Kinesis data stream `{}`.", stream_name))?;
    Ok(())
}

/// Deletes a Kinesis data stream. Only streams in `ACTIVE` state can be deleted.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeleteStream.html
async fn delete_stream(kinesis_client: &dyn Kinesis, stream_name: &str) -> anyhow::Result<()> {
    let request = DeleteStreamInput {
        stream_name: stream_name.to_string(),
        ..Default::default()
    };
    // TODO: Implement retry.
    kinesis_client
        .delete_stream(request)
        .await
        .with_context(|| format!("Failed to delete Kinesis data stream `{}`.", stream_name))?;
    Ok(())
}

/// Provides a summarized description of the specified Kinesis data stream without the shard list.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html
async fn describe_stream(
    kinesis_client: &dyn Kinesis,
    stream_name: &str,
) -> anyhow::Result<StreamDescriptionSummary> {
    let request = DescribeStreamSummaryInput {
        stream_name: stream_name.to_string(),
    };
    // TODO: Implement retry.
    let response = kinesis_client.describe_stream_summary(request).await?;
    Ok(response.stream_description_summary)
}

/// Gets records from a Kinesis data stream's shard.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
async fn get_records(
    kinesis_client: &dyn Kinesis,
    shard_iterator: String,
) -> anyhow::Result<GetRecordsOutput> {
    let request = GetRecordsInput {
        shard_iterator,
        limit: None,
    };
    // TODO: Implement retry.
    // TODO: Return an error other than `anyhow::Error` so that expired shard iterators can be
    // handled properly.
    let response = kinesis_client.get_records(request).await?;
    Ok(response)
}

/// Gets a Kinesis shard iterator. A shard iterator expires 5 minutes after it is returned
/// to the requester.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
async fn get_shard_iterator(
    kinesis_client: &dyn Kinesis,
    stream_name: &str,
    shard_id: &str,
    starting_sequence_number: Option<String>,
) -> anyhow::Result<Option<String>> {
    let shard_iterator_type = if starting_sequence_number.is_some() {
        "AFTER_SEQUENCE_NUMBER"
    } else {
        "TRIM_HORIZON"
    }
    .to_string();
    let request = GetShardIteratorInput {
        stream_name: stream_name.to_string(),
        shard_id: shard_id.to_string(),
        shard_iterator_type,
        starting_sequence_number,
        ..Default::default()
    };
    // TODO: Implement retry.
    let response = kinesis_client.get_shard_iterator(request).await?;
    Ok(response.shard_iterator)
}

/// Lists the shards in a stream and provides information about each shard. This operation has a
/// limit of 1000 transactions per second per data stream.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html
async fn list_shards(
    kinesis_client: &dyn Kinesis,
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
        let request = ListShardsInput {
            stream_name,
            next_token,
            max_results: limit_per_request.map(|limit| limit as i64).clone(),
            ..Default::default()
        };
        // TODO: Implement retry.
        let response = kinesis_client.list_shards(request).await?;

        if let Some(shrds) = response.shards {
            shards.extend_from_slice(&shrds);
        }
        if response.next_token.is_none() {
            return Ok(shards);
        }
        next_token = response.next_token;
    }
}

/// Lists the Kinesis data streams.
/// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListStreams.html
async fn list_streams(
    kinesis_client: &dyn Kinesis,
    mut exclusive_start_stream_name: Option<String>,
    limit_per_request: Option<usize>,
) -> anyhow::Result<BTreeSet<String>> {
    let mut stream_names = BTreeSet::new();
    let mut has_more_streams = true;

    while has_more_streams {
        let request = ListStreamsInput {
            exclusive_start_stream_name,
            limit: limit_per_request.map(|limit| limit as i64).clone(),
        };
        // TODO: Implement retry.
        let response = kinesis_client.list_streams(request).await?;
        exclusive_start_stream_name = response.stream_names.last().cloned();
        has_more_streams = response.has_more_streams;
        stream_names.extend(response.stream_names);
    }
    Ok(stream_names)
}

/// Waits for a Kinesis data stream's status to satisfy the specified predicate. This is done
/// through periodically polling the `[describe_stream]` API for the stream. Returns an error after
/// the specified `timeout` duration has passed.
async fn wait_for_stream_status<P>(
    kinesis_client: &dyn Kinesis,
    stream_name: &str,
    stream_status_predicate: P,
    timeout: Duration,
) -> Result<anyhow::Result<()>, tokio::time::error::Elapsed>
where
    P: Fn(&str) -> bool,
{
    tokio::time::timeout(timeout, async {
        let period = Duration::from_millis(if cfg!(test) { 100 } else { 5000 });
        let mut interval = tokio::time::interval(period);
        loop {
            interval.tick().await;
            let stream_status = describe_stream(kinesis_client, stream_name)
                .await?
                .stream_status;
            if stream_status_predicate(&stream_status) {
                return Ok(());
            }
        }
    })
    .await
}

#[cfg(all(test, feature = "kinesis-external-service"))]
mod kinesis_localstack_tests {
    use std::collections::HashMap;

    use quickwit_common::rand::append_random_suffix;
    use rusoto_core::Region;
    use rusoto_kinesis::{KinesisClient, PutRecordsInput, PutRecordsRequestEntry};

    use super::*;

    fn get_localstack_client() -> KinesisClient {
        KinesisClient::new(Region::Custom {
            name: "localstack".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        })
    }

    fn make_shard_id(id: usize) -> String {
        format!("shardId-{:0>12}", id)
    }

    fn parse_shard_id<S: AsRef<str>>(shard_id: S) -> Option<usize> {
        shard_id
            .as_ref()
            .strip_prefix("shardId-")
            .and_then(|shard_id| shard_id.parse::<usize>().ok())
    }

    async fn put_records_into_shards<I>(
        kinesis_client: &dyn Kinesis,
        stream_name: &str,
        records: I,
    ) -> anyhow::Result<HashMap<usize, Vec<String>>>
    where
        I: IntoIterator<Item = (usize, &'static str)>,
    {
        let shard_hash_keys: HashMap<usize, String> =
            list_shards(kinesis_client, &stream_name, None)
                .await?
                .into_iter()
                .map(|shard| {
                    (
                        parse_shard_id(shard.shard_id).unwrap(),
                        shard.hash_key_range.starting_hash_key,
                    )
                })
                .collect();

        let put_records_request_entries = records
            .into_iter()
            .map(|(shard_id, record)| PutRecordsRequestEntry {
                explicit_hash_key: shard_hash_keys.get(&shard_id).cloned(),
                partition_key: "Overridden by hash key".to_string(),
                data: bytes::Bytes::from(record),
            })
            .collect();

        let request = PutRecordsInput {
            stream_name: stream_name.to_string(),
            records: put_records_request_entries,
        };
        let response = kinesis_client.put_records(request).await?;

        let mut sequence_numbers = HashMap::new();
        for record in response.records {
            sequence_numbers
                .entry(record.shard_id.and_then(parse_shard_id).unwrap())
                .or_insert_with(Vec::new)
                .push(record.sequence_number.unwrap());
        }
        Ok(sequence_numbers)
    }

    async fn setup<S: AsRef<str>>(
        test_name: S,
        num_shards: usize,
    ) -> anyhow::Result<(KinesisClient, String)> {
        let stream_name = append_random_suffix(test_name.as_ref());
        let kinesis_client = get_localstack_client();
        create_stream(&kinesis_client, &stream_name, num_shards).await?;
        wait_for_active_stream(&kinesis_client, &stream_name).await??;
        Ok((kinesis_client, stream_name))
    }

    async fn teardown(kinesis_client: &dyn Kinesis, stream_name: &str) {
        let _delete_res = delete_stream(kinesis_client, stream_name).await;
    }

    async fn wait_for_active_stream(
        kinesis_client: &dyn Kinesis,
        stream_name: &str,
    ) -> Result<anyhow::Result<()>, tokio::time::error::Elapsed> {
        wait_for_stream_status(
            kinesis_client,
            stream_name,
            |stream_status| stream_status == "ACTIVE",
            Duration::from_secs(1),
        )
        .await
    }

    #[tokio::test]
    async fn test_create_stream() -> anyhow::Result<()> {
        let stream_name = append_random_suffix("test-create-stream");
        let kinesis_client = get_localstack_client();
        create_stream(&kinesis_client, &stream_name, 1).await?;
        wait_for_active_stream(&kinesis_client, &stream_name).await??;
        let description_summary = describe_stream(&kinesis_client, &stream_name).await?;
        assert_eq!(description_summary.stream_name, stream_name);
        assert_eq!(description_summary.open_shard_count, 1);
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_stream() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-delete-stream", 1).await?;
        delete_stream(&kinesis_client, &stream_name).await?;
        let _ = wait_for_stream_status(
            &kinesis_client,
            &stream_name,
            |stream_status| stream_status != "DELETING",
            Duration::from_secs(1),
        )
        .await;
        assert!(!list_streams(&kinesis_client, None, None,)
            .await?
            .contains(&stream_name));
        Ok(())
    }

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
        let shard_iterator =
            get_shard_iterator(&kinesis_client, &stream_name, &shard_id, None).await?;

        let get_records_output = get_records(&kinesis_client, shard_iterator.unwrap()).await?;
        assert_eq!(get_records_output.records.len(), 2);
        assert_eq!(
            std::str::from_utf8(&get_records_output.records[0].data)?,
            "Record #00"
        );
        assert_eq!(
            std::str::from_utf8(&get_records_output.records[1].data)?,
            "Record #01"
        );
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

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
            let shard_iterator =
                get_shard_iterator(&kinesis_client, &stream_name, &shard_id, None).await?;
            assert!(shard_iterator.is_some());

            let get_records_output = get_records(&kinesis_client, shard_iterator.unwrap()).await?;
            assert_eq!(get_records_output.records.len(), 1);
        }
        {
            let starting_sequence_number = sequence_numbers.get(&0).unwrap().first().cloned();
            let shard_iterator = get_shard_iterator(
                &kinesis_client,
                &stream_name,
                &shard_id,
                starting_sequence_number,
            )
            .await?;
            assert!(shard_iterator.is_some());

            let get_records_output = get_records(&kinesis_client, shard_iterator.unwrap()).await?;
            assert_eq!(get_records_output.records.len(), 0)
        }
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_list_shards() -> anyhow::Result<()> {
        let (kinesis_client, stream_name) = setup("test-list-shards", 2).await?;
        let shards = list_shards(&kinesis_client, &stream_name, Some(1)).await?;
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].shard_id, make_shard_id(0));
        assert_eq!(shards[1].shard_id, make_shard_id(1));
        teardown(&kinesis_client, &stream_name).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_list_streams() -> anyhow::Result<()> {
        let kinesis_client = get_localstack_client();
        let mut stream_names = Vec::new();

        for stream_name_suffix in ["foo", "bar"] {
            let (_kinesis_client, stream_name) =
                setup(format!("test-list-streams-{}", stream_name_suffix), 1).await?;
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
