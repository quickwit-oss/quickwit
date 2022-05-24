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

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use anyhow::bail;
    use once_cell::sync::Lazy;
    use quickwit_aws::retry::RetryParams;
    use quickwit_common::rand::append_random_suffix;
    use rusoto_core::Region;
    use rusoto_kinesis::{Kinesis, KinesisClient, PutRecordsInput, PutRecordsRequestEntry};
    use tracing::error;

    use crate::source::kinesis::api::list_shards;
    use crate::source::kinesis::api::tests::{
        create_stream, delete_stream, wait_for_stream_status,
    };

    pub static DEFAULT_RETRY_PARAMS: Lazy<RetryParams> = Lazy::new(RetryParams::default);

    pub fn get_localstack_client() -> KinesisClient {
        KinesisClient::new(Region::Custom {
            name: "localstack".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        })
    }

    pub fn make_shard_id(id: usize) -> String {
        format!("shardId-{:0>12}", id)
    }

    pub fn parse_shard_id<S: AsRef<str>>(shard_id: S) -> Option<usize> {
        shard_id
            .as_ref()
            .strip_prefix("shardId-")
            .and_then(|shard_id| shard_id.parse::<usize>().ok())
    }

    pub async fn put_records_into_shards<I>(
        kinesis_client: &KinesisClient,
        stream_name: &str,
        records: I,
    ) -> anyhow::Result<HashMap<usize, Vec<String>>>
    where
        I: IntoIterator<Item = (usize, &'static str)>,
    {
        let shard_hash_keys: HashMap<usize, String> =
            list_shards(kinesis_client, &DEFAULT_RETRY_PARAMS, &stream_name, None)
                .await?
                .into_iter()
                .flat_map(|shard| {
                    parse_shard_id(shard.shard_id)
                        .map(|shard_id| (shard_id, shard.hash_key_range.starting_hash_key))
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
            if let Some(sequence_number) = record.sequence_number {
                sequence_numbers
                    .entry(record.shard_id.and_then(parse_shard_id).unwrap())
                    .or_insert_with(Vec::new)
                    .push(sequence_number);
            } else {
                bail!("Sequence number is missing from record.");
            }
        }
        Ok(sequence_numbers)
    }

    pub async fn setup<S: AsRef<str>>(
        test_name: S,
        num_shards: usize,
    ) -> anyhow::Result<(KinesisClient, String)> {
        let stream_name = append_random_suffix(test_name.as_ref());
        let kinesis_client = get_localstack_client();
        create_stream(&kinesis_client, &stream_name, num_shards).await?;
        wait_for_active_stream(&kinesis_client, &stream_name).await??;
        Ok((kinesis_client, stream_name))
    }

    pub async fn teardown(kinesis_client: &dyn Kinesis, stream_name: &str) {
        if let Err(error) = delete_stream(kinesis_client, stream_name).await {
            error!(stream_name = %stream_name, error = ?error, "Failed to delete stream.")
        }
    }

    pub async fn wait_for_active_stream(
        kinesis_client: &dyn Kinesis,
        stream_name: &str,
    ) -> Result<anyhow::Result<()>, tokio::time::error::Elapsed> {
        wait_for_stream_status(
            kinesis_client,
            stream_name,
            |stream_status| stream_status == "ACTIVE",
            Duration::from_secs(30),
        )
        .await
    }
}
