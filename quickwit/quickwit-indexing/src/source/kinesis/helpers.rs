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

use aws_sdk_kinesis::config::{Region, SharedAsyncSleep};
use aws_sdk_kinesis::{Client, Config};
use quickwit_aws::{DEFAULT_AWS_REGION, aws_behavior_version, get_aws_config};
use quickwit_config::RegionOrEndpoint;

pub async fn get_kinesis_client(region_or_endpoint: RegionOrEndpoint) -> anyhow::Result<Client> {
    let aws_config = get_aws_config().await;

    let mut kinesis_config = Config::builder().behavior_version(aws_behavior_version());
    kinesis_config.set_retry_config(aws_config.retry_config().cloned());
    kinesis_config.set_credentials_provider(aws_config.credentials_provider());
    kinesis_config.set_http_client(aws_config.http_client());
    kinesis_config.set_timeout_config(aws_config.timeout_config().cloned());
    if let Some(identity_cache) = aws_config.identity_cache() {
        kinesis_config.set_identity_cache(identity_cache);
    }
    kinesis_config.set_sleep_impl(Some(SharedAsyncSleep::new(
        quickwit_aws::TokioSleep::default(),
    )));

    match region_or_endpoint {
        RegionOrEndpoint::Region(region) => {
            kinesis_config = kinesis_config.region(Some(Region::new(region)));
        }
        RegionOrEndpoint::Endpoint(endpoint) => {
            kinesis_config = kinesis_config.endpoint_url(endpoint);
            kinesis_config = kinesis_config.region(Some(DEFAULT_AWS_REGION));
        }
    }

    Ok(Client::from_conf(kinesis_config.build()))
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use anyhow::bail;
    use aws_sdk_kinesis::Client as KinesisClient;
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::{PutRecordsRequestEntry, StreamStatus};
    use once_cell::sync::Lazy;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::retry::RetryParams;
    use quickwit_config::RegionOrEndpoint;
    use tracing::error;

    use crate::source::kinesis::api::list_shards;
    use crate::source::kinesis::api::tests::{
        create_stream, delete_stream, wait_for_stream_status,
    };
    use crate::source::kinesis::helpers::get_kinesis_client;

    pub static DEFAULT_RETRY_PARAMS: Lazy<RetryParams> = Lazy::new(RetryParams::standard);

    pub async fn get_localstack_client() -> anyhow::Result<KinesisClient> {
        let endpoint = RegionOrEndpoint::Endpoint("http://localhost:4566".to_string());
        get_kinesis_client(endpoint).await
    }

    pub fn make_shard_id(id: usize) -> String {
        format!("shardId-{id:0>12}")
    }

    pub fn parse_shard_id<S: AsRef<str>>(shard_id: S) -> Option<usize> {
        shard_id
            .as_ref()
            .strip_prefix("shardId-")
            .and_then(|shard_id| shard_id.parse::<usize>().ok())
    }

    pub async fn put_records_into_shards<I>(
        kinesis_client: &aws_sdk_kinesis::Client,
        stream_name: &str,
        records: I,
    ) -> anyhow::Result<HashMap<usize, Vec<String>>>
    where
        I: IntoIterator<Item = (usize, &'static str)>,
    {
        let shard_hash_keys: HashMap<usize, String> =
            list_shards(kinesis_client, &DEFAULT_RETRY_PARAMS, stream_name, None)
                .await?
                .into_iter()
                .flat_map(|shard| {
                    let starting_hash_key = shard.hash_key_range?.starting_hash_key;
                    parse_shard_id(shard.shard_id).map(|shard_id| (shard_id, starting_hash_key))
                })
                .collect();

        let put_records_request_entries = records
            .into_iter()
            .map(|(shard_id, record)| {
                PutRecordsRequestEntry::builder()
                    .set_explicit_hash_key(shard_hash_keys.get(&shard_id).cloned())
                    .partition_key("Overridden by hash key".to_string())
                    .data(Blob::new(record.as_bytes()))
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let response = kinesis_client
            .put_records()
            .stream_name(stream_name.to_string())
            .set_records(Some(put_records_request_entries))
            .send()
            .await?;

        let mut sequence_numbers = HashMap::new();
        for record in response.records {
            if let Some(sequence_number) = record.sequence_number {
                sequence_numbers
                    .entry(record.shard_id.and_then(parse_shard_id).unwrap())
                    .or_insert_with(Vec::new)
                    .push(sequence_number);
            } else {
                bail!("sequence number is missing from record");
            }
        }
        Ok(sequence_numbers)
    }

    pub async fn setup<S: AsRef<str>>(
        test_name: S,
        num_shards: usize,
    ) -> anyhow::Result<(aws_sdk_kinesis::Client, String)> {
        let stream_name = append_random_suffix(test_name.as_ref());
        let kinesis_client = get_localstack_client().await?;
        create_stream(&kinesis_client, &stream_name, num_shards).await?;
        wait_for_active_stream(&kinesis_client, &stream_name).await??;
        Ok((kinesis_client, stream_name))
    }

    pub async fn teardown(kinesis_client: &aws_sdk_kinesis::Client, stream_name: &str) {
        if let Err(error) = delete_stream(kinesis_client, stream_name).await {
            error!(stream_name = %stream_name, error = ?error, "Failed to delete stream.")
        }
    }

    pub async fn wait_for_active_stream(
        kinesis_client: &aws_sdk_kinesis::Client,
        stream_name: &str,
    ) -> Result<anyhow::Result<()>, tokio::time::error::Elapsed> {
        wait_for_stream_status(
            kinesis_client,
            stream_name,
            |stream_status| stream_status == StreamStatus::Active,
            Duration::from_secs(30),
        )
        .await
    }
}
