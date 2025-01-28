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

mod api;
mod helpers;
pub mod kinesis_source;
mod shard_consumer;

use quickwit_common::retry::RetryParams;
use quickwit_config::KinesisSourceParams;

use crate::source::kinesis::api::{get_records, get_shard_iterator, list_shards};
use crate::source::kinesis::helpers::get_kinesis_client;
use crate::source::kinesis::kinesis_source::get_region;

/// Checks whether we can establish a connection to the Kinesis service and read some records.
pub(super) async fn check_connectivity(params: KinesisSourceParams) -> anyhow::Result<()> {
    let region = get_region(params.region_or_endpoint).await?;
    let kinesis_client = get_kinesis_client(region).await?;
    let retry_params = RetryParams::standard();
    let shards = list_shards(&kinesis_client, &retry_params, &params.stream_name, Some(1)).await?;

    if let Some(shard_id) = shards.first().map(|s| s.shard_id()) {
        let shard_iterator_opt = get_shard_iterator(
            &kinesis_client,
            &retry_params,
            &params.stream_name,
            shard_id,
            None,
        )
        .await?;

        if let Some(shard_iterator) = shard_iterator_opt {
            get_records(&kinesis_client, &retry_params, shard_iterator).await?;
        }
    }
    Ok(())
}
