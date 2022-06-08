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

mod api;
mod helpers;
pub mod kinesis_source;
mod shard_consumer;

use quickwit_aws::retry::RetryParams;
use quickwit_config::KinesisSourceParams;

use crate::source::kinesis::api::{get_records, get_shard_iterator, list_shards};
use crate::source::kinesis::helpers::get_kinesis_client;
use crate::source::kinesis::kinesis_source::get_region;

/// Checks whether we can establish a connection to the Kinesis service and read some records.
pub(super) async fn check_connectivity(params: KinesisSourceParams) -> anyhow::Result<()> {
    let region = get_region(params.region_or_endpoint)?;
    let kinesis_client = get_kinesis_client(region)?;
    let retry_params = RetryParams {
        max_attempts: 3,
        ..Default::default()
    };
    let shards = list_shards(&kinesis_client, &retry_params, &params.stream_name, Some(1)).await?;

    if let Some(shard) = shards.get(0) {
        let shard_iterator_opt = get_shard_iterator(
            &kinesis_client,
            &retry_params,
            &params.stream_name,
            &shard.shard_id,
            None,
        )
        .await?;

        if let Some(shard_iterator) = shard_iterator_opt {
            get_records(&kinesis_client, &retry_params, shard_iterator).await?;
        }
    }
    Ok(())
}
