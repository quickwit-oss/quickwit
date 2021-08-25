// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use super::FastFieldCollectorBuilder;
use crate::leaf::open_index;
use crate::leaf::warmup;
use crate::SearchError;
use futures::{FutureExt, StreamExt};
use quickwit_index_config::IndexConfig;
use quickwit_proto::LeafSearchStreamResult;
use quickwit_proto::OutputFormat;
use quickwit_proto::SearchRequest;
use quickwit_proto::SearchStreamRequest;
use quickwit_storage::Storage;
use std::sync::Arc;
use tantivy::schema::Type;
use tantivy::ReloadPolicy;

const CONCURRENT_SPLIT_SEARCH_STREAM: usize = 2;

/// `leaf` step of search stream.
// Note: we return a stream of a result with a tonic::Status error
// to be compatible with the stream coming from the grpc client.
// It would be better to have a SearchError but we need then
// to process stream in grpc_adapater.rs to change SearchError
// to tonic::Status as tonic::Status is required by the stream result
// signature defined by proto generated code.
pub async fn leaf_search_stream(
    index_config: Arc<dyn IndexConfig>,
    request: SearchStreamRequest,
    split_ids: Vec<String>,
    storage: Arc<dyn Storage>,
) -> impl futures::Stream<Item = crate::Result<LeafSearchStreamResult>> + Sync + Send + 'static {
    futures::stream::iter(split_ids)
        .map(move |split_id| {
            let index_config_clone = index_config.clone();
            let request_clone = request.clone();
            let split_storage: Arc<dyn Storage> =
                quickwit_storage::add_prefix_to_storage(storage.clone(), split_id);
            (index_config_clone, request_clone, split_storage)
        })
        .map(|(index_config_clone, request_clone, split_storage)| {
            leaf_search_stream_single_split(index_config_clone, request_clone, split_storage)
                .shared()
        })
        .buffered(CONCURRENT_SPLIT_SEARCH_STREAM)
}

/// Apply a leaf search on a single split.
async fn leaf_search_stream_single_split(
    index_config: Arc<dyn IndexConfig>,
    stream_request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
) -> crate::Result<LeafSearchStreamResult> {
    let index = open_index(storage).await?;
    let split_schema = index.schema();
    let fast_field_to_extract = stream_request.fast_field.clone();
    let fast_field = split_schema
        .get_field(&fast_field_to_extract)
        .ok_or_else(|| {
            SearchError::InvalidQuery(format!(
                "Fast field `{}` does not exist.",
                fast_field_to_extract
            ))
        })?;
    let fast_field_type = split_schema.get_field_entry(fast_field).field_type();
    let fast_field_collector_builder = FastFieldCollectorBuilder::new(
        fast_field_type.value_type(),
        stream_request.fast_field.clone(),
        index_config.timestamp_field_name(),
        index_config.timestamp_field(&split_schema),
        stream_request.start_timestamp,
        stream_request.end_timestamp,
    )?;

    let output_format = OutputFormat::from_i32(stream_request.output_format).ok_or_else(|| {
        SearchError::InternalError("Invalid output format specified.".to_string())
    })?;
    let search_request = SearchRequest::from(stream_request.clone());
    let query = index_config.query(split_schema, &search_request)?;

    let reader = index
        .reader_builder()
        .num_searchers(1)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    warmup(
        &*searcher,
        query.as_ref(),
        fast_field_collector_builder.fast_field_to_warm(),
    )
    .await?;
    let mut buffer = Vec::new();
    match fast_field_collector_builder.value_type() {
        Type::I64 => {
            let fast_field_collector = fast_field_collector_builder.build_i64();
            let fast_field_values = searcher.search(query.as_ref(), &fast_field_collector)?;
            super::serialize::<i64>(&fast_field_values, &mut buffer, output_format).map_err(
                |_| {
                    SearchError::InternalError(
                        "Error when serializing i64 during export".to_owned(),
                    )
                },
            )?;
        }
        Type::U64 => {
            let fast_field_collector = fast_field_collector_builder.build_u64();
            let fast_field_values = searcher.search(query.as_ref(), &fast_field_collector)?;
            super::serialize::<u64>(&fast_field_values, &mut buffer, output_format).map_err(
                |_| {
                    SearchError::InternalError(
                        "Error when serializing u64 during export".to_owned(),
                    )
                },
            )?;
        }
        value_type => {
            return Err(SearchError::InvalidQuery(format!(
                "Fast field type `{:?}` not supported",
                value_type
            )));
        }
    }
    Ok(LeafSearchStreamResult { data: buffer })
}
