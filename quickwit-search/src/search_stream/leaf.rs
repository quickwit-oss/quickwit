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
use quickwit_proto::SplitIdAndFooterOffsets;
use quickwit_storage::Storage;
use std::sync::Arc;
use tantivy::query::Query;
use tantivy::schema::Type;
use tantivy::LeasedItem;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::error;

// TODO: buffer of 5 seems to be sufficient to do the job locally, needs to be tested on a cluster.
const CONCURRENT_SPLIT_SEARCH_STREAM: usize = 5;

/// `leaf` step of search stream.
// Note: we return a stream of a result with a tonic::Status error
// to be compatible with the stream coming from the grpc client.
// It would be better to have a SearchError but we need then
// to process stream in grpc_adapater.rs to change SearchError
// to tonic::Status as tonic::Status is required by the stream result
// signature defined by proto generated code.
pub async fn leaf_search_stream(
    request: &SearchStreamRequest,
    storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    index_config: Arc<dyn IndexConfig>,
) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResult>> {
    let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
    let request_clone = request.clone();
    tokio::spawn(async move {
        let mut stream =
            leaf_search_results_stream(request_clone, storage, splits, index_config).await;
        while let Some(item) = stream.next().await {
            if let Err(error) = result_sender.send(item) {
                error!(
                    "Failed to send leaf search stream result. Stop sending. Cause: {}",
                    error
                );
                break;
            }
        }
    });
    UnboundedReceiverStream::new(result_receiver)
}

async fn leaf_search_results_stream(
    request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    index_config: Arc<dyn IndexConfig>,
) -> impl futures::Stream<Item = crate::Result<LeafSearchStreamResult>> + Sync + Send + 'static {
    futures::stream::iter(splits)
        .map(move |split| {
            (
                split,
                index_config.clone(),
                request.clone(),
                storage.clone(),
            )
        })
        .map(|(split, index_config_clone, request_clone, storage)| {
            leaf_search_stream_single_split(split, index_config_clone, request_clone, storage)
                .shared()
        })
        .buffer_unordered(CONCURRENT_SPLIT_SEARCH_STREAM)
}

/// Apply a leaf search on a single split.
async fn leaf_search_stream_single_split(
    split: SplitIdAndFooterOffsets,
    index_config: Arc<dyn IndexConfig>,
    stream_request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
) -> crate::Result<LeafSearchStreamResult> {
    let index = open_index(storage, &split).await?;
    let split_schema = index.schema();
    let fast_field_to_extract = stream_request.fast_field.clone();
    let fast_field = split_schema
        .get_field(&fast_field_to_extract)
        .ok_or_else(|| {
            SearchError::InvalidQuery(format!(
                "Fast field `{}` does not exist for split {}.",
                fast_field_to_extract, split.split_id,
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
        SearchError::InternalError(format!(
            "Invalid output format specified for split {}.",
            split.split_id
        ))
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
    let collect_handle = spawn_blocking(move || {
        collect_fast_field_values(
            &fast_field_collector_builder,
            &searcher,
            query,
            output_format,
        )
    });
    let buffer = collect_handle.await.map_err(|error| {
        error!(split_id = %split.split_id, fast_field=%fast_field_to_extract, error_message=%error, "Failed to collect fast field");
        SearchError::InternalError(format!("Error when collecting fast field values for split {}: {:?}", split.split_id, error))
    })??;
    Ok(LeafSearchStreamResult { data: buffer })
}

fn collect_fast_field_values(
    fast_field_collector_builder: &FastFieldCollectorBuilder,
    searcher: &LeasedItem<Searcher>,
    query: Box<dyn Query>,
    output_format: OutputFormat,
) -> crate::Result<Vec<u8>> {
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
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::{str::from_utf8, sync::Arc};

    use quickwit_core::TestSandbox;
    use quickwit_index_config::DefaultIndexConfigBuilder;

    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_leaf_search_stream_to_csv_output_with_filtering() -> anyhow::Result<()> {
        let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "body",
                    "type": "text"
                },
                {
                    "name": "ts",
                    "type": "i64",
                    "fast": true
                }
            ]
        }"#;
        let index_config =
            Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
        let index_id = "single-node-simple";
        let test_sandbox = TestSandbox::create("single-node-simple", index_config.clone()).await?;

        let mut docs = vec![];
        let mut filtered_timestamp_values = vec![];
        let end_timestamp = 20;
        for i in 0..30 {
            let body = format!("info @ t:{}", i + 1);
            docs.push(json!({"body": body, "ts": i+1}));
            if i + 1 < end_timestamp {
                filtered_timestamp_values.push((i + 1).to_string());
            }
        }
        test_sandbox.add_documents(docs).await?;

        let request = SearchStreamRequest {
            index_id: index_id.to_string(),
            query: "info".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: Some(end_timestamp),
            fast_field: "ts".to_string(),
            output_format: 0,
            tags: vec![],
        };
        let index_metadata = test_sandbox.metastore().index_metadata(index_id).await?;
        let splits = test_sandbox.metastore().list_all_splits(index_id).await?;
        let splits_offsets = splits
            .into_iter()
            .map(|split_meta| SplitIdAndFooterOffsets {
                split_id: split_meta.split_metadata.split_id,
                split_footer_start: split_meta.footer_offsets.start,
                split_footer_end: split_meta.footer_offsets.end,
            })
            .collect();
        let mut single_node_stream = leaf_search_stream(
            &request,
            test_sandbox
                .storage_uri_resolver()
                .resolve(&index_metadata.index_uri)?,
            splits_offsets,
            index_config,
        )
        .await;
        let res = single_node_stream.next().await.expect("no leaf result")?;
        assert_eq!(
            from_utf8(&res.data)?,
            format!("{}\n", filtered_timestamp_values.join("\n"))
        );
        Ok(())
    }
}
