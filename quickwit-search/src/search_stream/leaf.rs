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

use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use super::FastFieldCollector;
use super::collector::PartionnedFastFieldCollector;
use super::collector::PartitionValues;
use crate::leaf::open_index;
use crate::leaf::warmup;
use crate::SearchError;
use futures::{FutureExt, StreamExt};
use quickwit_index_config::IndexConfig;
use quickwit_proto::{
    LeafSearchStreamResult, OutputFormat, SearchRequest, SearchStreamRequest,
    SplitIdAndFooterOffsets,
};
use quickwit_storage::Storage;
use tantivy::fastfield::FastValue;
use tantivy::query::Query;
use tantivy::schema::Field;
use tantivy::schema::FieldType;
use tantivy::schema::Schema;
use tantivy::schema::Type;
use tantivy::{LeasedItem, ReloadPolicy, Searcher};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

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
    request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    index_config: Arc<dyn IndexConfig>,
) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResult>> {
    let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
    let span = info_span!("leaf_search_stream",);
    tokio::spawn(
        async move {
            let mut stream =
                leaf_search_results_stream(request, storage, splits, index_config).await;
            while let Some(item) = stream.next().await {
                if let Err(error) = result_sender.send(item) {
                    error!(
                        "Failed to send leaf search stream result. Stop sending. Cause: {}",
                        error
                    );
                    break;
                }
            }
        }
        .instrument(span),
    );
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
            leaf_search_stream_single_split(
                split,
                index_config.clone(),
                request.clone(),
                storage.clone(),
            )
            .shared()
        })
        .buffer_unordered(CONCURRENT_SPLIT_SEARCH_STREAM)
}

/// Apply a leaf search on a single split.
#[instrument(fields(split_id = %split.split_id), skip(split, index_config, stream_request, storage))]
async fn leaf_search_stream_single_split(
    split: SplitIdAndFooterOffsets,
    index_config: Arc<dyn IndexConfig>,
    stream_request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
) -> crate::Result<LeafSearchStreamResult> {
    let index = open_index(storage, &split).await?;
    let split_schema = index.schema();
    let fast_field = TypedFastField::new(&stream_request.fast_field, &split_schema)?;
    let timestamp_field = index_config.timestamp_field(&split_schema);
    let partition_by_fast_field =
        TypedFastField::from_option(stream_request.partition_by_field.as_deref(), &split_schema)?;
    let output_format = OutputFormat::from_i32(stream_request.output_format).ok_or_else(|| {
        SearchError::InternalError("Invalid output format specified.".to_string())
    })?;
    if partition_by_fast_field.is_some() && output_format != OutputFormat::ClickHouseRowBinary {
        return Err(SearchError::InternalError(
            "Invalid output format specified, only ClickHouseRowBinary is allowed when you provide a parition-by field.".to_string(),
        ));
    }
    let search_request = SearchRequest::from(stream_request.clone());
    let query = index_config.query(split_schema.clone(), &search_request)?;
    let reader = index
        .reader_builder()
        .num_searchers(1)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    warmup(
        &*searcher,
        query.as_ref(),
        &HashSet::new(), // FIXME: pass the right vec.
    )
    .await?;
    let fast_field_name = fast_field.field_name.to_string();
    let span = info_span!(
        "collect_fast_field",
        split_id = %split.split_id,
        fast_field=%fast_field_name,
    );
    let _ = span.enter();
    let collect_handle = spawn_blocking(move || {
        let mut buffer = Vec::new();
        match (fast_field.field_type.value_type(), partition_by_fast_field.as_ref().map(|ff| ff.field_type.value_type())) {
            (Type::I64, None) => {
                let collected_values = collect_values::<i64>(
                    &fast_field,
                    timestamp_field,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize::<i64>(&collected_values, &mut buffer, output_format)
                    .map_err(|_| {
                        SearchError::InternalError(
                            "Error when serializing i64 during export".to_owned(),
                        )
                    })?;
            }
            (Type::U64, Some(Type::U64)) => {
                let collected_values = collect_partitioned_values::<u64, u64>(
                    &fast_field,
                    partition_by_fast_field.clone().unwrap(),
                    timestamp_field,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize_partitions::<i64, u64>(&collected_values, &mut buffer)
                    .map_err(|_| {
                        SearchError::InternalError(
                            "Error when serializing i64 during export".to_owned(),
                        )
                    })?;
            }
            _ => ()
        };
        Result::<Vec<u8>, SearchError>::Ok(buffer)
    });
    let buffer = collect_handle.await.map_err(|error| {
        error!(split_id = %split.split_id, fast_field=%fast_field_name, error_message=%error, "Failed to collect fast field");
        SearchError::InternalError(format!("Error when collecting fast field values for split {}: {:?}", split.split_id, error))
    })??;

    Ok(LeafSearchStreamResult {
        data: buffer,
        split_id: split.split_id,
    })
}

fn collect_values<TFastValue: FastValue>(
    fast_field: &TypedFastField, 
    timestamp_field_opt: Option<Field>, 
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    searcher: LeasedItem<Searcher>,
    query: &dyn Query,
) -> Result<Vec<TFastValue>, SearchError> {
    let collector = FastFieldCollector::<TFastValue> {
        fast_field_to_collect: fast_field.field_name.to_string(),
        timestamp_field_opt,
        start_timestamp_opt: start_timestamp,
        end_timestamp_opt: end_timestamp,
        _marker: PhantomData,
    };
    let result = searcher.search(query, &collector)?;
    Ok(result)
}

fn collect_partitioned_values<TFastValue: FastValue, TPartitionValue: FastValue + Eq + Hash>(
    fast_field: &TypedFastField,
    partition_fast_field: TypedFastField,
    timestamp_field_opt: Option<Field>, 
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
    searcher: LeasedItem<Searcher>,
    query: &dyn Query,
) -> Result<Vec<PartitionValues<TFastValue, TPartitionValue>>, SearchError> {
    let collector = PartionnedFastFieldCollector::<TFastValue, TPartitionValue> {
        fast_field_to_collect: fast_field.field_name.to_string(),
        partition_by_fast_field: partition_fast_field.field_name.to_string(),
        timestamp_field_opt,
        start_timestamp_opt,
        end_timestamp_opt,
        _marker: PhantomData,
    };
    let result = searcher.search(query, &collector)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;
    use std::sync::Arc;

    use quickwit_index_config::DefaultIndexConfigBuilder;
    use quickwit_indexing::TestSandbox;
    use serde_json::json;

    use super::*;

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
            partition_by_field: None,
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
            request,
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

#[derive(Debug, Clone)]
struct TypedFastField {
    pub field_name: String,
    pub field_type: FieldType,
}
impl TypedFastField {
    pub fn from_option(
        field_name: Option<&str>,
        schema: &Schema,
    ) -> crate::Result<Option<TypedFastField>> {
        field_name
            .map(|e| TypedFastField::new(e, schema))
            .transpose()
    }
    pub fn new(field_name: &str, schema: &Schema) -> crate::Result<TypedFastField> {
        // TODO fail if the provided field is not a fast field
        let field = schema.get_field(field_name).ok_or_else(|| {
            SearchError::InvalidQuery(format!("Fast field `{}` does not exist.", field_name))
        })?;
        if schema.get_field_entry(field).is_fast() {
            let field_type = schema.get_field_entry(field).field_type();
            Ok(TypedFastField {
                field_type: field_type.clone(),
                field_name: field_name.to_string(),
            })
        } else {
            Err(SearchError::InvalidQuery(format!(
                "Field `{}` is not a fast field",
                field_name
            )))
        }
    }
}
