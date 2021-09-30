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

use futures::{FutureExt, StreamExt};
use quickwit_index_config::IndexConfig;
use quickwit_proto::{
    LeafSearchStreamResult, OutputFormat, SearchRequest, SearchStreamRequest,
    SplitIdAndFooterOffsets,
};
use quickwit_storage::Storage;
use tantivy::fastfield::FastValue;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema, Type};
use tantivy::{LeasedItem, ReloadPolicy, Searcher};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

use super::collector::{PartionnedFastFieldCollector, PartitionValues};
use super::FastFieldCollector;
use crate::leaf::{open_index, warmup};
use crate::{Result, SearchError};

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

    let request_fields = Arc::new(SearchStreamRequestFields::from_request(
        &stream_request,
        &split_schema,
        index_config.as_ref(),
    )?);

    let output_format = OutputFormat::from_i32(stream_request.output_format).ok_or_else(|| {
        SearchError::InternalError("Invalid output format specified.".to_string())
    })?;

    if request_fields.partition_by_fast_field.is_some()
        && output_format != OutputFormat::ClickHouseRowBinary
    {
        return Err(SearchError::InternalError(
            "Invalid output format specified, only ClickHouseRowBinary is allowed when you \
             provide a parition-by field."
                .to_string(),
        ));
    }

    let search_request = Arc::new(SearchRequest::from(stream_request.clone()));
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
        &request_fields.fast_fields_for_request(),
    )
    .await?;

    let span = info_span!(
        "collect_fast_field",
        split_id = %split.split_id,
        request_fields=%request_fields,
    );

    let _ = span.enter();
    let m_request_fields = request_fields.clone();
    let collect_handle = spawn_blocking(move || {
        let mut buffer = Vec::new();
        match m_request_fields.fast_field_types() {
            (Type::I64, None) => {
                let collected_values = collect_values::<i64>(
                    &m_request_fields,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize::<i64>(&collected_values, &mut buffer, output_format).map_err(
                    |_| {
                        SearchError::InternalError(
                            "Error when serializing i64 during export".to_owned(),
                        )
                    },
                )?;
            }
            (Type::U64, None) => {
                let collected_values = collect_values::<u64>(
                    &m_request_fields,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize::<u64>(&collected_values, &mut buffer, output_format).map_err(
                    |_| {
                        SearchError::InternalError(
                            "Error when serializing u64 during export".to_owned(),
                        )
                    },
                )?;
            }
            (Type::I64, Some(Type::I64)) => {
                let collected_values = collect_partitioned_values::<i64, i64>(
                    &m_request_fields,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize_partitions::<i64, i64>(collected_values.as_slice(), &mut buffer)
                    .map_err(|_| {
                    SearchError::InternalError(
                        "Error when serializing i64 during export".to_owned(),
                    )
                })?;
            }
            (Type::U64, Some(Type::U64)) => {
                let collected_values = collect_partitioned_values::<u64, u64>(
                    &m_request_fields,
                    stream_request.start_timestamp,
                    stream_request.end_timestamp,
                    searcher,
                    query.as_ref(),
                )?;
                super::serialize_partitions::<u64, u64>(collected_values.as_slice(), &mut buffer)
                    .map_err(|_| {
                    SearchError::InternalError(
                        "Error when serializing i64 during export".to_owned(),
                    )
                })?;
            }
            _ => {
                return Err(SearchError::InternalError(
                    "Mixed types (u64, i64) for fast field and partition field are not supported."
                        .to_owned(),
                ));
            }
        };
        Result::<Vec<u8>>::Ok(buffer)
    });
    let buffer = collect_handle.await.map_err(|error| {
        error!(split_id = %split.split_id, request_fields=%request_fields, error_message=%error, "Failed to collect fast field");
        SearchError::InternalError(format!("Error when collecting fast field values for split {}: {:?}", split.split_id, error))
    })??;

    Ok(LeafSearchStreamResult {
        data: buffer,
        split_id: split.split_id,
    })
}

fn collect_values<TFastValue: FastValue>(
    request_fields: &SearchStreamRequestFields,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    searcher: LeasedItem<Searcher>,
    query: &dyn Query,
) -> crate::Result<Vec<TFastValue>> {
    let collector = FastFieldCollector::<TFastValue> {
        fast_field_to_collect: request_fields.fast_field_name().to_string(),
        timestamp_field_opt: request_fields.timestamp_field,
        start_timestamp_opt: start_timestamp,
        end_timestamp_opt: end_timestamp,
        _marker: PhantomData,
    };
    let result = searcher.search(query, &collector)?;
    Ok(result)
}

fn collect_partitioned_values<TFastValue: FastValue, TPartitionValue: FastValue + Eq + Hash>(
    request_fields: &SearchStreamRequestFields,
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
    searcher: LeasedItem<Searcher>,
    query: &dyn Query,
) -> crate::Result<Vec<PartitionValues<TFastValue, TPartitionValue>>> {
    let collector = PartionnedFastFieldCollector::<TFastValue, TPartitionValue> {
        fast_field_to_collect: request_fields.fast_field_name().to_string(),
        partition_by_fast_field: request_fields
            .partition_by_fast_field_name()
            .expect("Please report a bug here, the partition_by_fast_field should be defined")
            .to_string(),
        timestamp_field_opt: request_fields.timestamp_field,
        start_timestamp_opt,
        end_timestamp_opt,
        _marker: PhantomData,
    };
    let result = searcher.search(query, &collector)?;
    Ok(result)
}

#[derive(Debug)]
// TODO move to owned values, implement Send + Sync
struct SearchStreamRequestFields {
    fast_field: Field,
    partition_by_fast_field: Option<Field>,
    timestamp_field: Option<Field>,
    schema: Schema,
}

impl<'a> std::fmt::Display for SearchStreamRequestFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fast_field: {},", self.fast_field_name())?;
        write!(
            f,
            "timestamp_field: {},",
            self.timestamp_field_name().unwrap_or("None")
        )?;
        write!(
            f,
            "partition_by_fast_field: {}",
            self.partition_by_fast_field_name().unwrap_or("None")
        )
    }
}

impl<'a> SearchStreamRequestFields {
    pub fn from_request(
        stream_request: &SearchStreamRequest,
        schema: &'a Schema,
        index_config: &dyn IndexConfig,
    ) -> crate::Result<SearchStreamRequestFields> {
        // TODO make sure it's a fast field
        let fast_field = schema
            .get_field(&stream_request.fast_field)
            .ok_or_else(|| {
                SearchError::InvalidQuery(format!(
                    "Field `{}` does not exist in schema",
                    &stream_request.fast_field
                ))
            })?;

        if !Self::is_fast_field(schema, &fast_field) {
            return Err(SearchError::InvalidQuery(format!(
                "Field `{}` is not a fast field",
                &stream_request.fast_field
            )));
        }

        let timestamp_field = index_config.timestamp_field(schema);
        let partition_by_fast_field = stream_request
            .partition_by_field
            .as_deref()
            .and_then(|field_name| schema.get_field(field_name));

        if partition_by_fast_field.is_some()
            && !Self::is_fast_field(schema, &partition_by_fast_field.unwrap())
        {
            return Err(SearchError::InvalidQuery(format!(
                "Field `{}` is not a fast field",
                &stream_request.partition_by_field.as_deref().unwrap()
            )));
        }

        Ok(SearchStreamRequestFields {
            schema: schema.to_owned(),
            fast_field,
            partition_by_fast_field,
            timestamp_field,
        })
    }

    pub fn fast_field_types(&self) -> (Type, Option<Type>) {
        (
            self.schema
                .get_field_entry(self.fast_field)
                .field_type()
                .value_type(),
            self.partition_by_fast_field
                .map(|field| self.schema.get_field_entry(field).field_type().value_type()),
        )
    }

    pub fn fast_fields_for_request(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        set.insert(self.fast_field_name().to_string());
        if let Some(timestamp_field) = self.timestamp_field_name() {
            set.insert(timestamp_field.to_string());
        }
        if let Some(partition_by_fast_field) = self.partition_by_fast_field_name() {
            set.insert(partition_by_fast_field.to_string());
        }
        set
    }

    pub fn timestamp_field_name(&self) -> Option<&str> {
        self.timestamp_field
            .map(|field| self.schema.get_field_name(field))
    }

    pub fn fast_field_name(&self) -> &str {
        self.schema.get_field_name(self.fast_field)
    }

    pub fn partition_by_fast_field_name(&self) -> Option<&str> {
        self.partition_by_fast_field
            .map(|field| self.schema.get_field_name(field))
    }

    fn is_fast_field(schema: &Schema, field: &Field) -> bool {
        schema.get_field_entry(*field).is_fast()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::TryInto;
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
        let test_sandbox = TestSandbox::create(index_id, index_config.clone()).await?;

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

    #[tokio::test]
    async fn test_leaf_search_stream_to_partitionned_clickhouse_binary_output_with_filtering(
    ) -> anyhow::Result<()> {
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
                },
                {
                    "name": "partition_by_fast_field",
                    "type": "u64",
                    "fast": true
                },
                {
                    "name": "fast_field",
                    "type": "u64",
                    "fast": true
                }

            ]
        }"#;
        let index_config =
            Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
        let index_id = "single-node-simple-2";
        let test_sandbox = TestSandbox::create(index_id, index_config.clone()).await?;

        let mut docs = vec![];
        let partition_by_fast_field_values = vec![1, 2, 3, 4, 5];
        let mut expected_output_tmp: HashMap<u64, Vec<u64>> = HashMap::new();
        let end_timestamp: i64 = 20;
        for i in 0..30 {
            let body = format!("info @ t:{}", i + 1);
            let partition_number = partition_by_fast_field_values[i % 5];
            let fast_field: u64 = (i * 2).try_into().unwrap();
            docs.push(json!({
                "body": body,
                "ts": i+1,
                "partition_by_fast_field": partition_number,
                "fast_field": fast_field,
            }));
            if i + 1 < end_timestamp.try_into().unwrap() {
                if let Some(values_for_partition) = expected_output_tmp.get_mut(&partition_number) {
                    values_for_partition.push(fast_field)
                } else {
                    expected_output_tmp.insert(partition_number, vec![fast_field]);
                }
            }
        }
        test_sandbox.add_documents(docs).await?;
        let mut expected_output: Vec<PartitionValues<u64, u64>> = expected_output_tmp
            .iter()
            .map(|(key, value)| PartitionValues {
                partition_value: *key,
                fast_field_values: value.to_vec(),
            })
            .collect();

        let request = SearchStreamRequest {
            index_id: index_id.to_string(),
            query: "info".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: Some(end_timestamp),
            fast_field: "fast_field".to_string(),
            output_format: 1,
            partition_by_field: Some(String::from("partition_by_fast_field")),
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
        let mut deserialized_output = deserialize_partitions(res.data);
        expected_output.sort_by(|l, r| l.partition_value.cmp(&r.partition_value));
        deserialized_output.sort_by(|l, r| l.partition_value.cmp(&r.partition_value));
        assert_eq!(expected_output, deserialized_output);
        Ok(())
    }

    fn deserialize_partitions(buffer: Vec<u8>) -> Vec<PartitionValues<u64, u64>> {
        // Note: this function is only meant to be used with valid payloads for testing purposes
        let mut cursor = 0;
        let mut partitions_values = vec![];
        while cursor < buffer.len() {
            let partition_slice: [u8; 8] = buffer[cursor..cursor + 8].try_into().unwrap();
            let partition = u64::from_le_bytes(partition_slice);
            cursor += 8;

            let payload_size_slice: [u8; 8] = buffer[cursor..cursor + 8].try_into().unwrap();
            let payload_size = u64::from_le_bytes(payload_size_slice);
            let nb_values: usize = (payload_size / 8).try_into().unwrap();
            cursor += 8;

            let mut partition_value = PartitionValues {
                partition_value: partition,
                fast_field_values: Vec::with_capacity(nb_values),
            };

            for _ in 0..nb_values {
                let value_slice: [u8; 8] = buffer[cursor..cursor + 8].try_into().unwrap();
                let value = u64::from_le_bytes(value_slice);
                cursor += 8;
                partition_value.fast_field_values.push(value);
            }
            partitions_values.push(partition_value);
        }
        partitions_values
    }
}
