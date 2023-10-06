// Copyright (C) 2023 Quickwit, Inc.
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
use quickwit_common::PrettySample;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::search::{
    LeafSearchStreamResponse, OutputFormat, SearchRequest, SearchStreamRequest,
    SplitIdAndFooterOffsets,
};
use quickwit_storage::Storage;
use tantivy::columnar::{DynamicColumn, HasAssociatedColumnType};
use tantivy::fastfield::Column;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema, Type};
use tantivy::{DateTime, ReloadPolicy, Searcher};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

use super::collector::{PartionnedFastFieldCollector, PartitionValues};
use super::FastFieldCollector;
use crate::filters::{create_timestamp_filter_builder, TimestampFilterBuilder};
use crate::leaf::{open_index_with_caches, rewrite_start_end_time_bounds, warmup};
use crate::service::SearcherContext;
use crate::{Result, SearchError};

/// `leaf` step of search stream.
// Note: we return a stream of a result with a tonic::Status error
// to be compatible with the stream coming from the grpc client.
// It would be better to have a SearchError but we need then
// to process stream in grpc_adapter.rs to change SearchError
// to tonic::Status as tonic::Status is required by the stream result
// signature defined by proto generated code.
#[instrument(skip_all, fields(index = request.index_id))]
pub async fn leaf_search_stream(
    searcher_context: Arc<SearcherContext>,
    request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    doc_mapper: Arc<dyn DocMapper>,
) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>> {
    info!(split_offsets = ?PrettySample::new(&splits, 5));
    let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
    let span = info_span!("leaf_search_stream",);
    tokio::spawn(
        async move {
            let mut stream =
                leaf_search_results_stream(searcher_context, request, storage, splits, doc_mapper)
                    .await;
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
    searcher_context: Arc<SearcherContext>,
    request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    doc_mapper: Arc<dyn DocMapper>,
) -> impl futures::Stream<Item = crate::Result<LeafSearchStreamResponse>> + Sync + Send + 'static {
    let max_num_concurrent_split_streams = searcher_context
        .searcher_config
        .max_num_concurrent_split_streams;
    futures::stream::iter(splits)
        .map(move |split| {
            leaf_search_stream_single_split(
                searcher_context.clone(),
                split,
                doc_mapper.clone(),
                request.clone(),
                storage.clone(),
            )
            .shared()
        })
        .buffer_unordered(max_num_concurrent_split_streams)
}

/// Apply a leaf search on a single split.
#[instrument(skip_all, fields(split_id = %split.split_id))]
async fn leaf_search_stream_single_split(
    searcher_context: Arc<SearcherContext>,
    split: SplitIdAndFooterOffsets,
    doc_mapper: Arc<dyn DocMapper>,
    mut stream_request: SearchStreamRequest,
    storage: Arc<dyn Storage>,
) -> crate::Result<LeafSearchStreamResponse> {
    let _leaf_split_stream_permit = searcher_context
        .split_stream_semaphore
        .acquire()
        .await
        .expect("Failed to acquire permit. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
    rewrite_start_end_time_bounds(
        &mut stream_request.start_timestamp,
        &mut stream_request.end_timestamp,
        &split,
    );

    let index = open_index_with_caches(
        &searcher_context,
        storage,
        &split,
        Some(doc_mapper.tokenizer_manager()),
        true,
    )
    .await?;
    let split_schema = index.schema();

    let request_fields = Arc::new(SearchStreamRequestFields::from_request(
        &stream_request,
        &split_schema,
        doc_mapper.as_ref(),
    )?);

    let output_format = OutputFormat::from_i32(stream_request.output_format)
        .ok_or_else(|| SearchError::Internal("invalid output format specified".to_string()))?;

    if request_fields.partition_by_fast_field.is_some()
        && output_format != OutputFormat::ClickHouseRowBinary
    {
        return Err(SearchError::Internal(
            "invalid output format specified, only ClickHouseRowBinary is allowed when providing \
             a partitioned-by field"
                .to_string(),
        ));
    }

    let search_request = Arc::new(SearchRequest::try_from(stream_request.clone())?);
    let query_ast = serde_json::from_str(&search_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let (query, mut warmup_info) = doc_mapper.query(split_schema.clone(), &query_ast, false)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    let timestamp_filter_builder_opt: Option<TimestampFilterBuilder> =
        create_timestamp_filter_builder(
            request_fields.timestamp_field_name(),
            search_request.start_timestamp,
            search_request.end_timestamp,
        );

    let requires_scoring = search_request
        .sort_fields
        .iter()
        .any(|sort| sort.field_name == "_score");

    // TODO no test fail if this line get removed
    warmup_info.field_norms |= requires_scoring;

    let fast_field_names =
        request_fields.fast_fields_for_request(timestamp_filter_builder_opt.as_ref());
    warmup_info.fast_field_names.extend(fast_field_names);

    warmup(&searcher, &warmup_info).await?;

    let span = info_span!(
        "collect_fast_field",
        split_id = %split.split_id,
        request_fields=%request_fields,
    );

    let _ = span.enter();
    let m_request_fields = request_fields.clone();
    let collect_handle = crate::run_cpu_intensive(move || {
        let mut buffer = Vec::new();
        match m_request_fields.fast_field_types() {
            (Type::I64, None) => {
                let collected_values = collect_values::<i64>(
                    &m_request_fields,
                    timestamp_filter_builder_opt,
                    &searcher,
                    &query,
                )?;
                super::serialize::<i64>(&collected_values, &mut buffer, output_format).map_err(
                    |_| {
                        SearchError::Internal("error when serializing i64 during export".to_owned())
                    },
                )?;
            }
            (Type::U64, None) => {
                let collected_values = collect_values::<u64>(
                    &m_request_fields,
                    timestamp_filter_builder_opt,
                    &searcher,
                    &query,
                )?;
                super::serialize::<u64>(&collected_values, &mut buffer, output_format).map_err(
                    |_| {
                        SearchError::Internal("error when serializing u64 during export".to_owned())
                    },
                )?;
            }
            (Type::Date, None) => {
                let collected_values = collect_values::<DateTime>(
                    &m_request_fields,
                    timestamp_filter_builder_opt,
                    &searcher,
                    &query,
                )?;
                // It may seem overkill and expensive considering DateTime is just a wrapper
                // over the i64, but the compiler is smarter than it looks and the code
                // below actually is zero-cost: No allocation and no copy happens.
                let collected_values_as_micros = collected_values
                    .into_iter()
                    .map(|date_time| date_time.into_timestamp_micros())
                    .collect::<Vec<_>>();
                // We serialize Date as i64 microseconds.
                super::serialize::<i64>(&collected_values_as_micros, &mut buffer, output_format)
                    .map_err(|_| {
                        SearchError::Internal("error when serializing i64 during export".to_owned())
                    })?;
            }
            (Type::I64, Some(Type::I64)) => {
                let collected_values = collect_partitioned_values::<i64, i64>(
                    &m_request_fields,
                    timestamp_filter_builder_opt,
                    &searcher,
                    &query,
                )?;
                super::serialize_partitions::<i64, i64>(collected_values.as_slice(), &mut buffer)
                    .map_err(|_| {
                    SearchError::Internal("error when serializing i64 during export".to_owned())
                })?;
            }
            (Type::U64, Some(Type::U64)) => {
                let collected_values = collect_partitioned_values::<u64, u64>(
                    &m_request_fields,
                    timestamp_filter_builder_opt,
                    &searcher,
                    &query,
                )?;
                super::serialize_partitions::<u64, u64>(collected_values.as_slice(), &mut buffer)
                    .map_err(|_| {
                    SearchError::Internal("error when serializing i64 during export".to_owned())
                })?;
            }
            (fast_field_type, None) => {
                return Err(SearchError::Internal(format!(
                    "search stream does not support fast field of type `{fast_field_type:?}`"
                )));
            }
            (fast_field_type, Some(partition_fast_field_type)) => {
                return Err(SearchError::Internal(format!(
                    "search stream does not support the combination of fast field type \
                     `{fast_field_type:?}` and partition fast field type \
                     `{partition_fast_field_type:?}`"
                )));
            }
        };
        Result::<Vec<u8>>::Ok(buffer)
    });
    let buffer = collect_handle.await.map_err(|_| {
        error!(split_id = %split.split_id, request_fields=%request_fields, "failed to collect fast field");
        SearchError::Internal(format!("error when collecting fast field values for split {}", split.split_id))
    })??;
    Ok(LeafSearchStreamResponse {
        data: buffer,
        split_id: split.split_id,
    })
}

fn collect_values<Item: HasAssociatedColumnType>(
    request_fields: &SearchStreamRequestFields,
    timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    searcher: &Searcher,
    query: &dyn Query,
) -> crate::Result<Vec<Item>>
where
    DynamicColumn: Into<Option<Column<Item>>>,
{
    let collector = FastFieldCollector::<Item> {
        fast_field_to_collect: request_fields.fast_field_name().to_string(),
        timestamp_filter_builder_opt,
        _marker: PhantomData,
    };
    let result = searcher.search(query, &collector)?;
    Ok(result)
}

fn collect_partitioned_values<
    Item: HasAssociatedColumnType,
    TPartitionValue: HasAssociatedColumnType + Eq + Hash,
>(
    request_fields: &SearchStreamRequestFields,
    timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    searcher: &Searcher,
    query: &dyn Query,
) -> crate::Result<Vec<PartitionValues<Item, TPartitionValue>>>
where
    DynamicColumn: Into<Option<Column<Item>>> + Into<Option<Column<TPartitionValue>>>,
{
    let collector = PartionnedFastFieldCollector::<Item, TPartitionValue> {
        fast_field_to_collect: request_fields.fast_field_name().to_string(),
        partition_by_fast_field: request_fields
            .partition_by_fast_field_name()
            .expect("`partition_by_fast_field` is not defined. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
            .to_string(),
        timestamp_filter_builder_opt,
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
    timestamp_field_name: Option<String>,
    schema: Schema,
}

impl std::fmt::Display for SearchStreamRequestFields {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        doc_mapper: &dyn DocMapper,
    ) -> crate::Result<SearchStreamRequestFields> {
        let fast_field = schema.get_field(&stream_request.fast_field)?;

        if !Self::is_fast_field(schema, &fast_field) {
            return Err(SearchError::InvalidQuery(format!(
                "field `{}` is not a fast field",
                &stream_request.fast_field
            )));
        }

        let timestamp_field_name = doc_mapper.timestamp_field_name().map(ToString::to_string);
        let partition_by_fast_field = stream_request
            .partition_by_field
            .as_deref()
            .and_then(|field_name| schema.get_field(field_name).ok());

        if partition_by_fast_field.is_some()
            && !Self::is_fast_field(schema, &partition_by_fast_field.unwrap())
        {
            return Err(SearchError::InvalidQuery(format!(
                "field `{}` is not a fast field",
                &stream_request.partition_by_field.as_deref().unwrap()
            )));
        }

        Ok(SearchStreamRequestFields {
            schema: schema.to_owned(),
            fast_field,
            partition_by_fast_field,
            timestamp_field_name,
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

    fn fast_fields_for_request(
        &self,
        timestamp_filter_builder_opt: Option<&TimestampFilterBuilder>,
    ) -> HashSet<String> {
        let mut set = HashSet::new();
        set.insert(self.fast_field_name().to_string());
        if let Some(timestamp_filter_builder) = timestamp_filter_builder_opt {
            set.insert(timestamp_filter_builder.timestamp_field_name.clone());
        }
        if let Some(partition_by_fast_field) = self.partition_by_fast_field_name() {
            set.insert(partition_by_fast_field.to_string());
        }
        set
    }

    pub fn timestamp_field_name(&self) -> Option<&str> {
        self.timestamp_field_name.as_deref()
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

    use itertools::Itertools;
    use quickwit_indexing::TestSandbox;
    use quickwit_query::query_ast::qast_json_helper;
    use serde_json::json;
    use tantivy::time::{Duration, OffsetDateTime};

    use super::*;
    use crate::extract_split_and_footer_offsets;

    #[tokio::test]
    async fn test_leaf_search_stream_to_csv_output_with_filtering() -> anyhow::Result<()> {
        let index_id = "single-node-simple";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["body"]).await?;

        let mut docs = Vec::new();
        let mut filtered_timestamp_values = Vec::new();
        let start_timestamp = 72057595;
        let end_timestamp = start_timestamp + 20;
        for i in 0..30 {
            let timestamp = start_timestamp + (i + 1) as i64;
            let body = format!("info @ t:{timestamp}");
            docs.push(json!({"body": body, "ts": timestamp}));
            if timestamp < end_timestamp {
                filtered_timestamp_values.push(timestamp);
            }
        }
        test_sandbox.add_documents(docs).await?;

        let request = SearchStreamRequest {
            index_id: index_id.to_string(),
            query_ast: qast_json_helper("info", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: Some(end_timestamp),
            fast_field: "ts".to_string(),
            output_format: 0,
            partition_by_field: None,
        };
        let splits = test_sandbox
            .metastore()
            .list_all_splits(test_sandbox.index_uid())
            .await?;
        let splits_offsets = splits
            .into_iter()
            .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
            .collect();
        let searcher_context = Arc::new(SearcherContext::for_test());
        let mut single_node_stream = leaf_search_stream(
            searcher_context,
            request,
            test_sandbox.storage(),
            splits_offsets,
            test_sandbox.doc_mapper(),
        )
        .await;
        let res = single_node_stream.next().await.expect("no leaf result")?;
        assert_eq!(
            from_utf8(&res.data)?,
            format!(
                "{}\n",
                filtered_timestamp_values
                    .iter()
                    .map(|timestamp_secs| (timestamp_secs * 1_000_000).to_string())
                    .join("\n")
            )
        );
        test_sandbox.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_leaf_search_stream_filtering_with_datetime() -> anyhow::Result<()> {
        let index_id = "single-node-simple-datetime";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats:
                  - "unix_timestamp"
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["body"]).await?;
        let mut docs = Vec::new();
        let mut filtered_timestamp_values = Vec::new();
        let start_date = OffsetDateTime::now_utc();
        let num_days = 20;
        for i in 0..30 {
            let dt = start_date.checked_add(Duration::days(i + 1)).unwrap();
            let body = format!("info @ t:{}", i + 1);
            docs.push(json!({"body": body, "ts": dt.unix_timestamp()}));
            if i + 1 < num_days {
                let ts_secs = dt.unix_timestamp() * 1_000_000;
                filtered_timestamp_values.push(ts_secs.to_string());
            }
        }
        test_sandbox.add_documents(docs).await?;

        let end_timestamp = start_date
            .checked_add(Duration::days(num_days))
            .unwrap()
            .unix_timestamp();
        let request = SearchStreamRequest {
            index_id: index_id.to_string(),
            query_ast: qast_json_helper("info", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: Some(end_timestamp),
            fast_field: "ts".to_string(),
            output_format: 0,
            partition_by_field: None,
        };
        let splits = test_sandbox
            .metastore()
            .list_all_splits(test_sandbox.index_uid())
            .await?;
        let splits_offsets = splits
            .into_iter()
            .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
            .collect();
        let searcher_context = Arc::new(SearcherContext::for_test());
        let mut single_node_stream = leaf_search_stream(
            searcher_context,
            request,
            test_sandbox.storage(),
            splits_offsets,
            test_sandbox.doc_mapper(),
        )
        .await;
        let res = single_node_stream.next().await.expect("no leaf result")?;
        assert_eq!(
            from_utf8(&res.data)?,
            format!("{}\n", filtered_timestamp_values.join("\n"))
        );
        test_sandbox.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_leaf_search_stream_with_string_fast_field_should_return_proper_error(
    ) -> anyhow::Result<()> {
        let index_id = "single-node-simple-string-fast-field";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: app
                type: text
                tokenizer: raw
                fast: true
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;

        test_sandbox
            .add_documents(vec![json!({"body": "body", "app": "my-app"})])
            .await?;

        let request = SearchStreamRequest {
            index_id: index_id.to_string(),
            query_ast: qast_json_helper("info", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "app".to_string(),
            output_format: 0,
            partition_by_field: None,
        };
        let splits = test_sandbox
            .metastore()
            .list_all_splits(test_sandbox.index_uid())
            .await?;
        let splits_offsets = splits
            .into_iter()
            .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
            .collect();
        let searcher_context = Arc::new(SearcherContext::for_test());
        let mut single_node_stream = leaf_search_stream(
            searcher_context,
            request,
            test_sandbox.storage(),
            splits_offsets,
            test_sandbox.doc_mapper(),
        )
        .await;
        let res = single_node_stream.next().await.expect("no leaf result");
        let error_message = res.unwrap_err().to_string();
        assert!(error_message.contains("search stream does not support fast field of type `Str`"),);
        test_sandbox.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_leaf_search_stream_to_partitionned_clickhouse_binary_output_with_filtering(
    ) -> anyhow::Result<()> {
        let index_id = "single-node-simple-2";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                fast: true
              - name: partition_by_fast_field
                type: u64
                fast: true
              - name: fast_field
                type: u64
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["body"]).await?;

        let mut docs = Vec::new();
        let partition_by_fast_field_values = [1, 2, 3, 4, 5];
        let mut expected_output_tmp: HashMap<u64, Vec<u64>> = HashMap::new();
        let start_timestamp = 72057595;
        let end_timestamp: i64 = start_timestamp + 20;
        for i in 0..30 {
            let timestamp = start_timestamp + (i + 1) as i64;
            let body = format!("info @ t:{timestamp}");
            let partition_number = partition_by_fast_field_values[i % 5];
            let fast_field: u64 = (i * 2).try_into().unwrap();
            docs.push(json!({
                "body": body,
                "ts":  timestamp,
                "partition_by_fast_field": partition_number,
                "fast_field": fast_field,
            }));
            if timestamp < end_timestamp {
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
            query_ast: qast_json_helper("info", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: Some(end_timestamp),
            fast_field: "fast_field".to_string(),
            output_format: 1,
            partition_by_field: Some(String::from("partition_by_fast_field")),
        };
        let splits = test_sandbox
            .metastore()
            .list_all_splits(test_sandbox.index_uid())
            .await?;
        let splits_offsets = splits
            .into_iter()
            .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
            .collect();
        let searcher_context = Arc::new(SearcherContext::for_test());
        let mut single_node_stream = leaf_search_stream(
            searcher_context,
            request,
            test_sandbox.storage(),
            splits_offsets,
            test_sandbox.doc_mapper(),
        )
        .await;
        let res = single_node_stream.next().await.expect("no leaf result")?;
        let mut deserialized_output = deserialize_partitions(res.data);
        expected_output.sort_by(|l, r| l.partition_value.cmp(&r.partition_value));
        deserialized_output.sort_by(|l, r| l.partition_value.cmp(&r.partition_value));
        assert_eq!(expected_output, deserialized_output);
        test_sandbox.assert_quit().await;
        Ok(())
    }

    fn deserialize_partitions(buffer: Vec<u8>) -> Vec<PartitionValues<u64, u64>> {
        // Note: this function is only meant to be used with valid payloads for testing purposes
        let mut cursor = 0;
        let mut partitions_values = Vec::new();
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
