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

use crate::export::ExportSerializer;
use crate::leaf::open_index;
use crate::leaf::warmup;
use crate::OutputFormat;
use crate::SearchError;
use quickwit_proto::LeafExportResult;
use quickwit_storage::Storage;
use std::sync::Arc;
use tantivy::schema::Type;
use tantivy::{query::Query, ReloadPolicy};
use tokio_stream::wrappers::ReceiverStream;

use super::FastFieldCollectorBuilder;

/// `leaf` step of export.
// Note: we return a stream of a result with a tonic::Status error
// to be compatible with the stream coming from the grpc client.
// It would be better to have a SearchError but we need then
// to process stream in grpc_adapater.rs to change SearchError
// to tonic::Status as tonic::Status is required by the stream result
// signature defined by proto generated code.
pub async fn leaf_export(
    query: Box<dyn Query>,
    fast_field_collector_builder: FastFieldCollectorBuilder,
    split_ids: Vec<String>,
    storage: Arc<dyn Storage>,
    output_format: OutputFormat,
) -> ReceiverStream<Result<LeafExportResult, tonic::Status>> {
    let (result_sender, result_receiver) = tokio::sync::mpsc::channel(10);
    for split_id in split_ids {
        let split_storage: Arc<dyn Storage> =
            quickwit_storage::add_prefix_to_storage(storage.clone(), split_id.clone());
        let collector_for_split = fast_field_collector_builder.clone();
        let query_clone = query.box_clone();
        let result_sender_clone = result_sender.clone();
        tokio::spawn(async move {
            let leaf_split_result = leaf_export_single_split(
                query_clone,
                collector_for_split,
                split_storage,
                output_format,
            )
            .await
            .map_err(|_| {
                SearchError::InternalError(format!("Export failed on split `{}`", split_id))
            })?;
            result_sender_clone
                .send(Ok(leaf_split_result))
                .await
                .map_err(|_| {
                    SearchError::InternalError(format!(
                        "Unable to send leaf export result for split `{}`",
                        split_id
                    ))
                })?;
            Result::<(), SearchError>::Ok(())
        });
    }
    ReceiverStream::new(result_receiver)
}

/// Apply a leaf search on a single split.
async fn leaf_export_single_split(
    query: Box<dyn Query>,
    fast_field_collector_builder: FastFieldCollectorBuilder,
    storage: Arc<dyn Storage>,
    output_format: OutputFormat,
) -> crate::Result<LeafExportResult> {
    let index = open_index(storage).await?;
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
    let mut writer = Vec::new();
    match fast_field_collector_builder.value_type() {
        Type::I64 => {
            let fast_field_collector = fast_field_collector_builder.build_i64();
            let fast_field_values = searcher.search(query.as_ref(), &fast_field_collector)?;
            let mut serializer = ExportSerializer {
                writer: &mut writer,
                output_format,
            };
            serializer.write_i64(fast_field_values)?;
        }
        Type::U64 => {
            let fast_field_collector = fast_field_collector_builder.build_u64();
            let fast_field_values = searcher.search(query.as_ref(), &fast_field_collector)?;
            let mut serializer = ExportSerializer {
                writer: &mut writer,
                output_format,
            };
            serializer.write_u64(fast_field_values)?;
        }
        value_type => {
            return Err(SearchError::InvalidQuery(format!(
                "Fast field type `{:?}` not supported",
                value_type
            )));
        }
    }

    Ok(LeafExportResult { data: writer })
}
