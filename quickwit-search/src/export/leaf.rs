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

use crate::leaf::open_index;
use crate::leaf::warmup;
use crate::OutputFormat;
use crate::SearchError;
use itertools::Itertools;
use quickwit_proto::LeafExportResult;
use quickwit_storage::Storage;
use std::io::Write;
use std::sync::Arc;
use tantivy::{query::Query, ReloadPolicy};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::collector::FastFieldCollector;

/// `leaf` step of export.
pub async fn leaf_export(
    query: Box<dyn Query>,
    fast_field_collector: FastFieldCollector<i64>,
    split_ids: Vec<String>,
    storage: Arc<dyn Storage>,
    output_format: OutputFormat,
) -> Result<ReceiverStream<Result<LeafExportResult, Status>>, SearchError> {
    let (result_sender, result_receiver) = tokio::sync::mpsc::channel(10);
    tokio::spawn(async move {
        let mut tasks = vec![];
        for split_id in split_ids {
            let split_storage: Arc<dyn Storage> =
                quickwit_storage::add_prefix_to_storage(storage.clone(), split_id.clone());
            let collector_for_split = fast_field_collector.clone();
            let query_clone = query.box_clone();
            let result_sender_clone = result_sender.clone();
            tasks.push(async move {
                let leaf_split_result = leaf_export_single_split(
                    query_clone,
                    collector_for_split,
                    split_storage,
                    output_format,
                )
                .await
                .map_err(|_| SearchError::InternalError(format!("Split failed {}", split_id)))?;

                result_sender_clone
                    .send(Ok(leaf_split_result))
                    .await
                    .map_err(|_| {
                        SearchError::InternalError(format!("Split failed {}", split_id))
                    })?;

                Result::<(), SearchError>::Ok(())
            });
        }

        let results = futures::future::join_all(tasks).await;
        let failed_splits = results
            .into_iter()
            .filter(|result| result.is_err())
            .map(|error| error.unwrap_err().to_string())
            .collect_vec();
        if !failed_splits.is_empty() {
            return Err(SearchError::InternalError(format!(
                "Some splits failed {:?}",
                failed_splits
            )));
        }

        Ok(())
    });

    Ok(ReceiverStream::new(result_receiver))
}

/// Apply a leaf search on a single split.
async fn leaf_export_single_split(
    query: Box<dyn Query>,
    fast_field_collector: FastFieldCollector<i64>,
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
    let fast_field_names = fast_field_collector.fast_field_names();
    warmup(&*searcher, query.as_ref(), fast_field_names).await?;
    let export = searcher.search(query.as_ref(), &fast_field_collector)?;
    let mut buffer = Vec::new();
    // TODO: isolate the serialization.
    match output_format {
        OutputFormat::CSV => {
            for row in export {
                writeln!(&mut buffer, "{}", row);
            }
        }
        OutputFormat::RowBinary => {
            for row in export {
                buffer.write(&row.to_le_bytes());
            }
        }
    };
    Ok(LeafExportResult { data: buffer })
}
