/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::sync::Arc;

use crate::indexing::split::Split;
use futures::StreamExt;
use quickwit_metastore::Metastore;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use tracing::warn;

use super::IndexingStatistics;

const MAX_CONCURRENT_SPLIT_TASKS: usize = if cfg!(test) { 2 } else { 10 };

/// Finilizes a split by performing the following actions
/// - Commit the split
/// - Merge all segments of the splits
/// - Stage the split
/// - Upload all split artifacts
/// - Publish the split
///
pub async fn finalize_split(
    index_id: String,
    split_receiver: Receiver<Split>,
    metastore: Arc<dyn Metastore>,
    statistics: Arc<IndexingStatistics>,
) -> anyhow::Result<()> {
    let stream = ReceiverStream::new(split_receiver);
    let mut finalize_stream = stream
        .map(|mut split| {
            let moved_statistics = statistics.clone();
            async move {
                debug!(split_id =% split.id, num_docs = split.metadata.num_records,  size_in_bytes = split.metadata.size_in_bytes, parse_errors = split.num_parsing_errors, "Split created");
                moved_statistics.num_local_splits.inc();

                split.commit().await?;
                split.merge_all_segments().await?;

                split.stage().await?;
                moved_statistics.num_staged_splits.inc();

                let manifest = split.upload().await?;
                moved_statistics.num_uploaded_splits.inc();
                moved_statistics
                    .total_size_splits
                    .add(manifest.split_size_in_bytes as usize);

                anyhow::Result::<Split>::Ok(split)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_SPLIT_TASKS);

    let mut split_ids = vec![];
    while let Some(finalize_result) = finalize_stream.next().await {
        let split = finalize_result.map_err(|error| {
            warn!("Some splits were not finalised.");
            error
        })?;
        split_ids.push(split.id.to_string());
    }

    // publish all splits atomically
    let split_ids = split_ids
        .iter()
        .map(|split_id| split_id.as_str())
        .collect::<Vec<_>>();
    let num_splits = split_ids.len();
    metastore
        .publish_splits(index_id.as_str(), split_ids)
        .await?;

    statistics.num_published_splits.add(num_splits);

    Ok(())
}
