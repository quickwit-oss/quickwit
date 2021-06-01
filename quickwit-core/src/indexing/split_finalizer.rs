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

use crate::indexing::INDEXING_STATISTICS;
use crate::indexing::split::Split;
use futures::StreamExt;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use tracing::warn;

const MAX_CONCURRENT_SPLIT_TASKS: usize = if cfg!(test) { 2 } else { 10 };

/// Finilizes a split by performing the following actions
/// - Commit the split
/// - Merge all segments of the splits
/// - Stage the split
/// - Upload all split artifacts
/// - Publish the split
///
pub async fn finalize_split(split_receiver: Receiver<Split>) -> anyhow::Result<()> {
    let stream = ReceiverStream::new(split_receiver);
    let mut finalize_stream = stream
        .map(|mut split| {
            async move {
                debug!(split_id =% split.id, num_docs = split.metadata.num_records,  size_in_bytes = split.metadata.size_in_bytes, parse_errors = split.num_parsing_errors, "Split created");
                INDEXING_STATISTICS.num_local_splits.inc();

                split.commit().await?;
                split.merge_all_segments().await?;
                split.stage().await?;
                split.upload().await?;
                anyhow::Result::<Split>::Ok(split)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_SPLIT_TASKS);

    let mut splits = vec![];
    let mut finalize_errors: usize = 0;
    while let Some(finalize_result) = finalize_stream.next().await {
        if finalize_result.is_ok() {
            let split = finalize_result?;
            splits.push(split);
        } else {
            finalize_errors += 1;
        }
    }

    if finalize_errors > 0 {
        warn!("Some splits were not finalised.");
    }

    // TODO: we want to atomically publish all splits.
    // See [https://github.com/quickwit-inc/quickwit/issues/71]
    let mut publish_stream = tokio_stream::iter(splits)
        .map(|split| {
            let moved_statistic_sender = statistic_sender.clone();
            async move {
                split.publish().await?;
                anyhow::Result::<()>::Ok(())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_SPLIT_TASKS);

    let mut publish_errors: usize = 0;
    while let Some(publish_result) = publish_stream.next().await {
        if publish_result.is_err() {
            publish_errors += 1;
        }
    }

    if publish_errors > 0 {
        warn!("Some splits were not published.");
    }

    Ok(())
}
