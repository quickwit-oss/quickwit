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

use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Semaphore,
    },
    task,
};

use crate::indexing::split::Split;
use crate::indexing::statistics::StatisticEvent;

const MAX_CONCURRENT_SPLIT_TASKS: usize = 20;

/// Finilizes a split by performing the following actions
/// - Commit the split
/// - Merge all segments of the splits
/// - Stage the split
/// - Upload all split artifacts
/// - Publish the split
///
pub async fn finalize_split(
    mut split_receiver: Receiver<Split>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<()> {
    // use semaphre to limit the number in flight tasks
    // https://docs.rs/tokio/1.6.0/tokio/sync/struct.Semaphore.html
    let finalise_task_authoriser = Arc::new(Semaphore::new(MAX_CONCURRENT_SPLIT_TASKS));
    let mut finalize_tasks = vec![];
    while let Some(mut split) = split_receiver.recv().await {
        // request a task execution permit.
        // this will block if `MAX_CONCURRENT_SPLIT_TASKS` are already running until
        // one of them completes.
        let permit = Arc::clone(&finalise_task_authoriser)
            .acquire_owned()
            .await?;

        // announce new split reception.
        statistic_sender
            .send(StatisticEvent::SplitCreated {
                id: split.id.to_string(),
                num_docs: split.metadata.num_records,
                size_in_bytes: split.metadata.size_in_bytes,
                num_failed_to_parse_docs: split.num_parsing_errors,
            })
            .await?;

        let moved_statistic_sender = statistic_sender.clone();
        let finalize_task = task::spawn(async move {
            let _permit = permit;
            split.commit().await?;
            split.merge_all_segments().await?;
            split.stage(moved_statistic_sender.clone()).await?;
            split.upload(moved_statistic_sender.clone()).await?;
            split.publish(moved_statistic_sender).await?;
            anyhow::Result::<()>::Ok(())
        });
        finalize_tasks.push(finalize_task);
    }

    futures::future::try_join_all(finalize_tasks).await?;
    Ok(())
}
