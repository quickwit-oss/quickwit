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

use crossterm::terminal::{Clear, ClearType};
use crossterm::{cursor, QueueableCommand};
use once_cell::sync::Lazy;
use std::io::{stdout, Write};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task;
use tokio::time::timeout;

use crate::counter::AtomicCounter;

/// The global indexing statistics holder.
pub static INDEXING_STATISTICS: Lazy<IndexingStatistics> = Lazy::new(IndexingStatistics::default);

/// A Struct that holds all statistical data about indexing
#[derive(Debug, Default)]
pub struct IndexingStatistics {
    /// Number of document processed
    pub num_docs: AtomicCounter,
    /// Number of document parse error
    pub num_parse_errors: AtomicCounter,
    /// Number of created split
    pub num_local_splits: AtomicCounter,
    /// Number of staged splits
    pub num_staged_splits: AtomicCounter,
    /// Number of uploaded splits
    pub num_uploaded_splits: AtomicCounter,
    ///Number of published splits
    pub num_published_splits: AtomicCounter,
    /// Size in byte of document processed
    pub total_bytes_processed: AtomicCounter,
    /// Size in bytes of resulting split
    pub total_size_splits: AtomicCounter,
}

/// Starts a tokio task that displays the indexing statistics
/// every once in awhile.
pub async fn start_statistics_reporting(
    task_completed_receiver: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    task::spawn(async move {
        let mut stdout_handle = stdout();
        let start_time = Instant::now();
        loop {
            // Try to receive with a timeout of 1 second.
            // 1 second is also the frequency at which we update statistic in the console
            let mut receiver = task_completed_receiver.clone();
            let is_done = timeout(Duration::from_secs(1), receiver.changed())
                .await
                .is_ok();

            let elapsed_secs = start_time.elapsed().as_secs();
            let throughput_mb_s = INDEXING_STATISTICS.total_bytes_processed.get() as f64
                / 1_000_000f64
                / elapsed_secs.max(1) as f64;
            let report_line = format!(
                "Documents: {} Errors: {}  Splits: {} Dataset Size: {}MB Throughput: {:.5$}MB/s",
                INDEXING_STATISTICS.num_docs.get(),
                INDEXING_STATISTICS.num_parse_errors.get(),
                INDEXING_STATISTICS.num_local_splits.get(),
                INDEXING_STATISTICS.total_bytes_processed.get() / 1_000_000,
                throughput_mb_s,
                2
            );

            stdout_handle.queue(cursor::SavePosition)?;
            stdout_handle.queue(Clear(ClearType::CurrentLine))?;
            stdout_handle.write_all(report_line.as_bytes())?;
            stdout_handle.write_all("\nPlease hold on.".as_bytes())?;
            stdout_handle.queue(cursor::RestorePosition)?;
            stdout_handle.flush()?;

            if is_done {
                break;
            }
        }

        //display end of task report
        println!();
        let elapsed_secs = start_time.elapsed().as_secs();
        if elapsed_secs >= 60 {
            println!(
                "Indexed {} documents in {:.2$}min",
                INDEXING_STATISTICS.num_docs.get(),
                elapsed_secs.max(1) as f64 / 60f64,
                2
            );
        } else {
            println!(
                "Indexed {} documents in {}s",
                INDEXING_STATISTICS.num_docs.get(),
                elapsed_secs.max(1)
            );
        }

        anyhow::Result::<()>::Ok(())
    })
    .await?
}
