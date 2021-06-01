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
use std::io::{stdout, Stdout};
use std::time::{Duration, Instant};
use tokio::task;
use tracing::debug;
use once_cell::sync::Lazy;

use crate::counter::AtomicCounter;

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

pub static INDEXING_STATISTICS: Lazy<IndexingStatistics> = Lazy::new(IndexingStatistics::default);



/// Start a tokio task that listen to the statistics event channel
/// and updates the statistic data
pub fn start_statistics_reporting()  {
    // let statistics_collector = Arc::new(Mutex::new(StatisticsCollector::new()));
    // let moved_statistics_collector = statistics_collector.clone();
    // let (event_sender, mut event_receiver) = channel(1000);
    task::spawn(async move {
        let stdout = stdout();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let start_time = Instant::now();
        loop {
            interval.tick().await;
            println!("hello");
            // display_inline_report()
        }
    });

    
    
}

// Display a one-shot report.
// pub fn display_report() {
//     let elapsed_secs = self.start_time.elapsed().as_secs();
//     println!();
//     if elapsed_secs >= 60 {
//         println!(
//             "Indexed {} documents in {:.2$}min",
//             self.num_docs,
//             elapsed_secs.max(1) as f64 / 60f64,
//             2
//         );
//     } else {
//         println!(
//             "Indexed {} documents in {}s",
//             self.num_docs,
//             elapsed_secs.max(1)
//         );
//     }
// }

// fn display_inline_report() -> anyhow::Result<()> {
//     let elapsed_secs = self.start_time.elapsed().as_secs();
//     self.stdout.queue(Clear(ClearType::CurrentLine))?;
//     self.stdout.queue(cursor::SavePosition)?;
//     let throughput_mb_s =
//         self.total_bytes_processed as f64 / 1_000_000f64 / elapsed_secs.max(1) as f64;

//     println!("Documents: {} Errors: {}  Splits: {} Dataset Size: {} Index Size: {} Throughput: {:.6$}MB/s \nPlease hold on.", 
//         self.num_docs, self.num_parse_errors,  self.num_local_splits,
//         self.total_bytes_processed / 1_000_000,
//         self.total_size_splits / 1_000_000,
//         throughput_mb_s, 2
//     );

//     self.stdout.queue(cursor::RestorePosition)?;
//     Ok(())
// }

