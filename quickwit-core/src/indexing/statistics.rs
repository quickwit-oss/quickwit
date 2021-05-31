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
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::{
    sync::mpsc::{channel, Sender},
    task,
};
use tracing::debug;

/// Represents an event that can be used to collect & aggregate statistic data
#[derive(Debug)]
pub enum StatisticEvent {
    NewDocument {
        size_in_bytes: usize,
        error: bool,
    },
    SplitCreated {
        id: String,
        num_docs: usize,
        size_in_bytes: usize,
        num_parse_errors: usize,
    },
    SplitStage {
        id: String,
        error: bool,
    },
    SplitUpload {
        uri: String,
        upload_size: usize,
        error: bool,
    },
    SplitPublish {
        uri: String,
        error: bool,
    },
}

/// This is a struct that collects & aggregates statistic data.
/// We are using channel to collect info because we need to display the statistics
/// as we are indexing.
#[derive(Debug)]
pub struct StatisticsCollector {
    /// Number of document processed
    num_docs: usize,
    /// Number of document parse error
    num_parse_errors: usize,
    /// Number of created split
    num_local_splits: usize,
    /// Number of staged splits
    num_staged_splits: usize,
    /// Number of uploaded splits
    num_uploaded_splits: usize,
    ///Number of published splits
    num_published_splits: usize,
    /// Size in byte of document processed
    total_bytes_processed: usize,
    /// Size in bytes of resulting split
    total_size_splits: usize,
    /// Denotes the time this collector started
    start_time: Instant,
    /// stdout handle for displaying live statistics
    stdout: Stdout,
}

impl StatisticsCollector {
    /// Create a new instance of statistic collector .
    pub fn new() -> Self {
        Self {
            num_docs: 0,
            num_parse_errors: 0,
            num_local_splits: 0,
            num_staged_splits: 0,
            num_uploaded_splits: 0,
            num_published_splits: 0,
            total_bytes_processed: 0,
            total_size_splits: 0,
            start_time: Instant::now(),
            stdout: stdout(),
        }
    }

    /// Start a tokio task that listen to the statistics event channel
    /// and updates the statistic data
    pub fn start_collection() -> (Arc<Mutex<StatisticsCollector>>, Sender<StatisticEvent>) {
        let statistics_collector = Arc::new(Mutex::new(StatisticsCollector::new()));
        let moved_statistics_collector = statistics_collector.clone();
        let (event_sender, mut event_receiver) = channel(1000);
        task::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                let mut statistics = moved_statistics_collector.lock().await;

                match event {
                    StatisticEvent::NewDocument {
                        size_in_bytes,
                        error,
                    } => {
                        debug!(size =% size_in_bytes, error = error, "New document");
                        statistics.num_docs += 1;
                        statistics.total_bytes_processed += size_in_bytes;
                        if error {
                            statistics.num_parse_errors += 1;
                        }
                    }
                    StatisticEvent::SplitCreated {
                        id,
                        num_docs,
                        size_in_bytes,
                        num_parse_errors,
                    } => {
                        debug!(split_id =% id, num_docs = num_docs,  size_in_bytes = size_in_bytes, parse_errors = num_parse_errors, "Split created");
                        statistics.num_local_splits += 1;
                    }
                    StatisticEvent::SplitStage { id, error } => {
                        debug!(split_id =% id, error = error, "Split staged");
                        if !error {
                            statistics.num_staged_splits += 1;
                        }
                    }
                    StatisticEvent::SplitUpload {
                        uri,
                        upload_size,
                        error,
                    } => {
                        debug!(split_uri =% uri, error = error, "Split uploaded");
                        if !error {
                            statistics.num_uploaded_splits += 1;
                            statistics.total_size_splits += upload_size;
                        }
                    }
                    StatisticEvent::SplitPublish { uri, error } => {
                        debug!(split_uri =% uri, error = error, "Split published");
                        if !error {
                            statistics.num_published_splits += 1;
                        }
                    }
                }
                statistics.display_inline_report().unwrap();
            }
        });
        (statistics_collector, event_sender)
    }

    /// Display a one-shot report.
    pub fn display_report(&self) {
        let elapsed_secs = self.start_time.elapsed().as_secs();
        println!();
        if elapsed_secs >= 60 {
            println!(
                "Indexded {} documents in {:.2$}min",
                self.num_docs,
                elapsed_secs.max(1) as f64 / 60f64,
                2
            );
        } else {
            println!(
                "Indexded {} documents in {}s",
                self.num_docs,
                elapsed_secs.max(1)
            );
        }
    }

    fn display_inline_report(&mut self) -> anyhow::Result<()> {
        let elapsed_secs = self.start_time.elapsed().as_secs();
        self.stdout.queue(Clear(ClearType::CurrentLine))?;
        self.stdout.queue(cursor::SavePosition)?;
        let throughput_mb_s =
            self.total_bytes_processed as f64 / 1_000_000f64 / elapsed_secs.max(1) as f64;

        println!("Documents: {} Errors: {}  Splits: {} Dataset Size: {} Index Size: {} Throughput: {:.6$}MB/s \nPlease hold on.", 
            self.num_docs, self.num_parse_errors,  self.num_local_splits,
            self.total_bytes_processed / 1_000_000,
            self.total_size_splits / 1_000_000,
            throughput_mb_s, 2
        );

        self.stdout.queue(cursor::RestorePosition)?;
        Ok(())
    }
}