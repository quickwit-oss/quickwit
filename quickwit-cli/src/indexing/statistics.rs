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

use tokio::{
    sync::mpsc::{channel, Sender},
    task,
};

/// StatisticEvent represent an event that can be used to collect & aggregate statistic data
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
        num_failed_to_parse_docs: usize,
    },
    SplitStage {
        id: String,
        error: bool,
    },
    SplitUpload {
        uri: String,
        error: bool,
    },
    SplitPublish {
        uri: String,
        error: bool,
    },
}

/// StatisticCollector is a struct that collects & aggregate statistic data
pub struct StatisticsCollector {
    num_docs: usize,
    num_failed_to_parse_docs: usize,
    num_local_splits: usize,
    num_staged_splits: usize,
    num_uploaded_splits: usize,
    num_published_splits: usize,
    total_bytes_processed: usize,
    total_size_splits: usize,
    event_sender: Option<Sender<StatisticEvent>>,
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self {
            num_docs: 0,
            num_failed_to_parse_docs: 0,
            num_local_splits: 0,
            num_staged_splits: 0,
            num_uploaded_splits: 0,
            num_published_splits: 0,
            total_bytes_processed: 0,
            total_size_splits: 0,
            event_sender: None,
        }
    }

    pub fn start_collection(&mut self) -> Sender<StatisticEvent> {
        let (event_sender, mut event_receiver) = channel(1000);
        self.event_sender = Some(event_sender.clone());
        task::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                println!("{:?}", event);
            }
        });
        event_sender
    }

    pub fn display_report(&self) {
        println!("a nice report");
    }
}
