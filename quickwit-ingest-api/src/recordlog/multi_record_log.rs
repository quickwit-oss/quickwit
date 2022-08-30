// Copyright (C) 2022 Quickwit, Inc.
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

use std::io;
use std::path::Path;

use super::rolling::RecordLogReader;
use super::{mem, rolling, Record};

pub struct MultiRecordLog {
    record_log_writer: rolling::RecordLogWriter,
    in_mem_queues: mem::MemQueues,
    last_position: u64,
}

impl MultiRecordLog {
    pub async fn open(directory_path: &Path) -> crate::Result<Self> {
        if !directory_path.exists() {
            tokio::fs::create_dir(directory_path).await?;
        }
        let mut record_log_reader = RecordLogReader::open(directory_path).await?;
        let mut in_mem_queues = mem::MemQueues::default();
        let mut last_position = 0u64;
        while let Some(record) = record_log_reader.read_record().await? {
            match record {
                Record::AddRecord {
                    position,
                    queue,
                    payload,
                } => {
                    in_mem_queues.add_record(queue, position, payload);
                    last_position = position;
                }
                Record::Truncate { position, queue } => {
                    in_mem_queues.truncate(queue, position);
                    last_position = position;
                }
            }
        }
        let record_log_writer = record_log_reader.into_writer();
        Ok(MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            last_position,
        })
    }

    #[cfg(test)]
    pub(crate) fn num_files(&self) -> usize {
        self.record_log_writer.num_files()
    }

    pub fn list_queues(&self) -> Vec<String> {
        self.in_mem_queues.list_queues()
    }

    // Returns a new position.
    fn inc_position(&mut self) -> u64 {
        self.last_position += 1;
        self.last_position
    }

    pub async fn append_record(&mut self, queue: &str, payload: &[u8]) -> io::Result<()> {
        let position = self.inc_position();
        let record = Record::AddRecord {
            position,
            queue,
            payload,
        };
        self.record_log_writer.write_record(record).await?;
        self.record_log_writer.flush().await?;
        self.in_mem_queues.add_record(queue, position, payload);
        Ok(())
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.in_mem_queues.queue_exists(queue)
    }

    /// Returns false if the queue was already existing.
    pub fn create_queue(&mut self, queue: &str) -> bool {
        // TODO append to the logs too.
        self.in_mem_queues.create_queue(queue)
    }

    /// Returns false if the queue does not exist.
    pub fn remove_queue(&mut self, queue: &str) -> bool {
        // TODO append to the logs too.
        self.in_mem_queues.remove_queue(queue)
    }

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, queue_id: &str, position: u64) -> Option<(u64, &[u8])> {
        self.in_mem_queues.get_after(queue_id, position)
    }

    pub async fn truncate(&mut self, queue: &str, position: u64) -> io::Result<()> {
        self.record_log_writer
            .write_record(Record::Truncate { position, queue })
            .await?;
        if let Some(position) = self.in_mem_queues.truncate(queue, position) {
            self.record_log_writer.truncate(position).await?;
        }
        Ok(())
    }
}
