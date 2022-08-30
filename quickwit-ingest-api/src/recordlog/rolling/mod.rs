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

mod directory;
mod reader;
mod writer;

use serde::{Deserialize, Serialize};

pub use self::directory::Directory;
pub(crate) use self::reader::RecordLogReader;
pub(crate) use self::writer::RecordLogWriter;

#[derive(Serialize, Deserialize)]
enum MultiQueueRecord<'a> {
    /// Appends a record to a given queue.
    Record {
        queue_id: &'a str,
        seq_number: u64,
        payload: &'a [u8],
    },
    /// Truncation happens at the level of a specific queue.
    /// It is simply added to the record log as an operation...
    /// The client will be in charge of detecting when a file can actually
    /// be deleted and will removing the file accordingly.
    Truncate { queue_id: &'a str, seq_number: u64 },
    /// After removing a log file, in order to avoid losing the last seq_number,
    /// we can append a `LastPosition` record to the log.
    LastPosition { queue_id: &'a str, seq_number: u64 },
}

#[cfg(test)]
mod tests;
