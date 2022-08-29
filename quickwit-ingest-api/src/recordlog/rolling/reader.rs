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

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::File;

use crate::recordlog::record::{ReadRecordError, RecordReader};
use crate::recordlog::rolling::{Directory, RecordLogWriter};
use crate::recordlog::Record;

pub(crate) struct RecordLogReader {
    directory: Directory,
    files: VecDeque<PathBuf>,
    reader_opt: Option<RecordReader<File>>,
}

impl RecordLogReader {
    pub async fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path).await?;
        let files = directory.file_paths().collect();
        Ok(RecordLogReader {
            files,
            directory,
            reader_opt: None,
        })
    }

    pub fn into_writer(self) -> RecordLogWriter {
        RecordLogWriter::open(self.directory.into())
    }

    async fn go_next_record_current_reader(&mut self) -> Result<bool, ReadRecordError> {
        if let Some(record_reader) = self.reader_opt.as_mut() {
            record_reader.go_next().await
        } else {
            Ok(false)
        }
    }

    async fn go_next_record(&mut self) -> Result<bool, ReadRecordError> {
        loop {
            if self.go_next_record_current_reader().await? {
                return Ok(true);
            }
            if !self.load_next_file().await? {
                return Ok(false);
            }
        }
    }

    async fn load_next_file(&mut self) -> io::Result<bool> {
        if let Some(next_file_path) = self.files.pop_front() {
            let next_file = File::open(next_file_path).await?;
            let record_reader = RecordReader::open(next_file);
            self.reader_opt = Some(record_reader);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn read_record<'a>(
        &'a mut self,
    ) -> Result<Option<Record<'a>>, ReadRecordError> {
        if self.go_next_record().await? {
            let record: Record = self
                .reader_opt
                .as_ref()
                .unwrap()
                .record()
                .ok_or(ReadRecordError::Corruption)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}
