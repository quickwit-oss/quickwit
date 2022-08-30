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

use tokio::io::{self, AsyncWrite};

use crate::recordlog::frame::{FrameType, FrameWriter};
use crate::recordlog::Serializable;

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
    buffer: Vec<u8>,
}

impl<W: io::AsyncWrite> RecordWriter<W> {}

fn frame_type(is_first_frame: bool, is_last_frame: bool) -> FrameType {
    match (is_first_frame, is_last_frame) {
        (true, true) => FrameType::FULL,
        (true, false) => FrameType::FIRST,
        (false, true) => FrameType::LAST,
        (false, false) => FrameType::MIDDLE,
    }
}

impl<W: io::AsyncWrite + Unpin> RecordWriter<W> {
    pub fn open(wrt: W) -> Self {
        let frame_writer = FrameWriter::create_with_aligned_write(wrt);
        RecordWriter {
            frame_writer,
            buffer: Vec::with_capacity(10_000),
        }
    }
}

impl<W: AsyncWrite + Unpin> RecordWriter<W> {
    /// Writes a record.
    ///
    /// Even if this call returns `Ok(())`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub async fn write_record(&mut self, record: impl Serializable<'_>) -> io::Result<()> {
        let mut is_first_frame = true;
        self.buffer.clear();
        record.serialize(&mut self.buffer);
        let mut payload = &self.buffer[..];
        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length()
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame);
            self.frame_writer
                .write_frame(frame_type, frame_payload)
                .await?;
            is_first_frame = false;
            if is_last_frame {
                break;
            }
        }
        Ok(())
    }

    /// Flushes and sync the data to disk.
    pub async fn flush(&mut self) -> io::Result<()> {
        // Empty the application buffer.
        self.frame_writer.flush().await?;
        Ok(())
    }

    pub fn get_underlying_wrt(&mut self) -> &mut W {
        self.frame_writer.get_underlying_wrt()
    }

    pub fn num_bytes_written(&self) -> u64 {
        self.frame_writer.num_bytes_written()
    }
}
