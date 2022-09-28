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
use std::ops::Range;

use async_trait::async_trait;
use rusoto_core::ByteStream;

use crate::OwnedBytes;

#[async_trait]
/// PutPayload is used to upload data and support multipart.
pub trait PutPayload: PutPayloadClone + Send + Sync {
    /// Return the total length of the payload.
    fn len(&self) -> u64;

    /// Retrieve bytestream for specified range.
    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream>;

    /// Retrieve complete bytestream.
    async fn byte_stream(&self) -> io::Result<ByteStream> {
        let total_len = self.len();
        let range = 0..total_len;
        self.range_byte_stream(range).await
    }

    /// Load the whole Payload into memory.
    async fn read_all(&self) -> io::Result<OwnedBytes> {
        let total_len = self.len();
        let range = 0..total_len;
        let mut reader = self.range_byte_stream(range).await?.into_async_read();

        let mut data: Vec<u8> = Vec::with_capacity(total_len as usize);
        tokio::io::copy(&mut reader, &mut data).await?;

        Ok(OwnedBytes::new(data))
    }
}

pub trait PutPayloadClone {
    fn box_clone(&self) -> Box<dyn PutPayload>;
}

impl<T> PutPayloadClone for T
where T: 'static + PutPayload + Clone
{
    fn box_clone(&self) -> Box<dyn PutPayload> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn PutPayload> {
    fn clone(&self) -> Box<dyn PutPayload> {
        self.box_clone()
    }
}

#[async_trait]
impl PutPayload for Vec<u8> {
    fn len(&self) -> u64 {
        self.len() as u64
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        Ok(ByteStream::from(
            self[range.start as usize..range.end as usize].to_vec(),
        ))
    }
}
