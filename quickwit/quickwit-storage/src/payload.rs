// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::ops::Range;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use tantivy::directory::OwnedBytes;

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
