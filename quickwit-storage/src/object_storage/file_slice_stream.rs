// Copyright (C) 2021 Quickwit, Inc.
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

use std::io::{self, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio_util::io::ReaderStream;

/// Offers a stream of byte over a specific range of bytes in a file.
///
/// This struct is useful when uploading an object to S3.
#[derive(Debug)]
pub struct FileSliceStream<R> {
    inner: ReaderStream<R>,
    remaining: u64,
}

impl<R> FileSliceStream<R>
where R: AsyncRead + AsyncSeek + Unpin
{
    pub async fn try_new(mut reader: R, range: Range<u64>) -> io::Result<Self> {
        if range.end < range.start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("range.end({}) < range.start ({})", range.end, range.start),
            ));
        }

        let seek_from = SeekFrom::Start(range.start);
        reader.seek(seek_from).await?;

        Ok(FileSliceStream {
            inner: ReaderStream::new(reader),
            remaining: range.end - range.start,
        })
    }
}

impl<R> Stream for FileSliceStream<R>
where R: AsyncRead + Unpin
{
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        let mut polled = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(ref mut bytes))) = polled {
            bytes.truncate(self.remaining as usize); // no-op when bytes.len() < remaining
            self.remaining -= bytes.len() as u64;
        }

        polled
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;
    use futures::StreamExt;

    use crate::object_storage::file_slice_stream::FileSliceStream;

    #[tokio::test]
    async fn test_file_slice_stream() -> anyhow::Result<()> {
        let bytes = b"abcdef";

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 0..0).await?;
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 10..15).await?;
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 0..1).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[..1]));
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 5..6).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[5..]));
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 2..4).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[2..4]));
        assert!(stream.next().await.is_none());

        Ok(())
    }
}
