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
use std::iter::Peekable;
use std::ops::Range;
use std::path::Path;

use futures::stream::{BoxStream, StreamExt};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_storage::{OwnedBytes, Storage};
use tantivy_common::VInt;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

// Download small gaps to avoid extra storage requests.
const MAX_GAP_BYTES: usize = 4 * 1024 * 1024;
// Coalesce small range lists inline to avoid a thread-pool hop, then continue on the CPU pool
// in bounded batches so Tokio workers stay responsive and cancellation is observed between batches.
const INLINE_COALESCE_LIMIT: usize = 4_096;
const CPU_POOL_COALESCE_BATCH: usize = 32_768;

type PeekableRanges = Peekable<Box<dyn Iterator<Item = Range<usize>> + Send>>;

/// Delta-encoded `(gap, len)` `VInt` pairs for one download.
///
/// An automaton query can coalesce millions of posting lists into one download outside
/// Quickwit's memory budget. Storing `(gap, len)` `VInt`s shrinks the buffer from 16 bytes
/// per range to ~2-3 bytes.
#[derive(Default)]
struct DeltaEncodedRanges {
    bytes: Vec<u8>,
    offset: usize,
}

impl DeltaEncodedRanges {
    fn push(&mut self, gap: usize, len: usize) {
        VInt(gap as u64).serialize_into_vec(&mut self.bytes);
        VInt(len as u64).serialize_into_vec(&mut self.bytes);
    }

    fn next(&mut self) -> io::Result<Option<(usize, usize)>> {
        let mut remaining = &self.bytes[self.offset..];
        if remaining.is_empty() {
            return Ok(None);
        }
        let start_len = remaining.len();
        let gap = VInt::deserialize_u64(&mut remaining)? as usize;
        let len = VInt::deserialize_u64(&mut remaining)? as usize;
        self.offset += start_len - remaining.len();
        Ok(Some((gap, len)))
    }

    /// Add up to `max_ranges` nearby ranges to `download_range`.
    /// Overlaps (including duplicates) and backward jumps end the current download so the reader
    /// can yield the requested bytes in input order without seeking.
    /// Returns `true` when no more ranges can join this download.
    fn coalesce_up_to(
        &mut self,
        download_range: &mut Range<usize>,
        ranges: &mut Peekable<impl Iterator<Item = Range<usize>>>,
        max_ranges: usize,
    ) -> bool {
        let mut count = 0;
        while count < max_ranges
            && let Some(next) = ranges.peek()
            && !next.is_empty()
            && let Some(gap) = next.start.checked_sub(download_range.end)
            && gap <= MAX_GAP_BYTES
        {
            self.push(gap, next.len());
            download_range.end = next.end;
            ranges.next();
            count += 1;
        }
        count < max_ranges
    }
}

async fn coalesce_ranges(
    mut download_range: Range<usize>,
    mut ranges: PeekableRanges,
    mut encoded_ranges: DeltaEncodedRanges,
) -> io::Result<(Range<usize>, PeekableRanges, DeltaEncodedRanges)> {
    encoded_ranges.bytes.clear();
    encoded_ranges.offset = 0;
    encoded_ranges.push(0, download_range.len());
    if encoded_ranges.coalesce_up_to(&mut download_range, &mut ranges, INLINE_COALESCE_LIMIT) {
        return Ok((download_range, ranges, encoded_ranges));
    }
    loop {
        let done;
        (done, download_range, ranges, encoded_ranges) = run_cpu_intensive(move || {
            let done = encoded_ranges.coalesce_up_to(
                &mut download_range,
                &mut ranges,
                CPU_POOL_COALESCE_BATCH,
            );
            (done, download_range, ranges, encoded_ranges)
        })
        .await
        .map_err(|_| io::Error::other("range coalescing task panicked"))?;
        if done {
            return Ok((download_range, ranges, encoded_ranges));
        }
    }
}

/// Download nearby ranges together and yield each range's bytes in input order.
pub(super) fn download_ranges<'a>(
    storage: &'a dyn Storage,
    path: &'a Path,
    ranges: Box<dyn Iterator<Item = Range<usize>> + Send>,
) -> BoxStream<'a, io::Result<OwnedBytes>> {
    futures::stream::try_unfold(
        (
            ranges.peekable(),
            DeltaEncodedRanges::default(),
            None::<BufReader<Box<dyn AsyncRead + Send + Unpin>>>,
        ),
        move |(mut ranges, mut encoded_ranges, mut active_download)| async move {
            loop {
                if let Some(reader) = &mut active_download {
                    if let Some((gap, len)) = encoded_ranges.next()? {
                        if gap > 0 {
                            tokio::io::copy_buf(
                                &mut reader.take(gap as u64),
                                &mut tokio::io::sink(),
                            )
                            .await?;
                        }
                        let mut bytes = vec![0; len];
                        reader.read_exact(&mut bytes).await?;
                        return Ok(Some((
                            OwnedBytes::new(bytes),
                            (ranges, encoded_ranges, active_download),
                        )));
                    }
                    active_download = None;
                }
                let Some(first) = ranges.next() else {
                    return Ok(None);
                };
                if first.is_empty() {
                    return Ok(Some((
                        OwnedBytes::empty(),
                        (ranges, encoded_ranges, active_download),
                    )));
                }
                let download_range;
                (download_range, ranges, encoded_ranges) =
                    coalesce_ranges(first, ranges, encoded_ranges).await?;
                let body = storage.get_slice_stream(path, download_range).await?;
                active_download = Some(BufReader::new(body));
            }
        },
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::TryStreamExt;
    use quickwit_storage::{MockStorage, StorageErrorKind};
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn emits_in_order_across_gaps_overlaps_and_empty_ranges() {
        let content = b"0123456789abcdef";
        let mut expected_downloads = [3..11, 8..11, 1..3].into_iter();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(3)
            .returning(move |_, range| {
                assert_eq!(range, expected_downloads.next().unwrap());
                Ok(Box::new(&content[range]))
            });
        let ranges = [0..0, 3..5, 8..11, 8..11, 1..3, 1..1];
        let mut stream = download_ranges(
            &storage,
            Path::new("file"),
            Box::new(ranges.clone().into_iter()),
        );
        for range in ranges {
            assert_eq!(
                stream.try_next().await.unwrap().unwrap().as_slice(),
                &content[range]
            );
        }
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn coalesces_up_to_gap_limit_and_offloads_large_batches() {
        const LIST: usize = 16;
        const NUM_DENSE: usize = INLINE_COALESCE_LIMIT + CPU_POOL_COALESCE_BATCH + 2;
        let dense_end = NUM_DENSE * LIST;
        let at_limit = dense_end + MAX_GAP_BYTES..dense_end + MAX_GAP_BYTES + LIST;
        let past_limit = at_limit.end + MAX_GAP_BYTES + 1..at_limit.end + MAX_GAP_BYTES + 1 + LIST;
        let mut expected_downloads = [0..at_limit.end, past_limit.clone()].into_iter();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(move |_, range| {
                assert_eq!(range, expected_downloads.next().unwrap());
                Ok(Box::new(tokio::io::repeat(7u8).take(range.len() as u64)))
            });
        let ranges = (0..NUM_DENSE)
            .map(|i| i * LIST..(i + 1) * LIST)
            .chain([at_limit, past_limit]);
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        for _ in 0..NUM_DENSE + 2 {
            let bytes = stream.try_next().await.unwrap().unwrap();
            assert_eq!((bytes.len(), bytes[0]), (LIST, 7));
        }
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn yields_first_range_before_download_finishes_and_drop_closes_reader() {
        let (list_size, first_batch) = (1024, 100);
        let after_gap = first_batch * list_size + MAX_GAP_BYTES + 1;
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = pulled.clone();
        let ranges = (0..first_batch)
            .map(move |i| i * list_size..(i + 1) * list_size)
            .chain([after_gap..after_gap + list_size])
            .inspect(move |_| {
                count.fetch_add(1, Ordering::Relaxed);
            });
        let (reader, mut writer) = tokio::io::duplex(list_size);
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .once()
            .return_once(move |_, range| {
                assert_eq!(range, 0..first_batch * list_size);
                Ok(Box::new(reader))
            });
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        assert_eq!(pulled.load(Ordering::Relaxed), 0);
        // Send only the first range; waiting for the whole download would stall.
        let first = vec![42u8; list_size];
        writer.write_all(&first).await.unwrap();
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), stream.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(bytes.as_slice(), first);
        drop(stream);
        assert_eq!(pulled.load(Ordering::Relaxed), first_batch + 1);
        assert_eq!(
            writer.write_all(b"x").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn propagates_open_and_truncation_errors() {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(3)
            .returning(|_, range| match range.start {
                0 => Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing file"))),
                1 => Ok(Box::new(&b"0123"[..])),
                _ => Ok(Box::new(&b"012345678"[..])),
            });
        let mut missing =
            download_ranges(&storage, Path::new("file"), Box::new([0..1].into_iter()));
        assert_eq!(
            missing.try_next().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        // Truncated in gap (start=1, 4 bytes) and in range (start=2, 9 bytes).
        for start in [1, 2] {
            let mut stream = download_ranges(
                &storage,
                Path::new("file"),
                Box::new([start..start + 2, start + 8..start + 10].into_iter()),
            );
            assert_eq!(stream.try_next().await.unwrap().unwrap().as_slice(), b"01");
            assert_eq!(
                stream.try_next().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            assert!(stream.try_next().await.unwrap().is_none());
        }
    }
}
