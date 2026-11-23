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

use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use quickwit_storage::{OwnedBytes, Storage};
use tantivy_common::VInt;
use tokio::io::{AsyncReadExt, BufReader};

// Bridge byte gaps up to this size into the same download to issue fewer storage requests.
const MAX_GAP_BYTES: usize = 4 * 1024 * 1024;
// Yield after coalescing this many ranges so Tokio workers stay responsive and observe
// cancellation on large range lists.
const COALESCE_YIELD_INTERVAL: usize = 4_096;

/// Delta-encoded `(gap, len)` `VInt` pairs for one download.
///
/// An automaton query can coalesce millions of posting lists into one download outside
/// Quickwit's memory budget. Storing `(gap, len)` `VInt`s shrinks the buffer from 16 bytes
/// per range to ~2-3 bytes.
#[derive(Default)]
struct DeltaEncodedRanges {
    bytes: Vec<u8>,
    read_pos: usize,
}

impl DeltaEncodedRanges {
    fn push_back(&mut self, gap: usize, len: usize) {
        VInt(gap as u64).serialize_into_vec(&mut self.bytes);
        VInt(len as u64).serialize_into_vec(&mut self.bytes);
    }

    fn pop_front(&mut self) -> Option<(usize, usize)> {
        let mut remaining = &self.bytes[self.read_pos..];
        if remaining.is_empty() {
            return None;
        }
        let start_len = remaining.len();
        let gap = VInt::deserialize_u64(&mut remaining).expect("valid VInt") as usize;
        let len = VInt::deserialize_u64(&mut remaining).expect("valid VInt") as usize;
        self.read_pos += start_len - remaining.len();
        Some((gap, len))
    }
}

/// Coalesce consecutive nearby ranges into one download. Overlaps (including
/// duplicates) and backward jumps end this download so the reader yields
/// requested bytes in input order without seeking.
async fn coalesce_ranges(
    ranges: &mut Peekable<impl Iterator<Item = Range<usize>>>,
) -> Option<(Range<usize>, DeltaEncodedRanges)> {
    let mut download_range = ranges.next()?;
    let mut encoded_ranges = DeltaEncodedRanges::default();
    encoded_ranges.push_back(0, download_range.len());
    let mut num_coalesced = 0;
    while let Some(next_range) = ranges.peek()
        && let Some(gap) = next_range.start.checked_sub(download_range.end)
        && gap <= MAX_GAP_BYTES
    {
        encoded_ranges.push_back(gap, next_range.len());
        download_range.end = next_range.end;
        ranges.next();
        num_coalesced += 1;
        if num_coalesced % COALESCE_YIELD_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    Some((download_range, encoded_ranges))
}

/// Download nearby ranges together and yield each range's bytes in input order.
pub(super) fn download_ranges<'a>(
    storage: &'a dyn Storage,
    path: &'a Path,
    ranges: Box<dyn Iterator<Item = Range<usize>> + Send>,
) -> BoxStream<'a, io::Result<OwnedBytes>> {
    futures::stream::try_unfold(ranges.peekable(), move |mut ranges| async move {
        let Some((download_range, encoded_ranges)) = coalesce_ranges(&mut ranges).await else {
            return io::Result::Ok(None);
        };
        let stream = storage.get_slice_stream(path, download_range).await?;
        let download = futures::stream::try_unfold(
            (BufReader::new(stream), encoded_ranges),
            |(mut reader, mut encoded_ranges)| async move {
                let Some((gap, len)) = encoded_ranges.pop_front() else {
                    return Ok(None);
                };
                if gap > 0 {
                    tokio::io::copy_buf(
                        &mut (&mut reader).take(gap as u64),
                        &mut tokio::io::sink(),
                    )
                    .await?;
                }
                let mut range_bytes = vec![0; len];
                reader.read_exact(&mut range_bytes).await?;
                Ok(Some((
                    OwnedBytes::new(range_bytes),
                    (reader, encoded_ranges),
                )))
            },
        );
        Ok(Some((download, ranges)))
    })
    .try_flatten()
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quickwit_storage::{MockStorage, StorageErrorKind};
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn emits_in_order_across_gaps_and_overlaps() {
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
        let ranges = [3..5, 8..11, 8..11, 1..3];
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
    async fn coalesces_up_to_gap_limit_and_yields_on_large_batches() {
        const RANGE_LEN: usize = 16;
        const NUM_CONTIGUOUS_RANGES: usize = COALESCE_YIELD_INTERVAL * 2 + 2;
        let contiguous_end = NUM_CONTIGUOUS_RANGES * RANGE_LEN;
        let at_gap_limit =
            contiguous_end + MAX_GAP_BYTES..contiguous_end + MAX_GAP_BYTES + RANGE_LEN;
        let past_gap_limit =
            at_gap_limit.end + MAX_GAP_BYTES + 1..at_gap_limit.end + MAX_GAP_BYTES + 1 + RANGE_LEN;
        let mut expected_downloads = [0..at_gap_limit.end, past_gap_limit.clone()].into_iter();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(move |_, range| {
                assert_eq!(range, expected_downloads.next().unwrap());
                Ok(Box::new(tokio::io::repeat(7u8).take(range.len() as u64)))
            });
        let ranges = (0..NUM_CONTIGUOUS_RANGES)
            .map(|i| i * RANGE_LEN..(i + 1) * RANGE_LEN)
            .chain([at_gap_limit, past_gap_limit]);
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        for _ in 0..NUM_CONTIGUOUS_RANGES + 2 {
            let bytes = stream.try_next().await.unwrap().unwrap();
            assert_eq!((bytes.len(), bytes[0]), (RANGE_LEN, 7));
        }
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn yields_first_range_before_download_finishes_and_drop_closes_reader() {
        let (range_len, num_first_download_ranges) = (1024, 100);
        let after_gap = num_first_download_ranges * range_len + MAX_GAP_BYTES + 1;
        let num_pulled_ranges = Arc::new(AtomicUsize::new(0));
        let pulled_counter = num_pulled_ranges.clone();
        let ranges = (0..num_first_download_ranges)
            .map(move |i| i * range_len..(i + 1) * range_len)
            .chain(std::iter::once(after_gap..after_gap + range_len))
            .inspect(move |_| {
                pulled_counter.fetch_add(1, Ordering::Relaxed);
            });
        let (reader, mut writer) = tokio::io::duplex(range_len);
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .once()
            .return_once(move |_, range| {
                assert_eq!(range, 0..num_first_download_ranges * range_len);
                Ok(Box::new(reader))
            });
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        assert_eq!(num_pulled_ranges.load(Ordering::Relaxed), 0);
        // Send only the first range. Waiting for the whole download would stall.
        let first_range_bytes = vec![42u8; range_len];
        writer.write_all(&first_range_bytes).await.unwrap();
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), stream.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(bytes.as_slice(), first_range_bytes);
        drop(stream);
        assert_eq!(
            num_pulled_ranges.load(Ordering::Relaxed),
            num_first_download_ranges + 1
        );
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
        let mut missing_stream =
            download_ranges(&storage, Path::new("file"), Box::new(std::iter::once(0..1)));
        assert_eq!(
            missing_stream.try_next().await.unwrap_err().kind(),
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
