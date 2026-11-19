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
use quickwit_storage::{OwnedBytes, Storage};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader};

// Download small gaps to avoid extra storage requests.
const MAX_GAP_BYTES: usize = 4 * 1024 * 1024;
// Limit how many requested ranges we collect before yielding the first one.
const MAX_RANGES_PER_DOWNLOAD: usize = 16 * 1024;

/// Group consecutive inputs into one download, starting with the first nonempty range.
/// Overlaps (including duplicates) and backward jumps start a new download so the reader
/// can yield the requested bytes in input order without seeking.
fn coalesce_ranges(
    (first, tag): (Range<usize>, u64),
    ranges: &mut Peekable<impl Iterator<Item = (Range<usize>, u64)>>,
    requested_ranges: &mut Vec<(Range<usize>, u64)>,
) -> Range<usize> {
    let mut download_range = first.clone();
    requested_ranges.clear();
    requested_ranges.push((first, tag));
    while requested_ranges.len() < MAX_RANGES_PER_DOWNLOAD {
        let Some((next, tag)) = ranges.next_if(|(next, _)| {
            !next.is_empty()
                && next.start >= download_range.end
                && next.start - download_range.end <= MAX_GAP_BYTES
        }) else {
            break;
        };
        download_range.end = next.end;
        requested_ranges.push((next, tag));
    }
    download_range
}

/// Discard unwanted bytes from the reader's buffer, without allocating for the gap.
async fn skip_gap(reader: &mut (impl AsyncBufRead + Unpin), mut len: usize) -> io::Result<()> {
    while len > 0 {
        let available = reader.fill_buf().await?.len();
        if available == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let skip = len.min(available);
        reader.consume(skip);
        len -= skip;
    }
    Ok(())
}

/// Download nearby ranges together and yield each range's bytes with its input tag.
pub(super) fn download_ranges<'a>(
    storage: &'a dyn Storage,
    path: &'a Path,
    ranges: Box<dyn Iterator<Item = (Range<usize>, u64)> + Send>,
) -> BoxStream<'a, io::Result<(OwnedBytes, u64)>> {
    async_stream::try_stream! {
        let mut ranges = ranges.peekable();
        let mut requested_ranges = Vec::new();
        while let Some((first, tag)) = ranges.next() {
            if first.is_empty() {
                yield (OwnedBytes::empty(), tag);
                continue;
            }
            let download_range = coalesce_ranges((first, tag), &mut ranges, &mut requested_ranges);
            let mut offset = download_range.start;
            let body = storage.get_slice_stream(path, download_range).await?;
            let mut reader = BufReader::new(body);
            for (range, tag) in &requested_ranges {
                skip_gap(&mut reader, range.start - offset).await?;
                let mut bytes = vec![0; range.len()];
                reader.read_exact(&mut bytes).await?;
                offset = range.end;
                yield (OwnedBytes::new(bytes), *tag);
            }
        }
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::TryStreamExt;
    use quickwit_storage::MockStorage;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn coalescing_limits_the_gap_and_range_count() {
        let mut requested_ranges = Vec::new();
        let second = 2 + MAX_GAP_BYTES..3 + MAX_GAP_BYTES;
        let third = second.end + MAX_GAP_BYTES + 1..second.end + MAX_GAP_BYTES + 2;
        let mut ranges = [(second.clone(), 2), (third.clone(), 3)]
            .into_iter()
            .peekable();
        assert_eq!(
            coalesce_ranges((0..2, 1), &mut ranges, &mut requested_ranges),
            0..second.end
        );
        assert_eq!(requested_ranges, [(0..2, 1), (second, 2)]);
        assert_eq!(ranges.next().unwrap(), (third, 3));

        let mut ranges = (1..).map(|start| (start..start + 1, 0)).peekable();
        assert_eq!(
            coalesce_ranges((0..1, 0), &mut ranges, &mut requested_ranges),
            0..MAX_RANGES_PER_DOWNLOAD
        );
        assert_eq!(
            ranges.next().unwrap(),
            (MAX_RANGES_PER_DOWNLOAD..MAX_RANGES_PER_DOWNLOAD + 1, 0)
        );
    }

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
        let ranges = [
            (0..0, u64::MAX),
            (3..5, 42),
            (8..11, 0),
            (8..11, 42),
            (1..3, 9),
            (1..1, 5),
        ];
        let mut stream = download_ranges(
            &storage,
            Path::new("file"),
            Box::new(ranges.clone().into_iter()),
        );
        for (range, expected_tag) in ranges {
            let (bytes, tag) = stream.try_next().await.unwrap().unwrap();
            assert_eq!(bytes.as_slice(), &content[range]);
            assert_eq!(tag, expected_tag);
        }
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn yields_first_range_before_download_finishes_and_drop_closes_reader() {
        // Send only the first list. Waiting for the whole download would stall the stream.
        let list_size = 1024;
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = pulled.clone();
        let ranges = (0..250 * 1024).map(move |i| {
            count.fetch_add(1, Ordering::Relaxed);
            (i * list_size..(i + 1) * list_size, i as u64)
        });
        let (reader, mut writer) = tokio::io::duplex(list_size);
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .once()
            .return_once(move |_, range| {
                assert_eq!(range, 0..MAX_RANGES_PER_DOWNLOAD * list_size);
                Ok(Box::new(reader))
            });
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        assert_eq!(pulled.load(Ordering::Relaxed), 0);
        let first = vec![42u8; list_size];
        writer.write_all(&first).await.unwrap();
        let (bytes, tag) =
            tokio::time::timeout(std::time::Duration::from_secs(5), stream.try_next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(bytes.as_slice(), first);
        assert_eq!(tag, 0);
        drop(stream);
        assert_eq!(pulled.load(Ordering::Relaxed), MAX_RANGES_PER_DOWNLOAD);
        assert_eq!(
            writer.write_all(b"x").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn dense_ranges_share_downloads_larger_than_16_mib() {
        const TOTAL: usize = 250 * 1024 * 1024;
        const LIST: usize = 4 * 1024;
        // 16,384 adjacent 4 KiB lists fill one 64 MiB download.
        const DOWNLOAD_BYTES: usize = 64 * 1024 * 1024;
        let mut storage = MockStorage::default();
        let mut next_start: usize = 0;
        storage
            .expect_get_slice_stream()
            .times(TOTAL.div_ceil(DOWNLOAD_BYTES))
            .returning(move |_, range| {
                assert_eq!(range, next_start..TOTAL.min(next_start + DOWNLOAD_BYTES));
                next_start = range.end;
                let marker = (range.start / DOWNLOAD_BYTES) as u8;
                Ok(Box::new(tokio::io::repeat(marker).take(range.len() as u64)))
            });
        let ranges = (0..TOTAL)
            .step_by(LIST)
            .map(|start| (start..start + LIST, start as u64));
        let mut stream = download_ranges(&storage, Path::new("file"), Box::new(ranges));
        for start in (0..TOTAL).step_by(LIST) {
            let (bytes, tag) = stream.try_next().await.unwrap().unwrap();
            assert_eq!(tag, start as u64);
            assert_eq!(bytes.len(), LIST);
            assert_eq!(bytes[0], (start / DOWNLOAD_BYTES) as u8);
        }
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncated_body_fails_in_gap_or_in_list() {
        for available in [4, 9] {
            let mut storage = MockStorage::default();
            storage
                .expect_get_slice_stream()
                .once()
                .returning(move |_, _| Ok(Box::new(&b"0123456789"[..available])));
            let ranges = [(0..2, 13), (8..10, 7)];
            let mut stream =
                download_ranges(&storage, Path::new("file"), Box::new(ranges.into_iter()));
            let (bytes, tag) = stream.try_next().await.unwrap().unwrap();
            assert_eq!(bytes.as_slice(), b"01");
            assert_eq!(tag, 13);
            assert_eq!(
                stream.try_next().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            assert!(stream.try_next().await.unwrap().is_none());
        }
    }
}
