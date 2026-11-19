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

// Trade small gaps for fewer storage requests. Bound both the bytes requested from storage
// and the metadata collected before yielding, even for millions of adjacent one-byte lists.
const MAX_GAP: usize = 4 * 1024 * 1024;
const MAX_SPAN: usize = 16 * 1024 * 1024;
const MAX_RANGES: usize = 16 * 1024;

/// Add forward, disjoint ranges to the first nonempty range. Overlaps (including duplicates)
/// start a new read: keeping input order then requires neither sorting nor a replay buffer.
fn plan_read(
    first: Range<usize>,
    ranges: &mut Peekable<impl Iterator<Item = Range<usize>>>,
    requested: &mut Vec<Range<usize>>,
) -> Range<usize> {
    let mut span = first.clone();
    requested.clear();
    requested.push(first);
    while requested.len() < MAX_RANGES {
        let Some(next) = ranges.next_if(|next| {
            !next.is_empty()
                && next.start >= span.end
                && next.start - span.end <= MAX_GAP
                && next.end - span.start <= MAX_SPAN
        }) else {
            break;
        };
        span.end = next.end;
        requested.push(next);
    }
    span
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

pub(super) fn read_ranges<'a>(
    storage: &'a dyn Storage,
    path: &'a Path,
    ranges: Box<dyn Iterator<Item = Range<usize>> + Send>,
) -> BoxStream<'a, io::Result<OwnedBytes>> {
    async_stream::try_stream! {
        let mut ranges = ranges.peekable();
        let mut requested = Vec::new();
        while let Some(first) = ranges.next() {
            if first.is_empty() {
                yield OwnedBytes::empty();
                continue;
            }
            let span = plan_read(first, &mut ranges, &mut requested);
            let mut offset = span.start;
            let body = storage.get_slice_stream(path, span).await?;
            let mut reader = BufReader::new(body);
            for range in &requested {
                skip_gap(&mut reader, range.start - offset).await?;
                let mut bytes = vec![0; range.len()];
                reader.read_exact(&mut bytes).await?;
                offset = range.end;
                yield OwnedBytes::new(bytes);
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
    fn planning_limits_gap_span_and_range_count() {
        let mut requested = Vec::new();
        let after_gap = 2 + MAX_GAP;
        let mut ranges = [after_gap..MAX_SPAN, MAX_SPAN..MAX_SPAN + 1]
            .into_iter()
            .peekable();
        assert_eq!(plan_read(0..2, &mut ranges, &mut requested), 0..MAX_SPAN);
        assert_eq!(requested, [0..2, after_gap..MAX_SPAN]);
        assert_eq!(ranges.next().unwrap(), MAX_SPAN..MAX_SPAN + 1);

        let mut ranges = (1..).map(|start| start..start + 1).peekable();
        assert_eq!(plan_read(0..1, &mut ranges, &mut requested), 0..MAX_RANGES);
        assert_eq!(ranges.next().unwrap(), MAX_RANGES..MAX_RANGES + 1);

        let mut ranges = std::iter::once(2 + MAX_GAP..3 + MAX_GAP).peekable();
        assert_eq!(plan_read(0..1, &mut ranges, &mut requested), 0..1);
        // A single list can be larger than the target span and must still arrive whole.
        let mut ranges = std::iter::empty().peekable();
        assert_eq!(
            plan_read(0..MAX_SPAN + 1, &mut ranges, &mut requested),
            0..MAX_SPAN + 1
        );
    }

    #[tokio::test]
    async fn emits_in_order_across_gaps_overlaps_and_empty_ranges() {
        let content = b"0123456789abcdef";
        let expected_spans = [3..11, 8..11, 1..3];
        let mut spans = expected_spans.into_iter();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(3)
            .returning(move |_, range| {
                assert_eq!(range, spans.next().unwrap());
                Ok(Box::new(&content[range]))
            });
        let ranges = [0..0, 3..5, 8..11, 8..11, 1..3, 1..1];
        let mut stream = read_ranges(
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
    async fn dense_read_yields_before_body_finishes_and_drop_closes_reader() {
        // Model 250 MiB of adjacent postings. Only send the first list: buffering the span
        // before yielding would deadlock, and opening another reader would fail the mock.
        let list_size = 1024;
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = pulled.clone();
        let ranges = (0..250 * 1024).map(move |i| {
            count.fetch_add(1, Ordering::Relaxed);
            i * list_size..(i + 1) * list_size
        });
        let (reader, mut writer) = tokio::io::duplex(list_size);
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .once()
            .return_once(move |_, range| {
                assert_eq!(range, 0..MAX_SPAN);
                Ok(Box::new(reader))
            });
        let mut stream = read_ranges(&storage, Path::new("file"), Box::new(ranges));
        assert_eq!(pulled.load(Ordering::Relaxed), 0);
        let first = vec![42u8; list_size];
        writer.write_all(&first).await.unwrap();
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), stream.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(bytes.as_slice(), first);
        drop(stream);
        assert_eq!(pulled.load(Ordering::Relaxed), MAX_RANGES);
        assert_eq!(
            writer.write_all(b"x").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn dense_250_mib_batch_reads_each_span_once() {
        const TOTAL: usize = 250 * 1024 * 1024;
        const LIST: usize = 4 * 1024;
        let mut storage = MockStorage::default();
        let mut next_start: usize = 0;
        storage
            .expect_get_slice_stream()
            .times(TOTAL.div_ceil(MAX_SPAN))
            .returning(move |_, range| {
                assert_eq!(range, next_start..TOTAL.min(next_start + MAX_SPAN));
                next_start = range.end;
                let marker = (range.start / MAX_SPAN) as u8;
                Ok(Box::new(tokio::io::repeat(marker).take(range.len() as u64)))
            });
        let ranges = (0..TOTAL).step_by(LIST).map(|start| start..start + LIST);
        let mut stream = read_ranges(&storage, Path::new("file"), Box::new(ranges));
        for start in (0..TOTAL).step_by(LIST) {
            let bytes = stream.try_next().await.unwrap().unwrap();
            assert_eq!(bytes.len(), LIST);
            assert_eq!(bytes[0], (start / MAX_SPAN) as u8);
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
            let ranges = [0..2, 8..10];
            let mut stream = read_ranges(&storage, Path::new("file"), Box::new(ranges.into_iter()));
            assert_eq!(stream.try_next().await.unwrap().unwrap().as_slice(), b"01");
            assert_eq!(
                stream.try_next().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            assert!(stream.try_next().await.unwrap().is_none());
        }
    }
}
