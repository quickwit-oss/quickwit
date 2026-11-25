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
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use ahash::HashMap;
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;

use super::{CLEANUP_INTERVAL, DebouncerKey};
use crate::{Storage, StorageErrorKind, StorageResult};

const JOIN_WINDOW: Duration = Duration::from_millis(5);
const CHUNK_NUM_BYTES: usize = 1024 * 1024;

/// Collects concurrent reads for the same path and byte range for 5 ms, then shares one storage
/// stream across those readers. Each reader buffers at most one chunk. A slow reader pauses the
/// producer, and dropping the last reader cancels it.
#[derive(Default)]
pub(super) struct StreamDebouncer {
    // The group map and the count of insertions since the last sweep share one lock.
    groups: Mutex<(HashMap<DebouncerKey, Weak<StreamGroup>>, usize)>,
}

#[derive(Clone)]
enum StreamItem {
    Chunk(Bytes),
    Eof,
    Error(Arc<io::Error>),
}

type OpenSender = oneshot::Sender<StorageResult<mpsc::Receiver<StreamItem>>>;

struct StreamGroup {
    // Only callers keep the group alive. Declared first so dropping the last caller aborts the
    // producer task before dropping the reader registration channel.
    _producer_task: AbortOnDropHandle<()>,
    // The producer task owns the receiver, so its termination fails callers waiting to open.
    joining_readers: mpsc::UnboundedSender<OpenSender>,
}

impl StreamDebouncer {
    pub(super) async fn get_slice_stream<T: Storage>(
        &self,
        storage: Arc<T>,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let (open_sender, open_receiver) = oneshot::channel();
        let group = self.join_or_start_group(storage, (path.to_owned(), range), open_sender);
        let item_receiver = open_receiver.await.map_err(|_| {
            StorageErrorKind::Internal.with_error(anyhow::anyhow!("stream producer stopped"))
        })??;
        let chunks = futures::stream::try_unfold(
            (item_receiver, group),
            |(mut item_receiver, group)| async {
                match item_receiver.recv().await {
                    Some(StreamItem::Chunk(bytes)) => Ok(Some((bytes, (item_receiver, group)))),
                    Some(StreamItem::Eof) => Ok(None),
                    Some(StreamItem::Error(error)) => Err(io::Error::new(error.kind(), error)),
                    None => Err(io::Error::other("stream producer stopped")),
                }
            },
        );
        Ok(Box::new(StreamReader::new(chunks.boxed())))
    }

    fn join_or_start_group<T: Storage>(
        &self,
        storage: Arc<T>,
        key: DebouncerKey,
        mut reader: OpenSender,
    ) -> Arc<StreamGroup> {
        let mut registry = self.groups.lock().unwrap();
        let (groups, inserts_since_cleanup) = &mut *registry;
        if let Some(group) = groups.get(&key).and_then(Weak::upgrade) {
            match group.joining_readers.send(reader) {
                Ok(()) => return group,
                // The producer closed registration.
                Err(error) => reader = error.0,
            }
        }
        let join_deadline = Instant::now() + JOIN_WINDOW;
        let (joining_readers_sender, joining_readers_receiver) = mpsc::unbounded_channel();
        joining_readers_sender
            .send(reader)
            .expect("receiver is still owned here");
        let task = tokio::spawn(
            share_stream(
                storage,
                key.clone(),
                join_deadline,
                joining_readers_receiver,
            )
            .in_current_span(),
        );
        let group = Arc::new(StreamGroup {
            _producer_task: AbortOnDropHandle::new(task),
            joining_readers: joining_readers_sender,
        });
        groups.insert(key, Arc::downgrade(&group));
        // Finished and cancelled groups leave dead weak references to reclaim periodically.
        *inserts_since_cleanup += 1;
        if *inserts_since_cleanup > CLEANUP_INTERVAL {
            *inserts_since_cleanup = 0;
            groups.retain(|_, group| group.strong_count() != 0);
        }
        group
    }
}

async fn share_stream<T: Storage>(
    storage: Arc<T>,
    key: DebouncerKey,
    join_deadline: Instant,
    mut joining_readers: mpsc::UnboundedReceiver<OpenSender>,
) {
    tokio::time::sleep_until(join_deadline).await;
    // Callers arriving after this point start a new stream group.
    joining_readers.close();
    let stream = storage.get_slice_stream(&key.0, key.1).await;
    let mut senders = Vec::new();
    while let Some(open_sender) = joining_readers.recv().await {
        match &stream {
            Ok(_) => {
                let (sender, receiver) = mpsc::channel(1);
                if open_sender.send(Ok(receiver)).is_ok() {
                    senders.push(sender);
                }
            }
            Err(error) => {
                let _ = open_sender.send(Err(error.clone()));
            }
        }
    }
    let Ok(stream) = stream else {
        return;
    };
    let mut chunks = ReaderStream::with_capacity(stream, CHUNK_NUM_BYTES);
    loop {
        senders.retain(|sender| !sender.is_closed());
        if senders.is_empty() {
            return;
        }
        let (item, finished) = match chunks.next().await {
            Some(Ok(bytes)) => (StreamItem::Chunk(bytes), false),
            Some(Err(error)) => (StreamItem::Error(Arc::new(error)), true),
            None => (StreamItem::Eof, true),
        };
        for sender in &senders {
            // A dropped reader leaves the group. Remaining readers still receive every chunk.
            let _ = sender.send(item.clone()).await;
        }
        if finished {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::MockStorage;
    use crate::debouncer::DebouncedStorage;

    #[tokio::test(start_paused = true)]
    async fn test_groups_by_path_range_and_fixed_window() {
        let start = Instant::now();
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .times(4)
            .returning(move |path, range| {
                assert!(Instant::now() >= start + JOIN_WINDOW);
                let byte = path.to_str().unwrap().as_bytes()[0] + range.start as u8;
                Ok(Box::new(io::Cursor::new([byte])))
            });
        let storage = DebouncedStorage::new(backend);
        let mut first = Box::pin(storage.get_slice_stream(Path::new("a"), 0..1));
        assert!(poll!(&mut first).is_pending());
        tokio::time::advance(Duration::from_millis(4)).await;
        let (first, second) = tokio::join!(first, storage.get_slice_stream(Path::new("a"), 0..1));
        assert_eq!(Instant::now(), start + JOIN_WINDOW);
        // Late readers and different keys each open a separate stream.
        let (late, other_path, other_range) = tokio::join!(
            storage.get_slice_stream(Path::new("a"), 0..1),
            storage.get_slice_stream(Path::new("z"), 0..1),
            storage.get_slice_stream(Path::new("a"), 1..2),
        );
        assert_eq!(Instant::now(), start + 2 * JOIN_WINDOW);
        drop(storage);
        for (reader, expected) in [
            (first, b'a'),
            (second, b'a'),
            (late, b'a'),
            (other_path, b'z'),
            (other_range, b'b'),
        ] {
            let mut buf = Vec::new();
            reader.unwrap().read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, [expected]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_backpressure_and_cancellation() {
        // Dropping the only caller during collection cancels before opening storage.
        let (reader, mut writer) = tokio::io::duplex(CHUNK_NUM_BYTES);
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .once()
            .return_once(|_, _| Ok(Box::new(reader)));
        let storage = DebouncedStorage::new(backend);
        let mut cancelled = Box::pin(storage.get_slice_stream(Path::new("cancelled"), 0..3));
        assert!(poll!(&mut cancelled).is_pending());
        drop(cancelled);
        tokio::time::advance(2 * JOIN_WINDOW).await;
        tokio::task::yield_now().await;

        let content = vec![7; 4 * CHUNK_NUM_BYTES];
        let (fast, slow) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 0..content.len()),
            storage.get_slice_stream(Path::new("file"), 0..content.len()),
        );
        drop(storage);
        let (mut fast, slow) = (fast.unwrap(), slow.unwrap());
        let mut bytes = vec![0; content.len()];
        let mut write = Box::pin(writer.write_all(&content));
        let mut read = Box::pin(fast.read_exact(&mut bytes));
        for _ in 0..6 {
            assert!(poll!(&mut write).is_pending() && poll!(&mut read).is_pending());
            tokio::task::yield_now().await;
        }
        // Dropping the slow reader releases backpressure. Dropping the last reader closes the
        // storage stream.
        drop(slow);
        let (written, read) = tokio::join!(write, read);
        written.unwrap();
        read.unwrap();
        assert_eq!(bytes, content);
        drop(fast);
        tokio::task::yield_now().await;
        assert_eq!(
            writer.write_all(b"x").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_errors_and_producer_panic_reach_all_readers() {
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .times(4)
            .returning(|_, range| match range.start {
                0 => Err(StorageErrorKind::Service
                    .with_error(anyhow::anyhow!("opening failed"))
                    .with_retry_after(Some(JOIN_WINDOW))),
                1 => Ok(Box::new(StreamReader::new(futures::stream::iter([
                    Ok(Bytes::from_static(b"prefix")),
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "interrupted",
                    )),
                ])))),
                2 => Ok(Box::new(StreamReader::new(futures::stream::poll_fn::<
                    io::Result<Bytes>,
                    _,
                >(|_| {
                    panic!("read panic")
                })))),
                _ => panic!("open panic"),
            });
        let storage = DebouncedStorage::new(backend);
        let (first, second) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 0..10),
            storage.get_slice_stream(Path::new("file"), 0..10),
        );
        for reader in [first, second] {
            let err = reader.err().unwrap();
            assert_eq!(
                (err.kind(), err.retry_after),
                (StorageErrorKind::Service, Some(JOIN_WINDOW))
            );
        }
        let (first, second) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 1..10),
            storage.get_slice_stream(Path::new("file"), 1..10),
        );
        for reader in [first, second] {
            let mut bytes = Vec::new();
            let err = reader.unwrap().read_to_end(&mut bytes).await.unwrap_err();
            assert_eq!(
                (bytes.as_slice(), err.kind()),
                (&b"prefix"[..], io::ErrorKind::ConnectionReset)
            );
        }
        let mut reader = storage
            .get_slice_stream(Path::new("file"), 2..10)
            .await
            .unwrap();
        assert_eq!(
            reader.read_u8().await.unwrap_err().to_string(),
            "stream producer stopped"
        );
        assert_eq!(
            storage
                .get_slice_stream(Path::new("file"), 3..10)
                .await
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Internal
        );
    }
}
