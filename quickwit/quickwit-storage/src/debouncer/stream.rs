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

use super::{CLEANUP_INTERVAL, DebouncerKey};
use crate::{Storage, StorageErrorKind, StorageResult};

const COLLECT_READERS_FOR: Duration = Duration::from_millis(5);
const CHUNK_SIZE: usize = 1024 * 1024;

/// Collect identical ranges for 5 ms, then share one backend stream. Each reader has one queued
/// chunk. A slow reader pauses the producer, and dropping the last reader cancels it.
#[derive(Default)]
pub(super) struct StreamDebouncer {
    // The map and its insertion count share the same lock.
    groups: Mutex<(HashMap<DebouncerKey, Weak<StreamGroup>>, usize)>,
}

#[derive(Clone)]
enum Message {
    Chunk(Bytes),
    Eof,
    Error(Arc<io::Error>),
}

type OpenSender = oneshot::Sender<StorageResult<mpsc::Receiver<Message>>>;

struct StreamGroup {
    // Only callers keep the group alive. Abort the producer before dropping the reader channel.
    _producer: AbortOnDropHandle<()>,
    join_deadline: Instant,
    // The producer owns the receiver so its termination also fails pending opens.
    new_readers: mpsc::UnboundedSender<OpenSender>,
}

impl StreamDebouncer {
    pub(super) async fn get<T: Storage>(
        &self,
        storage: Arc<T>,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let (open_sender, open_receiver) = oneshot::channel();
        let group = self.subscribe(storage, (path.to_owned(), range), open_sender);
        let receiver = open_receiver.await.map_err(|_| {
            StorageErrorKind::Internal.with_error(anyhow::anyhow!("stream producer stopped"))
        })??;
        let chunks =
            futures::stream::try_unfold((receiver, group), |(mut receiver, group)| async {
                match receiver.recv().await {
                    Some(Message::Chunk(bytes)) => Ok(Some((bytes, (receiver, group)))),
                    Some(Message::Eof) => Ok(None),
                    Some(Message::Error(error)) => Err(io::Error::new(error.kind(), error)),
                    None => Err(io::Error::other("stream producer stopped")),
                }
            });
        Ok(Box::new(StreamReader::new(chunks.boxed())))
    }

    fn subscribe<T: Storage>(
        &self,
        storage: Arc<T>,
        key: DebouncerKey,
        mut reader: OpenSender,
    ) -> Arc<StreamGroup> {
        let mut registry = self.groups.lock().unwrap();
        let (groups, inserts_since_cleanup) = &mut *registry;
        if let Some(group) = groups.get(&key).and_then(Weak::upgrade)
            && Instant::now() < group.join_deadline
        {
            match group.new_readers.send(reader) {
                Ok(()) => return group,
                Err(error) => reader = error.0, // The producer closed the group meanwhile.
            }
        }
        let join_deadline = Instant::now() + COLLECT_READERS_FOR;
        let (new_readers, receiver) = mpsc::unbounded_channel();
        new_readers
            .send(reader)
            .expect("receiver is still owned here");
        let task = tokio::spawn(share_stream(storage, key.clone(), join_deadline, receiver));
        let group = Arc::new(StreamGroup {
            _producer: AbortOnDropHandle::new(task),
            join_deadline,
            new_readers,
        });
        groups.insert(key, Arc::downgrade(&group));
        // As with buffered reads, finished and cancelled groups leave weak entries to reclaim
        // periodically.
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
    mut new_readers: mpsc::UnboundedReceiver<OpenSender>,
) {
    tokio::time::sleep_until(join_deadline).await;
    // Callers that miss this group start a new download group.
    new_readers.close();
    let stream = storage.get_slice_stream(&key.0, key.1).await;
    let mut senders = Vec::new();
    while let Some(open_sender) = new_readers.recv().await {
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
    let Ok(stream) = stream else { return };
    let mut chunks = ReaderStream::with_capacity(stream, CHUNK_SIZE);
    loop {
        senders.retain(|sender| !sender.is_closed());
        if senders.is_empty() {
            return;
        }
        let message = match chunks.next().await {
            Some(Ok(bytes)) => Message::Chunk(bytes),
            Some(Err(error)) => Message::Error(Arc::new(error)),
            None => Message::Eof,
        };
        let finished = !matches!(message, Message::Chunk(_));
        for sender in &senders {
            // A dropped reader leaves the group. Other readers still receive every chunk.
            let _ = sender.send(message.clone()).await;
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

    fn storage_with_stream(
        reader: impl AsyncRead + Send + Unpin + 'static,
        num_opens: usize,
    ) -> DebouncedStorage<MockStorage> {
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .times(num_opens)
            .return_once(|_, _| Ok(Box::new(reader)));
        DebouncedStorage::new(backend)
    }

    #[tokio::test(start_paused = true)]
    async fn test_groups_by_path_range_and_fixed_window() {
        let start = Instant::now();
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .times(4)
            .returning(move |path, range| {
                assert!(Instant::now() >= start + COLLECT_READERS_FOR);
                assert_eq!(range.len(), 1);
                let byte = path.to_str().unwrap().as_bytes()[0] + range.start as u8;
                Ok(Box::new(io::Cursor::new([byte])))
            });
        let storage = DebouncedStorage::new(backend);
        let mut first = Box::pin(storage.get_slice_stream(Path::new("a"), 0..1));
        assert!(poll!(&mut first).is_pending());
        tokio::time::advance(Duration::from_millis(4)).await;
        let mut second = Box::pin(storage.get_slice_stream(Path::new("a"), 0..1));
        assert!(poll!(&mut second).is_pending());
        let (first, second) = tokio::join!(first, second);
        assert_eq!(Instant::now(), start + COLLECT_READERS_FOR);
        // Late readers and different keys each open a separate stream.
        let (late, other_path, other_range) = tokio::join!(
            storage.get_slice_stream(Path::new("a"), 0..1),
            storage.get_slice_stream(Path::new("z"), 0..1),
            storage.get_slice_stream(Path::new("a"), 1..2),
        );
        assert_eq!(Instant::now(), start + 2 * COLLECT_READERS_FOR);
        drop(storage);
        for (reader, expected) in [
            (first, b'a'),
            (second, b'a'),
            (late, b'a'),
            (other_path, b'z'),
            (other_range, b'b'),
        ] {
            let mut reader = reader.unwrap();
            assert_eq!(reader.read_u8().await.unwrap(), expected);
            for _ in 0..2 {
                assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_backpressure_and_reader_cancellation() {
        let (reader, mut writer) = tokio::io::duplex(CHUNK_SIZE);
        let storage = storage_with_stream(reader, 1);
        let (fast, slow) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 0..8 * CHUNK_SIZE),
            storage.get_slice_stream(Path::new("file"), 0..8 * CHUNK_SIZE),
        );
        drop(storage);
        let mut fast = fast.unwrap();
        let slow = slow.unwrap();
        let content = vec![7; 8 * CHUNK_SIZE];
        let mut bytes = vec![0; content.len()];
        let mut write = Box::pin(writer.write_all(&content));
        let mut read = Box::pin(fast.read_exact(&mut bytes));
        for _ in 0..10 {
            assert!(poll!(&mut write).is_pending());
            assert!(poll!(&mut read).is_pending());
            tokio::task::yield_now().await;
        }
        // Dropping the slow reader releases backpressure. The other reader gets all bytes.
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
    async fn test_open_and_read_errors_reach_all_readers() {
        let mut backend = MockStorage::default();
        backend
            .expect_get_slice_stream()
            .times(2)
            .returning(|_, range| {
                if range.start == 0 {
                    return Err(StorageErrorKind::Service
                        .with_error(anyhow::anyhow!("opening failed"))
                        .with_retry_after(Some(COLLECT_READERS_FOR)));
                }
                let chunks = futures::stream::iter([
                    Ok(Bytes::from_static(b"prefix")),
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "interrupted",
                    )),
                ]);
                Ok(Box::new(StreamReader::new(chunks)))
            });
        let storage = DebouncedStorage::new(backend);
        let (first, second) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 0..10),
            storage.get_slice_stream(Path::new("file"), 0..10),
        );
        for reader in [first, second] {
            let error = reader.err().unwrap();
            assert_eq!(error.kind(), StorageErrorKind::Service);
            assert_eq!(error.retry_after, Some(COLLECT_READERS_FOR));
        }
        let (first, second) = tokio::join!(
            storage.get_slice_stream(Path::new("file"), 1..10),
            storage.get_slice_stream(Path::new("file"), 1..10),
        );
        for reader in [first, second] {
            let mut bytes = Vec::new();
            let error = reader.unwrap().read_to_end(&mut bytes).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            assert_eq!(bytes, b"prefix");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_cancellation_and_producer_failure() {
        // Dropping the only caller during collection cancels the producer before opening storage.
        let backend = Arc::new(MockStorage::default());
        let debouncer = StreamDebouncer::default();
        let mut opening = Box::pin(debouncer.get(backend.clone(), Path::new("file"), 0..3));
        assert!(poll!(&mut opening).is_pending());
        drop(opening);
        tokio::time::advance(2 * COLLECT_READERS_FOR).await;
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&backend), 1);

        // Aborting a producer fails both pending opens and active readers.
        let abort_group = |debouncer: &StreamDebouncer| {
            let (groups, _) = &*debouncer.groups.lock().unwrap();
            groups
                .values()
                .find_map(Weak::upgrade)
                .unwrap()
                ._producer
                .abort();
        };
        let mut opening = Box::pin(debouncer.get(backend.clone(), Path::new("file"), 0..3));
        assert!(poll!(&mut opening).is_pending());
        abort_group(&debouncer);
        assert_eq!(
            opening.await.err().unwrap().kind(),
            StorageErrorKind::Internal
        );

        let (reader, _writer) = tokio::io::duplex(1);
        let backend = Arc::new(storage_with_stream(reader, 1));
        let mut active = debouncer
            .get(backend, Path::new("file"), 0..3)
            .await
            .unwrap();
        abort_group(&debouncer);
        assert_eq!(
            active.read_u8().await.unwrap_err().to_string(),
            "stream producer stopped"
        );
    }
}
