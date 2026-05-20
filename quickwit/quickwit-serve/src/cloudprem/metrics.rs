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
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use quickwit_metrics::{Counter, LazyCounter, lazy_counter};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

static INGRESS_BYTES: LazyCounter = lazy_counter!(
    name: "ingress_bytes.count",
    description: "CloudPrem ingress bytes for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static EGRESS_BYTES: LazyCounter = lazy_counter!(
    name: "egress_bytes.count",
    description: "CloudPrem egress bytes for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

/// Wraps a stream and instruments its read and write operations
/// to report ingress/egress metrics.
#[pin_project]
pub struct InstrumentedStream<T> {
    #[pin]
    inner: T,
    ingress_bytes: Counter,
    egress_bytes: Counter,
}

impl<T> InstrumentedStream<T> {
    pub fn new(inner: T) -> Self {
        InstrumentedStream {
            inner,
            ingress_bytes: INGRESS_BYTES.clone(),
            egress_bytes: EGRESS_BYTES.clone(),
        }
    }
}

impl<T: AsyncRead> AsyncRead for InstrumentedStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        let bytes_before = buf.filled().len();
        let result = this.inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let bytes_read = buf.filled().len() - bytes_before;
            this.ingress_bytes.inc_by(bytes_read as u64);
        }
        result
    }
}

impl<T: AsyncWrite> AsyncWrite for InstrumentedStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        let result = this.inner.poll_write(cx, buf);
        if let Poll::Ready(Ok(bytes_written)) = &result {
            this.egress_bytes.inc_by(*bytes_written as u64);
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        this.inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        this.inner.poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn snapshot_as_map_for_test(snapshot: Snapshot) -> HashMap<String, DebugValue> {
        snapshot
            .into_vec()
            .into_iter()
            .map(|(composite_key, _, _, value)| (format!("{}", composite_key.key()), value))
            .collect()
    }

    #[tokio::test]
    async fn test_instrumented_stream_counters() {
        let recorder = DebuggingRecorder::default();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(Box::leak(Box::new(recorder)));

        let (client, _server) = tokio::io::duplex(1024);
        let mut stream = InstrumentedStream::new(client);
        let bytes_written = stream.write(&[1u8; 100]).await.unwrap();

        let (client, mut server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            server.write_all(&[1u8; 100]).await.unwrap();
        });

        let mut stream = InstrumentedStream::new(client);
        let mut buf = [0; 1024];
        let bytes_read = stream.read(&mut buf).await.unwrap();

        let snapshot = snapshot_as_map_for_test(snapshotter.snapshot());
        assert_eq!(snapshot.len(), 2);

        assert_eq!(
            snapshot.get("Key(cloudprem.ingress_bytes.count)").unwrap(),
            &DebugValue::Counter(bytes_read as u64)
        );
        assert_eq!(
            snapshot.get("Key(cloudprem.egress_bytes.count)").unwrap(),
            &DebugValue::Counter(bytes_written as u64)
        );
    }
}
