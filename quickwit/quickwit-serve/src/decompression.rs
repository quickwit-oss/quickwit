// Copyright (C) 2024 Quickwit, Inc.
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

use std::io::Read;

use bytes::Bytes;
use flate2::read::GzDecoder;
use once_cell::sync::OnceCell;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use thiserror::Error;
use tracing::error;
use warp::reject::Reject;
use warp::Filter;

fn thread_pool() -> &'static rayon::ThreadPool {
    static THREAD_POOL: OnceCell<rayon::ThreadPool> = OnceCell::new();
    THREAD_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .thread_name(|thread_id| format!("quickwit-rest-{thread_id}"))
            .panic_handler(|_my_panic| {
                error!("task running in the quickwit rest pool panicked");
            })
            .build()
            .expect("Failed to spawn the spawning pool")
    })
}

/// Function similar to `tokio::spawn_blocking`.
///
/// Here are two important differences however:
///
/// 1) The task is running on a rayon thread pool managed by quickwit.
/// This pool is specifically used only to run CPU intensive work
/// and is configured to contain `num_cpus` cores.
///
/// 2) Before the task is effectively scheduled, we check that
/// the spawner is still interested by its result.
///
/// It is therefore required to `await` the result of this
/// function to get anywork done.
///
/// This is nice, because it makes work that has been scheduled
/// but is not running yet "cancellable".
pub async fn run_cpu_intensive<F, R>(cpu_heavy_task: F) -> Result<R, ()>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    thread_pool().spawn(move || {
        if tx.is_closed() {
            return;
        }
        let task_result = cpu_heavy_task();
        let _ = tx.send(task_result);
    });
    rx.await.map_err(|_| ())
}

/// There are two ways to decompress the body:
/// - Stream the body through an async decompressor
/// - Fetch the body and then decompress the bytes
///
/// The first approach lowers the latency, while the second approach is more CPU efficient.
/// Ingesting data is usually CPU bound and there is considerable latency until the data is
/// searchable, so the second approach is more suitable for this use case.
async fn decompress_body(encoding: Option<String>, body: Bytes) -> Result<Bytes, warp::Rejection> {
    match encoding.as_deref() {
        Some("gzip" | "x-gzip") => {
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                let mut decoder = GzDecoder::new(body.as_ref());
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|_| warp::reject::custom(CorruptedData))?;
                Result::<_, warp::Rejection>::Ok(Bytes::from(decompressed))
            })
            .await
            .map_err(|_| warp::reject::custom(CorruptedData))??;
            Ok(decompressed)
        }
        Some("zstd") => {
            let decompressed = run_cpu_intensive(move || {
                zstd::decode_all(body.as_ref())
                    .map(Bytes::from)
                    .map_err(|_| warp::reject::custom(CorruptedData))
            })
            .await
            .map_err(|_| warp::reject::custom(CorruptedData))??;
            Ok(decompressed)
        }
        Some(encoding) => Err(warp::reject::custom(UnsupportedEncoding(
            encoding.to_string(),
        ))),
        _ => Ok(body),
    }
}

#[derive(Debug, Error)]
#[error("Error while decompressing the data")]
pub(crate) struct CorruptedData;

impl Reject for CorruptedData {}

#[derive(Debug, Error)]
#[error("Unsupported Content-Encoding {}. Supported encodings are 'gzip' and 'zstd'", self.0)]
pub(crate) struct UnsupportedEncoding(String);

impl Reject for UnsupportedEncoding {}

/// Custom filter for optional decompression
pub(crate) fn get_body_bytes() -> impl Filter<Extract = (Body,), Error = warp::Rejection> + Clone {
    warp::header::optional("content-encoding")
        .and(warp::body::bytes())
        .and_then(|encoding: Option<String>, body: Bytes| async move {
            decompress_body(encoding, body).await.map(Body::from)
        })
}

pub(crate) struct Body {
    pub content: Bytes,
    _gauge_guard: GaugeGuard,
}

impl From<Bytes> for Body {
    fn from(content: Bytes) -> Self {
        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.rest_server);
        gauge_guard.add(content.len() as i64);
        Body {
            content,
            _gauge_guard: gauge_guard,
        }
    }
}
