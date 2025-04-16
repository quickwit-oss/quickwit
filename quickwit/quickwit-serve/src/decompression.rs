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

use std::io::Read;
use std::sync::OnceLock;

use bytes::Bytes;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::thread_pool::run_cpu_intensive;
use thiserror::Error;
use warp::Filter;
use warp::reject::Reject;

use crate::load_shield::{LoadShield, LoadShieldPermit};

fn get_ingest_load_shield() -> &'static LoadShield {
    static LOAD_SHIELD: OnceLock<LoadShield> = OnceLock::new();
    LOAD_SHIELD.get_or_init(|| LoadShield::new("ingest"))
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
        Some("identity") => Ok(body),
        Some("gzip" | "x-gzip") => {
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                let mut decoder = MultiGzDecoder::new(body.as_ref());
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
        Some("deflate" | "x-deflate") => {
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                ZlibDecoder::new(body.as_ref())
                    .read_to_end(&mut decompressed)
                    .map_err(|_| warp::reject::custom(CorruptedData))?;
                Result::<_, warp::Rejection>::Ok(Bytes::from(decompressed))
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
            let permit = get_ingest_load_shield().acquire_permit().await?;
            decompress_body(encoding, body)
                .await
                .map(|content| Body::new(content, permit))
        })
}

pub(crate) struct Body {
    pub content: Bytes,
    _gauge_guard: GaugeGuard<'static>,
    _permit: LoadShieldPermit,
}

impl Body {
    pub fn new(content: Bytes, load_shield_permit: LoadShieldPermit) -> Body {
        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.rest_server);
        gauge_guard.add(content.len() as i64);
        Body {
            content,
            _gauge_guard: gauge_guard,
            _permit: load_shield_permit,
        }
    }
}
