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
use std::sync::OnceLock;

use bytes::Bytes;
use flate2::read::GzDecoder;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::thread_pool::run_cpu_intensive;
use thiserror::Error;
use warp::reject::Reject;
use warp::Filter;

use crate::load_shield::{LoadShield, LoadShieldPermit};

fn load_shield() -> &'static LoadShield {
    static LOAD_SHIELD: OnceLock<LoadShield> = OnceLock::new();
    LOAD_SHIELD.get_or_init(|| {
        let ingest_max_concurrency = quickwit_common::get_from_env("QW_INGEST_MAX_CONCURRENCY", 10);
        let ingest_max_pending = quickwit_common::get_from_env("QW_INGEST_MAX_PENDING", 150);
        LoadShield::new(ingest_max_concurrency, ingest_max_pending)
    })
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
            let permit = load_shield().acquire_permit().await?;
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
            _gauge_guard: GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.rest_server),
            _permit: load_shield_permit,
        }
    }
}
