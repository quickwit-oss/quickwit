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

use axum::extract::{FromRequest, Request};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use http::StatusCode;
use http::header::CONTENT_ENCODING;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::thread_pool::run_cpu_intensive;

use crate::load_shield::{LoadShield, LoadShieldPermit};

fn get_ingest_load_shield() -> &'static LoadShield {
    static LOAD_SHIELD: OnceLock<LoadShield> = OnceLock::new();
    LOAD_SHIELD.get_or_init(|| LoadShield::new("ingest"))
}

pub(crate) struct DecompressedBody(pub Bytes);

#[axum::async_trait]
impl<S> FromRequest<S> for DecompressedBody
where S: Send + Sync
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_encoding = req
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;

        let _permit = get_ingest_load_shield()
            .acquire_permit()
            .await
            .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "Too many requests").into_response())?;

        let decompressed = decompress_body(content_encoding, body).await.map_err(|_| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "Failed to decompress body",
            )
                .into_response()
        })?;

        Ok(Self(decompressed))
    }
}

/// There are two ways to decompress the body:
/// - Stream the body through an async decompressor
/// - Fetch the body and then decompress the bytes
///
/// The first approach lowers the latency, while the second approach is more CPU efficient.
/// Ingesting data is usually CPU bound and there is considerable latency until the data is
/// searchable, so the second approach is more suitable for this use case.
async fn decompress_body(encoding: Option<String>, body: Bytes) -> Result<Bytes, std::io::Error> {
    match encoding.as_deref() {
        Some("identity") => Ok(body),
        Some("gzip" | "x-gzip") => {
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                let mut decoder = MultiGzDecoder::new(body.as_ref());
                decoder.read_to_end(&mut decompressed)?;
                Ok::<_, std::io::Error>(Bytes::from(decompressed))
            })
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Task panicked"))??;
            Ok(decompressed)
        }
        Some("zstd") => {
            let decompressed = run_cpu_intensive(move || {
                zstd::decode_all(body.as_ref())
                    .map(Bytes::from)
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "zstd decompression failed")
                    })
            })
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Task panicked"))??;
            Ok(decompressed)
        }
        Some("deflate" | "x-deflate") => {
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                ZlibDecoder::new(body.as_ref()).read_to_end(&mut decompressed)?;
                Ok::<_, std::io::Error>(Bytes::from(decompressed))
            })
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Task panicked"))??;
            Ok(decompressed)
        }
        Some(_encoding) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Unsupported encoding",
        )),
        None => Ok(body),
    }
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
