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

use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::HttpError;

/// Writes all of `buf` to `stream`, bounding each individual `write` (and the
/// final flush) by `timeout`.
pub async fn write_all_timeout<W>(
    stream: &mut W,
    buf: &[u8],
    timeout: Duration,
) -> Result<(), HttpError>
where
    W: AsyncWrite + Unpin,
{
    let mut written = 0;
    while written < buf.len() {
        let n = match tokio::time::timeout(timeout, stream.write(&buf[written..])).await {
            Ok(res) => res?,
            Err(_) => return Err(HttpError::Timeout(timeout, "request write".to_string())),
        };
        if n == 0 {
            return Err(HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "wrote zero bytes",
            )));
        }
        written += n;
    }
    match tokio::time::timeout(timeout, stream.flush()).await {
        Ok(res) => Ok(res?),
        Err(_) => Err(HttpError::Timeout(timeout, "request write".to_string())),
    }
}
