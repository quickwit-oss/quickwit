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
use std::time::Duration;

use quickwit_common::retry::{RetryParams, Retryable, retry};
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use crate::{PutPayload, StorageError, StorageErrorKind, StorageResolverError};

/// Response payload of the `files/stat` RPC.
#[derive(Debug, Deserialize)]
pub(super) struct FilesStatResponse {
    /// CID of the MFS entry. This is the content address of the whole
    /// (chunked) file and can be shared, pinned or exported as a CAR file.
    #[serde(rename = "Hash")]
    pub cid: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "Type")]
    pub entry_type: String,
}

#[derive(Debug, Deserialize)]
struct RpcErrorBody {
    #[serde(rename = "Message")]
    message: String,
}

/// Error returned by the Kubo RPC API.
///
/// Kubo reports application errors (including "file does not exist") with an
/// HTTP 500 status and a JSON body, so error classification relies on the
/// error message rather than the status code.
#[derive(Debug, thiserror::Error)]
pub(super) enum IpfsRpcError {
    #[error("IPFS RPC transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("IPFS RPC error (status={status}): {message}")]
    Api { status: u16, message: String },
    #[error("failed to read payload: {0}")]
    PayloadIo(#[from] io::Error),
}

impl IpfsRpcError {
    pub fn is_not_found(&self) -> bool {
        match self {
            IpfsRpcError::Api { message, .. } => {
                message.contains("does not exist") || message.contains("no such file")
            }
            _ => false,
        }
    }
}

impl Retryable for IpfsRpcError {
    fn is_retryable(&self) -> bool {
        match self {
            IpfsRpcError::Transport(error) => error.is_timeout() || error.is_connect(),
            IpfsRpcError::Api { status, .. } => matches!(status, 429 | 502 | 503 | 504),
            IpfsRpcError::PayloadIo(_) => false,
        }
    }
}

impl From<IpfsRpcError> for StorageError {
    fn from(rpc_error: IpfsRpcError) -> Self {
        if rpc_error.is_not_found() {
            return StorageErrorKind::NotFound.with_error(rpc_error);
        }
        match &rpc_error {
            IpfsRpcError::Transport(error) if error.is_timeout() => {
                StorageErrorKind::Timeout.with_error(rpc_error)
            }
            IpfsRpcError::Transport(error) if error.is_connect() => {
                StorageErrorKind::Service.with_error(rpc_error)
            }
            IpfsRpcError::Transport(_) | IpfsRpcError::PayloadIo(_) => {
                StorageErrorKind::Io.with_error(rpc_error)
            }
            IpfsRpcError::Api { .. } => StorageErrorKind::Internal.with_error(rpc_error),
        }
    }
}

/// Minimal client for the subset of the Kubo RPC API (`/api/v0/files/*`)
/// required to serve Quickwit splits from MFS.
#[derive(Clone)]
pub(super) struct IpfsRpcClient {
    http_client: reqwest::Client,
    api_base_url: String,
    chunker: String,
    raw_leaves: bool,
    request_timeout: Duration,
    retry_params: RetryParams,
}

impl IpfsRpcClient {
    pub fn new(
        api_endpoint: &str,
        chunker: String,
        raw_leaves: bool,
        request_timeout: Duration,
    ) -> Result<Self, StorageResolverError> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| StorageResolverError::FailedToOpenStorage {
                kind: StorageErrorKind::Service,
                message: format!("failed to build IPFS HTTP client: {error}"),
            })?;
        let api_base_url = format!("{}/api/v0", api_endpoint.trim_end_matches('/'));
        Ok(Self {
            http_client,
            api_base_url,
            chunker,
            raw_leaves,
            request_timeout,
            retry_params: RetryParams::standard(),
        })
    }

    fn endpoint_url(&self, endpoint: &str) -> String {
        format!("{}/{endpoint}", self.api_base_url)
    }

    /// Issues a small (non-streaming) RPC and returns the successful response.
    async fn post_small(
        &self,
        endpoint: &str,
        query: &[(&str, &str)],
    ) -> Result<reqwest::Response, IpfsRpcError> {
        let response = self
            .http_client
            .post(self.endpoint_url(endpoint))
            .timeout(self.request_timeout)
            .query(query)
            .send()
            .await?;
        into_checked_response(response).await
    }

    pub async fn version(&self) -> Result<(), IpfsRpcError> {
        self.post_small("version", &[]).await?;
        Ok(())
    }

    pub async fn files_mkdir(&self, mfs_path: &str) -> Result<(), IpfsRpcError> {
        retry(&self.retry_params, || async {
            self.post_small("files/mkdir", &[("arg", mfs_path), ("parents", "true")])
                .await
        })
        .await?;
        Ok(())
    }

    pub async fn files_stat(&self, mfs_path: &str) -> Result<FilesStatResponse, IpfsRpcError> {
        let response = retry(&self.retry_params, || async {
            self.post_small("files/stat", &[("arg", mfs_path)]).await
        })
        .await?;
        let stat_response: FilesStatResponse = response.json().await?;
        Ok(stat_response)
    }

    /// Removes an MFS entry. Missing entries are not treated as an error.
    pub async fn files_rm(&self, mfs_path: &str) -> Result<(), IpfsRpcError> {
        let rm_result = retry(&self.retry_params, || async {
            self.post_small(
                "files/rm",
                &[("arg", mfs_path), ("recursive", "true"), ("force", "true")],
            )
            .await
        })
        .await;
        match rm_result {
            Ok(_) => Ok(()),
            Err(rpc_error) if rpc_error.is_not_found() => Ok(()),
            Err(rpc_error) => Err(rpc_error),
        }
    }

    pub async fn files_mv(&self, src_path: &str, dst_path: &str) -> Result<(), IpfsRpcError> {
        retry(&self.retry_params, || async {
            self.post_small("files/mv", &[("arg", src_path), ("arg", dst_path)])
                .await
        })
        .await?;
        Ok(())
    }

    /// Writes the full payload to `mfs_path`, creating parent directories and
    /// truncating any existing entry. Retries restart the upload from scratch,
    /// which is safe because `truncate=true` makes the write idempotent.
    pub async fn files_write(
        &self,
        mfs_path: &str,
        payload: &dyn PutPayload,
    ) -> Result<(), IpfsRpcError> {
        retry(&self.retry_params, || async {
            self.files_write_attempt(mfs_path, payload).await
        })
        .await
    }

    async fn files_write_attempt(
        &self,
        mfs_path: &str,
        payload: &dyn PutPayload,
    ) -> Result<(), IpfsRpcError> {
        let payload_len = payload.len();
        let payload_reader = payload.byte_stream().await?.into_async_read();
        let body = reqwest::Body::wrap_stream(ReaderStream::new(payload_reader));
        let part = reqwest::multipart::Part::stream_with_length(body, payload_len)
            .file_name("data")
            .mime_str("application/octet-stream")?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let raw_leaves = if self.raw_leaves { "true" } else { "false" };
        let response = self
            .http_client
            .post(self.endpoint_url("files/write"))
            .query(&[
                ("arg", mfs_path),
                ("create", "true"),
                ("truncate", "true"),
                ("parents", "true"),
                ("raw-leaves", raw_leaves),
                ("chunker", &self.chunker),
            ])
            .multipart(form)
            .send()
            .await?;
        into_checked_response(response).await?;
        Ok(())
    }

    /// Reads `count` bytes starting at `offset` and returns them in memory.
    pub async fn files_read_range(
        &self,
        mfs_path: &str,
        offset: u64,
        count: u64,
    ) -> Result<Vec<u8>, IpfsRpcError> {
        let bytes = retry(&self.retry_params, || async {
            let response = self
                .files_read_response(mfs_path, offset, Some(count))
                .await?;
            let bytes = response.bytes().await?;
            Ok::<_, IpfsRpcError>(bytes)
        })
        .await?;
        Ok(bytes.to_vec())
    }

    /// Opens a streaming read starting at `offset`. When `count` is `None`,
    /// the stream covers the remainder of the file.
    pub async fn files_read_stream(
        &self,
        mfs_path: &str,
        offset: u64,
        count: Option<u64>,
    ) -> Result<reqwest::Response, IpfsRpcError> {
        retry(&self.retry_params, || async {
            self.files_read_response(mfs_path, offset, count).await
        })
        .await
    }

    async fn files_read_response(
        &self,
        mfs_path: &str,
        offset: u64,
        count_opt: Option<u64>,
    ) -> Result<reqwest::Response, IpfsRpcError> {
        let offset_str = offset.to_string();
        let mut query: Vec<(&str, &str)> = vec![("arg", mfs_path), ("offset", &offset_str)];
        let count_str: String;
        if let Some(count) = count_opt {
            count_str = count.to_string();
            query.push(("count", &count_str));
        }
        let response = self
            .http_client
            .post(self.endpoint_url("files/read"))
            .query(&query)
            .send()
            .await?;
        into_checked_response(response).await
    }
}

/// Turns a Kubo HTTP response into an error if the status is not a success,
/// extracting the error message from the JSON error body when present.
async fn into_checked_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, IpfsRpcError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let message = match response.json::<RpcErrorBody>().await {
        Ok(error_body) => error_body.message,
        Err(_) => format!("unexpected HTTP status {status}"),
    };
    Err(IpfsRpcError::Api {
        status: status.as_u16(),
        message,
    })
}
