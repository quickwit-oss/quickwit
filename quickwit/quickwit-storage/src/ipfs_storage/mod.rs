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

//! IPFS storage backend.
//!
//! Stores Quickwit splits in the MFS (Mutable File System) of an IPFS node
//! (e.g. [Kubo](https://github.com/ipfs/kubo)) through its RPC API. MFS
//! presents content-addressed, chunked DAGs as regular files supporting
//! offset/count reads, which matches the byte-range access pattern of
//! [`Storage::get_slice`] on split files.
//!
//! URI scheme: `ipfs://<root-dir>/<prefix...>`, mapped to the MFS directory
//! `/<root-dir>/<prefix...>` on the node targeted by the configured RPC API
//! endpoint. Every file written this way has a CID (retrievable via
//! `files/stat`) and is therefore addressable, pinnable, and exportable as a
//! CAR file from the wider IPFS network.

mod client;

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::{IpfsStorageConfig, StorageBackend};
use tantivy::directory::OwnedBytes;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio_util::io::StreamReader;
use tracing::instrument;

use self::client::IpfsRpcClient;
use crate::debouncer::DebouncedStorage;
use crate::metrics::object_storage_get_slice_in_flight_guards;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, DeleteFailure, PutPayload, Storage, StorageError, StorageErrorKind,
    StorageFactory, StorageResolverError, StorageResult,
};

/// IPFS storage resolver.
pub struct IpfsStorageFactory {
    storage_config: IpfsStorageConfig,
}

impl IpfsStorageFactory {
    /// Creates a new IPFS storage factory.
    pub fn new(storage_config: IpfsStorageConfig) -> Self {
        Self { storage_config }
    }
}

#[async_trait]
impl StorageFactory for IpfsStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Ipfs
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = IpfsStorage::from_uri(&self.storage_config, uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

/// IPFS storage implementation backed by the MFS of an IPFS node.
pub struct IpfsStorage {
    uri: Uri,
    /// MFS directory acting as the root of this storage, e.g. `/quickwit-indexes/my-index`.
    mfs_root: String,
    client: IpfsRpcClient,
}

impl fmt::Debug for IpfsStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IpfsStorage")
            .field("uri", &self.uri)
            .field("mfs_root", &self.mfs_root)
            .finish()
    }
}

impl IpfsStorage {
    /// Creates an [`IpfsStorage`] from an `ipfs://` URI and a config.
    pub fn from_uri(
        storage_config: &IpfsStorageConfig,
        uri: &Uri,
    ) -> Result<Self, StorageResolverError> {
        if uri.protocol() != Protocol::Ipfs {
            let message = format!("URI `{uri}` is not a valid IPFS URI");
            return Err(StorageResolverError::InvalidUri(message));
        }
        let mfs_root = mfs_root_from_uri(uri)?;
        let client = IpfsRpcClient::new(
            &storage_config.resolve_api_endpoint(),
            storage_config.chunker.clone(),
            storage_config.raw_leaves,
            Duration::from_secs(storage_config.request_timeout_secs),
        )?;
        Ok(Self {
            uri: uri.clone(),
            mfs_root,
            client,
        })
    }

    /// Returns the CID of the file at `path`, i.e. the content address under
    /// which the whole split can be fetched, pinned, or exported from IPFS.
    pub async fn file_cid(&self, path: &Path) -> StorageResult<String> {
        let mfs_path = self.mfs_path(path)?;
        let stat_response = self
            .client
            .files_stat(&mfs_path)
            .await
            .map_err(StorageError::from)?;
        Ok(stat_response.cid)
    }

    fn mfs_path(&self, relative_path: &Path) -> StorageResult<String> {
        ensure_valid_relative_path(relative_path)?;
        let relative_str = relative_path.to_string_lossy();
        if relative_str.is_empty() {
            return Ok(self.mfs_root.clone());
        }
        Ok(format!("{}/{relative_str}", self.mfs_root))
    }

    async fn delete_single_file(&self, path: &Path) -> StorageResult<()> {
        let mfs_path = self.mfs_path(path)?;
        self.client
            .files_rm(&mfs_path)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

/// Extracts the MFS root directory from an `ipfs://` URI:
/// `ipfs://indexes/my-index` -> `/indexes/my-index`.
fn mfs_root_from_uri(uri: &Uri) -> Result<String, StorageResolverError> {
    let raw_path = uri
        .as_str()
        .strip_prefix("ipfs://")
        .unwrap_or_default()
        .trim_matches('/');
    if raw_path.is_empty() {
        let message = format!("URI `{uri}` is missing an MFS root directory");
        return Err(StorageResolverError::InvalidUri(message));
    }
    Ok(format!("/{raw_path}"))
}

/// Rejects paths that could escape the storage root (`..`, absolute paths).
fn ensure_valid_relative_path(path: &Path) -> StorageResult<()> {
    for component in path.components() {
        match component {
            std::path::Component::RootDir
            | std::path::Component::ParentDir
            | std::path::Component::Prefix(_) => {
                return Err(StorageErrorKind::Unauthorized.with_error(anyhow::anyhow!(
                    "path `{}` is forbidden. only simple relative paths are allowed",
                    path.display()
                )));
            }
            std::path::Component::CurDir | std::path::Component::Normal(_) => {}
        }
    }
    Ok(())
}

/// Wraps a byte stream and enforces that exactly `expected_len` bytes flow
/// through it, failing fast on truncated responses.
struct ExactLenReader<R> {
    inner: R,
    num_bytes_read: u64,
    expected_len: u64,
}

impl<R: AsyncRead + Unpin> AsyncRead for ExactLenReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let num_new_bytes = (buf.filled().len() - filled_before) as u64;
                if num_new_bytes == 0 && self.num_bytes_read < self.expected_len {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "IPFS read truncated: got {} bytes, expected {}",
                            self.num_bytes_read, self.expected_len
                        ),
                    )));
                }
                self.num_bytes_read += num_new_bytes;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[async_trait]
impl Storage for IpfsStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.client.version().await?;
        // Creating the root directory doubles as a write-permission check.
        self.client.files_mkdir(&self.mfs_root).await?;
        Ok(())
    }

    #[instrument(name = "storage.ipfs.put", level = "debug", skip(self, payload), fields(payload_len = payload.len()))]
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        let mfs_path = self.mfs_path(path)?;
        // Write to a temporary MFS entry, then move it into place. `files/mv`
        // only relinks directory entries, so readers never observe a
        // partially written split.
        let temp_mfs_path = format!("{mfs_path}.temp");
        self.client
            .files_write(&temp_mfs_path, payload.as_ref())
            .await
            .map_err(StorageError::from)?;
        // `files/mv` fails if the destination exists: remove any previous
        // version first. Splits are immutable and ULID-named, so overwrites
        // only happen when retrying a failed upload of the same content.
        self.client
            .files_rm(&mfs_path)
            .await
            .map_err(StorageError::from)?;
        self.client
            .files_mv(&temp_mfs_path, &mfs_path)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    #[instrument(name = "storage.ipfs.copy_to", level = "debug", skip(self, output))]
    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let mfs_path = self.mfs_path(path)?;
        let expected_len = self
            .client
            .files_stat(&mfs_path)
            .await
            .map_err(StorageError::from)?
            .size;
        let response = self
            .client
            .files_read_stream(&mfs_path, 0, None)
            .await
            .map_err(StorageError::from)?;
        let byte_stream = response.bytes_stream().map_err(std::io::Error::other);
        let mut reader = ExactLenReader {
            inner: StreamReader::new(byte_stream),
            num_bytes_read: 0,
            expected_len,
        };
        tokio::io::copy(&mut reader, output).await?;
        output.flush().await?;
        Ok(())
    }

    #[instrument(name = "storage.ipfs.get_slice", level = "debug", skip(self))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let mfs_path = self.mfs_path(path)?;
        // Kubo interprets `count=0` on `files/read` as "read to EOF".
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let _in_flight_guards = object_storage_get_slice_in_flight_guards(range.len());
        let bytes = self
            .client
            .files_read_range(&mfs_path, range.start as u64, range.len() as u64)
            .await
            .map_err(StorageError::from)?;
        if bytes.len() != range.len() {
            return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                "IPFS ranged read returned {} bytes, expected {} (path: {mfs_path})",
                bytes.len(),
                range.len()
            )));
        }
        Ok(OwnedBytes::new(bytes))
    }

    #[instrument(name = "storage.ipfs.get_slice_stream", level = "debug", skip(self))]
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let mfs_path = self.mfs_path(path)?;
        // Kubo interprets `count=0` on `files/read` as "read to EOF".
        if range.is_empty() {
            return Ok(Box::new(tokio::io::empty()));
        }
        let response = self
            .client
            .files_read_stream(&mfs_path, range.start as u64, Some(range.len() as u64))
            .await
            .map_err(StorageError::from)?;
        let byte_stream = response.bytes_stream().map_err(std::io::Error::other);
        let reader = ExactLenReader {
            inner: StreamReader::new(byte_stream),
            num_bytes_read: 0,
            expected_len: range.len() as u64,
        };
        Ok(Box::new(reader))
    }

    #[instrument(name = "storage.ipfs.get_all", level = "debug", skip(self))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let mfs_path = self.mfs_path(path)?;
        let num_bytes = self
            .client
            .files_stat(&mfs_path)
            .await
            .map_err(StorageError::from)?
            .size;
        if num_bytes == 0 {
            return Ok(OwnedBytes::empty());
        }
        let bytes = self
            .client
            .files_read_range(&mfs_path, 0, num_bytes)
            .await
            .map_err(StorageError::from)?;
        if bytes.len() as u64 != num_bytes {
            return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                "IPFS full read returned {} bytes, expected {num_bytes} (path: {mfs_path})",
                bytes.len(),
            )));
        }
        Ok(OwnedBytes::new(bytes))
    }

    #[instrument(name = "storage.ipfs.delete", level = "debug", skip(self))]
    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.delete_single_file(path).await
    }

    #[instrument(name = "storage.ipfs.bulk_delete", level = "debug", skip(self, paths), fields(num_paths = paths.len()))]
    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let mut successes = Vec::with_capacity(paths.len());
        let mut failures = std::collections::HashMap::new();
        let delete_futures: Vec<_> = paths
            .iter()
            .map(|path: &&Path| {
                let path: &Path = path;
                async move {
                    let delete_result = self.delete_single_file(path).await;
                    (path, delete_result)
                }
            })
            .collect();
        let mut delete_stream = futures::stream::iter(delete_futures).buffer_unordered(10);
        while let Some((path, delete_result)) = delete_stream.next().await {
            match delete_result {
                Ok(()) => successes.push(path.to_path_buf()),
                Err(storage_error) => {
                    let failure = DeleteFailure {
                        error: Some(storage_error),
                        ..Default::default()
                    };
                    failures.insert(path.to_path_buf(), failure);
                }
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(BulkDeleteError {
            successes,
            failures,
            ..Default::default()
        })
    }

    #[instrument(name = "storage.ipfs.file_num_bytes", level = "debug", skip(self))]
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let mfs_path = self.mfs_path(path)?;
        let stat_response = self
            .client
            .files_stat(&mfs_path)
            .await
            .map_err(StorageError::from)?;
        if stat_response.entry_type != "file" {
            return Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!(
                "MFS entry `{mfs_path}` is not a regular file, cannot determine its size"
            )));
        }
        Ok(stat_response.size)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_mfs_root_from_uri() {
        let uri = Uri::from_str("ipfs://indexes/my-index").unwrap();
        assert_eq!(mfs_root_from_uri(&uri).unwrap(), "/indexes/my-index");

        let uri = Uri::from_str("ipfs://indexes").unwrap();
        assert_eq!(mfs_root_from_uri(&uri).unwrap(), "/indexes");

        let uri = Uri::from_str("ipfs://").unwrap();
        mfs_root_from_uri(&uri).unwrap_err();
    }

    #[test]
    fn test_ipfs_storage_from_uri() {
        let storage_config = IpfsStorageConfig::default();
        let uri = Uri::from_str("ipfs://indexes/my-index").unwrap();
        let ipfs_storage = IpfsStorage::from_uri(&storage_config, &uri).unwrap();
        assert_eq!(ipfs_storage.uri(), &uri);
        assert_eq!(ipfs_storage.mfs_root, "/indexes/my-index");

        let s3_uri = Uri::from_str("s3://bucket/key").unwrap();
        let resolver_error = IpfsStorage::from_uri(&storage_config, &s3_uri).unwrap_err();
        assert!(matches!(
            resolver_error,
            StorageResolverError::InvalidUri(_)
        ));
    }

    #[test]
    fn test_mfs_path_forbids_parent_dir() {
        let storage_config = IpfsStorageConfig::default();
        let uri = Uri::from_str("ipfs://indexes/my-index").unwrap();
        let ipfs_storage = IpfsStorage::from_uri(&storage_config, &uri).unwrap();

        let mfs_path = ipfs_storage
            .mfs_path(Path::new("splits/split-1.split"))
            .unwrap();
        assert_eq!(mfs_path, "/indexes/my-index/splits/split-1.split");

        let path_error = ipfs_storage
            .mfs_path(Path::new("../escape.split"))
            .unwrap_err();
        assert_eq!(path_error.kind(), StorageErrorKind::Unauthorized);
    }
}
