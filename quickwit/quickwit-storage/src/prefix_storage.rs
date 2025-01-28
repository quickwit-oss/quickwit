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

use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, OwnedBytes, Storage};

/// This storage acts as a proxy to another storage that simply modifies each API call
/// by preceding each path with a given a prefix.
struct PrefixStorage {
    pub storage: Arc<dyn Storage>,
    pub prefix: PathBuf,
    uri: Uri,
}

impl fmt::Debug for PrefixStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixStorage")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[async_trait]
impl Storage for PrefixStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        self.storage.put(&self.prefix.join(path), payload).await
    }

    async fn copy_to(
        &self,
        path: &Path,
        output: &mut dyn SendableAsync,
    ) -> crate::StorageResult<()> {
        self.storage.copy_to(&self.prefix.join(path), output).await
    }

    async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<OwnedBytes> {
        self.storage.get_slice(&self.prefix.join(path), range).await
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<OwnedBytes> {
        self.storage.get_all(&self.prefix.join(path)).await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.storage
            .get_slice_stream(&self.prefix.join(path), range)
            .await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.storage.delete(&self.prefix.join(path)).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let prefixed_pathbufs: Vec<PathBuf> =
            paths.iter().map(|path| self.prefix.join(path)).collect();
        let prefixed_paths: Vec<&Path> = prefixed_pathbufs
            .iter()
            .map(|pathbuf| pathbuf.as_path())
            .collect();
        self.storage
            .bulk_delete(&prefixed_paths)
            .await
            .map_err(|error| strip_prefix_from_error(error, &self.prefix))?;
        Ok(())
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        self.storage.exists(&self.prefix.join(path)).await
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> crate::StorageResult<u64> {
        self.storage.file_num_bytes(&self.prefix.join(path)).await
    }
}

/// Creates a [`PrefixStorage`] using an underlying storage and a prefix.
pub(crate) fn add_prefix_to_storage(
    storage: Arc<dyn Storage>,
    prefix: PathBuf,
    uri: Uri,
) -> Arc<dyn Storage> {
    Arc::new(PrefixStorage {
        storage,
        prefix,
        uri,
    })
}

fn strip_prefix_from_error(error: BulkDeleteError, prefix: &Path) -> BulkDeleteError {
    if prefix == Path::new("") {
        return error;
    }
    let successes = error
        .successes
        .into_iter()
        .map(|path| {
            path.strip_prefix(prefix)
                .expect(
                    "The prefix should have been prepended to the path before the bulk delete \
                     call.",
                )
                .to_path_buf()
        })
        .collect();
    let failures = error
        .failures
        .into_iter()
        .map(|(path, failure)| {
            (
                path.strip_prefix(prefix)
                    .expect(
                        "The prefix should have been prepended to the path before the bulk delete \
                         call.",
                    )
                    .to_path_buf(),
                failure,
            )
        })
        .collect();
    let unattempted = error
        .unattempted
        .into_iter()
        .map(|path| {
            path.strip_prefix(prefix)
                .expect(
                    "The prefix should have been prepended to the path before the bulk delete \
                     call.",
                )
                .to_path_buf()
        })
        .collect();
    BulkDeleteError {
        error: error.error,
        successes,
        failures,
        unattempted,
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::DeleteFailure;

    #[test]
    fn test_strip_prefix_from_error() {
        {
            let error = BulkDeleteError {
                error: None,
                successes: vec![PathBuf::from("ram:///indexes/foo")],
                unattempted: vec![PathBuf::from("ram:///indexes/bar")],
                failures: HashMap::from_iter([(
                    PathBuf::from("ram:///indexes/baz"),
                    DeleteFailure::default(),
                )]),
            };
            let stripped_error = strip_prefix_from_error(error, Path::new(""));

            assert_eq!(
                stripped_error.successes,
                vec![PathBuf::from("ram:///indexes/foo")],
            );
            assert_eq!(
                stripped_error.unattempted,
                vec![PathBuf::from("ram:///indexes/bar")],
            );
            assert_eq!(
                stripped_error.failures.keys().next().unwrap(),
                &PathBuf::from("ram:///indexes/baz"),
            );
        }
        {
            let error = BulkDeleteError {
                error: None,
                successes: vec![PathBuf::from("ram:///indexes/foo")],
                unattempted: vec![PathBuf::from("ram:///indexes/bar")],
                failures: HashMap::from_iter([(
                    PathBuf::from("ram:///indexes/baz"),
                    DeleteFailure::default(),
                )]),
            };
            let stripped_error = strip_prefix_from_error(error, Path::new("ram:///indexes"));

            assert_eq!(stripped_error.successes, vec![PathBuf::from("foo")],);
            assert_eq!(stripped_error.unattempted, vec![PathBuf::from("bar")],);
            assert_eq!(
                stripped_error.failures.keys().next().unwrap(),
                &PathBuf::from("baz"),
            );
        }
        {
            let error = BulkDeleteError {
                error: None,
                successes: vec![PathBuf::from("ram:///indexes/foo")],
                unattempted: vec![PathBuf::from("ram:///indexes/bar")],
                failures: HashMap::from_iter([(
                    PathBuf::from("ram:///indexes/baz"),
                    DeleteFailure::default(),
                )]),
            };
            let stripped_error = strip_prefix_from_error(error, Path::new("ram:///indexes/"));

            assert_eq!(stripped_error.successes, vec![PathBuf::from("foo")],);
            assert_eq!(stripped_error.unattempted, vec![PathBuf::from("bar")],);
            assert_eq!(
                stripped_error.failures.keys().next().unwrap(),
                &PathBuf::from("baz"),
            );
        }
    }
}
