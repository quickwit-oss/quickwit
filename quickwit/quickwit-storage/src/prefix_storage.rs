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

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, ListObjectsStream, ObjectMetadata, OwnedBytes, Storage, StorageErrorKind,
    StorageGetSlice, StorageResult,
};

/// This storage acts as a proxy to another storage that simply modifies each API call
/// by preceding each path with a given a prefix.
#[derive(Clone)]
pub(crate) struct PrefixStorage<T> {
    pub storage: T,
    pub prefix: PathBuf,
    uri: Uri,
}

impl<T> fmt::Debug for PrefixStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixStorage")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[async_trait]
impl<T: StorageGetSlice> Storage for PrefixStorage<T> {
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
        self.get_slice_unboxed(path, range).await
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

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        /// Makes listed paths relative to this storage root again, undoing the prefix added by
        /// [`PrefixStorage::list`].
        fn strip_prefix_from_objects(
            objects: Vec<ObjectMetadata>,
            prefix: &Path,
        ) -> StorageResult<Vec<ObjectMetadata>> {
            if prefix == Path::new("") {
                return Ok(objects);
            }
            let prefix_bytes = prefix.as_os_str().as_encoded_bytes();
            let mut relative_objects = Vec::with_capacity(objects.len());
            for mut object in objects {
                match object.path.strip_prefix(prefix) {
                    Ok(relative_path) => {
                        object.path = relative_path.to_path_buf();
                        relative_objects.push(object);
                    }
                    Err(error) => {
                        let is_under_prefix = object
                            .path
                            .as_os_str()
                            .as_encoded_bytes()
                            .starts_with(prefix_bytes);
                        if !is_under_prefix {
                            return Err(StorageErrorKind::Internal.with_error(anyhow::anyhow!(
                                "listed object `{}` is not under storage prefix `{}`: {error}",
                                object.path.display(),
                                prefix.display()
                            )));
                        }
                    }
                }
            }
            Ok(relative_objects)
        }

        let storage_prefix = self.prefix.clone();
        self.storage
            .list(&self.prefix.join(prefix))
            .map(move |objects_res| {
                let objects = objects_res?;
                strip_prefix_from_objects(objects, &storage_prefix)
            })
            .boxed()
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

impl<T: StorageGetSlice> StorageGetSlice for PrefixStorage<T> {
    async fn get_slice_unboxed(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        let prefixed_path = self.prefix.join(path);
        self.storage.get_slice_unboxed(&prefixed_path, range).await
    }
}

/// Creates a [`PrefixStorage`] using an underlying storage and a prefix.
pub(crate) fn add_prefix_to_storage<T: StorageGetSlice>(
    storage: T,
    prefix: PathBuf,
    uri: Uri,
) -> PrefixStorage<T> {
    PrefixStorage {
        storage,
        prefix,
        uri,
    }
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
    use std::sync::Arc;
    use std::time::SystemTime;

    use futures::{TryStreamExt, stream};

    use super::*;
    use crate::{DeleteFailure, MockStorage};

    #[tokio::test]
    async fn test_prefix_storage_list() {
        let mut mock_storage = MockStorage::default();
        mock_storage.expect_list().times(1).returning(|prefix| {
            assert_eq!(prefix, Path::new("ram:///indexes/splits"));
            let objects = vec![ObjectMetadata {
                path: PathBuf::from("ram:///indexes/splits/foo.split"),
                size: bytesize::ByteSize(11),
                last_modified: SystemTime::UNIX_EPOCH,
            }];
            stream::once(async move { Ok(objects) }).boxed()
        });
        let storage = add_prefix_to_storage(
            Arc::new(mock_storage),
            PathBuf::from("ram:///indexes"),
            Uri::for_test("ram:///indexes"),
        );
        let pages: Vec<Vec<ObjectMetadata>> = storage
            .list(Path::new("splits"))
            .try_collect()
            .await
            .unwrap();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].len(), 1);
        assert_eq!(pages[0][0].path, Path::new("splits/foo.split"));
        assert_eq!(pages[0][0].size, bytesize::ByteSize(11));
    }

    #[tokio::test]
    async fn test_prefix_storage_list_filters_lexical_siblings() {
        let mut mock_storage = MockStorage::default();
        mock_storage.expect_list().times(1).returning(|prefix| {
            assert_eq!(prefix, Path::new("ram:///indexes"));
            let objects = vec![
                ObjectMetadata {
                    path: PathBuf::from("ram:///indexes/foo.split"),
                    size: bytesize::ByteSize(11),
                    last_modified: SystemTime::UNIX_EPOCH,
                },
                ObjectMetadata {
                    path: PathBuf::from("ram:///indexes-old/unrelated.split"),
                    size: bytesize::ByteSize(13),
                    last_modified: SystemTime::UNIX_EPOCH,
                },
            ];
            stream::once(async move { Ok(objects) }).boxed()
        });
        let storage = add_prefix_to_storage(
            Arc::new(mock_storage),
            PathBuf::from("ram:///indexes"),
            Uri::for_test("ram:///indexes"),
        );
        let pages: Vec<Vec<ObjectMetadata>> =
            storage.list(Path::new("")).try_collect().await.unwrap();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].len(), 1);
        assert_eq!(pages[0][0].path, Path::new("foo.split"));
    }

    #[tokio::test]
    async fn test_prefix_storage_list_rejects_unrelated_paths() {
        let mut mock_storage = MockStorage::default();
        mock_storage.expect_list().times(1).returning(|prefix| {
            assert_eq!(prefix, Path::new("ram:///indexes"));
            let objects = vec![ObjectMetadata {
                path: PathBuf::from("ram:///unrelated/foo.split"),
                size: bytesize::ByteSize(11),
                last_modified: SystemTime::UNIX_EPOCH,
            }];
            stream::once(async move { Ok(objects) }).boxed()
        });
        let storage = add_prefix_to_storage(
            Arc::new(mock_storage),
            PathBuf::from("ram:///indexes"),
            Uri::for_test("ram:///indexes"),
        );
        let error = storage
            .list(Path::new(""))
            .try_collect::<Vec<Vec<ObjectMetadata>>>()
            .await
            .unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::Internal);
    }

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
