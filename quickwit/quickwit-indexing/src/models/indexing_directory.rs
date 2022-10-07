// Copyright (C) 2022 Quickwit, Inc.
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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use anyhow::Context;
use quickwit_common::ignore_io_error;
use tokio::fs;

use super::ScratchDirectory;

const SCRATCH: &str = "scratch";

/// Root of an [`IndexingDirectory`].
enum Root {
    /// The root of the indexing directory.
    Dir(PathBuf),
    // A temporary directory created for the duration of a test.
    #[cfg(any(test, feature = "testsuite"))]
    TempDir(tempfile::TempDir),
}

/// An indexing directory is created in the data directory on the local file system for each index
/// at the following location: `<data dir>/indexing/<index ID>/<source ID>`.
/// The indexing directory consists of two directories:
/// - a scratch directory that stores temporary intermediate files
/// - a cache directory that stores frequently accessed data structures
/// While the scratch directory is emptied upon restart, the cache directory is not, and it is
/// the responsability of the users of this folder to properly manage the lifecycle of the data
/// that they write to it.
#[derive(Clone)]
pub struct IndexingDirectory {
    inner: Arc<InnerIndexingDirectory>,
}

struct InnerIndexingDirectory {
    root: Root,
    pub scratch_directory: ScratchDirectory,
}

/// A weak reference to an [`IndexingDirectory`].
pub struct WeakIndexingDirectory {
    inner: Weak<InnerIndexingDirectory>,
}

impl WeakIndexingDirectory {
    pub fn upgrade(&self) -> Option<IndexingDirectory> {
        self.inner
            .upgrade()
            .map(|inner| IndexingDirectory { inner })
    }
}

impl IndexingDirectory {
    pub async fn create_in_dir<P: AsRef<Path>>(dir_path: P) -> anyhow::Result<IndexingDirectory> {
        let root_dir = dir_path.as_ref().to_path_buf();

        // Delete if exists and recreate scratch directory.
        let scratch_directory_path = root_dir.join(SCRATCH);
        fs::create_dir_all(&scratch_directory_path).await?;

        ignore_io_error!(
            io::ErrorKind::NotFound,
            fs::remove_dir_all(&scratch_directory_path).await
        )
        .with_context(|| {
            format!(
                "Failed to empty scratch directory `{}`.",
                scratch_directory_path.display(),
            )
        })?;
        fs::create_dir(&scratch_directory_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to create scratch directory `{}`. ",
                    scratch_directory_path.display(),
                )
            })?;
        let scratch_directory = ScratchDirectory::new_in_dir(scratch_directory_path);
        let inner = InnerIndexingDirectory {
            root: Root::Dir(root_dir),
            scratch_directory,
        };
        let indexing_directory = IndexingDirectory {
            inner: Arc::new(inner),
        };
        Ok(indexing_directory)
    }

    pub fn scratch_directory(&self) -> &ScratchDirectory {
        &self.inner.scratch_directory
    }

    pub fn path(&self) -> &Path {
        match &self.inner.root {
            Root::Dir(root) => root,
            #[cfg(any(test, feature = "testsuite"))]
            Root::TempDir(tempdir) => tempdir.path(),
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn for_test() -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let scratch_directory_path = tempdir.path().join(SCRATCH);
        fs::create_dir_all(&scratch_directory_path).await.unwrap();

        let scratch_directory = ScratchDirectory::new_in_dir(scratch_directory_path);

        let inner = InnerIndexingDirectory {
            root: Root::TempDir(tempdir),
            scratch_directory,
        };
        IndexingDirectory {
            inner: Arc::new(inner),
        }
    }

    pub fn downgrade(&self) -> WeakIndexingDirectory {
        WeakIndexingDirectory {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_indexing_directory() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let indexing_directory = IndexingDirectory::create_in_dir(tempdir.path()).await?;
        let indexing_directory_path = indexing_directory.path().to_path_buf();
        assert_eq!(indexing_directory_path, tempdir.path());

        let scratch_directory_path = indexing_directory_path.join("scratch");
        assert!(scratch_directory_path.exists());
        assert_eq!(
            indexing_directory.scratch_directory().path(),
            scratch_directory_path
        );
        let scratch_file_path = scratch_directory_path.join("file");
        tokio::fs::File::create(&scratch_file_path).await?;
        assert!(scratch_file_path.exists());
        let _indexing_directory = IndexingDirectory::create_in_dir(tempdir.path()).await?;
        assert!(!scratch_file_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_directory_for_test() -> anyhow::Result<()> {
        let indexing_directory = IndexingDirectory::for_test().await;
        let indexing_directory_path = indexing_directory.path().to_path_buf();
        drop(indexing_directory);
        assert!(!indexing_directory_path.exists());
        Ok(())
    }
}
