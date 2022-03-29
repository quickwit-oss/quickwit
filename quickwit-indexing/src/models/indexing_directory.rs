// Copyright (C) 2021 Quickwit, Inc.
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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use quickwit_common::fs::empty_dir;
use tempfile::TempDir;
use tokio::fs;

use super::ScratchDirectory;

pub const CACHE: &str = "cache";

/// Root of an [`IndexingDirectory`].
#[derive(Clone)]
enum Root {
    /// The root of the indexing directory.
    Dir(PathBuf),
    // A temporary directory created for the duration of a test.
    TempDir(Arc<TempDir>),
}

/// An indexing directory is created in the data directory on the local file system for each index
/// at the following location: `<data dir>/<index name>`.
/// The indexing directory consists of two directories:
/// - a scratch directory that stores temporary intermediate files
/// - a cache directory that stores frequently accessed data structures
/// While the scratch directory is emptied upon restart, the cache directory is not, and it is
/// the responsability of the users of this folder to properly manage the lifecycle of the data
/// that they write to it.
#[derive(Clone)]
pub struct IndexingDirectory {
    root: Root,
    pub cache_directory: PathBuf,
    pub scratch_directory: ScratchDirectory,
}

impl IndexingDirectory {
    pub async fn create_in_dir<P: AsRef<Path>>(dir_path: P) -> anyhow::Result<IndexingDirectory> {
        // Create cache directory if does not exist.
        let cache_directory_path = dir_path.as_ref().join(CACHE);
        fs::create_dir_all(&cache_directory_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to create cache directory `{}`. ",
                    cache_directory_path.display(),
                )
            })?;
        // Create scratch directory if does not exist.
        let scratch_directory_path = dir_path.as_ref().join("scratch");
        fs::create_dir_all(&scratch_directory_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to create scratch directory `{}`. ",
                    scratch_directory_path.display(),
                )
            })?;
        // Empty scratch directory if already exists.
        empty_dir(&scratch_directory_path).await.with_context(|| {
            format!(
                "Failed to empty scratch directory `{}`.",
                scratch_directory_path.display(),
            )
        })?;
        let scratch_directory = ScratchDirectory::new_in_dir(scratch_directory_path);

        let indexing_directory = Self {
            root: Root::Dir(dir_path.as_ref().to_path_buf()),
            cache_directory: cache_directory_path,
            scratch_directory,
        };
        Ok(indexing_directory)
    }

    pub fn path(&self) -> &Path {
        match &self.root {
            Root::Dir(root) => root,
            Root::TempDir(tempdir) => tempdir.path(),
        }
    }

    pub async fn for_test() -> anyhow::Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let mut indexing_directory = IndexingDirectory::create_in_dir(tempdir.path()).await?;
        indexing_directory.root = Root::TempDir(Arc::new(tempdir));
        Ok(indexing_directory)
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

        let cache_directory_path = indexing_directory_path.join("cache");
        assert!(cache_directory_path.exists());
        assert_eq!(indexing_directory.cache_directory, cache_directory_path);

        let scratch_directory_path = indexing_directory_path.join("scratch");
        assert!(scratch_directory_path.exists());
        assert_eq!(
            indexing_directory.scratch_directory.path(),
            scratch_directory_path
        );
        {
            let scratch_file_path = scratch_directory_path.join("file");
            tokio::fs::File::create(&scratch_file_path).await?;
            assert!(scratch_file_path.exists());
            let _indexing_directory = IndexingDirectory::create_in_dir(tempdir.path()).await?;
            assert!(!scratch_file_path.exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_directory_for_test() -> anyhow::Result<()> {
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_directory_path = indexing_directory.path().to_path_buf();
        drop(indexing_directory);
        assert!(!indexing_directory_path.exists());
        Ok(())
    }
}
