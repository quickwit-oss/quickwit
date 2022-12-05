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

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::{fmt, io};

use anyhow::Context;
use quickwit_common::ignore_error_kind;
use tokio::fs;

static SCRATCH: &str = "scratch";

enum ScratchDirectoryType {
    Path(PathBuf),
    TempDir(tempfile::TempDir),
}

/// Helps creating a hierarchy of temp directory.
///
/// Upon drop the directory get automatically deleted.
/// The root of the tree may or may not be a directory that we do not delete,
/// depending on whether it was build using
/// `new_in_path` or `try_new_temp`.
///
/// Children keep a handle over their father, in order to extend naturally the
/// life of the directories.
#[derive(Clone)]
pub struct ScratchDirectory {
    inner: Arc<InnerScratchDirectory>,
}

impl fmt::Debug for ScratchDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("ScratchDirectory")
            .field("dir", &self.path())
            .finish()
    }
}

struct InnerScratchDirectory {
    // The goal of this handle to _parent is just to ensure that it does not get deleted before
    // its child.
    _parent: Option<Arc<InnerScratchDirectory>>,
    dir: ScratchDirectoryType,
}

impl ScratchDirectory {
    pub async fn create_in_dir<P: AsRef<Path>>(dir_path: P) -> anyhow::Result<Self> {
        let root_dir = dir_path.as_ref().to_path_buf();

        // Delete if exists and recreate scratch directory.
        let directory_path = root_dir.join(SCRATCH);
        fs::create_dir_all(&directory_path).await?;

        ignore_error_kind!(
            io::ErrorKind::NotFound,
            fs::remove_dir_all(&directory_path).await
        )
        .with_context(|| {
            format!(
                "Failed to empty scratch directory `{}`.",
                directory_path.display(),
            )
        })?;
        fs::create_dir(&directory_path).await.with_context(|| {
            format!(
                "Failed to create scratch directory `{}`. ",
                directory_path.display(),
            )
        })?;
        Ok(Self::new_in_dir(directory_path))
    }

    /// Creates a new ScratchDirectory in an existing directory.
    pub fn new_in_dir(dir_path: PathBuf) -> ScratchDirectory {
        let inner = InnerScratchDirectory {
            _parent: None,
            dir: ScratchDirectoryType::Path(dir_path),
        };
        ScratchDirectory {
            inner: Arc::new(inner),
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    /// Creates a new ScratchDirectory in an existing directory.
    /// The directory location will depend on the OS settings.
    pub fn for_test() -> ScratchDirectory {
        let tempdir = tempfile::tempdir().unwrap();
        let inner = InnerScratchDirectory {
            _parent: None,
            dir: ScratchDirectoryType::TempDir(tempdir),
        };
        ScratchDirectory {
            inner: Arc::new(inner),
        }
    }

    pub fn path(&self) -> &Path {
        match &self.inner.dir {
            ScratchDirectoryType::Path(path) => path,
            ScratchDirectoryType::TempDir(tempdir) => tempdir.path(),
        }
    }

    /// Creates a new child `ScratchDirectory`.
    ///
    /// A child scratch directory keeps an handle on its father to
    /// prevent its premature deletion.
    pub fn named_temp_child<S: AsRef<OsStr>>(&self, prefix: S) -> io::Result<Self> {
        let temp_dir = tempfile::Builder::new()
            .prefix(prefix.as_ref())
            .tempdir_in(self.path())?;
        let inner = InnerScratchDirectory {
            _parent: Some(self.inner.clone()),
            dir: ScratchDirectoryType::TempDir(temp_dir),
        };
        Ok(ScratchDirectory {
            inner: Arc::new(inner),
        })
    }

    pub fn downgrade(&self) -> WeakScratchDirectory {
        WeakScratchDirectory {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

/// A weak reference to an [`InnerScratchDirectory`].
pub struct WeakScratchDirectory {
    inner: Weak<InnerScratchDirectory>,
}

impl WeakScratchDirectory {
    pub fn upgrade(&self) -> Option<ScratchDirectory> {
        self.inner.upgrade().map(|inner| ScratchDirectory { inner })
    }
}
#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn test_scratch_directory() -> io::Result<()> {
        let parent = ScratchDirectory::for_test();
        let parent_path = parent.path().to_path_buf();

        let child = parent.named_temp_child("child-")?;
        let child_path = child.path().to_path_buf();
        assert!(child_path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .map(|file_name| file_name.contains("child-"))
            .unwrap_or(false));

        mem::drop(parent);
        assert!(parent_path.exists());
        assert!(child_path.exists());

        mem::drop(child);
        assert!(!parent_path.exists());
        assert!(!child_path.exists());
        Ok(())
    }

    #[test]
    fn test_scratch_directory_remove_content() -> io::Result<()> {
        let parent = ScratchDirectory::for_test();
        let parent_path = parent.path().to_path_buf();
        std::fs::write(parent.path().join("hello.txt"), b"hello")?;
        assert!(parent_path.exists());

        mem::drop(parent);
        assert!(!parent_path.exists());
        Ok(())
    }

    #[test]
    fn test_scratch_directory_in_path() -> io::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tempdir_path = tempdir.path().to_path_buf();
        assert!(tempdir_path.exists());

        let parent = ScratchDirectory::new_in_dir(tempdir_path.clone());
        assert_eq!(parent.path(), tempdir.path());
        assert!(tempdir.path().exists());

        let child = parent.named_temp_child("child-")?;
        let child_path = child.path().to_path_buf();
        assert!(child_path.exists());

        mem::drop(child);
        assert!(!child_path.exists());

        mem::drop(parent);
        assert!(!child_path.exists());

        mem::drop(tempdir);
        assert!(!tempdir_path.exists());
        Ok(())
    }
}
