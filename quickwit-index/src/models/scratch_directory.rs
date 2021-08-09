// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::fmt;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
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
    inner: Arc<Inner>,
}

impl fmt::Debug for ScratchDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScratchDirectory")
            .field("dir", &self.path())
            .finish()
    }
}

pub struct Inner {
    // The goal of this handle to _parent is just to ensure that it does not get deleted before
    // its child.
    _parent: Option<Arc<Inner>>,
    dir: ScratchDirectoryType,
}

impl ScratchDirectory {
    /// Creates a new ScratchDirectory in an existing directory.
    pub fn new_in_path(path: PathBuf) -> ScratchDirectory {
        let inner = Inner {
            _parent: None,
            dir: ScratchDirectoryType::Path(path),
        };
        ScratchDirectory {
            inner: Arc::new(inner),
        }
    }

    /// Creates a new ScratchDirectory in an existing directory.
    /// The directory location will depend on the OS settings.
    pub fn try_new_temp() -> io::Result<ScratchDirectory> {
        let temp_dir = tempfile::tempdir()?;
        let inner = Inner {
            _parent: None,
            dir: ScratchDirectoryType::TempDir(temp_dir),
        };
        Ok(ScratchDirectory {
            inner: Arc::new(inner),
        })
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
    pub fn temp_child(&self) -> io::Result<Self> {
        let temp_dir = tempfile::tempdir_in(self.path())?;
        let inner = Inner {
            _parent: Some(self.inner.clone()),
            dir: ScratchDirectoryType::TempDir(temp_dir),
        };
        Ok(ScratchDirectory {
            inner: Arc::new(inner),
        })
    }
}

// TODO add unit tests
