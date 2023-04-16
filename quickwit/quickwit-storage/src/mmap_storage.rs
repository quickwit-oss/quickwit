// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};

use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use stable_deref_trait::StableDeref;
use std::fs::{self, File, OpenOptions};
use memmap2::Mmap;

pub type ArcBytes = Arc<dyn Deref<Target = [u8]> + Send + Sync + 'static>;
pub type WeakArcBytes = Weak<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

/// Create a default io error given a string.
pub(crate) fn make_io_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

/// Returns `None` iff the file exists, can be read, but is empty (and hence
/// cannot be mmapped)
fn open_mmap(full_path: &Path) -> Option<Mmap> {
    let file = File::open(full_path).expect("File must be there");

    let meta_data = file
        .metadata()
        .expect("File metadata must be there");
    if meta_data.len() == 0 {
        // if the file size is 0, it will not be possible
        // to mmap the file, so we return None
        // instead.
        return None;
    }
    unsafe {
        memmap2::Mmap::map(&file)
            .map(Some)
            .expect("Mmap must be possible")
    }
}



#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct CacheCounters {
    /// Number of time the cache prevents to call `mmap`
    pub hit: usize,
    /// Number of time tantivy had to call `mmap`
    /// as no entry was in the cache.
    pub miss: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheInfo {
    pub counters: CacheCounters,
    pub mmapped: Vec<PathBuf>,
}

#[derive(Default)]
pub(crate) struct MmapCache {
    counters: CacheCounters,
    cache: HashMap<PathBuf, WeakArcBytes>,
}

impl MmapCache {
    fn get_info(&self) -> CacheInfo {
        let paths: Vec<PathBuf> = self.cache.keys().cloned().collect();
        CacheInfo {
            counters: self.counters.clone(),
            mmapped: paths,
        }
    }

    fn remove_weak_ref(&mut self) {
        let keys_to_remove: Vec<PathBuf> = self
            .cache
            .iter()
            .filter(|(_, mmap_weakref)| mmap_weakref.upgrade().is_none())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys_to_remove {
            self.cache.remove(&key);
        }
    }

    // Returns None if the file exists but as a len of 0 (and hence is not mmappable).
    pub(crate) fn get_mmap(&mut self, full_path: &Path) -> Option<ArcBytes> {
        if let Some(mmap_weak) = self.cache.get(full_path) {
            if let Some(mmap_arc) = mmap_weak.upgrade() {
                self.counters.hit += 1;
                return Some(mmap_arc);
            }
        }
        self.cache.remove(full_path);
        self.counters.miss += 1;
        let mmap_opt = open_mmap(full_path);
        mmap_opt.map(|mmap| {
            let mmap_arc: ArcBytes = Arc::new(mmap);
            let mmap_weak = Arc::downgrade(&mmap_arc);
            self.cache.insert(full_path.to_owned(), mmap_weak);
            mmap_arc
        })
    }
}

#[derive(Clone)]
pub(crate) struct MmapArc(pub(crate) Arc<dyn Deref<Target = [u8]> + Send + Sync>);

impl Deref for MmapArc {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for MmapArc {}
