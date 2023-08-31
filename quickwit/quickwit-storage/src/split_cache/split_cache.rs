use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tantivy::directory::OwnedBytes;

use crate::StorageCache;
use crate::split_cache::split_state_table;
use crate::split_cache::split_state_table::SplitStateTable;


pub struct SplitCache {
    root_path: PathBuf,
    split_state_table: SplitStateTable,
}

struct LockFile {
    path: PathBuf,
    lock: Arc<()>,
}


impl SplitCache {
    pub fn new(root_path: PathBuf, split_state_table: SplitStateTable) -> SplitCache {
        SplitCache {
            root_path,
            split_state_table,
        }
    }
}

impl SplitCache {
    fn is_split_in_cache(&self, split_id: &Ulid) -> bool {
        self.split_state_table.is_downloaded(split_id)
    }

    async fn acknowledge_split(split_uri: &Uri) {
        todo!()
    }
}


#[async_trait]
impl StorageCache for SplitCache {

    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {

        todo!()

    }
    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        todo!()
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        todo!()
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        todo!()
    }
}
