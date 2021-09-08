/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]

/*! `quickwit-core` provides all the core functions used in quickwit cli:
- `create_index` for creating a new index
- `index_data` for indexing new-line delimited json documents
- `search_index` for searching an index
- `delete_index` for deleting an index
*/

mod index;

pub use index::{create_index, delete_index, garbage_collect_index, reset_index};
use quickwit_metastore::SplitMetadataAndFooterOffsets;

#[allow(missing_docs)]
#[derive(Debug)]
pub struct FileEntry {
    /// The file_name is a file name, within an index directory.
    pub file_name: String,
    /// File size in bytes.
    pub file_size_in_bytes: u64, //< TODO switch to `byte_unit::Byte`.
}

impl From<&SplitMetadataAndFooterOffsets> for FileEntry {
    fn from(split: &SplitMetadataAndFooterOffsets) -> Self {
        FileEntry {
            file_name: quickwit_common::split_file(&split.split_metadata.split_id),
            file_size_in_bytes: split.footer_offsets.end,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_indexing::TestSandbox;

    use crate::FileEntry;

    #[tokio::test]
    async fn test_file_entry_from_split() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-index";
        let test_sandbox =
            TestSandbox::create(index_id, Arc::new(WikipediaIndexConfig::new())).await?;
        test_sandbox.add_documents(vec![
            serde_json::json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
        ]).await?;
        let splits = test_sandbox.metastore().list_all_splits(index_id).await?;
        let file_entries: Vec<FileEntry> = splits.iter().map(FileEntry::from).collect();
        assert_eq!(file_entries.len(), 1);
        let index_meta = test_sandbox.metastore().index_metadata(index_id).await?;
        let storage = test_sandbox
            .storage_uri_resolver()
            .resolve(&index_meta.index_uri)?;
        for file_entry in file_entries {
            let split_num_bytes = storage
                .file_num_bytes(Path::new(file_entry.file_name.as_str()))
                .await?;
            assert_eq!(split_num_bytes, file_entry.file_size_in_bytes);
        }
        Ok(())
    }
}
