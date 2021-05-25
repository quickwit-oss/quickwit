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

use std::fmt;

use crate::indexing::statistics::StatisticEvent;
use quickwit_hot_directory::write_hotcache;
use quickwit_metastore::SplitMetadata;
use std::{path::PathBuf, usize};
use tantivy::{directory::MmapDirectory, merge_policy::NoMergePolicy, schema::Schema, Document};
use tokio::{fs, sync::mpsc::Sender};
use uuid::Uuid;

use super::IndexDataParams;

const MAX_DOC_PER_SPLIT: usize = 5_000_000;

/// Struct that represents an instance of split
pub struct Split {
    pub id: Uuid,
    pub metadata: SplitMetadata,
    pub local_directory: PathBuf,
    pub index_uri: String,
    pub index: tantivy::Index,
    pub index_writer: Option<tantivy::IndexWriter>,
    pub num_parsing_errors: usize,
}

impl fmt::Debug for Split {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Split")
            .field("id", &self.id)
            .field("metadata", &self.metadata)
            .field("local_directory", &self.local_directory)
            .field("index_uri", &self.index_uri)
            .field("num_parsing_errors", &self.num_parsing_errors)
            .finish()
    }
}

impl Split {
    /// Create a new instance of an index split.
    pub async fn create(params: &IndexDataParams, schema: Schema) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let local_directory = params.temp_dir.join(format!("{}", id));
        fs::create_dir(local_directory.as_path()).await?;
        let index = tantivy::Index::create_in_dir(local_directory.as_path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(params.num_threads, params.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let index_uri = params.index_uri.to_string_lossy().to_string();
        let metadata = SplitMetadata::new(id.to_string());
        Ok(Self {
            id,
            metadata,
            local_directory,
            index_uri,
            index,
            index_writer: Some(index_writer),
            num_parsing_errors: 0,
        })
    }

    /// Add document to the index split.
    pub fn add_document(&mut self, doc: Document) -> anyhow::Result<()> {
        //TODO: handle time range
        self.metadata.num_records += 1;
        self.index_writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .add_document(doc);
        Ok(())
    }

    /// Checks to see if the split has enough documents.
    pub fn has_enough_docs(&self) -> bool {
        self.metadata.num_records >= MAX_DOC_PER_SPLIT
    }

    /// Commits the split.
    pub async fn commit(&mut self) -> anyhow::Result<u64> {
        let directory_path = self.local_directory.to_path_buf();
        let mut index_writer = self.index_writer.take().unwrap();

        let (moved_index_writer, commit_opstamp) = tokio::task::spawn_blocking(move || {
            let opstamp = index_writer.commit()?;
            let hotcache_path = directory_path.join("hotcache");
            let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
            let mmap_directory = MmapDirectory::open(directory_path)?;
            write_hotcache(mmap_directory, &mut hotcache_file)?;
            anyhow::Result::<(tantivy::IndexWriter, u64)>::Ok((index_writer, opstamp))
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))??;

        self.index_writer = Some(moved_index_writer);
        Ok(commit_opstamp)
    }

    /// Merge all segments of the split into one.
    pub async fn merge_all_segments(&mut self) -> anyhow::Result<tantivy::SegmentMeta> {
        let segment_ids = self.index.searchable_segment_ids()?;
        self.index_writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .merge(&segment_ids)
            .await
            .map_err(|e| e.into())
    }

    /// Stage a split in the metastore.
    pub async fn stage(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO: using metastore
        statistic_sender
            .send(StatisticEvent::SplitStage {
                id: self.id.to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }

    /// Upload all split artifacts using the storage.
    pub async fn upload(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO: using storage

        statistic_sender
            .send(StatisticEvent::SplitUpload {
                uri: self.id.to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }

    /// Publish the split in the metastore.
    pub async fn publish(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO: using metastore

        statistic_sender
            .send(StatisticEvent::SplitPublish {
                uri: self.id.to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }
}
