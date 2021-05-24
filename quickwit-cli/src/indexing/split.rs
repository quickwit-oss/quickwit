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

use crate::indexing::{index, statistics::StatisticEvent};
use crate::{cli_command::IndexDataArgs, indexing::split_metadata::SplitMetaData};
use std::{path::PathBuf, usize};
use tantivy::{directory::MmapDirectory, merge_policy::NoMergePolicy, schema::Schema, Document};
use tokio::{fs, sync::mpsc::Sender, sync::RwLock};
use uuid::Uuid;

const MAX_DOC_PER_SPLIT: usize = 5_000_000;

pub struct Split {
    pub id: Uuid,
    pub metadata: SplitMetaData,
    pub local_directory: PathBuf,
    pub index_uri: String,
    pub index: tantivy::Index,
    pub index_writer: Option<tantivy::IndexWriter>,
}

impl Split {
    pub async fn create(args: &IndexDataArgs, schema: Schema) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let local_directory = args.temp_dir.join(format!("/{}", id));
        fs::create_dir(local_directory.as_path()).await?;

        let index = tantivy::Index::create_in_dir(local_directory.as_path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(args.num_threads, args.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let index_uri = args.index_uri.to_string_lossy().to_string();
        let metadata = SplitMetaData::new(id, index_uri.clone());
        Ok(Self {
            id,
            metadata,
            local_directory,
            index_uri,
            index,
            index_writer: Some(index_writer),
        })
    }

    pub fn add_document(&mut self, doc: Document) -> anyhow::Result<()> {
        //TODO handle time range
        self.metadata.num_records += 1;
        self.index_writer
            .as_ref()
            .ok_or(anyhow::anyhow!("Missing index writer."))?
            .add_document(doc);
        Ok(())
    }

    pub fn has_enough_docs(&self) -> bool {
        self.metadata.num_records >= MAX_DOC_PER_SPLIT
    }

    pub async fn commit(&mut self) -> anyhow::Result<u64> {
        let directory_path = self.local_directory.to_path_buf();
        let mut index_writer = self.index_writer.take().unwrap();

        let (moved_index_writer, commit_opstamp) = tokio::task::spawn_blocking(move || {
            let opstamp = index_writer.commit()?;
            let hotcache_path = directory_path.join("hotcache");
            let mut _hotcache_file = std::fs::File::create(&hotcache_path)?;
            let _mmap_directory = MmapDirectory::open(directory_path)?;
            //write_hotcache(mmap_directory, &mut hotcache_file)?;
            anyhow::Result::<(tantivy::IndexWriter, u64)>::Ok((index_writer, opstamp))
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))??;

        self.index_writer = Some(moved_index_writer);
        Ok(commit_opstamp)
    }

    pub async fn merge_all_segments(&mut self) -> anyhow::Result<tantivy::SegmentMeta> {
        let segment_ids = self.index.searchable_segment_ids()?;
        self.index_writer
            .as_mut()
            .ok_or(anyhow::anyhow!("Missing index writer."))?
            .merge(&segment_ids)
            .await
            .map_err(|e| e.into())
    }

    pub async fn stage(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO using metastore
        statistic_sender
            .send(StatisticEvent::SplitStage {
                id: self.id.to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }

    pub async fn upload(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO using metastore

        statistic_sender
            .send(StatisticEvent::SplitUpload {
                uri: "s3://todo".to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }

    pub async fn publish(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        //TODO using metastore

        statistic_sender
            .send(StatisticEvent::SplitPublish {
                uri: "s3://todo".to_string(),
                error: false,
            })
            .await?;
        Ok(())
    }
}
