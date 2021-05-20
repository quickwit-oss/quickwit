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

use crate::cli_command::IndexDataArgs;
use std::{path::PathBuf, usize};
use tantivy::{merge_policy::NoMergePolicy, schema::Schema, Document};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::{self, BufReader};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use uuid::Uuid;

const MAX_DOC_PER_SPLIT: usize = 5_000_000;

struct DocumentRetriever {}

impl DocumentRetriever {
    async fn retrieve_documents(
        input_uri: Option<PathBuf>,
        document_sender: Sender<String>,
    ) -> anyhow::Result<()> {
        match input_uri {
            Some(path_buf) => {
                let file = File::open(path_buf).await?;
                let reader = BufReader::new(file);
                let mut lines = reader.lines();
                while let Some(line) = lines.next_line().await? {
                    document_sender.send(line).await?;
                }
            }
            None => {
                let file = io::stdin();
                let reader = BufReader::new(file);
                let mut lines = reader.lines();
                while let Some(line) = lines.next_line().await? {
                    document_sender.send(line).await?;
                }
            }
        };
        Ok(())
    }
}

struct LocalSplit {
    pub id: Uuid,
    pub local_directory: PathBuf,
    pub index: tantivy::Index,
    pub index_writer: tantivy::IndexWriter,
    pub num_docs: usize,
}

impl LocalSplit {
    pub async fn create(args: IndexDataArgs, schema: Schema) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let local_directory = args.temp_dir.join(format!("/{}", id));
        fs::create_dir(local_directory.as_path()).await?;

        let index = tantivy::Index::create_in_dir(local_directory.as_path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(args.num_threads, args.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        Ok(Self {
            id,
            local_directory,
            index,
            index_writer,
            num_docs: 0,
        })
    }

    pub fn add_document(&mut self, doc: Document) -> anyhow::Result<()> {
        self.num_docs += 1;
        self.index_writer.add_document(doc);
        Ok(())
    }

    pub fn has_enough_docs(&self) -> bool {
        self.num_docs >= MAX_DOC_PER_SPLIT
    }

    pub async fn merge_all_segements(&mut self) -> anyhow::Result<tantivy::SegmentMeta> {
        let segment_ids = self.index.searchable_segment_ids()?;
        self.index_writer
            .merge(&segment_ids)
            .await
            .map_err(|e| e.into())
    }
}

struct DocumentIndexer {
    pub params: IndexDataArgs,
    pub document_receiver: Receiver<String>,
    pub splits: Vec<LocalSplit>,
}

impl DocumentIndexer {
    pub async fn index_documents(
        args: IndexDataArgs,
        mut document_receiver: Receiver<String>,
    ) -> anyhow::Result<Vec<LocalSplit>> {
        //TODO get schema from docBuilder
        let schema = Schema::builder().build();

        let mut split_merge_tasks = vec![];

        let mut current_split = LocalSplit::create(args.clone(), schema.clone()).await?;
        while let Some(raw_doc) = document_receiver.recv().await {
            //TODO num_read_docs++;
            let parse_result = DocumentIndexer::parse_document(raw_doc);
            let doc = match parse_result {
                Ok(doc) => doc,
                Err(_) => {
                    //TODO num_parse_errors++;
                    continue;
                }
            };

            // add document
            current_split.add_document(doc)?;
            if current_split.has_enough_docs() {
                let split = std::mem::replace(
                    &mut current_split,
                    LocalSplit::create(args.clone(), schema.clone()).await?,
                );
                split_merge_tasks.push(task::spawn(async move {
                    DocumentIndexer::merge_segements(split).await
                }));
            }
        }

        let merge_results = futures::future::try_join_all(split_merge_tasks).await?;
        // let (splits, errors) = merge_results.iter().partition(|item| item.is_ok());

        let splits = merge_results
            .into_iter()
            .filter(|result| result.is_ok())
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        Ok(splits)
    }

    pub fn parse_document(_raw_doc: String) -> anyhow::Result<Document> {
        //TODO convert raw json to doc
        Ok(Document::default())
    }

    pub async fn merge_segements(mut split: LocalSplit) -> anyhow::Result<LocalSplit> {
        split.merge_all_segements().await?;
        Ok(split)
    }
}

pub async fn reset_index(_args: IndexDataArgs) -> anyhow::Result<()> {
    //TODO: list splits via metastore

    //TODO: delete all splits via metastore

    Ok(())
}

pub async fn index_data(args: IndexDataArgs) -> anyhow::Result<()> {
    if args.overwrite {
        reset_index(args.clone()).await?;
    }

    if args.input_uri.is_none() {
        println!("Please enter your new line delimited json documents.");
    }

    let (sender, receiver) = channel::<String>(1000);
    DocumentRetriever::retrieve_documents(args.input_uri.clone(), sender).await?;
    let splits = DocumentIndexer::index_documents(args.clone(), receiver).await?;
    SplitUploader::upload(args, splits).await?;

    //TODO  display report

    Ok(())
}

struct SplitUploader {}

impl SplitUploader {
    pub async fn upload(_args: IndexDataArgs, _splits: Vec<LocalSplit>) -> anyhow::Result<()> {
        //TODO upload all split concurrently
        Ok(())
    }
}
