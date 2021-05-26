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

use quickwit_storage::StorageUriResolver;
use std::sync::Arc;
use tantivy::{schema::Schema, Document};
use tokio::sync::mpsc::Sender;

use crate::indexing::document_retriever::DocumentRetriever;
use crate::indexing::split::Split;
use crate::indexing::statistics::StatisticEvent;

use super::IndexDataParams;

/// Receives json documents, parses and adds them to a `tantivy::Index`
pub async fn index_documents(
    params: &IndexDataParams,
    mut document_retriever: Box<dyn DocumentRetriever>,
    split_sender: Sender<Split>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<()> {
    //TODO replace with  DocMapper::schema()
    let schema = Schema::builder().build();
    let storage_resolver = Arc::new(StorageUriResolver::default());

    let mut current_split =
        Split::create(&params, storage_resolver.clone(), schema.clone()).await?;
    while let Some(raw_doc) = document_retriever.next_document().await? {
        let doc_size = raw_doc.as_bytes().len();
        //TODO: replace with DocMapper::doc_from_json(raw_doc)
        let parse_result = parse_document(raw_doc);

        let doc = match parse_result {
            Ok(doc) => {
                statistic_sender
                    .send(StatisticEvent::NewDocument {
                        size_in_bytes: doc_size,
                        error: false,
                    })
                    .await?;
                current_split.metadata.num_records += 1;
                current_split.metadata.size_in_bytes += doc_size;
                doc
            }
            Err(_) => {
                statistic_sender
                    .send(StatisticEvent::NewDocument {
                        size_in_bytes: doc_size,
                        error: true,
                    })
                    .await?;
                current_split.num_parsing_errors += 1;
                continue;
            }
        };

        current_split.add_document(doc)?;
        if current_split.has_enough_docs() {
            let split = std::mem::replace(
                &mut current_split,
                Split::create(&params, storage_resolver.clone(), schema.clone()).await?,
            );
            split_sender.send(split).await?;
        }
    }

    // build last split if it has docs
    if current_split.metadata.num_records > 0 {
        let split = std::mem::replace(
            &mut current_split,
            Split::create(&params, storage_resolver, schema.clone()).await?,
        );
        split_sender.send(split).await?;
    }

    Ok(())
}

fn parse_document(_raw_doc: String) -> anyhow::Result<Document> {
    //TODO: remove this when using docMapper
    Ok(Document::default())
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use tokio::sync::mpsc::channel;

    use crate::indexing::{
        document_retriever::CursorDocumentRetriever, split::Split, statistics::StatisticEvent,
        IndexDataParams,
    };

    use super::index_documents;

    #[tokio::test]
    async fn test_index_document() -> anyhow::Result<()> {
        let split_dir = tempfile::tempdir()?;
        let params = IndexDataParams {
            index_uri: PathBuf::from_str("file://test")?,
            input_uri: None,
            temp_dir: split_dir.into_path(),
            num_threads: 1,
            heap_size: 3000000,
            overwrite: false,
        };

        let document_retriever = Box::new(CursorDocumentRetriever::new("one\ntwo\nthree\nfour"));
        let (split_sender, _split_receiver) = channel::<Split>(20);
        let (statistic_sender, mut statistic_receiver) = channel::<StatisticEvent>(20);

        let index_future =
            index_documents(&params, document_retriever, split_sender, statistic_sender);
        let test_statistic_future = async move {
            let mut num_docs = 0;
            let mut size_bytes = 0;
            while let Some(event) = statistic_receiver.recv().await {
                //TODO: check constructed split when all is good
                if let StatisticEvent::NewDocument { size_in_bytes, .. } = event {
                    num_docs += 1;
                    size_bytes += size_in_bytes;
                }
            }

            assert_eq!(num_docs, 4);
            assert_eq!(size_bytes, 15);
        };

        let _ = futures::future::join(index_future, test_statistic_future).await;
        Ok(())
    }
}
