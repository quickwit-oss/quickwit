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

use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
use std::sync::Arc;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::{schema::Schema, Document};
use tokio::sync::mpsc::Sender;

use crate::indexing::document_retriever::DocumentRetriever;
use crate::indexing::split::Split;
use crate::indexing::statistics::StatisticEvent;

use super::IndexDataParams;

/// Receives json documents, parses and adds them to a `tantivy::Index`
pub async fn index_documents(
    index_id: String,
    params: &IndexDataParams,
    metastore: Arc<dyn Metastore>,
    storage_resolver: Arc<StorageUriResolver>,
    mut document_retriever: Box<dyn DocumentRetriever>,
    split_sender: Sender<Split>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<()> {
    //TODO replace with  DocMapper::schema()
    let mut schema_builder = Schema::builder();
    let text_options = TextOptions::default()
        .set_stored()
        .set_indexing_options(TextFieldIndexing::default());
    schema_builder.add_text_field("title", text_options.clone());
    schema_builder.add_text_field("body", text_options.clone());
    schema_builder.add_text_field("url", text_options);
    let schema = schema_builder.build();

    let mut current_split = Split::create(
        index_id.to_string(),
        &params,
        storage_resolver.clone(),
        metastore.clone(),
        schema.clone(),
    )
    .await?;
    while let Some(raw_doc) = document_retriever.next_document().await? {
        let doc_size = raw_doc.as_bytes().len();
        //TODO: replace with DocMapper::doc_from_json(raw_doc)
        let parse_result = parse_document(&raw_doc, &schema);

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
                Split::create(
                    index_id.to_string(),
                    &params,
                    storage_resolver.clone(),
                    metastore.clone(),
                    schema.clone(),
                )
                .await?,
            );
            split_sender.send(split).await?;
        }
    }

    // build last split if it has docs
    if current_split.metadata.num_records > 0 {
        let split = std::mem::replace(
            &mut current_split,
            Split::create(
                index_id.to_string(),
                &params,
                storage_resolver,
                metastore,
                schema.clone(),
            )
            .await?,
        );
        split_sender.send(split).await?;
    }

    Ok(())
}

fn parse_document(doc_json: &str, schema: &Schema) -> anyhow::Result<Document> {
    //TODO: remove this when using docMapper
    schema
        .parse_document(doc_json)
        .map_err(anyhow::Error::new)
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr, sync::Arc};

    use crate::indexing::{
        document_retriever::StringDocumentSource, split::Split, statistics::StatisticEvent,
        IndexDataParams,
    };
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::StorageUriResolver;
    use tokio::sync::mpsc::channel;

    use super::index_documents;

    #[tokio::test]
    async fn test_index_document() -> anyhow::Result<()> {
        let split_dir = tempfile::tempdir()?;
        let index_id = "test";
        let params = IndexDataParams {
            index_uri: PathBuf::from_str("file://test")?,
            input_uri: None,
            temp_dir: split_dir.into_path(),
            num_threads: 1,
            heap_size: 3000000,
            overwrite: false,
        };

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .times(0)
            .returning(|_index_uri, _split_id| Ok(()));
        mock_metastore
            .expect_publish_split()
            .times(0)
            .returning(|_uri, _id| Ok(()));
        let metastore = Arc::new(mock_metastore);
        let storage_resolver = Arc::new(StorageUriResolver::default());

        const NUM_DOCS: usize = 780;
        let docs = (0..NUM_DOCS)
            .map(|num| format!("doc_{}", num))
            .collect::<Vec<String>>();
        let total_bytes = docs
            .iter()
            .fold(0, |total, size| total + size.as_bytes().len());
        let input = docs.join("\n");

        let document_retriever = Box::new(StringDocumentSource::new(input));
        let (split_sender, _split_receiver) = channel::<Split>(20);
        let (statistic_sender, mut statistic_receiver) = channel::<StatisticEvent>(20);

        let index_future = index_documents(
            index_id.to_owned(),
            &params,
            metastore,
            storage_resolver,
            document_retriever,
            split_sender,
            statistic_sender,
        );
        let test_statistic_future = async move {
            let mut received_num_docs = 0;
            let mut received_size_bytes = 0;
            while let Some(event) = statistic_receiver.recv().await {
                //TODO: check constructed split when all metastore feature complete
                if let StatisticEvent::NewDocument { size_in_bytes, .. } = event {
                    received_num_docs += 1;
                    received_size_bytes += size_in_bytes;
                }
            }

            assert_eq!(received_num_docs, NUM_DOCS);
            assert_eq!(received_size_bytes, total_bytes);
        };

        let _ = futures::future::join(index_future, test_statistic_future).await;
        Ok(())
    }
}
