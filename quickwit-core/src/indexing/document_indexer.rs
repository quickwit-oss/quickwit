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
use tantivy::{schema::Schema, Document};
use tokio::sync::mpsc::Sender;

use crate::indexing::document_retriever::DocumentRetriever;
use crate::indexing::split::Split;

use super::IndexDataParams;
use super::INDEXING_STATISTICS;

/// Receives json documents, parses and adds them to a `tantivy::Index`
pub async fn index_documents(
    index_id: String,
    params: &IndexDataParams,
    metastore: Arc<dyn Metastore>,
    storage_resolver: Arc<StorageUriResolver>,
    mut document_retriever: Box<dyn DocumentRetriever>,
    split_sender: Sender<Split>,
) -> anyhow::Result<()> {
    //TODO replace with  DocMapper::schema()
    let schema = Schema::builder().build();

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
        let parse_result = parse_document(raw_doc);

        let doc = match parse_result {
            Ok(doc) => {
                current_split.metadata.size_in_bytes += doc_size;

                INDEXING_STATISTICS.num_docs.inc();
                INDEXING_STATISTICS.total_bytes_processed.add(doc_size);
                doc
            }
            Err(_) => {
                current_split.num_parsing_errors += 1;

                INDEXING_STATISTICS.num_docs.inc();
                INDEXING_STATISTICS.num_parse_errors.inc();
                INDEXING_STATISTICS.total_bytes_processed.add(doc_size);
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

fn parse_document(_raw_doc: String) -> anyhow::Result<Document> {
    //TODO: remove this when using docMapper
    Ok(Document::default())
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr, sync::Arc};

    use crate::indexing::{
        document_retriever::StringDocumentSource, split::Split, IndexDataParams,
        INDEXING_STATISTICS,
    };
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::StorageUriResolver;
    use tokio::sync::mpsc::channel;

    use super::index_documents;

    #[tokio::test]
    async fn test_index_document() -> anyhow::Result<()> {
        let index_id = "test";
        let split_dir = tempfile::tempdir()?;
        let index_dir = tempfile::tempdir()?;
        let index_uri = format!("file://{}/{}", index_dir.path().display(), index_id);
        let params = IndexDataParams {
            index_uri: PathBuf::from_str(&index_uri)?,
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

        index_documents(
            index_id.to_owned(),
            &params,
            metastore,
            storage_resolver,
            document_retriever,
            split_sender,
        )
        .await?;

        assert_eq!(INDEXING_STATISTICS.num_docs.get(), NUM_DOCS);
        assert_eq!(INDEXING_STATISTICS.total_bytes_processed.get(), total_bytes);
        Ok(())
    }
}
