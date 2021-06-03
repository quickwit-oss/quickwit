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
use tokio::sync::mpsc::Sender;

use crate::indexing::document_retriever::DocumentRetriever;
use crate::indexing::split::Split;

use super::IndexDataParams;
use super::IndexingStatistics;

/// Receives json documents, parses and adds them to a `tantivy::Index`
pub async fn index_documents(
    index_id: String,
    params: &IndexDataParams,
    metastore: Arc<dyn Metastore>,
    storage_resolver: Arc<StorageUriResolver>,
    mut document_retriever: Box<dyn DocumentRetriever>,
    split_sender: Sender<Split>,
    statistics: Arc<IndexingStatistics>,
) -> anyhow::Result<()> {
    let index_metadata = metastore.index_metadata(&index_id).await?;
    let schema = index_metadata.doc_mapper.schema();

    let mut current_split = Split::create(
        index_id.to_string(),
        &params,
        storage_resolver.clone(),
        metastore.clone(),
        schema.clone(),
    )
    .await?;
    while let Some(raw_json_doc) = document_retriever.next_document().await? {
        let doc_size = raw_json_doc.as_bytes().len();
        let parse_result = index_metadata.doc_mapper.doc_from_json(&raw_json_doc);

        let doc = match parse_result {
            Ok(doc) => {
                current_split.metadata.size_in_bytes += doc_size;

                statistics.num_docs.inc();
                statistics.total_bytes_processed.add(doc_size);
                doc
            }
            Err(_) => {
                current_split.num_parsing_errors += 1;

                statistics.num_docs.inc();
                statistics.num_parse_errors.inc();
                statistics.total_bytes_processed.add(doc_size);
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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr, sync::Arc};

    use crate::indexing::{
        document_retriever::StringDocumentSource, split::Split, IndexDataParams, IndexingStatistics,
    };
    use quickwit_doc_mapping::{AllFlattenDocMapper, DocMapper};
    use quickwit_metastore::{IndexMetadata, MockMetastore};
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
            .expect_index_metadata()
            .times(1)
            .returning(move |index_id| {
                Ok(IndexMetadata {
                    index_id: index_id.to_string(),
                    index_uri: index_uri.clone(),
                    doc_mapper: AllFlattenDocMapper::new()
                        .map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
                        .unwrap(),
                })
            });
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

        let statistics = Arc::new(IndexingStatistics::default());
        index_documents(
            index_id.to_owned(),
            &params,
            metastore,
            storage_resolver,
            document_retriever,
            split_sender,
            statistics.clone(),
        )
        .await?;

        assert_eq!(statistics.num_docs.get(), NUM_DOCS);
        assert_eq!(statistics.total_bytes_processed.get(), total_bytes);
        Ok(())
    }
}
