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

use tantivy::{schema::Schema, Document};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::indexing::statistics::StatisticEvent;

use crate::indexing::split::Split;

use super::IndexDataParams;

/// Receives json documents, parses and adds them to a `tantivy::Index` 
pub async fn index_documents(
    params: &IndexDataParams,
    mut document_receiver: Receiver<String>,
    split_sender: Sender<Split>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<()> {
    //TODO replace with  DocMapper::schema()
    let schema = Schema::builder().build();

    let mut current_split = Split::create(&params, schema.clone()).await?;
    while let Some(raw_doc) = document_receiver.recv().await {
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
        println!("EVAN");
        current_split.add_document(doc)?;
        if current_split.has_enough_docs() {
            let split = std::mem::replace(
                &mut current_split,
                Split::create(&params, schema.clone()).await?,
            );
            split_sender.send(split).await?;
        }
    }

    // build last split if it has docs
    if current_split.metadata.num_records > 0 {
        let split = std::mem::replace(
            &mut current_split,
            Split::create(&params, schema.clone()).await?,
        );
        split_sender.send(split).await?;
    }

    Ok(())
}

fn parse_document(_raw_doc: String) -> anyhow::Result<Document> {
    //TODO: remove this when using docMapper
    Ok(Document::default())
}
