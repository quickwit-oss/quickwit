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
use tantivy::{schema::Schema, Document};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::task::JoinHandle;

use crate::indexing::statistics::StatisticEvent;

use crate::indexing::split::Split;

pub async fn index_documents(
    args: &IndexDataArgs,
    mut document_receiver: Receiver<String>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<Vec<Split>> {
    //TODO get schema from docBuilder
    let schema = Schema::builder().build();

    let mut split_merge_tasks = vec![];

    let mut current_split = Split::create(&args, schema.clone()).await?;
    while let Some(raw_doc) = document_receiver.recv().await {
        let doc_size = raw_doc.as_bytes().len();
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
                current_split.metadata.num_parsing_errors += 1;
                continue;
            }
        };

        current_split.add_document(doc)?;
        if current_split.has_enough_docs() {
            let split = std::mem::replace(
                &mut current_split,
                Split::create(&args, schema.clone()).await?,
            );

            let finalise_split_task = finalize_split(split, statistic_sender.clone()).await?;
            split_merge_tasks.push(finalise_split_task);
        }
    }

    // Finalise last split if it has docs
    if current_split.metadata.num_records > 0 {
        let split = std::mem::replace(
            &mut current_split,
            Split::create(&args, schema.clone()).await?,
        );

        let finalise_split_task = finalize_split(split, statistic_sender.clone()).await?;
        split_merge_tasks.push(finalise_split_task);
    }

    // for all split finilisation tasks
    let merge_results = futures::future::try_join_all(split_merge_tasks).await?;
    //TODO let (splits, errors) = merge_results.iter().partition(|item| item.is_ok());

    let splits = merge_results
        .into_iter()
        .filter(|result| result.is_ok())
        .map(|result| result.unwrap())
        .collect::<Vec<_>>();

    Ok(splits)
}

fn parse_document(_raw_doc: String) -> anyhow::Result<Document> {
    //TODO convert raw json to doc
    Ok(Document::default())
}

async fn finalize_split(
    mut split: Split,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<JoinHandle<anyhow::Result<Split>>> {
    statistic_sender
        .send(StatisticEvent::SplitCreated {
            id: split.id.to_string(),
            num_docs: split.metadata.num_records,
            size_in_bytes: split.metadata.size_in_bytes,
            num_failed_to_parse_docs: split.metadata.num_parsing_errors,
        })
        .await?;

    Ok(task::spawn(async move {
        split.commit().await?;
        split.merge_all_segments().await?;
        anyhow::Result::<Split>::Ok(split)
    }))
}
