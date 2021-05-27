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

use std::path::PathBuf;

use futures::try_join;
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use tokio::sync::mpsc::channel;
use tracing::warn;

use crate::index::garbage_collect;
use crate::indexing::document_retriever::DocumentSource;
use crate::indexing::split_finalizer::finalize_split;
use crate::indexing::statistics::StatisticsCollector;
use crate::indexing::{document_indexer::index_documents, split::Split};

const SPLIT_CHANNEL_SIZE: usize = 30;

/// A struct to bundle index cli args
/// TODO: remove when there is a better structure
#[derive(Debug, Clone)]
pub struct IndexDataParams {
    pub index_uri: PathBuf,
    pub input_uri: Option<PathBuf>,
    pub temp_dir: PathBuf,
    pub num_threads: usize,
    pub heap_size: u64,
    pub overwrite: bool,
}

/// The entry point for the index command.
/// It reads a new-line deleimited json documents, add them to the index while building
/// and publishing splits metastore.
pub async fn index_data(params: IndexDataParams) -> anyhow::Result<()> {
    if params.overwrite {
        reset_index(params.clone()).await?;
    }

    if params.input_uri.is_none() {
        println!("Please enter your new line delimited json documents.");
    }

    let index_uri = params.index_uri.to_string_lossy().to_string();
    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;

    let document_retriever = Box::new(DocumentSource::create(&params.input_uri).await?);
    let (statistic_collector, statistic_sender) = StatisticsCollector::start_collection();
    let (split_sender, split_receiver) = channel::<Split>(SPLIT_CHANNEL_SIZE);
    try_join!(
        index_documents(
            &params,
            metastore,
            document_retriever,
            split_sender,
            statistic_sender.clone()
        ),
        finalize_split(split_receiver, statistic_sender.clone()),
    )?;

    statistic_collector.lock().await.display_report();
    Ok(())
}

/// Clears the index by applying the following actions
/// - mark all split as deleted
/// - delete the artifacts of all splits marked as deleted using garbage collection
/// - delete the splits from the metastore
///
async fn reset_index(params: IndexDataParams) -> anyhow::Result<()> {
    let index_uri = params.index_uri.to_string_lossy().to_string();
    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    let splits = metastore
        .list_splits(index_uri.clone(), SplitState::Published, None)
        .await?;

    let mark_split_as_deleted_tasks = splits
        .iter()
        .map(|split| metastore.mark_split_as_deleted(index_uri.clone(), split.split_id.clone()))
        .collect::<Vec<_>>();
    futures::future::try_join_all(mark_split_as_deleted_tasks).await?;

    let storage = quickwit_storage::StorageUriResolver::default().resolve(&index_uri)?;
    let garbage_collection_result =
        garbage_collect(index_uri.clone(), storage, metastore.clone()).await;
    if garbage_collection_result.is_err() {
        warn!(index_uri =% index_uri, "All split files could not be removed during garbage collection.");
    }

    let delete_tasks = splits
        .iter()
        .map(|split| metastore.delete_split(index_uri.clone(), split.split_id.clone()))
        .collect::<Vec<_>>();
    futures::future::try_join_all(delete_tasks).await?;

    Ok(())
}
