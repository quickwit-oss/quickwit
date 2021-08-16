// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::path::Path;
use std::sync::Arc;

use crate::actors::source::FileSource;
use crate::actors::Indexer;
use crate::actors::Packager;
use crate::actors::Publisher;
use crate::actors::Uploader;
use crate::models::CommitPolicy;
use quickwit_actors::Actor;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::Universe;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
use tracing::info;

pub mod actors;
pub mod models;
pub(crate) mod semaphore;

pub async fn spawn_indexing_pipeline(
    universe: &Universe,
    index_id: String,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<(ActorExitStatus, <Publisher as Actor>::ObservableState)> {
    info!(index_id=%index_id, "start-indexing-pipeline");
    let index_metadata = metastore.index_metadata(&index_id).await?;
    let index_storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;

    // TODO supervisor with respawn, one for all and respawn system.
    // TODO add a supervisition that checks the progress of all of these actors.
    let publisher = Publisher::new(metastore.clone());
    let (publisher_mailbox, publisher_handler) = universe.spawn(publisher);
    let uploader = Uploader::new(metastore, index_storage, publisher_mailbox);
    let (uploader_mailbox, _uploader_handler) = universe.spawn(uploader);
    info!(actor_name=%uploader_mailbox.actor_instance_id());
    let packager = Packager::new(uploader_mailbox);
    let (packager_mailbox, _packager_handler) = universe.spawn_sync_actor(packager);
    let indexer = Indexer::try_new(
        index_id,
        index_metadata.index_config,
        None,
        CommitPolicy::default(),
        packager_mailbox,
    )?;
    let (indexer_mailbox, _indexer_handler) = universe.spawn_sync_actor(indexer);

    // TODO the source is hardcoded here.
    let source = FileSource::try_new(Path::new("data/test_corpus.json"), indexer_mailbox).await?;

    let (_source_mailbox, _source_handle) = universe.spawn(source);
    let (actor_termination, observation) = publisher_handler.join().await;
    Ok((actor_termination, observation))
}

#[cfg(test)]
mod tests {

    use super::spawn_indexing_pipeline;
    use quickwit_actors::Universe;
    use quickwit_metastore::IndexMetadata;
    use quickwit_metastore::MockMetastore;
    use quickwit_metastore::SplitState;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_indexing_pipeline() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .times(1)
            .returning(|_| {
                let index_metadata = IndexMetadata {
                    index_id: "test-index".to_string(),
                    index_uri: "ram://test-index".to_string(),
                    index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
                    checkpoint: Default::default(),
                };
                Ok(index_metadata)
            });
        metastore
            .expect_stage_split()
            .withf(move |index_id, split_metadata| -> bool {
                (index_id == "test-index") && split_metadata.split_state == SplitState::New
            })
            .times(1)
            .returning(|_, _| Ok(()));
        metastore
            .expect_publish_splits()
            .withf(move |index_id, splits, checkpoint_delta| -> bool {
                index_id == "test-index"
                    && splits.len() == 1
                    && format!("{:?}", checkpoint_delta)
                        .ends_with(":(00000000000000000000..00000000000000000070])")
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        let universe = Universe::new();
        let (publisher_termination, publisher_counters) = spawn_indexing_pipeline(
            &universe,
            "test-index".to_string(),
            Arc::new(metastore),
            Default::default(),
        )
        .await?;
        assert!(publisher_termination.is_success());
        assert_eq!(publisher_counters.num_published_splits, 1);
        Ok(())
    }
}
