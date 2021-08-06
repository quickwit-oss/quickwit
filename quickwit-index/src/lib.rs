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
use quickwit_actors::AsyncActor;
use quickwit_actors::KillSwitch;
use quickwit_actors::SyncActor;
use quickwit_metastore::Metastore;
use quickwit_storage::Storage;

pub mod actors;
pub mod models;
pub(crate) mod semaphore;

pub async fn run_indexing(
    index_id: String,
    metastore: Arc<dyn Metastore>,
    index_storage: Arc<dyn Storage>,
) -> anyhow::Result<()> {
    let index_metadata = metastore.index_metadata(&index_id).await?;
    // TODO add a supervisition that checks the progress of all of these actors.
    let kill_switch = KillSwitch::default();
    let publisher = Publisher::new(metastore.clone());
    let publisher_handler = publisher.spawn(kill_switch.clone());
    let uploader = Uploader::new(
        metastore,
        index_storage,
        publisher_handler.mailbox().clone(),
    );
    let uploader_handler = uploader.spawn(kill_switch.clone());
    let packager = Packager::new(uploader_handler.mailbox().clone());
    let packager_handler = packager.spawn(kill_switch.clone());
    let indexer = Indexer::try_new(
        index_id,
        index_metadata.index_config.into(),
        None,
        CommitPolicy::default(),
        packager_handler.mailbox().clone(),
    )?;
    let indexer_handler = indexer.spawn(kill_switch.clone());
    let source = FileSource::try_new(
        Path::new("data/test_corpus.json"),
        indexer_handler.mailbox().clone(),
    )
    .await?;
    let _source = source.spawn(kill_switch.clone());
    Ok(())
}

// TODO supervisor with respawn, one for all and respawn system.

#[cfg(test)]
pub mod test_util {
    use std::sync::Once;
    static INIT: Once = Once::new();
    pub fn setup_logging_for_tests() {
        INIT.call_once(env_logger::init);
    }
}
