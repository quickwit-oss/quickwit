// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
use tracing::info;

const RUN_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(60) // 1min
} else {
    Duration::from_secs(60 * 60) // 1h
};

#[derive(Clone, Debug, Default)]
pub struct JanitorServiceCounters {
    /// Number of passes.
    pub num_passes: usize,
}

pub struct JanitorService {
    _data_dir_path: PathBuf,
    _metastore: Arc<dyn Metastore>,
    _storage_resolver: StorageUriResolver,
    counters: JanitorServiceCounters,
}

impl JanitorService {
    pub fn new(
        data_dir_path: PathBuf,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
    ) -> Self {
        Self {
            _data_dir_path: data_dir_path,
            _metastore: metastore,
            _storage_resolver: storage_resolver,
            counters: JanitorServiceCounters::default(),
        }
    }
}

#[async_trait]
impl Actor for JanitorService {
    type ObservableState = JanitorServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle(Loop, ctx).await
    }
}

#[derive(Debug)]
struct Loop;

#[async_trait]
impl Handler<Loop> for JanitorService {
    type Reply = ();

    async fn handle(&mut self, _: Loop, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        info!("janitor-service-operation");
        self.counters.num_passes += 1;
        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_common::uri::Uri;
    use quickwit_metastore::quickwit_metastore_uri_resolver;

    use super::*;

    #[tokio::test]
    async fn test_janitor_service() {
        let metastore_uri = Uri::new("ram:///metastore".to_string());
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let storage_resolver = StorageUriResolver::for_test();
        let janitor_service =
            JanitorService::new(data_dir_path, metastore.clone(), storage_resolver.clone());
        let universe = Universe::new();
        let (_, handle) = universe.spawn_actor(janitor_service).spawn();
        let counters = handle.observe().await;
        assert_eq!(counters.num_passes, 1);

        // 30 secs later
        universe.simulate_time_shift(Duration::from_secs(30)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);

        // 60 secs later
        universe.simulate_time_shift(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 2);
    }
}
