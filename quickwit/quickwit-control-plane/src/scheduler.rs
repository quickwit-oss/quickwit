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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus};
use quickwit_cluster::Cluster;
use quickwit_metastore::Metastore;

// TODO: implement scheduling.
#[allow(dead_code)]
pub struct IndexingScheduler {
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
}

#[async_trait]
impl Actor for IndexingScheduler {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexerScheduler".to_string()
    }

    async fn initialize(&mut self, _: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

impl IndexingScheduler {
    pub fn new(cluster: Arc<Cluster>, metastore: Arc<dyn Metastore>) -> Self {
        Self { cluster, metastore }
    }
}
