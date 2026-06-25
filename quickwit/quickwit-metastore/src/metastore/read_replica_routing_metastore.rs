// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use quickwit_proto::metastore::{
    IndexMetadataRequest, IndexMetadataResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListMetricsSplitsRequest, ListMetricsSplitsResponse,
    ListSketchSplitsRequest, ListSketchSplitsResponse, ListSplitsRequest, ListSplitsResponse,
    MetastoreResult, MetastoreService, MetastoreServiceClient, MetastoreServiceStream,
};
use tokio::sync::watch;

use crate::MetastoreReadService;

/// A [`MetastoreReadService`] that routes read RPCs to a read replica metastore when one is
/// available, and to the primary metastore otherwise.
///
/// Because it only implements the read-only [`MetastoreReadService`] surface, writes cannot be
/// issued through it at all — the type system rules them out, so there is nothing to delegate.
///
/// Routing is decided per request from the live connection set of the read replica balance
/// channel: when no replica node is connected, reads fall back to the primary. The check is a
/// synchronous read of a [`watch`] channel and never holds a borrow across an await point.
#[derive(Clone)]
pub struct ReadReplicaRoutingMetastore {
    primary: MetastoreServiceClient,
    read_replica: MetastoreServiceClient,
    read_replica_connections: watch::Receiver<HashSet<SocketAddr>>,
}

impl fmt::Debug for ReadReplicaRoutingMetastore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReadReplicaRoutingMetastore").finish()
    }
}

impl ReadReplicaRoutingMetastore {
    /// Creates a new [`ReadReplicaRoutingMetastore`].
    ///
    /// `read_replica_connections` is the connection-set watcher of the read replica balance
    /// channel (see `BalanceChannel::connection_keys_watcher`).
    pub fn new(
        primary: MetastoreServiceClient,
        read_replica: MetastoreServiceClient,
        read_replica_connections: watch::Receiver<HashSet<SocketAddr>>,
    ) -> Self {
        Self {
            primary,
            read_replica,
            read_replica_connections,
        }
    }

    /// Returns the read replica when at least one replica node is connected, the primary otherwise.
    fn read_metastore(&self) -> &MetastoreServiceClient {
        if self.read_replica_connections.borrow().is_empty() {
            &self.primary
        } else {
            &self.read_replica
        }
    }
}

#[async_trait]
impl MetastoreReadService for ReadReplicaRoutingMetastore {
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        MetastoreService::index_metadata(self.read_metastore(), request).await
    }

    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        MetastoreService::list_indexes_metadata(self.read_metastore(), request).await
    }

    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        MetastoreService::list_splits(self.read_metastore(), request).await
    }

    async fn list_metrics_splits(
        &self,
        request: ListMetricsSplitsRequest,
    ) -> MetastoreResult<ListMetricsSplitsResponse> {
        MetastoreService::list_metrics_splits(self.read_metastore(), request).await
    }

    async fn list_sketch_splits(
        &self,
        request: ListSketchSplitsRequest,
    ) -> MetastoreResult<ListSketchSplitsResponse> {
        MetastoreService::list_sketch_splits(self.read_metastore(), request).await
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::metastore::MockMetastoreService;
    use tokio::sync::watch;

    use super::*;
    use crate::ListIndexesMetadataResponseExt;

    /// Builds a metastore client whose `list_indexes_metadata` is expected to be called exactly
    /// `times` times.
    fn mock_metastore_expecting_list_indexes(times: usize) -> MetastoreServiceClient {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(times)
            .returning(|_request| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    #[tokio::test]
    async fn test_routes_reads_to_primary_when_no_replica_is_connected() {
        let (_connections_tx, connections_rx) = watch::channel(HashSet::<SocketAddr>::new());
        let metastore = ReadReplicaRoutingMetastore::new(
            mock_metastore_expecting_list_indexes(1),
            mock_metastore_expecting_list_indexes(0),
            connections_rx,
        );
        metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_routes_reads_to_replica_when_connected() {
        let replica_addr: SocketAddr = "127.0.0.1:7281".parse().unwrap();
        let (_connections_tx, connections_rx) = watch::channel(HashSet::from([replica_addr]));
        let metastore = ReadReplicaRoutingMetastore::new(
            mock_metastore_expecting_list_indexes(0),
            mock_metastore_expecting_list_indexes(1),
            connections_rx,
        );
        metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap();
    }
}
