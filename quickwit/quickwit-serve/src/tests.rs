// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::ChannelTransport;
use quickwit_cluster::create_cluster_for_test;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::{IndexMetadata, MockMetastore};
use tokio::sync::{oneshot, watch};

use crate::{check_cluster_configuration, node_readiness_reporting_task};

#[tokio::test]
async fn test_check_cluster_configuration() {
    let services = HashSet::from_iter([QuickwitService::Metastore]);
    let peer_seeds = ["192.168.0.12:7280".to_string()];
    let mut metastore = MockMetastore::new();

    metastore
        .expect_uri()
        .return_const(Uri::for_test("file:///qwdata/indexes"));

    metastore.expect_list_indexes_metadatas().return_once(|| {
        Ok(vec![IndexMetadata::for_test(
            "test-index",
            "file:///qwdata/indexes/test-index",
        )])
    });

    check_cluster_configuration(&services, &peer_seeds, Arc::new(metastore))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_readiness_updates() {
    let transport = ChannelTransport::default();
    let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
        .await
        .unwrap();
    let (metastore_readiness_tx, metastore_readiness_rx) = watch::channel(false);
    let mut metastore = MockMetastore::new();
    metastore.expect_check_connectivity().returning(move || {
        if *metastore_readiness_rx.borrow() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Metastore not ready"))
        }
    });
    let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel();
    let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel();
    tokio::spawn(node_readiness_reporting_task(
        cluster.clone(),
        Arc::new(metastore),
        grpc_readiness_signal_rx,
        rest_readiness_signal_rx,
    ));
    assert!(!cluster.is_self_node_ready().await);

    grpc_readiness_trigger_tx.send(()).unwrap();
    rest_readiness_trigger_tx.send(()).unwrap();
    assert!(!cluster.is_self_node_ready().await);

    metastore_readiness_tx.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(cluster.is_self_node_ready().await);

    metastore_readiness_tx.send(false).unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!cluster.is_self_node_ready().await);
}
