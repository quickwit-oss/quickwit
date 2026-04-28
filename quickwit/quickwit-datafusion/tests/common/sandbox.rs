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
//
//! Lightweight test harness that replaces the full `ClusterSandbox` for
//! DataFusion integration tests.
//!
//! - [`TestSandbox::start`] spins up an in-process file-backed metastore and an unconfigured
//!   `StorageResolver`. No gRPC, no cluster, no CLI startup.
//! - [`spawn_df_worker`] exposes a `DataFusionSessionBuilder` over a real tonic listener on
//!   `127.0.0.1:<random>` so distributed tests can wire a `SearcherPool` to real worker endpoints
//!   without a full Quickwit cluster.
//!
//! Lives here rather than in `quickwit-integration-tests` so this crate's tests
//! do not depend on `quickwit-cli` / `quickwit-serve`.
//!
//! The `#[allow(dead_code)]` on `common/mod.rs` is intentional: individual test
//! files only use a subset of the helpers, and cargo compiles a distinct test
//! binary per `tests/*.rs` file.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use quickwit_common::uri::Uri;
use quickwit_datafusion::grpc::DataFusionServiceGrpcImpl;
use quickwit_datafusion::proto::data_fusion_service_server::DataFusionServiceServer;
use quickwit_datafusion::{DataFusionService, DataFusionSessionBuilder, build_worker};
use quickwit_metastore::MetastoreResolver;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// In-process metastore + storage harness.
///
/// Holds the tempdir backing the file-backed metastore so it is not dropped
/// before the test finishes. `data_dir` is a separate directory for parquet
/// split files the test writes via `publish_split`.
pub struct TestSandbox {
    pub metastore: MetastoreServiceClient,
    pub storage_resolver: StorageResolver,
    pub data_dir: TempDir,
    _metastore_dir: TempDir,
}

impl TestSandbox {
    pub async fn start() -> Self {
        // SAFETY: tests are single-threaded by default; setting env before the
        // first metastore resolve is fine.
        unsafe { std::env::set_var("QW_DISABLE_TELEMETRY", "1") };

        let metastore_dir = tempfile::tempdir().expect("metastore tempdir");
        let data_dir = tempfile::tempdir().expect("data tempdir");

        let metastore_uri_str = format!("file://{}/metastore", metastore_dir.path().display());
        let metastore_uri = Uri::from_str(&metastore_uri_str).expect("valid metastore uri");

        let metastore = MetastoreResolver::unconfigured()
            .resolve(&metastore_uri)
            .await
            .expect("resolve file-backed metastore");

        Self {
            metastore,
            storage_resolver: StorageResolver::unconfigured(),
            data_dir,
            _metastore_dir: metastore_dir,
        }
    }
}

/// Handle to a DataFusion worker gRPC server bound to a random localhost port.
///
/// Drop the handle to shut the server down. The shutdown signal fires on drop,
/// and tonic exits cleanly so tests do not leak background tasks.
///
/// Only `tests/distributed.rs` uses this; `metrics.rs` / `null_columns.rs`
/// compile the common module but not these symbols.
#[allow(dead_code)]
pub struct SpawnedWorker {
    pub addr: SocketAddr,
    _shutdown: oneshot::Sender<()>,
    _task: tokio::task::JoinHandle<()>,
}

/// Spawn a tonic server on `127.0.0.1:<random>` running the DataFusion
/// `DataFusionService` and the `datafusion-distributed` `WorkerService`, both
/// backed by the given session builder.
///
/// Mirrors the pair of services that `quickwit-serve/src/grpc.rs` mounts on the
/// searcher gRPC port, so distributed tests exercise the real worker codepath.
#[allow(dead_code)]
pub async fn spawn_df_worker(builder: Arc<DataFusionSessionBuilder>) -> SpawnedWorker {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("listener local_addr");

    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let grpc_service = DataFusionServiceServer::new(DataFusionServiceGrpcImpl::new(
        DataFusionService::new(Arc::clone(&builder)),
    ));
    let worker_service = build_worker(Arc::clone(&builder)).into_worker_server();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = shutdown_rx.await;
    };

    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(grpc_service)
            .add_service(worker_service)
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
    });

    SpawnedWorker {
        addr,
        _shutdown: shutdown_tx,
        _task: task,
    }
}
