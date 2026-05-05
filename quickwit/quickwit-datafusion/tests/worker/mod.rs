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

use std::net::SocketAddr;
use std::sync::Arc;

use quickwit_datafusion::grpc::DataFusionServiceGrpcImpl;
use quickwit_datafusion::proto::data_fusion_service_server::DataFusionServiceServer;
use quickwit_datafusion::{DataFusionService, DataFusionSessionBuilder, build_worker};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Handle to a DataFusion worker gRPC server bound to a random localhost port.
///
/// Drop the handle to shut the server down. The shutdown signal fires on drop,
/// and tonic exits cleanly so tests do not leak background tasks.
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
