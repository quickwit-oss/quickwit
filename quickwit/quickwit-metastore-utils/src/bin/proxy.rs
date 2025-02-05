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
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_metastore_utils::{GrpcCall, GrpcRequest};
use quickwit_proto::metastore::*;
use quickwit_proto::tonic;
use quickwit_proto::tonic::transport::Channel;
use quickwit_proto::tonic::{Request, Response, Status};
use structopt::StructOpt;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio::time::Instant;

struct Inner {
    start: Instant,
    client: MetastoreServiceClient,
    file: BufWriter<File>,
}

struct MetastoreProxyService {
    inner: Arc<Mutex<Inner>>,
}

impl MetastoreProxyService {
    pub fn new(client: MetastoreServiceClient, record_file: File) -> Self {
        let inner = Inner {
            start: Instant::now(),
            client,
            file: BufWriter::new(record_file),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl Inner {
    async fn record<T: Into<GrpcRequest>>(&mut self, req: T) -> anyhow::Result<()> {
        let now = Instant::now();
        let grpc_request = req.into();
        let elapsed = now - self.start;
        let grpc_call = GrpcCall {
            ts: elapsed.as_millis() as u64,
            grpc_request,
        };
        let mut buf = serde_json::to_vec(&grpc_call)?;
        buf.push(b'\n');
        self.file.write_all(&buf).await?;
        Ok(())
    }
}

#[async_trait]
impl MetastoreService for MetastoreProxyService {
    /// Creates an index.
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.create_index(request).await?;
        Ok(resp)
    }
    /// Gets an index metadata.
    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.index_metadata(request).await?;
        Ok(resp)
    }
    /// Gets an indexes metadatas.
    async fn list_indexes_metadata(
        &self,
        request: tonic::Request<ListIndexesMetadataRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadataResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.list_indexes_metadata(request).await?;
        Ok(resp)
    }
    /// Deletes an index
    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<DeleteIndexResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.delete_index(request).await?;
        Ok(resp)
    }
    /// Gets splits from index.
    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.list_splits(request).await?;
        Ok(resp)
    }
    /// Stages several splits.
    async fn stage_splits(
        &self,
        request: Request<StageSplitsRequest>,
    ) -> Result<Response<SplitResponse>, Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.stage_splits(request).await?;
        Ok(resp)
    }
    /// Publishes split.
    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.publish_splits(request).await?;
        Ok(resp)
    }
    /// Marks splits for deletion.
    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.mark_splits_for_deletion(request).await?;
        Ok(resp)
    }
    /// Deletes splits.
    async fn delete_splits(
        &self,
        request: tonic::Request<DeleteSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.delete_splits(request).await?;
        Ok(resp)
    }
    /// Adds source.
    async fn add_source(
        &self,
        request: tonic::Request<AddSourceRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.add_source(request).await?;
        Ok(resp)
    }
    /// Toggles source.
    async fn toggle_source(
        &self,
        request: tonic::Request<ToggleSourceRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.toggle_source(request).await?;
        Ok(resp)
    }
    /// Removes source.
    async fn delete_source(
        &self,
        request: tonic::Request<DeleteSourceRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.delete_source(request).await?;
        Ok(resp)
    }
    /// Resets source checkpoint.
    async fn reset_source_checkpoint(
        &self,
        request: tonic::Request<ResetSourceCheckpointRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.reset_source_checkpoint(request).await?;
        Ok(resp)
    }
    /// Gets last opstamp for a given `index_id`.
    async fn last_delete_opstamp(
        &self,
        request: tonic::Request<LastDeleteOpstampRequest>,
    ) -> Result<tonic::Response<LastDeleteOpstampResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.last_delete_opstamp(request).await?;
        Ok(resp)
    }
    /// Creates a delete task.
    async fn create_delete_task(
        &self,
        request: tonic::Request<DeleteQuery>,
    ) -> Result<tonic::Response<DeleteTask>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.create_delete_task(request).await?;
        Ok(resp)
    }
    /// Updates splits `delete_opstamp`.
    async fn update_splits_delete_opstamp(
        &self,
        request: tonic::Request<UpdateSplitsDeleteOpstampRequest>,
    ) -> Result<tonic::Response<UpdateSplitsDeleteOpstampResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.update_splits_delete_opstamp(request).await?;
        Ok(resp)
    }
    /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &self,
        request: tonic::Request<ListDeleteTasksRequest>,
    ) -> Result<tonic::Response<ListDeleteTasksResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.list_delete_tasks(request).await?;
        Ok(resp)
    }
    //// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
    async fn list_stale_splits(
        &self,
        request: tonic::Request<ListStaleSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        let mut lock = self.inner.lock().await;
        lock.record(request.get_ref().clone()).await.unwrap();
        let resp = lock.client.list_stale_splits(request).await?;
        Ok(resp)
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "proxy", about = "A quickwit-metastore recording proxy.")]
struct Opt {
    #[structopt(default_value = "127.0.0.1:7291")]
    listen_to: SocketAddr,
    #[structopt(long, default_value = "http://127.0.0.1:7281")]
    forward_to: String,
    #[structopt(long, default_value = "./replay.ndjson")]
    file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    let client = MetastoreServiceClient::connect(opt.forward_to.clone()).await?;
    let file = File::create(&opt.file).await?;
    let service = MetastoreProxyService::new(client, file);
    let server = MetastoreServiceServer::new(service);
    println!(
        "Listening to {}, Forwarding to {}",
        opt.listen_to, opt.forward_to
    );
    tonic::transport::Server::builder()
        .add_service(server)
        .serve(opt.listen_to)
        .await?;
    Ok(())
}
