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

//! gRPC handler that bridges [`quickwit_datafusion::DataFusionService`] to the
//! tonic-generated `DataFusionService` server trait.
//!
//! Each streaming response batch is encoded as Arrow IPC (stream format) using
//! [`arrow::ipc::writer::StreamWriter`] and returned as raw bytes in
//! `ExecuteSubstraitResponse::arrow_ipc_bytes` /
//! `ExecuteSqlResponse::arrow_ipc_bytes`.
//!
//! ## Error mapping
//!
//! `datafusion::error::DataFusionError` is mapped to `tonic::Status`:
//! - Plan / Schema errors → `InvalidArgument`
//! - I/O errors → `Internal`
//! - Everything else → `Internal`

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use futures::StreamExt;
use quickwit_datafusion::DataFusionService;
use quickwit_proto::datafusion::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExecuteSubstraitRequest, ExecuteSubstraitResponse,
    data_fusion_service_server,
};
use quickwit_proto::tonic;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

/// Converts a DataFusion error (represented as any `std::error::Error`) to an
/// appropriate `tonic::Status`.
///
/// Plan / schema errors are surfaced as `InvalidArgument`; everything else as
/// `Internal`.  The distinction is made by inspecting the `Display` output
/// since we avoid a hard dependency on the `datafusion` crate in quickwit-serve.
fn df_error_to_status(err: impl std::fmt::Display) -> tonic::Status {
    let msg = err.to_string();
    // DataFusion plan/schema errors start with "Error during planning:" or
    // "Schema error:".  Map those to invalid argument; everything else is internal.
    if msg.starts_with("Error during planning") || msg.starts_with("Schema error") {
        tonic::Status::invalid_argument(msg)
    } else {
        tonic::Status::internal(msg)
    }
}

/// Serialize a single `RecordBatch` to Arrow IPC stream format bytes.
fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, tonic::Status> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size());
    let mut writer = StreamWriter::try_new(Cursor::new(&mut buf), batch.schema_ref())
        .map_err(|e| tonic::Status::internal(format!("failed to create Arrow IPC writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| tonic::Status::internal(format!("failed to write Arrow IPC batch: {e}")))?;
    writer
        .finish()
        .map_err(|e| tonic::Status::internal(format!("failed to finish Arrow IPC stream: {e}")))?;
    drop(writer);
    Ok(buf)
}

/// tonic gRPC adapter that wraps [`DataFusionService`].
///
/// Implements the tonic-generated `DataFusionService` trait and converts the
/// streaming `RecordBatch` results to Arrow IPC bytes.
pub struct DataFusionServiceGrpcImpl {
    service: Arc<DataFusionService>,
}

impl DataFusionServiceGrpcImpl {
    pub fn new(service: DataFusionService) -> Self {
        Self {
            service: Arc::new(service),
        }
    }
}

#[async_trait::async_trait]
impl data_fusion_service_server::DataFusionService for DataFusionServiceGrpcImpl {
    type ExecuteSubstraitStream = ReceiverStream<Result<ExecuteSubstraitResponse, tonic::Status>>;
    type ExecuteSqlStream = ReceiverStream<Result<ExecuteSqlResponse, tonic::Status>>;

    async fn execute_substrait(
        &self,
        request: tonic::Request<ExecuteSubstraitRequest>,
    ) -> Result<tonic::Response<Self::ExecuteSubstraitStream>, tonic::Status> {
        let req = request.into_inner();
        let service = Arc::clone(&self.service);

        // Route to the appropriate DataFusionService method:
        // - substrait_plan_bytes: production path (pre-encoded protobuf, for callers that already hold an encoded plan)
        // - substrait_plan_json:  dev/tooling path (grpcurl, rollup JSON files)
        let mut stream = if !req.substrait_plan_bytes.is_empty() {
            service
                .execute_substrait(&req.substrait_plan_bytes)
                .await
                .map_err(df_error_to_status)?
        } else if !req.substrait_plan_json.is_empty() {
            service
                .execute_substrait_json(&req.substrait_plan_json)
                .await
                .map_err(df_error_to_status)?
        } else {
            return Err(tonic::Status::invalid_argument(
                "either substrait_plan_bytes or substrait_plan_json must be set",
            ));
        };

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                let item = match result {
                    Ok(batch) => batch_to_ipc_bytes(&batch)
                        .map(|ipc_bytes| ExecuteSubstraitResponse { arrow_ipc_bytes: ipc_bytes }),
                    Err(err) => Err(tonic::Status::internal(format!("stream error: {err}"))),
                };
                if tx.send(item).await.is_err() {
                    // receiver dropped — client disconnected
                    break;
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn execute_sql(
        &self,
        request: tonic::Request<ExecuteSqlRequest>,
    ) -> Result<tonic::Response<Self::ExecuteSqlStream>, tonic::Status> {
        let req = request.into_inner();
        let service = Arc::clone(&self.service);

        let mut stream = service
            .execute_sql(&req.sql)
            .await
            .map_err(|err| {
                warn!(error = %err, "DataFusion SQL execution error");
                df_error_to_status(err)
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                let item = match result {
                    Ok(batch) => batch_to_ipc_bytes(&batch)
                        .map(|ipc_bytes| ExecuteSqlResponse { arrow_ipc_bytes: ipc_bytes }),
                    Err(err) => Err(tonic::Status::internal(format!("stream error: {err}"))),
                };
                if tx.send(item).await.is_err() {
                    // receiver dropped — client disconnected
                    break;
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
