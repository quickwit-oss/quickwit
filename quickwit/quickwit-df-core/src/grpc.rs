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

//! gRPC handler that bridges [`crate::service::DataFusionService`] to the
//! tonic-generated `DataFusionService` server trait.
//!
//! Each streaming response batch is encoded as Arrow IPC (stream format) using
//! [`arrow::ipc::writer::StreamWriter`] and returned as raw bytes in
//! `ExecuteSubstraitResponse::arrow_ipc_bytes` /
//! `ExecuteSqlResponse::arrow_ipc_bytes`.
//!
//! This adapter is intentionally thin: it forwards requests to
//! [`crate::service::DataFusionService`], serializes `RecordBatch`es to Arrow
//! IPC bytes, and maps `DataFusionError` values onto gRPC status codes.
//!
//! ## Error mapping
//!
//! `datafusion::error::DataFusionError` is mapped to `tonic::Status`:
//! - SQL / plan / schema / execution / configuration / Substrait errors → `InvalidArgument`
//! - `NotImplemented` → `Unimplemented`
//! - `ResourcesExhausted` → `ResourceExhausted`
//! - Everything else → `Internal`

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use datafusion::arrow;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use tracing::warn;

use crate::proto::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExecuteSubstraitRequest, ExecuteSubstraitResponse,
    data_fusion_service_server,
};
use crate::service::DataFusionService;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GrpcErrorKind {
    InvalidArgument,
    ResourceExhausted,
    Unimplemented,
    Internal,
}

impl GrpcErrorKind {
    fn into_status(self, message: String) -> tonic::Status {
        match self {
            Self::InvalidArgument => tonic::Status::invalid_argument(message),
            Self::ResourceExhausted => tonic::Status::resource_exhausted(message),
            Self::Unimplemented => tonic::Status::unimplemented(message),
            Self::Internal => tonic::Status::internal(message),
        }
    }
}

fn classify_df_error(err: &DataFusionError) -> GrpcErrorKind {
    match err {
        DataFusionError::Shared(err) => classify_df_error(err),
        DataFusionError::Context(_, err) => classify_df_error(err),
        DataFusionError::Diagnostic(_, err) => classify_df_error(err),
        DataFusionError::Collection(errors) => errors
            .first()
            .map(classify_df_error)
            .unwrap_or(GrpcErrorKind::Internal),
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Execution(_)
        | DataFusionError::Substrait(_) => GrpcErrorKind::InvalidArgument,
        DataFusionError::NotImplemented(_) => GrpcErrorKind::Unimplemented,
        DataFusionError::ResourcesExhausted(_) => GrpcErrorKind::ResourceExhausted,
        DataFusionError::ArrowError(_, _)
        | DataFusionError::IoError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::Internal(_)
        | DataFusionError::ExecutionJoin(_)
        | DataFusionError::External(_)
        | DataFusionError::Ffi(_) => GrpcErrorKind::Internal,
        _ => GrpcErrorKind::Internal,
    }
}

/// Converts a `DataFusionError` to the corresponding `tonic::Status`.
fn df_error_to_status(err: &DataFusionError) -> tonic::Status {
    classify_df_error(err).into_status(err.to_string())
}

/// Map a `SendableRecordBatchStream` into a pinned `Stream` of gRPC responses.
///
/// Each batch is encoded as Arrow IPC bytes via [`batch_to_ipc_bytes`] and
/// wrapped with `wrap`. Errors from the upstream stream are propagated as
/// `tonic::Status::internal`. No background task is spawned — the stream is
/// driven directly by tonic, so client disconnection cancels the future naturally
/// and panics in encoding are surfaced immediately to the caller.
fn map_batch_stream<R>(
    stream: SendableRecordBatchStream,
    wrap: impl Fn(Vec<u8>) -> R + Send + 'static,
) -> Pin<Box<dyn Stream<Item = Result<R, tonic::Status>> + Send>>
where
    R: Send + 'static,
{
    Box::pin(stream.map(move |result| match result {
        Ok(batch) => batch_to_ipc_bytes(&batch).map(&wrap),
        Err(err) => Err(df_error_to_status(&err)),
    }))
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
    type ExecuteSubstraitStream =
        Pin<Box<dyn Stream<Item = Result<ExecuteSubstraitResponse, tonic::Status>> + Send>>;
    type ExecuteSqlStream =
        Pin<Box<dyn Stream<Item = Result<ExecuteSqlResponse, tonic::Status>> + Send>>;

    async fn execute_substrait(
        &self,
        request: tonic::Request<ExecuteSubstraitRequest>,
    ) -> Result<tonic::Response<Self::ExecuteSubstraitStream>, tonic::Status> {
        let req = request.into_inner();
        let service = Arc::clone(&self.service);

        // Route to the appropriate DataFusionService method:
        // - substrait_plan_bytes: production path (pre-encoded protobuf)
        // - substrait_plan_json:  dev/tooling path (grpcurl, rollup JSON files)
        // When `explain` is set, the server returns the EXPLAIN output
        // (no storage I/O) instead of executing the plan.
        let stream = match (
            !req.substrait_plan_bytes.is_empty(),
            !req.substrait_plan_json.is_empty(),
            req.explain,
        ) {
            (true, _, false) => service
                .execute_substrait(&req.substrait_plan_bytes, &req.properties)
                .await
                .map_err(|err| df_error_to_status(&err))?,
            (true, _, true) => service
                .explain_substrait(&req.substrait_plan_bytes, &req.properties)
                .await
                .map_err(|err| df_error_to_status(&err))?,
            (false, true, false) => service
                .execute_substrait_json(&req.substrait_plan_json, &req.properties)
                .await
                .map_err(|err| df_error_to_status(&err))?,
            (false, true, true) => service
                .explain_substrait_json(&req.substrait_plan_json, &req.properties)
                .await
                .map_err(|err| df_error_to_status(&err))?,
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "either substrait_plan_bytes or substrait_plan_json must be set",
                ));
            }
        };

        let response_stream = map_batch_stream(stream, |ipc_bytes| ExecuteSubstraitResponse {
            arrow_ipc_bytes: ipc_bytes,
        });
        Ok(tonic::Response::new(response_stream))
    }

    async fn execute_sql(
        &self,
        request: tonic::Request<ExecuteSqlRequest>,
    ) -> Result<tonic::Response<Self::ExecuteSqlStream>, tonic::Status> {
        let req = request.into_inner();
        let service = Arc::clone(&self.service);

        let stream = service
            .execute_sql(&req.sql, &req.properties)
            .await
            .map_err(|err| {
                warn!(error = %err, "DataFusion SQL execution error");
                df_error_to_status(&err)
            })?;

        let response_stream = map_batch_stream(stream, |ipc_bytes| ExecuteSqlResponse {
            arrow_ipc_bytes: ipc_bytes,
        });
        Ok(tonic::Response::new(response_stream))
    }
}
