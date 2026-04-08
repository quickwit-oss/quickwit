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

//! Parquet ingest processor for Arrow IPC to RecordBatch conversion.

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tracing::{debug, instrument, warn};

use crate::metrics::PARQUET_ENGINE_METRICS;
use crate::schema::validate_required_fields;

/// Error type for ingest operations.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// DocBatchV2 is not in ArrowIpc format.
    #[error("Invalid format: expected ArrowIpc format")]
    InvalidFormat,

    /// Arrow IPC deserialization failed.
    #[error("IPC decode error: {0}")]
    IpcDecode(#[from] arrow::error::ArrowError),

    /// RecordBatch schema validation failed.
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),

    /// Expected exactly one RecordBatch in IPC stream.
    #[error("Expected 1 RecordBatch in IPC stream, got {0}")]
    UnexpectedBatchCount(usize),

    /// Storage error during split writing.
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::ParquetWriteError),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Processor that converts Arrow IPC bytes to RecordBatch.
///
/// Validates that the decoded batch contains all required fields.
pub struct ParquetIngestProcessor;

impl ParquetIngestProcessor {
    /// Convert Arrow IPC bytes to RecordBatch.
    ///
    /// Returns error if IPC is malformed or schema doesn't match.
    #[instrument(skip(self, ipc_bytes), fields(bytes_len = ipc_bytes.len()))]
    pub fn process_ipc(&self, ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
        // Record bytes ingested
        PARQUET_ENGINE_METRICS
            .ingest_bytes_total
            .with_label_values(["points"])
            .inc_by(ipc_bytes.len() as u64);

        let batch = match ipc_to_record_batch(ipc_bytes) {
            Ok(batch) => batch,
            Err(e) => {
                PARQUET_ENGINE_METRICS
                    .errors_total
                    .with_label_values(["ingest", "points"])
                    .inc();
                return Err(e);
            }
        };

        if let Err(e) = self.validate_schema(&batch) {
            PARQUET_ENGINE_METRICS
                .errors_total
                .with_label_values(["ingest", "points"])
                .inc();
            return Err(e);
        }

        debug!(
            num_rows = batch.num_rows(),
            "successfully decoded IPC to RecordBatch"
        );
        Ok(batch)
    }

    /// Validate that the RecordBatch schema contains all required fields.
    fn validate_schema(&self, batch: &RecordBatch) -> Result<(), IngestError> {
        validate_required_fields(batch.schema().as_ref())
            .map_err(|e| IngestError::SchemaValidation(e.to_string()))
    }
}

/// Deserialize Arrow IPC stream format to a RecordBatch.
pub(crate) fn ipc_to_record_batch(ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    if batches.len() != 1 {
        return Err(IngestError::UnexpectedBatchCount(batches.len()));
    }

    Ok(batches
        .into_iter()
        .next()
        .expect("len verified to be 1 above"))
}

/// Serialize a RecordBatch to Arrow IPC stream format.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, IngestError> {
    use arrow::ipc::writer::StreamWriter;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::create_test_batch;

    #[test]
    fn test_process_ipc() {
        let processor = ParquetIngestProcessor;

        // Create a valid batch
        let batch = create_test_batch(10);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();

        // Process the IPC bytes
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_ok());

        let recovered = result.unwrap();
        assert_eq!(recovered.num_rows(), 10);
        assert_eq!(recovered.num_columns(), 6); // 4 required + 2 tags
    }

    #[test]
    fn test_process_ipc_invalid_bytes() {
        let processor = ParquetIngestProcessor;

        let result = processor.process_ipc(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_ipc_round_trip() {
        let batch = create_test_batch(5);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let recovered = ipc_to_record_batch(&ipc_bytes).unwrap();

        assert_eq!(recovered.num_rows(), batch.num_rows());
        assert_eq!(recovered.num_columns(), batch.num_columns());
        assert_eq!(recovered.schema(), batch.schema());
    }
}
