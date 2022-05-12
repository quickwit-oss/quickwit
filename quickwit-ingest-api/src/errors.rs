// Copyright (C) 2021 Quickwit, Inc.
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

use quickwit_proto::tonic;
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug, Serialize)]
pub enum IngestApiError {
    #[error("Rocks DB Error: {msg}.")]
    Corruption { msg: String },
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },
    #[error("Ingest API service is down")]
    IngestAPIServiceDown,
}

#[derive(Error, Debug)]
#[error("Key should contain 16 bytes. It contained {0} bytes.")]
pub struct CorruptedKey(pub usize);

impl From<rocksdb::Error> for IngestApiError {
    fn from(err: rocksdb::Error) -> Self {
        IngestApiError::Corruption {
            msg: format!("RocksDB error: {err:?}"),
        }
    }
}

impl From<CorruptedKey> for IngestApiError {
    fn from(err: CorruptedKey) -> Self {
        IngestApiError::Corruption {
            msg: format!("CorruptedKey: {err:?}"),
        }
    }
}

impl From<IngestApiError> for tonic::Status {
    fn from(error: IngestApiError) -> tonic::Status {
        let code = match &error {
            IngestApiError::Corruption { .. } => tonic::Code::Internal,
            IngestApiError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestApiError::IngestAPIServiceDown => tonic::Code::Internal,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

pub type Result<T> = std::result::Result<T, IngestApiError>;
