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

use std::fmt;

use quickwit_actors::AskError;

// Service errors have to be handwritten before codegen.
#[derive(Debug, thiserror::Error)]
pub enum HelloError {
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::Status),
}

// Service errors must implement `From<tonic::Status>` and `Into<tonic::Status>`.
impl From<HelloError> for tonic::Status {
    fn from(error: HelloError) -> Self {
        match error {
            HelloError::InternalError(message) => tonic::Status::internal(message),
            HelloError::TransportError(status) => status,
        }
    }
}

impl<E> From<AskError<E>> for HelloError
where E: fmt::Debug
{
    fn from(error: AskError<E>) -> Self {
        HelloError::InternalError(format!("{error:?}"))
    }
}
