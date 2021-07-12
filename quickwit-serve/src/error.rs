/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use serde::ser::SerializeMap;
use thiserror::Error;
use warp::http;
use warp::hyper::StatusCode;

use quickwit_search::SearchError;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("InvalidArgument: {0}.")]
    InvalidArgument(String),
    // TODO rely on some an error serialization in the messsage
    // to rebuild a structured SearchError, (the tonic Code is pretty useless)
    // and build a meaningful ApiError instead of this
    // silly wrapping.
    #[error("Search error. {0}.")]
    SearchError(#[from] SearchError),
    #[error("Route not found")]
    NotFound,
}

impl ApiError {
    pub fn http_status_code(&self) -> http::StatusCode {
        match &self {
            ApiError::SearchError(search_error) => match search_error {
                SearchError::IndexDoesNotExist { .. } => http::StatusCode::NOT_FOUND,
                SearchError::InternalError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
                SearchError::StorageResolverError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
                SearchError::InvalidQuery(_) => http::StatusCode::BAD_REQUEST,
            },
            ApiError::InvalidArgument(_err) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => http::StatusCode::NOT_FOUND,
        }
    }

    pub fn message(&self) -> String {
        // TODO fixme
        format!("{}", self)
    }
}

// TODO implement nicer serialization of errors.
impl serde::Serialize for ApiError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_key("error")?;
        map.serialize_value(&self.message())?;
        map.end()
    }
}
