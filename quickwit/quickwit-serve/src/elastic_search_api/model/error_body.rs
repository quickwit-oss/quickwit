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

use elasticsearch_dsl::search::ErrorCause;
use quickwit_proto::ServiceError;
use quickwit_search::SearchError;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ElasticSearchError {
    pub status: u16,
    pub error: ErrorCause,
}

impl ElasticSearchError {
    pub fn new(status: u16, error: anyhow::Error) -> Self {
        let reason = ErrorCause {
            caused_by: None,
            reason: Some(error.to_string()),
            root_cause: vec![],
            stack_trace: None,
            suppressed: vec![],
            ty: None,
            additional_details: Default::default(),
        };
        ElasticSearchError {
            status,
            error: reason,
        }
    }
}

impl From<SearchError> for ElasticSearchError {
    fn from(search_error: SearchError) -> Self {
        let status = search_error.status_code().to_http_status_code().as_u16();
        let reason = ErrorCause {
            caused_by: None,
            reason: Some(search_error.to_string()),
            root_cause: vec![],
            stack_trace: None,
            suppressed: vec![],
            ty: None,
            additional_details: Default::default(),
        };
        ElasticSearchError {
            status,
            error: reason,
        }
    }
}
