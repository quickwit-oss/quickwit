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

//! QueryDSL partially compatible with Elasticsearch/Opensearch QueryDSL.
//! See documentation here:
//! <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>

// As you add queries in this file please insert it in the order of the OpenSearch 2.6
// documentation (the opensearch documentation has a nicer structure than that of ES).
// https://opensearch.org/docs/2.6/query-dsl/term/
//
// For the individual detailed API documentation however, you should refer to elastic
// documentation.

mod elastic_query_dsl;
mod json_literal;
pub mod query_ast;
mod tokenizers;

mod error;
mod not_nan_f32;

pub use elastic_query_dsl::{ElasticQueryDsl, OneFieldMap};
pub use error::InvalidQuery;
pub use json_literal::{InterpretUserInput, JsonLiteral};
pub(crate) use not_nan_f32::NotNaNf32;
pub use query_ast::utils::find_field_or_hit_dynamic;
use serde::{Deserialize, Serialize};
pub use tantivy::query::Query as TantivyQuery;
pub use tokenizers::{get_quickwit_fastfield_normalizer_manager, get_quickwit_tokenizer_manager};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, Eq, PartialEq)]
pub enum BooleanOperand {
    #[serde(alias = "AND")]
    And,
    #[default]
    #[serde(alias = "OR")]
    Or,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Default)]
pub enum MatchAllOrNone {
    #[serde(rename = "none")]
    #[default]
    MatchNone,
    #[serde(rename = "all")]
    MatchAll,
}

impl MatchAllOrNone {
    pub fn is_none(&self) -> bool {
        self == &MatchAllOrNone::MatchNone
    }
}
