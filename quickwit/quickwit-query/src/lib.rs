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

//! QueryDSL partially compatible with Elasticsearch/Opensearch QueryDSL.
//! See documentation here:
//! <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>

// As you add queries in this file please insert it in the order of the OpenSearch 2.6
// documentation (the opensearch documentation has a nicer structure than that of ES).
// https://opensearch.org/docs/2.6/query-dsl/term/
//
// For the individual detailed API documentation however, you should refer to elastic
// documentation.

pub mod aggregations;
mod elastic_query_dsl;
mod error;
mod json_literal;
mod not_nan_f32;
pub mod query_ast;
pub mod tokenizers;

pub use elastic_query_dsl::{ElasticQueryDsl, OneFieldMap};
pub use error::InvalidQuery;
pub use json_literal::{InterpretUserInput, JsonLiteral};
pub(crate) use not_nan_f32::NotNaNf32;
pub use query_ast::utils::find_field_or_hit_dynamic;
use serde::{Deserialize, Serialize};
pub use tantivy::query::Query as TantivyQuery;
#[cfg(feature = "multilang")]
pub use tokenizers::MultiLangTokenizer;
pub use tokenizers::{
    CodeTokenizer, DEFAULT_REMOVE_TOKEN_LENGTH, create_default_quickwit_tokenizer_manager,
    get_quickwit_fastfield_normalizer_manager,
};

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
