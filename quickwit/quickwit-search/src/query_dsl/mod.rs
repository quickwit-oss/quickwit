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

// Full-text queries

// Term-level queries
mod term_query;

// Compound queries
mod bool_query;

mod build_tantivy_query;

use quickwit_doc_mapper::DocMapper;
use serde::{Deserialize, Serialize};

use crate::query_dsl::bool_query::BoolQuery;
use crate::query_dsl::build_tantivy_query::BuildTantivyQuery;
use crate::query_dsl::term_query::TermQuery;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryDsl {
    Bool(BoolQuery),
    Term(TermQuery),
}

impl From<TermQuery> for QueryDsl {
    fn from(term_query: TermQuery) -> Self {
        QueryDsl::Term(term_query)
    }
}

impl From<BoolQuery> for QueryDsl {
    fn from(bool_query: BoolQuery) -> Self {
        QueryDsl::Bool(bool_query)
    }
}

impl BuildTantivyQuery for QueryDsl {
    fn build_tantivy_query(
        &self,
        doc_mapper: &dyn DocMapper,
    ) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
        match self {
            QueryDsl::Bool(bool_query) => bool_query.build_tantivy_query(doc_mapper),
            QueryDsl::Term(term_query) => term_query.build_tantivy_query(doc_mapper),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_dsl::term_query::TermQuery;
    use crate::query_dsl::QueryDsl;

    #[test]
    fn test_query_dsl_deserialize_simple() {
        let term_query_json = r#"{
            "term": {
                "product_id": { "value": "61809" }
            }
        }"#;
        let query_dsl = serde_json::from_str(term_query_json).unwrap();
        let QueryDsl::Term(term_query) = query_dsl else { panic!() };
        assert_eq!(
            &term_query,
            &TermQuery::from_field_value("product_id", "61809")
        );
    }
}
