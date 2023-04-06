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

use serde::{Deserialize, Serialize};
use serde_with::formats::PreferMany;
use serde_with::{serde_as, OneOrMany};

use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::quickwit_query_ast::{self, QueryAst};

/// # Unsupported features
/// - minimum_should_match
/// - named queries
#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct BoolQuery {
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    must: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    must_not: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    should: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    filter: Vec<ElasticQueryDslInner>,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
}

fn convert_vec(
    query_dsls: Vec<ElasticQueryDslInner>,
    default_search_fields: &[&str],
) -> anyhow::Result<Vec<QueryAst>> {
    query_dsls
        .into_iter()
        .map(|query_dsl| query_dsl.convert_to_query_ast(default_search_fields))
        .collect()
}

impl ConvertableToQueryAst for BoolQuery {
    fn convert_to_query_ast(self, default_search_fields: &[&str]) -> anyhow::Result<QueryAst> {
        let bool_query_ast = quickwit_query_ast::BoolQuery {
            must: convert_vec(self.must, default_search_fields)?,
            must_not: convert_vec(self.must_not, default_search_fields)?,
            should: convert_vec(self.should, default_search_fields)?,
            filter: convert_vec(self.filter, default_search_fields)?,
        };
        Ok(bool_query_ast.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_query_dsl::bool_query::BoolQuery;
    use crate::elastic_query_dsl::term_query::TermQuery;

    #[test]
    fn test_dsl_bool_query_deserialize_simple() {
        let bool_query_json = r#"{
            "must": [
                { "term": {"product_id": {"value": "1" }} },
                { "term": {"product_id": {"value": "2" }} }
            ]
        }"#;
        let bool_query: BoolQuery = serde_json::from_str(bool_query_json).unwrap();
        assert_eq!(
            &bool_query,
            &BoolQuery {
                must: vec![
                    TermQuery::from_field_value("product_id", "1").into(),
                    TermQuery::from_field_value("product_id", "2").into(),
                ],
                must_not: Vec::new(),
                should: Vec::new(),
                filter: Vec::new(),
                boost: None,
            }
        );
    }

    #[test]
    fn test_dsl_query_single() {
        let bool_query_json = r#"{
            "must": { "term": {"product_id": {"value": "1" }} },
            "filter": { "term": {"product_id": {"value": "2" }} }
        }"#;
        let bool_query: BoolQuery = serde_json::from_str(bool_query_json).unwrap();
        assert_eq!(
            &bool_query,
            &BoolQuery {
                must: vec![TermQuery::from_field_value("product_id", "1").into(),],
                must_not: Vec::new(),
                should: Vec::new(),
                filter: vec![TermQuery::from_field_value("product_id", "2").into(),],
                boost: None,
            }
        );
    }
}
