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

mod bool_query;
mod query_string_query;
mod range_query;
mod term_query;

use bool_query::BoolQuery;
pub(crate) use query_string_query::QueryStringQuery;
use range_query::RangeQuery;
use term_query::TermQuery;

use crate::not_nan_f32::NotNaNf32;
use crate::quickwit_query_ast::QueryAst;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Copy, Default)]
struct MatchAllQuery {
    pub boost: Option<NotNaNf32>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Copy)]
struct MatchNoneQuery;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
enum ElasticQueryDslInner {
    QueryString(QueryStringQuery),
    Bool(BoolQuery),
    Term(TermQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
    Range(RangeQuery),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(transparent)]
pub struct ElasticQueryDsl(ElasticQueryDslInner);

impl ElasticQueryDsl {
    pub fn convert_to_query_ast(self, default_search_fields: &[&str]) -> anyhow::Result<QueryAst> {
        self.0.convert_to_query_ast(default_search_fields)
    }
}

pub(crate) trait ConvertableToQueryAst {
    fn convert_to_query_ast(self, default_search_fields: &[&str]) -> anyhow::Result<QueryAst>;
}

impl ConvertableToQueryAst for ElasticQueryDslInner {
    fn convert_to_query_ast(self, default_search_fields: &[&str]) -> anyhow::Result<QueryAst> {
        match self {
            Self::QueryString(query_string_query) => {
                query_string_query.convert_to_query_ast(default_search_fields)
            }
            Self::Bool(bool_query) => bool_query.convert_to_query_ast(default_search_fields),
            Self::Term(term_query) => term_query.convert_to_query_ast(default_search_fields),
            Self::MatchAll(match_all_query) => {
                if let Some(boost) = match_all_query.boost {
                    Ok(QueryAst::Boost {
                        boost,
                        underlying: Box::new(QueryAst::MatchAll),
                    })
                } else {
                    Ok(QueryAst::MatchAll)
                }
            }
            Self::MatchNone(_) => Ok(QueryAst::MatchNone),
            Self::Range(range_query) => range_query.convert_to_query_ast(default_search_fields),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_dsl_deserialize_simple() {
        let term_query_json = r#"{
            "term": {
                "product_id": { "value": "61809" }
            }
        }"#;
        let query_dsl = serde_json::from_str(term_query_json).unwrap();
        let ElasticQueryDsl(ElasticQueryDslInner::Term(term_query)) = query_dsl else { panic!() };
        assert_eq!(
            &term_query,
            &TermQuery::from_field_value("product_id", "61809")
        );
    }
}
