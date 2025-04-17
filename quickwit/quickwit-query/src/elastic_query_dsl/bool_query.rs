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

use serde::Deserialize;
use serde_with::formats::PreferMany;
use serde_with::{DefaultOnNull, OneOrMany, serde_as};

use crate::elastic_query_dsl::{ConvertibleToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::{self, QueryAst};

/// # Unsupported features
/// - minimum_should_match
/// - named queries
#[serde_as]
#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct BoolQuery {
    #[serde_as(deserialize_as = "DefaultOnNull<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    must: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "DefaultOnNull<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    must_not: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "DefaultOnNull<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    should: Vec<ElasticQueryDslInner>,
    #[serde_as(deserialize_as = "DefaultOnNull<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    filter: Vec<ElasticQueryDslInner>,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
    #[serde(default)]
    pub minimum_should_match: Option<MinimumShouldMatch>,
}

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(untagged)]
pub enum MinimumShouldMatch {
    Str(String),
    Int(isize),
}

impl MinimumShouldMatch {
    fn resolve(&self, num_should_clauses: usize) -> anyhow::Result<MinimumShouldMatchResolved> {
        match self {
            MinimumShouldMatch::Str(minimum_should_match_dsl) => {
                let Some(percentage) = parse_percentage(minimum_should_match_dsl) else {
                    anyhow::bail!(
                        "Unsupported minimum should match dsl {}. quickwit currently only \
                         supports the format '35%' and `-35%`",
                        minimum_should_match_dsl
                    );
                };
                let min_should_match = percentage * num_should_clauses as isize / 100;
                MinimumShouldMatch::Int(min_should_match).resolve(num_should_clauses)
            }
            MinimumShouldMatch::Int(neg_num_missing_should_clauses)
                if *neg_num_missing_should_clauses < 0 =>
            {
                let num_missing_should_clauses = -neg_num_missing_should_clauses as usize;
                if num_missing_should_clauses >= num_should_clauses {
                    Ok(MinimumShouldMatchResolved::Unspecified)
                } else {
                    Ok(MinimumShouldMatchResolved::Min(
                        num_should_clauses - num_missing_should_clauses,
                    ))
                }
            }
            MinimumShouldMatch::Int(num_required_should_clauses) => {
                let num_required_should_clauses: usize = *num_required_should_clauses as usize;
                if num_required_should_clauses > num_should_clauses {
                    Ok(MinimumShouldMatchResolved::NoMatch)
                } else {
                    Ok(MinimumShouldMatchResolved::Min(num_required_should_clauses))
                }
            }
        }
    }
}

#[derive(Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
enum MinimumShouldMatchResolved {
    Unspecified,
    Min(usize),
    NoMatch,
}

fn parse_percentage(s: &str) -> Option<isize> {
    let percentage_str = s.strip_suffix('%')?;
    let percentage_isize = percentage_str.parse::<isize>().ok()?;
    if percentage_isize.abs() > 100 {
        return None;
    }
    Some(percentage_isize)
}

impl BoolQuery {
    fn resolve_minimum_should_match(&self) -> anyhow::Result<MinimumShouldMatchResolved> {
        let num_should_clauses = self.should.len();
        let Some(minimum_should_match) = &self.minimum_should_match else {
            return Ok(MinimumShouldMatchResolved::Unspecified);
        };
        minimum_should_match.resolve(num_should_clauses)
    }
}

impl BoolQuery {
    // Combines a list of children queries into a boolean union.
    pub(crate) fn union(children: Vec<ElasticQueryDslInner>) -> BoolQuery {
        BoolQuery {
            must: Vec::new(),
            must_not: Vec::new(),
            should: children,
            filter: Vec::new(),
            boost: None,
            minimum_should_match: None,
        }
    }
}

fn convert_vec(query_dsls: Vec<ElasticQueryDslInner>) -> anyhow::Result<Vec<QueryAst>> {
    query_dsls
        .into_iter()
        .map(|query_dsl| query_dsl.convert_to_query_ast())
        .collect()
}

impl ConvertibleToQueryAst for BoolQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let minimum_should_match_resolved = self.resolve_minimum_should_match()?;
        let must = convert_vec(self.must)?;
        let must_not = convert_vec(self.must_not)?;
        let should = convert_vec(self.should)?;
        let filter = convert_vec(self.filter)?;

        let minimum_should_match_opt = match minimum_should_match_resolved {
            MinimumShouldMatchResolved::Unspecified => None,
            MinimumShouldMatchResolved::Min(minimum_should_match) => Some(minimum_should_match),
            MinimumShouldMatchResolved::NoMatch => {
                return Ok(QueryAst::MatchNone);
            }
        };
        let bool_query_ast = query_ast::BoolQuery {
            must,
            must_not,
            should,
            filter,
            minimum_should_match: minimum_should_match_opt,
        };
        Ok(bool_query_ast.into())
    }
}

impl From<BoolQuery> for ElasticQueryDslInner {
    fn from(bool_query: BoolQuery) -> Self {
        ElasticQueryDslInner::Bool(bool_query)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_percentage;
    use crate::elastic_query_dsl::ConvertibleToQueryAst;
    use crate::elastic_query_dsl::bool_query::{
        BoolQuery, MinimumShouldMatch, MinimumShouldMatchResolved,
    };
    use crate::elastic_query_dsl::term_query::term_query_from_field_value;
    use crate::query_ast::QueryAst;

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
                    term_query_from_field_value("product_id", "1").into(),
                    term_query_from_field_value("product_id", "2").into(),
                ],
                must_not: Vec::new(),
                should: Vec::new(),
                filter: Vec::new(),
                boost: None,
                minimum_should_match: None
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
                must: vec![term_query_from_field_value("product_id", "1").into(),],
                must_not: Vec::new(),
                should: Vec::new(),
                filter: vec![term_query_from_field_value("product_id", "2").into(),],
                boost: None,
                minimum_should_match: None,
            }
        );
    }

    #[test]
    fn test_dsl_query_with_null_values() {
        let bool_query_json = r#"{
            "must": null,
            "must_not": null,
            "should": null,
            "filter": null,
            "boost": null
        }"#;
        let bool_query: BoolQuery = serde_json::from_str(bool_query_json).unwrap();
        assert_eq!(
            &bool_query,
            &BoolQuery {
                must: Vec::new(),
                must_not: Vec::new(),
                should: Vec::new(),
                filter: Vec::new(),
                boost: None,
                minimum_should_match: None,
            }
        );
    }

    #[test]
    fn test_dsl_bool_query_deserialize_minimum_should_match() {
        let bool_query: super::BoolQuery = serde_json::from_str(
            r#"{
            "must": [
                { "term": {"product_id": {"value": "1" }} },
                { "term": {"product_id": {"value": "2" }} }
            ],
            "minimum_should_match": -2
        }"#,
        )
        .unwrap();
        assert_eq!(
            bool_query.minimum_should_match.as_ref().unwrap(),
            &MinimumShouldMatch::Int(-2)
        );
    }

    #[test]
    fn test_dsl_query_with_minimum_should_match() {
        let bool_query_json = r#"{
                "should": [
                    { "term": {"product_id": {"value": "1" }} },
                    { "term": {"product_id": {"value": "2" }} },
                    { "term": {"product_id": {"value": "3" }} }
                ],
                "minimum_should_match": 2
            }"#;
        let bool_query: BoolQuery = serde_json::from_str(bool_query_json).unwrap();
        assert_eq!(bool_query.should.len(), 3);
        assert_eq!(
            bool_query.minimum_should_match.as_ref().unwrap(),
            &super::MinimumShouldMatch::Int(2)
        );
        let QueryAst::Bool(bool_query_ast) = bool_query.convert_to_query_ast().unwrap() else {
            panic!();
        };
        assert_eq!(bool_query_ast.should.len(), 3);
        assert_eq!(bool_query_ast.minimum_should_match, Some(2));
    }

    #[test]
    fn test_parse_percentage() {
        assert_eq!(parse_percentage("10%"), Some(10));
        assert_eq!(parse_percentage("101%"), None);
        assert_eq!(parse_percentage("0%"), Some(0));
        assert_eq!(parse_percentage("100%"), Some(100));
        assert_eq!(parse_percentage("-20%"), Some(-20));
        assert_eq!(parse_percentage("20"), None);
        assert_eq!(parse_percentage("20a%"), None);
    }

    #[test]
    fn test_resolve_minimum_should_match() {
        assert_eq!(
            MinimumShouldMatch::Str("30%".to_string())
                .resolve(10)
                .unwrap(),
            MinimumShouldMatchResolved::Min(3)
        );
        // not supported yet
        assert_eq!(
            MinimumShouldMatch::Str("-30%".to_string())
                .resolve(10)
                .unwrap(),
            MinimumShouldMatchResolved::Min(7)
        );
        assert!(
            MinimumShouldMatch::Str("-30!".to_string())
                .resolve(10)
                .is_err()
        );
        assert_eq!(
            MinimumShouldMatch::Int(10).resolve(11).unwrap(),
            MinimumShouldMatchResolved::Min(10)
        );
        assert_eq!(
            MinimumShouldMatch::Int(-10).resolve(11).unwrap(),
            MinimumShouldMatchResolved::Min(1)
        );
        assert_eq!(
            MinimumShouldMatch::Int(-12).resolve(11).unwrap(),
            MinimumShouldMatchResolved::Unspecified
        );
        assert_eq!(
            MinimumShouldMatch::Int(12).resolve(11).unwrap(),
            MinimumShouldMatchResolved::NoMatch
        );
    }
}
