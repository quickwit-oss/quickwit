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

use quickwit_doc_mapper::DocMapper;
use serde::{Deserialize, Serialize};
use serde_with::formats::PreferMany;
use serde_with::{serde_as, OneOrMany};
use tantivy::query::{BooleanQuery, BoostQuery, Occur};

use crate::query_dsl::build_tantivy_query::BuildTantivyQuery;
use crate::query_dsl::QueryDsl;
use crate::TantivyQuery;

const DEFAULT_BOOST: NotNaNf32 = NotNaNf32(1.0f32);

/// # Unsupported features
/// - minimum_should_match
/// - named queries
#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BoolQuery {
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    must: Vec<QueryDsl>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    must_not: Vec<QueryDsl>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    should: Vec<QueryDsl>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    filter: Vec<QueryDsl>,
    #[serde(default = "default_boost")]
    boost: NotNaNf32,
}

fn default_boost() -> NotNaNf32 {
    DEFAULT_BOOST
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(into = "f32", try_from = "f32")]
struct NotNaNf32(f32);

impl From<NotNaNf32> for f32 {
    fn from(not_nan_f32: NotNaNf32) -> f32 {
        not_nan_f32.0
    }
}

impl TryFrom<f32> for NotNaNf32 {
    type Error = &'static str;

    fn try_from(possibly_nan: f32) -> Result<NotNaNf32, &'static str> {
        if possibly_nan.is_nan() {
            return Err("NaN is not supported as a boost value.");
        }
        Ok(NotNaNf32(possibly_nan))
    }
}

impl Eq for NotNaNf32 {}

impl BuildTantivyQuery for BoolQuery {
    fn build_tantivy_query(
        &self,
        doc_mapper: &dyn DocMapper,
    ) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
        let mut clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
        for must in &self.must {
            let must_leaf = must.build_tantivy_query(doc_mapper)?;
            clauses.push((Occur::Must, must_leaf));
        }
        for must_not in &self.must_not {
            let must_not_leaf = must_not.build_tantivy_query(doc_mapper)?;
            clauses.push((Occur::MustNot, must_not_leaf));
        }
        for should in &self.should {
            let must_not_leaf = should.build_tantivy_query(doc_mapper)?;
            clauses.push((Occur::MustNot, must_not_leaf));
        }
        for filter in &self.filter {
            let filter_leaf = filter.build_tantivy_query(doc_mapper)?;
            clauses.push((Occur::Must, Box::new(BoostQuery::new(filter_leaf, 0.0f32))));
        }
        let bool_query = Box::new(BooleanQuery::from(clauses));
        if self.boost == DEFAULT_BOOST {
            return Ok(bool_query);
        }
        Ok(Box::new(BoostQuery::new(bool_query, self.boost.0)))
    }
}

#[cfg(test)]
mod tests {
    use crate::query_dsl::bool_query::{BoolQuery, DEFAULT_BOOST};
    use crate::query_dsl::term_query::TermQuery;

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
                boost: DEFAULT_BOOST,
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
                boost: DEFAULT_BOOST,
            }
        );
    }
}
