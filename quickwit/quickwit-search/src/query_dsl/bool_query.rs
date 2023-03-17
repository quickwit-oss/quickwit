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

use quickwit_doc_mapper::DocMapper;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tantivy::query::{BooleanQuery, BoostQuery, Occur};

use crate::query_dsl::build_tantivy_query::BuildTantivyQuery;
use crate::query_dsl::QueryDsl;
use crate::TantivyQuery;

/// # Unsupported features
/// - minimum_should_match
/// - named queries
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BoolQuery {
    #[serde(default, deserialize_with = "deserialize_query_dsl_vec")]
    pub must: Vec<QueryDsl>,
    #[serde(default, deserialize_with = "deserialize_query_dsl_vec")]
    pub must_not: Vec<QueryDsl>,
    #[serde(default, deserialize_with = "deserialize_query_dsl_vec")]
    pub should: Vec<QueryDsl>,
    #[serde(default, deserialize_with = "deserialize_query_dsl_vec")]
    pub filter: Vec<QueryDsl>,
    boost: Option<NotNaNf32>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum QueryDslOneOrVec {
    Single(QueryDsl),
    List(Vec<QueryDsl>),
    // TODO Do we need to support null?
}

impl Into<Vec<QueryDsl>> for QueryDslOneOrVec {
    fn into(self) -> Vec<QueryDsl> {
        match self {
            QueryDslOneOrVec::Single(query_dsl) => vec![query_dsl],
            QueryDslOneOrVec::List(query_dsls) => query_dsls,
        }
    }
}

// This method is a trick to accept both a list of `[query_dsl]` or a singleto `{ query_dsl }` when
// deserializing a `Vec<QueryDsl>`.
fn deserialize_query_dsl_vec<'de, D>(deserializer: D) -> Result<Vec<QueryDsl>, D::Error>
where D: Deserializer<'de> {
    Ok(QueryDslOneOrVec::deserialize(deserializer)?.into())
}

#[derive(Serialize, Debug, PartialEq, Eq)]
#[serde(transparent)]
pub struct QueryVec {
    els: Vec<QueryDsl>,
}

struct QueryVecVisitor;

impl<'de> Visitor<'de> for QueryVecVisitor {
    type Value = QueryVec;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of queries")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where A: SeqAccess<'de> {
        let mut els = Vec::new();
        while let Some(el) = seq.next_element()? {
            els.push(el);
        }
        Ok(QueryVec { els })
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where A: MapAccess<'de> {
        let mut obj: serde_json::Map<String, Value> = Default::default();
        while let Some((key, value)) = map.next_entry()? {
            obj.insert(key, value);
        }
        let query_dsl: QueryDsl =
            serde_json::from_value(Value::Object(obj)).map_err(serde::de::Error::custom)?;
        Ok(QueryVec {
            els: vec![query_dsl],
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(into = "f32", try_from = "f32")]
struct NotNaNf32(f32);

impl Into<f32> for NotNaNf32 {
    fn into(self) -> f32 {
        self.0
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
        if let Some(boost) = self.boost {
            Ok(Box::new(BoostQuery::new(bool_query, boost.0)))
        } else {
            Ok(bool_query)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_dsl::bool_query::BoolQuery;
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
                boost: None
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
                boost: None
            }
        );
    }
}
