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

use serde::{Deserialize, Serialize};

mod bool_query;
mod exists_query;
mod match_bool_prefix;
mod match_phrase_query;
mod match_query;
mod multi_match;
mod one_field_map;
mod phrase_prefix_query;
mod prefix_query;
mod query_string_query;
mod range_query;
mod regex_query;
mod string_or_struct;
mod term_query;
mod terms_query;
mod wildcard_query;

use bool_query::BoolQuery;
pub use one_field_map::OneFieldMap;
use phrase_prefix_query::MatchPhrasePrefixQuery;
use prefix_query::PrefixQuery;
pub(crate) use query_string_query::QueryStringQuery;
use range_query::RangeQuery;
pub(crate) use string_or_struct::StringOrStructForSerialization;
use term_query::TermQuery;

use crate::elastic_query_dsl::exists_query::ExistsQuery;
use crate::elastic_query_dsl::match_bool_prefix::MatchBoolPrefixQuery;
use crate::elastic_query_dsl::match_phrase_query::MatchPhraseQuery;
use crate::elastic_query_dsl::match_query::MatchQuery;
use crate::elastic_query_dsl::multi_match::MultiMatchQuery;
use crate::elastic_query_dsl::regex_query::RegexQuery;
use crate::elastic_query_dsl::terms_query::TermsQuery;
use crate::elastic_query_dsl::wildcard_query::WildcardQuery;
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::QueryAst;

/// Quickwit and Elasticsearch have different interpretations of leniency:
/// - In Quickwit, lenient mode allows ignoring parts of the query that reference non-existing
///   columns. This is a behavior that Elasticsearch supports by default.
/// - In Elasticsearch, lenient mode primarily addresses type errors (such as searching for text in
///   an integer field). Quickwit always supports this behavior, regardless of the `lenient`
///   setting.
pub type LeniencyBool = bool;

fn default_max_expansions() -> u32 {
    50
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatchAllQuery {
    pub boost: Option<NotNaNf32>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Copy)]
pub(crate) struct MatchNoneQuery;

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ElasticQueryDslInner {
    QueryString(QueryStringQuery),
    Bool(BoolQuery),
    Term(TermQuery),
    Terms(TermsQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
    Match(MatchQuery),
    MatchBoolPrefix(MatchBoolPrefixQuery),
    MatchPhrase(MatchPhraseQuery),
    MatchPhrasePrefix(MatchPhrasePrefixQuery),
    MultiMatch(MultiMatchQuery),
    Range(RangeQuery),
    Exists(ExistsQuery),
    Regexp(RegexQuery),
    Wildcard(WildcardQuery),
    Prefix(PrefixQuery),
}

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(transparent)]
pub struct ElasticQueryDsl(ElasticQueryDslInner);

impl TryFrom<ElasticQueryDsl> for QueryAst {
    type Error = anyhow::Error;

    fn try_from(es_dsl: ElasticQueryDsl) -> anyhow::Result<Self> {
        es_dsl.0.convert_to_query_ast()
    }
}

pub(crate) trait ConvertibleToQueryAst {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst>;
}

impl ConvertibleToQueryAst for ElasticQueryDslInner {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        match self {
            Self::QueryString(query_string_query) => query_string_query.convert_to_query_ast(),
            Self::Bool(bool_query) => bool_query.convert_to_query_ast(),
            Self::Term(term_query) => term_query.convert_to_query_ast(),
            Self::Terms(terms_query) => terms_query.convert_to_query_ast(),
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
            Self::MatchBoolPrefix(match_bool_prefix_query) => {
                match_bool_prefix_query.convert_to_query_ast()
            }
            Self::MatchPhrase(match_phrase_query) => match_phrase_query.convert_to_query_ast(),
            Self::MatchPhrasePrefix(match_phrase_prefix) => {
                match_phrase_prefix.convert_to_query_ast()
            }
            Self::Range(range_query) => range_query.convert_to_query_ast(),
            Self::Match(match_query) => match_query.convert_to_query_ast(),
            Self::Exists(exists_query) => exists_query.convert_to_query_ast(),
            Self::MultiMatch(multi_match_query) => multi_match_query.convert_to_query_ast(),
            Self::Regexp(regex_query) => regex_query.convert_to_query_ast(),
            Self::Wildcard(wildcard_query) => wildcard_query.convert_to_query_ast(),
            Self::Prefix(prefix_query) => prefix_query.convert_to_query_ast(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elastic_query_dsl::term_query::term_query_from_field_value;

    #[test]
    fn test_query_dsl_deserialize_simple() {
        let term_query_json = r#"{
            "term": {
                "product_id": { "value": "61809" }
            }
        }"#;
        let query_dsl = serde_json::from_str(term_query_json).unwrap();
        let ElasticQueryDsl(ElasticQueryDslInner::Term(term_query)) = query_dsl else {
            panic!()
        };
        assert_eq!(
            &term_query,
            &term_query_from_field_value("product_id", "61809")
        );
    }
}
