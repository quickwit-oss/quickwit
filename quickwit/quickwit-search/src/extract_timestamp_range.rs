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

use std::ops::Bound;

use quickwit_query::query_ast::{
    BoolQuery, QueryAst, QueryAstTransformer, RangeQuery, TermQuery, TermSetQuery,
};
use tantivy::DateTime;
use tracing::warn;

const NANOS_IN_SEC: i64 = 1_000_000_000;

/// Extract timestamp range in QueryAst.
///
/// This can allow MatchAll queries (post timestamp range removal) to be optimized.
pub(crate) fn extract_start_end_timestamp_from_ast(
    query_ast: QueryAst,
    timestamp_field: &str,
    start_timestamp: &mut Option<i64>,
    end_timestamp: &mut Option<i64>,
) -> QueryAst {
    let mut timestamp_range_extractor = ExtractTimestampRange {
        timestamp_field,
        start_timestamp: start_timestamp
            .map(|t| DateTime::from_timestamp_nanos(t.saturating_mul(NANOS_IN_SEC)))
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded),
        end_timestamp: end_timestamp
            .map(|t| DateTime::from_timestamp_nanos(t.saturating_mul(NANOS_IN_SEC)))
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded),
    };

    let new_ast = timestamp_range_extractor
        .transform(query_ast)
        .expect("can't fail unwrapping Infallible")
        .unwrap_or(QueryAst::MatchAll);

    *start_timestamp = match timestamp_range_extractor.start_timestamp {
        Bound::Included(x) => Some(x.into_timestamp_secs()),
        Bound::Excluded(x) => Some(x.into_timestamp_secs().saturating_add(1)),
        Bound::Unbounded => None,
    };

    *end_timestamp = match timestamp_range_extractor.end_timestamp {
        Bound::Included(x) => Some(x.into_timestamp_secs().saturating_add(1)),
        Bound::Excluded(x) => {
            let round_up = (x.into_timestamp_nanos() % NANOS_IN_SEC) != 0;
            if round_up {
                Some(x.into_timestamp_secs().saturating_add(1))
            } else {
                Some(x.into_timestamp_secs())
            }
        }
        Bound::Unbounded => None,
    };

    new_ast
}

/// Boundaries identified as being implied by the QueryAst.
///
/// `start_timestamp` is to be interpreted as Inclusive (or Unbounded)
/// `end_timestamp` is to be interpreted as Exclusive (or Unbounded)
/// In other word, this is a `[start_timestamp..end_timestamp)` interval.
#[derive(Debug, Clone)]
struct ExtractTimestampRange<'a> {
    timestamp_field: &'a str,
    start_timestamp: Bound<DateTime>,
    end_timestamp: Bound<DateTime>,
}

impl ExtractTimestampRange<'_> {
    fn update_start_timestamp(
        &mut self,
        lower_bound: &quickwit_query::JsonLiteral,
        included: bool,
    ) {
        use quickwit_query::InterpretUserInput;
        let Some(lower_bound) = DateTime::interpret_json(lower_bound) else {
            // we shouldn't be able to get here, we would have errored much earlier
            warn!("unparsable time bound in search: {lower_bound:?}");
            return;
        };
        let bound = if included {
            Bound::Included(lower_bound)
        } else {
            Bound::Excluded(lower_bound)
        };

        self.start_timestamp = max_bound(self.start_timestamp, bound);
    }

    fn update_end_timestamp(&mut self, upper_bound: &quickwit_query::JsonLiteral, included: bool) {
        use quickwit_query::InterpretUserInput;
        let Some(upper_bound) = DateTime::interpret_json(upper_bound) else {
            // we shouldn't be able to get here, we would have errored much earlier
            warn!("unparsable time bound in search: {upper_bound:?}");
            return;
        };
        let bound = if included {
            Bound::Included(upper_bound)
        } else {
            Bound::Excluded(upper_bound)
        };

        self.end_timestamp = min_bound(self.end_timestamp, bound);
    }
}

impl QueryAstTransformer for ExtractTimestampRange<'_> {
    type Err = std::convert::Infallible;

    fn transform_bool(&mut self, mut bool_query: BoolQuery) -> Result<Option<QueryAst>, Self::Err> {
        // we only want to visit sub-queries which are strict (positive) requirements
        bool_query.must = bool_query
            .must
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .filter(|result| result != &Ok(QueryAst::MatchAll))
            .collect::<Result<Vec<_>, _>>()?;
        bool_query.filter = bool_query
            .filter
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .filter(|result| result != &Ok(QueryAst::MatchAll))
            .collect::<Result<Vec<_>, _>>()?;

        let ast = if bool_query == BoolQuery::default() {
            QueryAst::MatchAll
        } else {
            QueryAst::Bool(bool_query)
        };

        Ok(Some(ast))
    }

    fn transform_range(&mut self, range_query: RangeQuery) -> Result<Option<QueryAst>, Self::Err> {
        use std::ops::Bound;

        if range_query.field != self.timestamp_field {
            return Ok(Some(range_query.into()));
        }

        match range_query.lower_bound {
            Bound::Included(lower_bound) => {
                self.update_start_timestamp(&lower_bound, true);
            }
            Bound::Excluded(lower_bound) => {
                self.update_start_timestamp(&lower_bound, false);
            }
            Bound::Unbounded => (),
        };

        match range_query.upper_bound {
            Bound::Included(upper_bound) => {
                self.update_end_timestamp(&upper_bound, true);
            }
            Bound::Excluded(upper_bound) => {
                self.update_end_timestamp(&upper_bound, false);
            }
            Bound::Unbounded => (),
        };

        Ok(Some(QueryAst::MatchAll))
    }

    // if we visit a term, limit the range to DATE..=DATE
    fn transform_term(&mut self, term_query: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
        if term_query.field != self.timestamp_field {
            return Ok(Some(term_query.into()));
        }

        // TODO when fixing #3323, this may need to be modified to support numbers too
        let json_term = quickwit_query::JsonLiteral::String(term_query.value.clone());
        self.update_start_timestamp(&json_term, true);
        self.update_end_timestamp(&json_term, true);
        Ok(Some(QueryAst::MatchAll))
    }

    // if we visit a termset, limit the range to LOWEST..=HIGHEST
    fn transform_term_set(
        &mut self,
        mut term_query: TermSetQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        let Some(term_set) = term_query.terms_per_field.remove(self.timestamp_field) else {
            return Ok(Some(term_query.into()));
        };

        // rfc3339 is lexicographically ordered if YEAR <= 9999, so we can use string
        // ordering to get the start and end quickly.
        if let Some(first) = term_set.first() {
            let json_term = quickwit_query::JsonLiteral::String(first.clone());
            self.update_start_timestamp(&json_term, true);
        }
        if let Some(last) = term_set.last() {
            let json_term = quickwit_query::JsonLiteral::String(last.clone());
            self.update_end_timestamp(&json_term, true);
        }

        let ast = if term_query.terms_per_field.is_empty() {
            QueryAst::MatchAll
        } else {
            term_query.into()
        };

        Ok(Some(ast))
    }
}

// returns the max of left and right, that isn't unbounded. Useful for making
// the intersection of lower bound of ranges
fn max_bound<T: Ord + Copy>(left: Bound<T>, right: Bound<T>) -> Bound<T> {
    use Bound::*;
    match (left, right) {
        (Unbounded, right) => right,
        (left, Unbounded) => left,
        (Included(left), Included(right)) => Included(left.max(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.max(right)),
        (excluded_total @ Excluded(excluded), included_total @ Included(included)) => {
            if included > excluded {
                included_total
            } else {
                excluded_total
            }
        }
        (included_total @ Included(included), excluded_total @ Excluded(excluded)) => {
            if included > excluded {
                included_total
            } else {
                excluded_total
            }
        }
    }
}

// returns the min of left and right, that isn't unbounded. Useful for making
// the intersection of upper bound of ranges
fn min_bound<T: Ord + Copy>(left: Bound<T>, right: Bound<T>) -> Bound<T> {
    use Bound::*;
    match (left, right) {
        (Unbounded, right) => right,
        (left, Unbounded) => left,
        (Included(left), Included(right)) => Included(left.min(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.min(right)),
        (excluded_total @ Excluded(excluded), included_total @ Included(included)) => {
            if included < excluded {
                included_total
            } else {
                excluded_total
            }
        }
        (included_total @ Included(included), excluded_total @ Excluded(excluded)) => {
            if included < excluded {
                included_total
            } else {
                excluded_total
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_timestamp_range_from_ast() {
        use std::ops::Bound;

        use quickwit_query::JsonLiteral;

        let timestamp_field = "timestamp";

        let simple_range: QueryAst = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Excluded(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();

        // direct range
        let mut start_timestamp = None;
        let mut end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                simple_range.clone(),
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618353941));
        assert_eq!(end_timestamp, Some(1620283879));

        // range inside a must bool query
        let bool_query_must = quickwit_query::query_ast::BoolQuery {
            must: vec![simple_range.clone()],
            ..Default::default()
        };
        start_timestamp = None;
        end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                bool_query_must.into(),
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618353941));
        assert_eq!(end_timestamp, Some(1620283879));

        // range inside a should bool query
        let bool_query_should: QueryAst = quickwit_query::query_ast::BoolQuery {
            should: vec![simple_range.clone()],
            ..Default::default()
        }
        .into();
        start_timestamp = Some(123);
        end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                bool_query_should.clone(),
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            bool_query_should
        );
        assert_eq!(start_timestamp, Some(123));
        assert_eq!(end_timestamp, None);

        // start bound was already more restrictive
        start_timestamp = Some(1618601297);
        end_timestamp = Some(i64::MAX);
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                simple_range.clone(),
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618601297));
        assert_eq!(end_timestamp, Some(1620283879));

        // end bound was already more restrictive
        start_timestamp = Some(1);
        end_timestamp = Some(1618601297);
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                simple_range,
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618353941));
        assert_eq!(end_timestamp, Some(1618601297));

        // bounds are (start..end] instead of [start..end)
        let unusual_bounds = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Excluded(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Included(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();
        start_timestamp = None;
        end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                unusual_bounds,
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618353942));
        assert_eq!(end_timestamp, Some(1620283880));

        let wrong_field: QueryAst = quickwit_query::query_ast::RangeQuery {
            field: "other_field".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Excluded(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();
        start_timestamp = None;
        end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                wrong_field.clone(),
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            wrong_field
        );
        assert_eq!(start_timestamp, None);
        assert_eq!(end_timestamp, None);

        let high_precision = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String(
                "2021-04-13T22:45:41.001Z".to_owned(),
            )),
            upper_bound: Bound::Excluded(JsonLiteral::String(
                "2021-05-06T06:51:19.001Z".to_owned(),
            )),
        }
        .into();

        // the upper bound should be rounded up as to includes documents from X.000 to X.001
        start_timestamp = None;
        end_timestamp = None;
        assert_eq!(
            extract_start_end_timestamp_from_ast(
                high_precision,
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            ),
            QueryAst::MatchAll
        );
        assert_eq!(start_timestamp, Some(1618353941));
        assert_eq!(end_timestamp, Some(1620283880));
    }
}
