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
use tantivy::fastfield::Column;
use tantivy::query::{ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED, TantivyError};

use super::tantivy_query_ast::TantivyQueryAst;
use super::{BuildTantivyAst, BuildTantivyAstContext, QueryAst};
use crate::InvalidQuery;

/// QueryAst node matching documents where `lo <= (V as u64) & mask < hi`.
///
/// Useful for probability-based sampling over integer fast fields: with sequential V and
/// `mask = 2^n - 1`, this selects 1-in-2^n documents in a round-robin pattern.
///
/// Use `sampling_params_range` to derive `(mask, lo, hi)` from a `[lo_f, hi_f)` probability range.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BitwiseMaskRangeQuery {
    pub field: String,
    /// Bitmask of the form `2^n - 1` applied to the field value (cast to u64).
    pub mask: u64,
    /// Inclusive lower bound on `(V as u64) & mask`.
    pub lo: u64,
    /// Exclusive upper bound on `(V as u64) & mask`.
    pub hi: u64,
}

impl From<BitwiseMaskRangeQuery> for QueryAst {
    fn from(query: BitwiseMaskRangeQuery) -> QueryAst {
        QueryAst::BitwiseMaskRange(query)
    }
}

impl BuildTantivyAst for BitwiseMaskRangeQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let Some((_field, field_entry, _json_path)) =
            super::utils::find_field_or_hit_dynamic(&self.field, context.schema)
        else {
            return Ok(TantivyQueryAst::match_none());
        };
        if !field_entry.is_fast() {
            return Err(InvalidQuery::SchemaError(format!(
                "bitwise mask range queries require a fast field (`{}` is not a fast field)",
                field_entry.name()
            )));
        }
        match field_entry.field_type() {
            tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {}
            _ => {
                return Err(InvalidQuery::SchemaError(format!(
                    "bitwise mask range queries require an integer fast field (`{}` is not an \
                     integer field)",
                    field_entry.name()
                )));
            }
        }
        Ok(TantivyQueryAst::Leaf(Box::new(
            BitwiseMaskRangeTantivyQuery {
                field_name: self.field.clone(),
                mask: self.mask,
                lo: self.lo,
                hi: self.hi,
            },
        )))
    }
}

/// Computes `(mask, lo, hi)` such that `lo <= (V as u64) & mask < hi` fires with probability
/// approximately `hi_f - lo_f`, with at most 3.125% relative overshoot.
///
/// `mask` is of the form `2^n - 1` with `n` as small as possible, capped at `max_bits`.
/// Pass `max_bits = 32` for full precision. Use a smaller value (e.g. 16 for i16 fields) to
/// avoid sampling bits that carry sign-extension noise rather than entropy.
///
/// # Panics
///
/// Panics in debug builds if `lo_f >= hi_f` or either is outside `[0.0, 1.0]`, or
/// `max_bits` is 0 or exceeds 63.
pub fn sampling_params_range(lo_f: f64, hi_f: f64, max_bits: u32) -> (u64, u64, u64) {
    debug_assert!(
        lo_f >= 0.0 && hi_f <= 1.0 && lo_f < hi_f,
        "lo_f={lo_f} hi_f={hi_f}: must satisfy 0 <= lo_f < hi_f <= 1"
    );
    debug_assert!(
        max_bits > 0 && max_bits <= 63,
        "max_bits={max_bits} must be in 1..=63"
    );
    let width = hi_f - lo_f;
    for n in 0u32..max_bits {
        let scale = (1u64 << n) as f64;
        let lo = (lo_f * scale).floor() as u64;
        let hi = (hi_f * scale).ceil() as u64;
        let actual_width = (hi - lo) as f64 / scale;
        if actual_width <= width * (33.0 / 32.0) {
            return ((1u64 << n) - 1, lo, hi);
        }
    }
    // max_bits fallback: best achievable precision with the given bit budget
    let scale = (1u64 << max_bits) as f64;
    (
        (1u64 << max_bits) - 1,
        (lo_f * scale).floor() as u64,
        (hi_f * scale).ceil() as u64,
    )
}

// --- Tantivy Query ---

#[derive(Clone, Debug)]
struct BitwiseMaskRangeTantivyQuery {
    field_name: String,
    mask: u64,
    lo: u64,
    hi: u64,
}

impl Query for BitwiseMaskRangeTantivyQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(BitwiseMaskRangeWeight {
            field_name: self.field_name.clone(),
            mask: self.mask,
            lo: self.lo,
            hi: self.hi,
        }))
    }
}

// --- Weight ---

struct BitwiseMaskRangeWeight {
    field_name: String,
    mask: u64,
    lo: u64,
    hi: u64,
}

impl Weight for BitwiseMaskRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        // u64_lenient opens both I64 and U64 fast fields as Column<u64> via bit reinterpretation,
        // which is correct for the masking operation regardless of the original signedness.
        let Some((column, _column_type)) = reader.fast_fields().u64_lenient(&self.field_name)?
        else {
            return Err(TantivyError::InvalidArgument(format!(
                "fast field `{}` not found or not numeric",
                self.field_name
            )));
        };
        let docset = BitwiseMaskRangeDocSet::new(column, self.mask, self.lo, self.hi);
        Ok(Box::new(ConstScorer::new(docset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "document #{doc} does not match"
            )));
        }
        Ok(Explanation::new("BitwiseMaskRange", scorer.score()))
    }
}

// --- DocSet ---

struct BitwiseMaskRangeDocSet {
    column: Column<u64>,
    mask: u64,
    lo: u64,
    hi: u64,
    current: DocId,
}

impl BitwiseMaskRangeDocSet {
    fn new(column: Column<u64>, mask: u64, lo: u64, hi: u64) -> Self {
        let mut docset = Self {
            column,
            mask,
            lo,
            hi,
            current: 0,
        };
        if !docset.matches(0) {
            docset.advance();
        }
        docset
    }

    #[inline]
    fn matches(&self, doc: DocId) -> bool {
        let Some(val) = self.column.first(doc) else {
            return false;
        };
        let masked = val & self.mask;
        masked >= self.lo && masked < self.hi
    }
}

impl DocSet for BitwiseMaskRangeDocSet {
    fn advance(&mut self) -> DocId {
        if self.current == TERMINATED {
            return TERMINATED;
        }
        let num_docs = self.column.num_docs();
        loop {
            self.current += 1;
            if self.current >= num_docs {
                self.current = TERMINATED;
                return TERMINATED;
            }
            if self.matches(self.current) {
                return self.current;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if self.current >= target {
            return self.current;
        }
        self.current = target;
        if self.matches(self.current) {
            return self.current;
        }
        self.advance()
    }

    fn doc(&self) -> DocId {
        self.current
    }

    fn size_hint(&self) -> u32 {
        let fraction = (self.hi - self.lo) as f64 / (self.mask as f64 + 1.0);
        (fraction * self.column.num_docs() as f64) as u32
    }

    fn cost(&self) -> u64 {
        // Must scan the full column in the worst case; same cost as RangeDocSet.
        (self.column.num_docs() as f64 * 0.8) as u64
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{NumericOptions, SchemaBuilder};
    use tantivy::{DocSet, TERMINATED};

    use super::{BitwiseMaskRangeDocSet, sampling_params_range};

    fn make_docset(values: &[i64], mask: u64, lo: u64, hi: u64) -> BitwiseMaskRangeDocSet {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_i64_field("val", NumericOptions::default().set_fast());
        let schema = schema_builder.build();
        let index = tantivy::IndexBuilder::new()
            .schema(schema)
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for &v in values {
            let mut doc = tantivy::TantivyDocument::default();
            doc.add_field_value(field, &v);
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let (column, _) = segment_reader
            .fast_fields()
            .u64_lenient("val")
            .unwrap()
            .unwrap();
        BitwiseMaskRangeDocSet::new(column, mask, lo, hi)
    }

    #[test]
    fn test_docset_advance_collects_correct_docs() {
        // 16 sequential docs with values 0..15, mask=3 (2-bit), [0,2) → values 0 and 1 match
        let values: Vec<i64> = (0i64..16).collect();
        let mut docset = make_docset(&values, 3, 0, 2);

        let mut matched = Vec::new();
        while docset.doc() != TERMINATED {
            matched.push(docset.doc());
            docset.advance();
        }
        // docs 0,1,4,5,8,9,12,13 have value & 3 in [0,2)
        assert_eq!(matched, vec![0, 1, 4, 5, 8, 9, 12, 13]);
        // advance on TERMINATED stays TERMINATED
        assert_eq!(docset.advance(), TERMINATED);
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_docset_seek() {
        let values: Vec<i64> = (0i64..16).collect();
        // mask=3, [0,2): matching docs are 0,1,4,5,8,9,12,13
        let mut docset = make_docset(&values, 3, 0, 2);

        // seek to a matching doc
        assert_eq!(docset.seek(4), 4);
        assert_eq!(docset.doc(), 4);

        // seek to a non-matching doc advances to next match
        assert_eq!(docset.seek(6), 8);
        assert_eq!(docset.doc(), 8);

        // seek backward is a no-op (current stays)
        assert_eq!(docset.seek(5), 8);

        // seek past last match
        assert_eq!(docset.seek(14), TERMINATED);
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_docset_no_match() {
        // mask=1, [1,2): only odd docs match. All values are even → no match.
        let values: Vec<i64> = vec![0, 2, 4, 6];
        let docset = make_docset(&values, 1, 1, 2);
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_docset_all_match() {
        // mask=0, [0,1): v & 0 == 0 always → all docs match
        let values: Vec<i64> = (0i64..8).collect();
        let mut docset = make_docset(&values, 0, 0, 1);
        let mut count = 0u32;
        while docset.doc() != TERMINATED {
            count += 1;
            docset.advance();
        }
        assert_eq!(count, 8);
    }

    #[test]
    fn test_docset_sampling_fraction() {
        // 64 sequential docs, sample ~25% via 2-bit mask [0,1)
        let values: Vec<i64> = (0i64..64).collect();
        let mut docset = make_docset(&values, 3, 0, 1);
        let mut count = 0u32;
        while docset.doc() != TERMINATED {
            count += 1;
            docset.advance();
        }
        // exactly 1/4 of 64 = 16 docs (values 0,4,8,...,60 have v & 3 == 0)
        assert_eq!(count, 16);
    }

    #[test]
    fn test_docset_negative_values() {
        // tiebreakers can be negative i64. Cast to u64 must give correct low bits.
        // -4i64 as u64 = 0xFFFF_FFFF_FFFF_FFFC, low 2 bits = 0b00
        // -3i64 as u64 = ...FD, low 2 bits = 0b01
        // -2i64 as u64 = ...FE, low 2 bits = 0b10
        // -1i64 as u64 = ...FF, low 2 bits = 0b11
        let values: Vec<i64> = vec![-4, -3, -2, -1, 0, 1, 2, 3];
        let mut docset = make_docset(&values, 3, 0, 1); // match: v & 3 == 0
        let mut matched_values: Vec<i64> = Vec::new();
        while docset.doc() != TERMINATED {
            matched_values.push(values[docset.doc() as usize]);
            docset.advance();
        }
        // -4 and 0 have low 2 bits = 0b00
        assert_eq!(matched_values, vec![-4, 0]);
    }

    #[test]
    fn test_sampling_params_range_half() {
        let (mask, lo, hi) = sampling_params_range(0.0, 0.5, 32);
        assert_eq!(mask, 1);
        assert_eq!(lo, 0);
        assert_eq!(hi, 1);
        // actual probability: 1/2 = 0.5
    }

    #[test]
    fn test_sampling_params_range_eighth() {
        let (mask, lo, hi) = sampling_params_range(0.0, 0.125, 32);
        // 0.125 = 1/8: exact fit with 3-bit mask (n=3 is minimal)
        assert_eq!(mask, 7, "expected 3-bit mask");
        assert_eq!(lo, 0);
        assert_eq!(hi, 1);
        // actual probability: 1/8 = 0.125 exactly
    }

    #[test]
    fn test_sampling_params_range_interior() {
        let (mask, lo, hi) = sampling_params_range(0.2, 0.4, 32);
        let width = 0.2_f64;
        let actual = (hi - lo) as f64 / (mask + 1) as f64;
        assert!(actual >= width, "actual {actual} < target width {width}");
        assert!(
            actual <= width * (33.0 / 32.0),
            "actual {actual} > 3.125% above target {width}"
        );
        // distinct from [0.0, 0.2] and [0.4, 0.6]
        let (_, lo2, hi2) = sampling_params_range(0.0, 0.2, 32);
        assert!(lo2 != lo || hi2 != hi, "ranges should differ");
    }

    #[test]
    fn test_sampling_params_range_full() {
        let (mask, lo, hi) = sampling_params_range(0.0, 1.0, 32);
        // hi = ceil(1.0 * 2^n) = 2^n > mask = 2^n - 1, so always matches
        assert!(
            hi > mask,
            "hi={hi} should exceed mask={mask} for full range"
        );
        let _ = lo;
    }

    #[test]
    fn test_sampling_params_range_relative_error_bound() {
        for &(lo_f, hi_f) in &[
            (0.0_f64, 0.3_f64),
            (0.1, 0.6),
            (0.5, 0.9),
            (0.9, 1.0),
            (0.0, 0.01),
        ] {
            let (mask, lo, hi) = sampling_params_range(lo_f, hi_f, 32);
            let width = hi_f - lo_f;
            let actual = (hi - lo) as f64 / (mask + 1) as f64;
            assert!(
                actual >= width,
                "lo_f={lo_f} hi_f={hi_f}: actual {actual} < width {width}"
            );
            assert!(
                actual <= width * (33.0 / 32.0),
                "lo_f={lo_f} hi_f={hi_f}: actual {actual} exceeds 3.125% overshoot above {width}"
            );
        }
    }
}
