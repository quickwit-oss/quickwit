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

use std::cmp::{Ord, Ordering};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_opentelemetry::otlp::TraceId;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::StrColumn;
use tantivy::fastfield::Column;
use tantivy::{DateTime, DocId, Score, SegmentReader};

type TermOrd = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: TraceId,
    #[serde(with = "serde_datetime")]
    pub span_timestamp: DateTime,
}

impl Span {
    fn new(trace_id: TraceId, span_timestamp: DateTime) -> Self {
        Self {
            trace_id,
            span_timestamp,
        }
    }
}

impl Hash for Span {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.trace_id.hash(state);
    }
}

impl Ord for Span {
    fn cmp(&self, other: &Self) -> Ordering {
        self.span_timestamp
            .cmp(&other.span_timestamp)
            .reverse()
            .then(self.trace_id.cmp(&other.trace_id))
    }
}

impl PartialOrd for Span {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Span {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Span {}

#[derive(Debug)]
pub struct TraceIdTermOrd {
    pub term_ord: TermOrd,
    pub span_timestamp: DateTime,
}

impl TraceIdTermOrd {
    pub fn new(term_ord: TermOrd, span_timestamp: DateTime) -> Self {
        Self {
            term_ord,
            span_timestamp,
        }
    }
}

impl Ord for TraceIdTermOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        self.span_timestamp
            .cmp(&other.span_timestamp)
            .reverse()
            .then(self.term_ord.cmp(&other.term_ord))
    }
}

impl PartialOrd for TraceIdTermOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TraceIdTermOrd {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TraceIdTermOrd {}

/// Finds the most recent trace ids among a set of matching spans. Multiple spans belonging to the
/// same trace can be found in the document set. As a result, this problem is akin to finding the
/// top k elements with duplicates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindTraceIdsCollector {
    /// The number of traces to select.
    pub num_traces: usize,
    /// The name of the fast field storing the trace IDs.
    pub trace_id_field_name: String,
    /// The name of the fast field recording the spans' start timestamp.
    pub span_timestamp_field_name: String,
}

impl FindTraceIdsCollector {
    /// The names of the fast fields accessed by this collector.
    pub fn fast_field_names(&self) -> HashSet<String> {
        HashSet::from_iter([
            self.trace_id_field_name.clone(),
            self.span_timestamp_field_name.clone(),
        ])
    }

    /// The field names of the term dictionaries accessed by this collector.
    pub fn term_dict_field_names(&self) -> HashSet<String> {
        HashSet::from_iter([self.trace_id_field_name.clone()])
    }
}

impl Collector for FindTraceIdsCollector {
    type Fruit = Vec<Span>;
    type Child = FindTraceIdsSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let trace_id_column = segment_reader
            .fast_fields()
            .str(&self.trace_id_field_name)?
            .ok_or_else(|| {
                let err_msg = format!(
                    "Failed to find column for trace_id field `{}`",
                    self.trace_id_field_name
                );
                tantivy::TantivyError::InternalError(err_msg)
            })?;
        let span_timestamp_column: Column<DateTime> = segment_reader
            .fast_fields()
            .date(&self.span_timestamp_field_name)?;
        Ok(FindTraceIdsSegmentCollector {
            trace_id_column,
            span_timestamp_column,
            select_trace_ids: SelectTraceIds::new(self.num_traces),
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(merge_segment_fruits(segment_fruits, self.num_traces))
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

fn merge_segment_fruits(mut segment_fruits: Vec<Vec<Span>>, num_traces: usize) -> Vec<Span> {
    for segment_fruit in &mut segment_fruits {
        segment_fruit.sort_unstable()
    }
    let mut trace_ids = Vec::with_capacity(num_traces);
    let mut seen_trace_ids = FnvHashSet::default();

    for trace_id in segment_fruits.into_iter().kmerge() {
        if !seen_trace_ids.contains(&trace_id.trace_id) {
            seen_trace_ids.insert(trace_id.trace_id);
            trace_ids.push(trace_id);

            if trace_ids.len() == num_traces {
                break;
            }
        }
    }
    trace_ids
}

pub struct FindTraceIdsSegmentCollector {
    trace_id_column: StrColumn,
    span_timestamp_column: Column<DateTime>,
    select_trace_ids: SelectTraceIds,
}

impl FindTraceIdsSegmentCollector {
    fn trace_id_term_ord(&self, doc: DocId) -> TermOrd {
        self.trace_id_column
            .term_ords(doc)
            .next()
            .unwrap_or_default()
    }

    fn span_timestamp(&self, doc: DocId) -> DateTime {
        self.span_timestamp_column.first(doc).unwrap_or_default()
    }
}

impl SegmentCollector for FindTraceIdsSegmentCollector {
    type Fruit = Vec<Span>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let term_ord = self.trace_id_term_ord(doc);
        let span_timestamp = self.span_timestamp(doc);
        self.select_trace_ids.collect(term_ord, span_timestamp);
    }

    fn harvest(self) -> Self::Fruit {
        let mut buffer = String::with_capacity(TraceId::BASE64_LENGTH);
        self.select_trace_ids
            .harvest()
            .into_iter()
            .map(|trace_id_term_ord| {
                let span_timestamp = trace_id_term_ord.span_timestamp;
                let found_term = self
                    .trace_id_column
                    .ord_to_str(trace_id_term_ord.term_ord, &mut buffer)
                    .expect("Failed to lookup trace ID in the column term dictionary");
                debug_assert!(found_term);
                debug_assert_eq!(buffer.len(), TraceId::BASE64_LENGTH);
                let trace_id = buffer[..]
                    .parse()
                    .expect("The term dict should store Base64 trace IDs.");
                Span::new(trace_id, span_timestamp)
            })
            .collect()
    }
}

struct SelectTraceIds {
    num_traces: usize,
    dedup_workbench: FnvHashMap<TermOrd, DateTime>,
    select_workbench: Vec<TraceIdTermOrd>,
    running_term_ord: Option<TermOrd>,
    running_span_timestamp: DateTime,
    span_timestamp_sentinel: DateTime,
}

impl SelectTraceIds {
    fn new(num_traces: usize) -> Self {
        Self {
            num_traces,
            dedup_workbench: FnvHashMap::with_capacity_and_hasher(
                2 * num_traces,
                Default::default(),
            ),
            select_workbench: Vec::with_capacity(2 * num_traces),
            running_term_ord: None,
            running_span_timestamp: DateTime::default(),
            span_timestamp_sentinel: DateTime::from_timestamp_micros(i64::MIN),
        }
    }

    fn collect(&mut self, term_ord: TermOrd, span_timestamp: DateTime) {
        if self.running_term_ord.is_none() {
            self.running_term_ord = Some(term_ord);
            self.running_span_timestamp = span_timestamp;
            return;
        }
        if self.span_timestamp_sentinel >= span_timestamp {
            return;
        }
        let running_term_ord = self
            .running_term_ord
            .expect("The running trace ID should be set.");

        if running_term_ord == term_ord {
            self.running_span_timestamp = self.running_span_timestamp.max(span_timestamp);
        } else {
            self.dedup(running_term_ord, self.running_span_timestamp);
            self.truncate();
            self.running_term_ord = Some(term_ord);
            self.running_span_timestamp = span_timestamp;
        }
    }

    fn dedup(&mut self, term_ord: TermOrd, span_timestamp: DateTime) {
        self.dedup_workbench
            .entry(term_ord)
            .and_modify(|entry| {
                if *entry < span_timestamp {
                    *entry = span_timestamp
                }
            })
            .or_insert(span_timestamp);
    }

    fn select(&mut self) {
        if self.num_traces == 0 || self.dedup_workbench.is_empty() {
            return;
        }
        self.select_workbench.clear();

        for (term_ord, span_timestamp) in self.dedup_workbench.drain() {
            let trace_id = TraceIdTermOrd::new(term_ord, span_timestamp);
            self.select_workbench.push(trace_id);
        }
        let select_len = self.num_traces.min(self.select_workbench.len());
        let select_index = select_len - 1;
        self.select_workbench.select_nth_unstable(select_index);
        self.select_workbench.truncate(select_len);
        self.span_timestamp_sentinel = self.select_workbench[select_index].span_timestamp;
    }

    fn truncate(&mut self) {
        if self.dedup_workbench.len() < 2 * self.num_traces {
            return;
        }
        self.select();

        for trace_id in self.select_workbench.drain(..self.num_traces) {
            self.dedup_workbench
                .insert(trace_id.term_ord, trace_id.span_timestamp);
        }
    }

    fn harvest(mut self) -> Vec<TraceIdTermOrd> {
        if let Some(running_term_ord) = self.running_term_ord.take() {
            self.dedup(running_term_ord, self.running_span_timestamp);
        }
        self.select();
        self.select_workbench
    }
}

mod serde_datetime {
    use serde::{Deserialize, Deserializer, Serializer};
    use tantivy::DateTime;

    pub(crate) fn serialize<S>(datetime: &DateTime, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_i64(datetime.into_timestamp_micros())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<DateTime, D::Error>
    where D: Deserializer<'de> {
        let datetime_64: i64 = Deserialize::deserialize(deserializer)?;
        Ok(DateTime::from_timestamp_micros(datetime_64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::QuickwitAggregations;

    impl Span {
        fn for_test(bytes: &[u8], span_timestamp_micros: i64) -> Self {
            let mut trace_id = [0u8; 16];
            trace_id[..bytes.len()].copy_from_slice(bytes);
            let span_timestamp = DateTime::from_timestamp_micros(span_timestamp_micros);
            Self::new(TraceId::new(trace_id), span_timestamp)
        }
    }

    impl TraceIdTermOrd {
        fn for_test(term_ord: TermOrd, span_timestamp_micros: i64) -> Self {
            Self {
                term_ord,
                span_timestamp: DateTime::from_timestamp_micros(span_timestamp_micros),
            }
        }
    }

    impl SelectTraceIds {
        fn collect_for_test(&mut self, term_ord: TermOrd, span_timestamp_micros: i64) {
            let span_timestamp = DateTime::from_timestamp_micros(span_timestamp_micros);
            self.collect(term_ord, span_timestamp)
        }
    }

    #[test]
    fn test_find_trace_ids_collector_serde() {
        let collector_json = serde_json::to_string(&FindTraceIdsCollector {
            num_traces: 10,
            trace_id_field_name: "trace_id".to_string(),
            span_timestamp_field_name: "span_timestamp".to_string(),
        })
        .unwrap();
        let aggregation: QuickwitAggregations = serde_json::from_str(&collector_json).unwrap();
        let QuickwitAggregations::FindTraceIdsAggregation(collector) = aggregation else {
            panic!("Expected FindTraceIdsAggregation");
        };
        assert_eq!(collector.num_traces, 10);
        assert_eq!(collector.trace_id_field_name, "trace_id");
        assert_eq!(collector.span_timestamp_field_name, "span_timestamp");
    }

    #[test]
    fn test_span_serde() {
        let expected_span = Span::for_test(b"trace_id", 123456789);
        let span_json = serde_json::to_string(&expected_span).unwrap();
        let span = serde_json::from_str::<Span>(&span_json).unwrap();
        assert_eq!(span, expected_span);
    }

    #[test]
    fn test_select_trace_ids() {
        {
            let select_trace_ids = SelectTraceIds::new(0);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(trace_ids, &[]);
        }
        {
            let select_trace_ids = SelectTraceIds::new(3);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(trace_ids, &[]);
        }
        {
            let mut select_trace_ids = SelectTraceIds::new(0);
            select_trace_ids.collect_for_test(0, 0);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(trace_ids, &[]);
        }
        {
            let mut select_trace_ids = SelectTraceIds::new(3);
            select_trace_ids.collect_for_test(0, 0);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(trace_ids, &[TraceIdTermOrd::for_test(0, 0)]);
        }
        {
            let mut select_trace_ids = SelectTraceIds::new(3);
            select_trace_ids.collect_for_test(0, 1);
            select_trace_ids.collect_for_test(0, 0);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(trace_ids, &[TraceIdTermOrd::for_test(0, 1)]);
        }
        {
            let mut select_trace_ids = SelectTraceIds::new(3);
            select_trace_ids.collect_for_test(0, 2);
            select_trace_ids.collect_for_test(1, 1);
            select_trace_ids.collect_for_test(2, 0);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(
                trace_ids,
                &[
                    TraceIdTermOrd::for_test(0, 2),
                    TraceIdTermOrd::for_test(1, 1),
                    TraceIdTermOrd::for_test(2, 0),
                ]
            );
        }
        {
            let mut select_trace_ids = SelectTraceIds::new(3);
            select_trace_ids.collect_for_test(0, 7);
            select_trace_ids.collect_for_test(1, 6);
            select_trace_ids.collect_for_test(2, 5);
            select_trace_ids.collect_for_test(3, 4);
            select_trace_ids.collect_for_test(4, 3);
            select_trace_ids.collect_for_test(5, 2);
            select_trace_ids.collect_for_test(6, 1);
            select_trace_ids.collect_for_test(7, 0);

            assert_eq!(select_trace_ids.select_workbench.capacity(), 6);

            let mut trace_ids = select_trace_ids.harvest();
            trace_ids.sort();

            assert_eq!(
                trace_ids,
                &[
                    TraceIdTermOrd::for_test(0, 7),
                    TraceIdTermOrd::for_test(1, 6),
                    TraceIdTermOrd::for_test(2, 5),
                ]
            );
        }
    }

    #[test]
    fn test_merge_segment_fruits() {
        {
            let segment_fruits = Vec::new();
            let merged_fruit = merge_segment_fruits(segment_fruits, 0);
            assert_eq!(merged_fruit, &[]);
        }
        {
            let segment_fruits = vec![vec![Span::for_test(b"foo", 0), Span::for_test(b"foo", 1)]];
            let merged_fruit = merge_segment_fruits(segment_fruits, 3);
            assert_eq!(merged_fruit, &[Span::for_test(b"foo", 1)]);
        }
        {
            let segment_fruits = vec![
                vec![Span::for_test(b"foo", 0), Span::for_test(b"foo", 1)],
                vec![Span::for_test(b"foo", 1), Span::for_test(b"foo", 2)],
            ];
            let merged_fruit = merge_segment_fruits(segment_fruits, 3);
            assert_eq!(merged_fruit, &[Span::for_test(b"foo", 2)]);
        }
        {
            let segment_fruits = vec![
                vec![
                    Span::for_test(b"foo", 0),
                    Span::for_test(b"foo", 1),
                    Span::for_test(b"foo", 2),
                ],
                vec![Span::for_test(b"foo", 2), Span::for_test(b"bar", 2)],
                vec![Span::for_test(b"foo", 2), Span::for_test(b"bar", 3)],
            ];
            let merged_fruit = merge_segment_fruits(segment_fruits, 3);
            assert_eq!(
                merged_fruit,
                &[Span::for_test(b"bar", 3), Span::for_test(b"foo", 2)]
            );
        }
        {
            let segment_fruits = vec![
                vec![
                    Span::for_test(b"foo", 0),
                    Span::for_test(b"foo", 1),
                    Span::for_test(b"foo", 2),
                ],
                vec![Span::for_test(b"foo", 2), Span::for_test(b"bar", 2)],
                vec![Span::for_test(b"foo", 2), Span::for_test(b"bar", 3)],
                vec![Span::for_test(b"qux", 4)],
            ];
            let merged_fruit = merge_segment_fruits(segment_fruits, 3);
            assert_eq!(
                merged_fruit,
                &[
                    Span::for_test(b"qux", 4),
                    Span::for_test(b"bar", 3),
                    Span::for_test(b"foo", 2)
                ]
            );
        }
    }
}
