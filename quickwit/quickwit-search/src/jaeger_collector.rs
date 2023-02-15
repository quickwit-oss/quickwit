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

use std::collections::HashSet;
use std::sync::Arc;

use fnv::FnvHashMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::{Column, MultiValuedFastFieldReader};
use tantivy::{DateTime, DocId, InvertedIndexReader, Score, SegmentReader};

type TraceId = Vec<u8>;
type TraceIdTermOrd = u64;
type SpanTimestamp = i64;

#[derive(Debug, Serialize, Deserialize)]
pub struct TraceIdSpanTimestamp {
    pub trace_id: TraceId,
    pub span_timestamp: SpanTimestamp,
}

impl TraceIdSpanTimestamp {
    fn new(trace_id: TraceId, span_timestamp: i64) -> Self {
        Self {
            trace_id,
            span_timestamp,
        }
    }
}

/// Finds the most recent trace ids among a set of matching spans. Multiple spans that belong to the
/// same trace can be found in the document set. As a result, this problem is akin to finding the
/// top k elements with duplicates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindTraceIdsCollector {
    num_traces: usize,
    trace_id_field_name: String,
    span_timestamp_field_name: String,
}

impl FindTraceIdsCollector {
    pub fn fast_field_names(&self) -> HashSet<String> {
        HashSet::from_iter([
            self.trace_id_field_name.clone(),
            self.span_timestamp_field_name.clone(),
        ])
    }

    pub fn term_dict_field_names(&self) -> HashSet<String> {
        HashSet::from_iter([self.trace_id_field_name.clone()])
    }
}

impl Collector for FindTraceIdsCollector {
    type Fruit = Vec<TraceIdSpanTimestamp>;
    type Child = FindTraceIdsSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let trace_id_ff_reader = segment_reader
            .fast_fields()
            .u64s(&self.trace_id_field_name)?;
        let span_timestamp_column = segment_reader
            .fast_fields()
            .date(&self.span_timestamp_field_name)?;

        let trace_id_field = segment_reader
            .schema()
            .get_field(&self.trace_id_field_name)?;
        let inverted_index_reader = segment_reader.inverted_index(trace_id_field)?;

        Ok(Self::Child {
            num_traces: self.num_traces,
            trace_id_ff_reader,
            span_timestamp_column,
            inverted_index_reader,
            buckets: FnvHashMap::default(),
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut buckets = FnvHashMap::default();
        for segment_fruit in segment_fruits {
            for fruit in segment_fruit {
                buckets
                    .entry(fruit.trace_id)
                    .and_modify(|entry| {
                        if *entry < fruit.span_timestamp {
                            *entry = fruit.span_timestamp
                        }
                    })
                    .or_insert(fruit.span_timestamp);
            }
        }
        let merged_fruits = buckets
            .into_iter()
            .sorted_by_key(|(_, span_timestamp)| *span_timestamp)
            .rev()
            .take(self.num_traces)
            .map(|(trace_id, span_timestamp)| TraceIdSpanTimestamp::new(trace_id, span_timestamp))
            .collect();
        Ok(merged_fruits)
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

/// This a naive implementation of a top k algorithm with duplicates. A more robust implementation
/// will follow.
pub struct FindTraceIdsSegmentCollector {
    num_traces: usize,
    trace_id_ff_reader: MultiValuedFastFieldReader<u64>,
    span_timestamp_column: Arc<dyn Column<DateTime>>,
    inverted_index_reader: Arc<InvertedIndexReader>,
    buckets: FnvHashMap<TraceIdTermOrd, SpanTimestamp>,
}

impl FindTraceIdsSegmentCollector {
    fn trace_id_term_ord(&self, doc: DocId) -> TraceIdTermOrd {
        self.trace_id_ff_reader
            .get_first_val(doc)
            .expect("There should be exactly one trace ID per span.")
    }

    fn trace_id(&self, trace_id_term_ord: TraceIdTermOrd) -> TraceId {
        let mut trace_id = vec![0u8; 16];
        self.inverted_index_reader
            .terms()
            .ord_to_term(trace_id_term_ord, &mut trace_id)
            .unwrap();
        trace_id
    }

    fn span_timestamp(&self, doc: DocId) -> SpanTimestamp {
        self.span_timestamp_column
            .get_val(doc)
            .into_timestamp_micros()
    }
}

impl SegmentCollector for FindTraceIdsSegmentCollector {
    type Fruit = Vec<TraceIdSpanTimestamp>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let trace_id_term_ord = self.trace_id_term_ord(doc);
        let span_timestamp = self.span_timestamp(doc);
        self.buckets
            .entry(trace_id_term_ord)
            .and_modify(|entry| {
                if *entry < span_timestamp {
                    *entry = span_timestamp
                }
            })
            .or_insert(span_timestamp);
    }

    fn harvest(self) -> Self::Fruit {
        self.buckets
            .iter()
            .take(self.num_traces)
            .map(|(trace_id_term_ord, span_timestamp)| {
                TraceIdSpanTimestamp::new(self.trace_id(*trace_id_term_ord), *span_timestamp)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::QuickwitAggregations;

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
}
