// Copyright (C) 2024 Quickwit, Inc.
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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;

use itertools::Itertools;
use quickwit_common::binary_heap::{SortKeyMapper, TopK};
use quickwit_doc_mapper::WarmupInfo;
use quickwit_proto::search::{
    LeafSearchResponse, PartialHit, SearchRequest, SortByValue, SortOrder, SortValue,
    SplitSearchError,
};
use quickwit_proto::types::SplitId;
use serde::Deserialize;
use tantivy::aggregation::agg_req::{get_fast_field_names, Aggregations};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{AggregationLimits, AggregationSegmentCollector};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{ColumnType, MonotonicallyMappableToU64};
use tantivy::fastfield::Column;
use tantivy::{DateTime, DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

use crate::find_trace_ids_collector::{FindTraceIdsCollector, FindTraceIdsSegmentCollector, Span};
use crate::top_k_collector::{specialized_top_k_segment_collector, QuickwitSegmentTopKCollector};
use crate::GlobalDocAddress;

#[derive(Clone, Debug)]
pub(crate) enum SortByComponent {
    DocId {
        order: SortOrder,
    },
    FastField {
        field_name: String,
        order: SortOrder,
    },
    Score {
        order: SortOrder,
    },
}
impl From<SortByComponent> for SortByPair {
    fn from(value: SortByComponent) -> Self {
        Self {
            first: value,
            second: None,
        }
    }
}
#[derive(Clone)]
pub(crate) struct SortByPair {
    first: SortByComponent,
    second: Option<SortByComponent>,
}
impl SortByPair {
    pub fn sort_orders(&self) -> (SortOrder, SortOrder) {
        (
            self.first.sort_order(),
            self.second
                .as_ref()
                .map(|sort_by| sort_by.sort_order())
                .unwrap_or(SortOrder::Desc),
        )
    }
}
impl SortByComponent {
    fn to_sorting_field_extractor_component(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<SortingFieldExtractorComponent> {
        match self {
            SortByComponent::DocId { .. } => Ok(SortingFieldExtractorComponent::DocId),
            SortByComponent::FastField { field_name, .. } => {
                let sort_column_opt: Option<(Column<u64>, ColumnType)> =
                    segment_reader.fast_fields().u64_lenient(field_name)?;
                let (sort_column, column_type) = sort_column_opt.unwrap_or_else(|| {
                    (
                        Column::build_empty_column(segment_reader.max_doc()),
                        ColumnType::U64,
                    )
                });
                let sort_field_type = SortFieldType::try_from(column_type)?;
                Ok(SortingFieldExtractorComponent::FastField {
                    sort_column,
                    sort_field_type,
                })
            }
            SortByComponent::Score { .. } => Ok(SortingFieldExtractorComponent::Score),
        }
    }
    pub fn requires_scoring(&self) -> bool {
        match self {
            SortByComponent::DocId { .. } => false,
            SortByComponent::FastField { .. } => false,
            SortByComponent::Score { .. } => true,
        }
    }
    pub fn add_fast_field(&self, set: &mut HashSet<String>) {
        if let SortByComponent::FastField {
            field_name,
            order: _,
        } = self
        {
            set.insert(field_name.clone());
        }
    }
    pub fn sort_order(&self) -> SortOrder {
        match self {
            SortByComponent::DocId { order } => *order,
            SortByComponent::FastField { order, .. } => *order,
            SortByComponent::Score { order } => *order,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum SortFieldType {
    U64,
    I64,
    F64,
    DateTime,
    Bool,
}

/// The `SortingFieldExtractor` is used to extract a score, which can either be a true score,
/// a value from a fast field, or nothing (sort by DocId).
pub(crate) enum SortingFieldExtractorComponent {
    /// If undefined, we simply sort by DocIds.
    DocId,
    FastField {
        sort_column: Column<u64>,
        sort_field_type: SortFieldType,
    },
    Score,
}

impl SortingFieldExtractorComponent {
    pub fn is_score(&self) -> bool {
        matches!(self, SortingFieldExtractorComponent::Score)
    }
    pub fn is_fast_field(&self) -> bool {
        matches!(self, SortingFieldExtractorComponent::FastField { .. })
    }
    /// Loads the fast field values for the given doc_ids in its u64 representation. The returned
    /// u64 representation maintains the ordering of the original value.
    #[inline]
    pub fn extract_typed_sort_values_block(&self, doc_ids: &[DocId], values: &mut [Option<u64>]) {
        // In the collect block case we don't have scores to extract
        if let SortingFieldExtractorComponent::FastField { sort_column, .. } = self {
            let values = &mut values[..doc_ids.len()];
            sort_column.first_vals(doc_ids, values);
        }
    }

    /// Returns the sort value for the given element in its u64 representation. The returned u64
    /// representation maintains the ordering of the original value.
    ///
    /// The function returns None if the sort key is a fast field, for which we have no value
    /// for the given doc_id, or we sort by DocId.
    #[inline]
    fn extract_typed_sort_value_opt(&self, doc_id: DocId, score: Score) -> Option<u64> {
        match self {
            // Tie breaks are not handled here, but in SegmentPartialHit
            SortingFieldExtractorComponent::DocId => None,
            SortingFieldExtractorComponent::FastField { sort_column, .. } => {
                sort_column.first(doc_id)
            }
            SortingFieldExtractorComponent::Score { .. } => Some((score as f64).to_u64()),
        }
    }

    #[inline]
    /// Converts u64 fast field values to its correct type.
    /// The conversion is delayed for performance reasons.
    ///
    /// This is used to convert `search_after` sort value to a u64 representation that will respect
    /// the same order as the `SortValue` representation.
    pub fn convert_u64_ff_val_to_sort_value(&self, sort_value: u64) -> SortValue {
        let map_fast_field_to_value = |fast_field_value, field_type| match field_type {
            SortFieldType::U64 => SortValue::U64(fast_field_value),
            SortFieldType::I64 => SortValue::I64(i64::from_u64(fast_field_value)),
            SortFieldType::F64 => SortValue::F64(f64::from_u64(fast_field_value)),
            SortFieldType::DateTime => SortValue::I64(i64::from_u64(fast_field_value)),
            SortFieldType::Bool => SortValue::Boolean(fast_field_value != 0u64),
        };
        match self {
            SortingFieldExtractorComponent::DocId => SortValue::U64(sort_value),
            SortingFieldExtractorComponent::FastField {
                sort_field_type, ..
            } => map_fast_field_to_value(sort_value, *sort_field_type),
            SortingFieldExtractorComponent::Score => SortValue::F64(f64::from_u64(sort_value)),
        }
    }
    /// Converts fast field values into their u64 fast field representation.
    ///
    /// Returns None if value is out of bounds of target value.
    /// None means that the search_after will be disabled and everything matches.
    ///
    /// What's currently missing is to signal that _nothing_ matches to generate an optimized
    /// query. For now we just choose the max value of the target type.
    #[inline]
    pub fn convert_to_u64_ff_val(
        &self,
        sort_value: SortValue,
        sort_order: SortOrder,
    ) -> Option<u64> {
        match self {
            SortingFieldExtractorComponent::DocId => match sort_value {
                SortValue::U64(val) => Some(val),
                _ => panic!("Internal error: Got non-U64 sort value for DocId."),
            },
            SortingFieldExtractorComponent::FastField {
                sort_field_type, ..
            } => {
                // We need to convert a (potential user provided) value in the correct u64
                // representation of the fast field.
                // This requires this weird conversion of first casting into the target type
                // (if possible) and then to its u64 presentation.
                //
                // For the conversion into the target type it's important to know if the target
                // type does not cover the whole range of the source type. In that case we need to
                // add additional conversion checks, to see if it matches everything
                // or nothing. (Which also depends on the sort order).
                // Below are the visual representations of the value ranges of the different types.
                // Note: DateTime is equal to I64 and omitted.
                //
                //     Bool value range (0, 1):
                //                        <->
                //
                //     I64 value range (signed 64-bit integer):
                //     <------------------------------------>
                //     -2^63                             2^63-1
                //     U64 value range (unsigned 64-bit integer):
                //                        <------------------------------------>
                //                        0                                  2^64-1
                // F64 value range (64-bit floating point, conceptual, not to scale):
                // <-------------------------------------------------------------------->
                // Very negative numbers                                       Very positive numbers
                //
                // Those conversions have limited target type value space:
                // - [X] U64 -> I64
                // - [X] F64 -> I64
                // - [X] I64 -> U64
                // - [X] F64 -> U64
                //
                // - [X] F64 -> Bool
                // - [X] I64 -> Bool
                // - [X] U64 -> Bool
                //
                let val = match (sort_value, sort_field_type) {
                    // Same field type, no conversion needed.
                    (SortValue::U64(val), SortFieldType::U64) => val,
                    (SortValue::F64(val), SortFieldType::F64) => val.to_u64(),
                    (SortValue::Boolean(val), SortFieldType::Bool) => val.to_u64(),
                    (SortValue::I64(val), SortFieldType::I64) => val.to_u64(),
                    (SortValue::U64(mut val), SortFieldType::I64) => {
                        if sort_order == SortOrder::Desc && val > i64::MAX as u64 {
                            return None;
                        }
                        // Add a limit to avoid overflow.
                        val = val.min(i64::MAX as u64);
                        (val as i64).to_u64()
                    }
                    (SortValue::U64(val), SortFieldType::F64) => (val as f64).to_u64(),
                    (SortValue::U64(mut val), SortFieldType::DateTime) => {
                        // Match everything
                        if sort_order == SortOrder::Desc && val > i64::MAX as u64 {
                            return None;
                        }
                        // Add a limit to avoid overflow.
                        val = val.min(i64::MAX as u64);
                        DateTime::from_timestamp_nanos(val as i64).to_u64()
                    }
                    (SortValue::I64(val), SortFieldType::U64) => {
                        if val < 0 && sort_order == SortOrder::Asc {
                            return None;
                        }
                        if val < 0 && sort_order == SortOrder::Desc {
                            u64::MIN // matches nothing as search_after is not inclusive
                        } else {
                            val as u64
                        }
                    }
                    (SortValue::I64(val), SortFieldType::F64) => (val as f64).to_u64(),
                    (SortValue::I64(val), SortFieldType::DateTime) => {
                        DateTime::from_timestamp_nanos(val).to_u64()
                    }
                    (SortValue::F64(val), SortFieldType::U64) => {
                        let all_values_ahead1 =
                            val < u64::MIN as f64 && sort_order == SortOrder::Asc;
                        let all_values_ahead2 =
                            val > u64::MAX as f64 && sort_order == SortOrder::Desc;
                        if all_values_ahead1 || all_values_ahead2 {
                            return None;
                        }
                        // f64 cast already handles under/overflow and clamps the value
                        (val as u64).to_u64()
                    }
                    (SortValue::F64(val), SortFieldType::I64)
                    | (SortValue::F64(val), SortFieldType::DateTime) => {
                        let all_values_ahead1 =
                            val < i64::MIN as f64 && sort_order == SortOrder::Asc;
                        let all_values_ahead2 =
                            val > i64::MAX as f64 && sort_order == SortOrder::Desc;
                        if all_values_ahead1 || all_values_ahead2 {
                            return None;
                        }
                        // f64 cast already handles under/overflow and clamps the value
                        let val_i64 = val as i64;

                        if *sort_field_type == SortFieldType::DateTime {
                            DateTime::from_timestamp_nanos(val_i64).to_u64()
                        } else {
                            val_i64.to_u64()
                        }
                    }
                    // Not sure when we hit this, it's probably are very rare case.
                    (SortValue::Boolean(val), SortFieldType::U64) => val as u64,
                    (SortValue::Boolean(val), SortFieldType::F64) => (val as u64 as f64).to_u64(),
                    (SortValue::Boolean(val), SortFieldType::I64) => (val as i64).to_u64(),
                    (SortValue::Boolean(val), SortFieldType::DateTime) => {
                        DateTime::from_timestamp_nanos(val as i64).to_u64()
                    }
                    (SortValue::U64(mut val), SortFieldType::Bool) => {
                        let all_values_ahead1 = val > 1 && sort_order == SortOrder::Desc;
                        if all_values_ahead1 {
                            return None;
                        }
                        // clamp value for comparison
                        val = val.min(1).max(0);
                        (val == 1).to_u64()
                    }
                    (SortValue::I64(mut val), SortFieldType::Bool) => {
                        let all_values_ahead1 = val > 1 && sort_order == SortOrder::Desc;
                        let all_values_ahead2 = val < 0 && sort_order == SortOrder::Asc;
                        if all_values_ahead1 || all_values_ahead2 {
                            return None;
                        }
                        // clamp value for comparison
                        val = val.min(1).max(0);
                        (val == 1).to_u64()
                    }
                    (SortValue::F64(mut val), SortFieldType::Bool) => {
                        let all_values_ahead1 = val > 1.0 && sort_order == SortOrder::Desc;
                        let all_values_ahead2 = val < 0.0 && sort_order == SortOrder::Asc;
                        if all_values_ahead1 || all_values_ahead2 {
                            return None;
                        }
                        val = val.min(1.0).max(0.0);
                        (val >= 0.5).to_u64() // Is this correct?
                    }
                };
                Some(val)
            }
            SortingFieldExtractorComponent::Score => match sort_value {
                SortValue::F64(val) => Some(val.to_u64()),
                _ => panic!("Internal error: Got non-F64 sort value for Score."),
            },
        }
    }
}

impl From<SortingFieldExtractorComponent> for SortingFieldExtractorPair {
    fn from(value: SortingFieldExtractorComponent) -> Self {
        Self {
            first: value,
            second: None,
        }
    }
}

pub(crate) struct SortingFieldExtractorPair {
    pub first: SortingFieldExtractorComponent,
    pub second: Option<SortingFieldExtractorComponent>,
}

impl SortingFieldExtractorPair {
    pub fn is_score(&self) -> bool {
        self.first.is_score()
            || self
                .second
                .as_ref()
                .map(|second| second.is_score())
                .unwrap_or(false)
    }
    /// Returns the list of sort values for the given element
    ///
    /// See also [`SortingFieldExtractorComponent::extract_typed_sort_values_block`] for more
    /// information.
    #[inline]
    pub(crate) fn extract_typed_sort_values(
        &self,
        doc_ids: &[DocId],
        values1: &mut [Option<u64>],
        values2: &mut [Option<u64>],
    ) {
        self.first
            .extract_typed_sort_values_block(doc_ids, &mut values1[..doc_ids.len()]);
        if let Some(second) = self.second.as_ref() {
            second.extract_typed_sort_values_block(doc_ids, &mut values2[..doc_ids.len()]);
        }
    }
    /// Returns the list of sort values for the given element
    ///
    /// See also [`SortingFieldExtractorComponent::extract_typed_sort_value_opt`] for more
    /// information.
    #[inline]
    pub(crate) fn extract_typed_sort_value(
        &self,
        doc_id: DocId,
        score: Score,
    ) -> (Option<u64>, Option<u64>) {
        let first = self.first.extract_typed_sort_value_opt(doc_id, score);
        let second = self
            .second
            .as_ref()
            .and_then(|second| second.extract_typed_sort_value_opt(doc_id, score));
        (first, second)
    }
}

impl TryFrom<ColumnType> for SortFieldType {
    type Error = tantivy::TantivyError;

    fn try_from(column_type: ColumnType) -> tantivy::Result<Self> {
        match column_type {
            ColumnType::U64 => Ok(SortFieldType::U64),
            ColumnType::I64 => Ok(SortFieldType::I64),
            ColumnType::F64 => Ok(SortFieldType::F64),
            ColumnType::DateTime => Ok(SortFieldType::DateTime),
            ColumnType::Bool => Ok(SortFieldType::Bool),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unsupported sort field type `{:?}`.",
                column_type
            ))),
        }
    }
}

/// Takes a user-defined sorting criteria and resolves it to a
/// segment specific `SortingFieldExtractorPair`.
fn get_score_extractor(
    sort_by: &SortByPair,
    segment_reader: &SegmentReader,
) -> tantivy::Result<SortingFieldExtractorPair> {
    Ok(SortingFieldExtractorPair {
        first: sort_by
            .first
            .to_sorting_field_extractor_component(segment_reader)?,
        second: sort_by
            .second
            .as_ref()
            .map(|first| first.to_sorting_field_extractor_component(segment_reader))
            .transpose()?,
    })
}

enum AggregationSegmentCollectors {
    FindTraceIdsSegmentCollector(Box<FindTraceIdsSegmentCollector>),
    TantivyAggregationSegmentCollector(AggregationSegmentCollector),
}

/// Quickwit collector working at the scale of the segment.
pub struct QuickwitSegmentCollector {
    segment_top_k_collector: Option<Box<dyn QuickwitSegmentTopKCollector>>,
    aggregation: Option<AggregationSegmentCollectors>,
    num_hits: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct SegmentPartialHit {
    /// Normalized to u64, the typed value can be reconstructed with
    /// SortingFieldExtractorComponent.
    pub sort_value: Option<u64>,
    pub sort_value2: Option<u64>,
    pub doc_id: DocId,
}

impl SegmentPartialHit {
    pub fn into_partial_hit(
        self,
        split_id: SplitId,
        segment_ord: SegmentOrdinal,
        first: &SortingFieldExtractorComponent,
        second: &Option<SortingFieldExtractorComponent>,
    ) -> PartialHit {
        PartialHit {
            sort_value: self
                .sort_value
                .map(|sort_value| first.convert_u64_ff_val_to_sort_value(sort_value))
                .map(|sort_value| SortByValue {
                    sort_value: Some(sort_value),
                }),
            sort_value2: self
                .sort_value2
                .map(|sort_value| {
                    second
                        .as_ref()
                        .expect("Internal error: Got sort_value2, but no sort extractor")
                        .convert_u64_ff_val_to_sort_value(sort_value)
                })
                .map(|sort_value| SortByValue {
                    sort_value: Some(sort_value),
                }),
            doc_id: self.doc_id,
            split_id,
            segment_ord,
        }
    }
}

impl SegmentCollector for QuickwitSegmentCollector {
    type Fruit = tantivy::Result<LeafSearchResponse>;

    #[inline]
    fn collect_block(&mut self, filtered_docs: &[DocId]) {
        // Update results
        self.num_hits += filtered_docs.len() as u64;

        if let Some(segment_top_k_collector) = self.segment_top_k_collector.as_mut() {
            segment_top_k_collector.collect_top_k_block(filtered_docs);
        }

        match self.aggregation.as_mut() {
            Some(AggregationSegmentCollectors::FindTraceIdsSegmentCollector(collector)) => {
                collector.collect_block(filtered_docs)
            }
            Some(AggregationSegmentCollectors::TantivyAggregationSegmentCollector(collector)) => {
                collector.collect_block(filtered_docs)
            }
            None => (),
        }
    }

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        self.num_hits += 1;
        if let Some(segment_top_k_collector) = self.segment_top_k_collector.as_mut() {
            segment_top_k_collector.collect_top_k(doc_id, score);
        }

        match self.aggregation.as_mut() {
            Some(AggregationSegmentCollectors::FindTraceIdsSegmentCollector(collector)) => {
                collector.collect(doc_id, score)
            }
            Some(AggregationSegmentCollectors::TantivyAggregationSegmentCollector(collector)) => {
                collector.collect(doc_id, score)
            }
            None => (),
        }
    }

    fn harvest(self) -> Self::Fruit {
        let mut partial_hits: Vec<PartialHit> = Vec::new();
        if let Some(segment_top_k_collector) = self.segment_top_k_collector {
            partial_hits = segment_top_k_collector.get_top_k();
        }

        let intermediate_aggregation_result = match self.aggregation {
            Some(AggregationSegmentCollectors::FindTraceIdsSegmentCollector(collector)) => {
                let fruit: Vec<Span> = collector.harvest();
                let serialized =
                    postcard::to_allocvec(&fruit).expect("Collector fruit should be serializable.");
                Some(serialized)
            }
            Some(AggregationSegmentCollectors::TantivyAggregationSegmentCollector(collector)) => {
                let serialized = postcard::to_allocvec(&collector.harvest()?)
                    .expect("Collector fruit should be serializable.");
                Some(serialized)
            }
            None => None,
        };
        Ok(LeafSearchResponse {
            intermediate_aggregation_result,
            num_hits: self.num_hits,
            partial_hits,
            failed_splits: Vec::new(),
            num_attempted_splits: 1,
        })
    }
}

/// Available aggregation types.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum QuickwitAggregations {
    /// Aggregation used by the Jaeger service to find trace IDs that match a
    /// [`quickwit_proto::jaeger::storage::v1::FindTraceIDsRequest`].
    FindTraceIdsAggregation(FindTraceIdsCollector),
    /// Your classic Tantivy aggregation.
    TantivyAggregations(Aggregations),
}

impl QuickwitAggregations {
    fn fast_field_names(&self) -> HashSet<String> {
        match self {
            QuickwitAggregations::FindTraceIdsAggregation(collector) => {
                collector.fast_field_names()
            }
            QuickwitAggregations::TantivyAggregations(aggregations) => {
                get_fast_field_names(aggregations)
            }
        }
    }

    fn maybe_incremental_aggregator(&self) -> QuickwitIncrementalAggregations {
        match self {
            QuickwitAggregations::FindTraceIdsAggregation(aggreg) => {
                QuickwitIncrementalAggregations::FindTraceIdsAggregation(aggreg.clone(), Vec::new())
            }
            QuickwitAggregations::TantivyAggregations(aggreg) => {
                QuickwitIncrementalAggregations::TantivyAggregations(aggreg.clone(), Vec::new())
            }
        }
    }
}

#[derive(Clone)]
enum QuickwitIncrementalAggregations {
    FindTraceIdsAggregation(FindTraceIdsCollector, Vec<Vec<Span>>),
    TantivyAggregations(Aggregations, Vec<Vec<u8>>),
    NoAggregation,
}

impl QuickwitIncrementalAggregations {
    fn add(&mut self, intermediate_result: Vec<u8>) -> tantivy::Result<()> {
        match self {
            QuickwitIncrementalAggregations::FindTraceIdsAggregation(collector, ref mut state) => {
                let fruits: Vec<Span> =
                    postcard::from_bytes(&intermediate_result).map_err(map_error)?;
                state.push(fruits);
                if state.iter().map(Vec::len).sum::<usize>() >= collector.num_traces {
                    let new_state = collector.merge_fruits(std::mem::take(state))?;
                    state.push(new_state);
                }
            }
            QuickwitIncrementalAggregations::TantivyAggregations(_, state) => {
                state.push(intermediate_result);
            }
            QuickwitIncrementalAggregations::NoAggregation => (),
        }
        Ok(())
    }

    fn virtual_worst_hit(&self) -> Option<PartialHit> {
        match self {
            QuickwitIncrementalAggregations::FindTraceIdsAggregation(collector, state) => {
                if let Some(first) = state.first() {
                    if first.len() >= collector.num_traces {
                        if let Some(last_elem) = first.last() {
                            let timestamp = last_elem.span_timestamp.into_timestamp_nanos();
                            return Some(PartialHit {
                                sort_value: Some(SortByValue {
                                    sort_value: Some(SortValue::I64(timestamp)),
                                }),
                                sort_value2: None,
                                split_id: SplitId::new(),
                                segment_ord: 0,
                                doc_id: 0,
                            });
                        }
                    }
                }
                None
            }
            QuickwitIncrementalAggregations::TantivyAggregations(_, _) => None,
            QuickwitIncrementalAggregations::NoAggregation => None,
        }
    }

    fn finalize(self) -> tantivy::Result<Option<Vec<u8>>> {
        match self {
            QuickwitIncrementalAggregations::FindTraceIdsAggregation(collector, mut state) => {
                let merged_fruit = if state.len() > 1 {
                    collector.merge_fruits(state)?
                } else {
                    state.pop().unwrap_or_default()
                };
                let serialized = postcard::to_allocvec(&merged_fruit).map_err(map_error)?;
                Ok(Some(serialized))
            }
            QuickwitIncrementalAggregations::TantivyAggregations(aggregation, state) => {
                merge_intermediate_aggregation_result(
                    &Some(QuickwitAggregations::TantivyAggregations(aggregation)),
                    state.iter().map(|vec| vec.as_slice()),
                )
            }
            QuickwitIncrementalAggregations::NoAggregation => Ok(None),
        }
    }
}

/// The quickwit collector is the tantivy Collector used in Quickwit.
///
/// It defines the data that should be accumulated about the documents matching
/// the query.
#[derive(Clone)]
pub(crate) struct QuickwitCollector {
    pub split_id: SplitId,
    pub start_offset: usize,
    pub max_hits: usize,
    pub sort_by: SortByPair,
    pub aggregation: Option<QuickwitAggregations>,
    pub aggregation_limits: AggregationLimits,
    search_after: Option<PartialHit>,
}

impl QuickwitCollector {
    pub fn is_count_only(&self) -> bool {
        self.max_hits == 0 && self.aggregation.is_none()
    }
    /// Updates search parameters affecting the returned documents.
    /// Does not update aggregations.
    pub fn update_search_param(&mut self, search_request: &SearchRequest) {
        let sort_by = sort_by_from_request(search_request);
        self.sort_by = sort_by;
        self.max_hits = search_request.max_hits as usize;
        self.start_offset = search_request.start_offset as usize;
        self.search_after.clone_from(&search_request.search_after);
    }
    pub fn fast_field_names(&self) -> HashSet<String> {
        let mut fast_field_names = HashSet::default();
        self.sort_by.first.add_fast_field(&mut fast_field_names);
        if let Some(sort_by_second) = &self.sort_by.second {
            sort_by_second.add_fast_field(&mut fast_field_names);
        }
        if let Some(aggregations) = &self.aggregation {
            fast_field_names.extend(aggregations.fast_field_names());
        }
        fast_field_names
    }

    pub fn warmup_info(&self) -> WarmupInfo {
        WarmupInfo {
            fast_field_names: self.fast_field_names(),
            field_norms: self.requires_scoring(),
            ..WarmupInfo::default()
        }
    }
}

impl Collector for QuickwitCollector {
    type Child = QuickwitSegmentCollector;
    type Fruit = LeafSearchResponse;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        // Regardless of the start_offset, we need to collect top-K
        // starting from 0 for every leaves.
        let leaf_max_hits = self.max_hits + self.start_offset;

        let aggregation = match &self.aggregation {
            Some(QuickwitAggregations::FindTraceIdsAggregation(collector)) => {
                Some(AggregationSegmentCollectors::FindTraceIdsSegmentCollector(
                    Box::new(collector.for_segment(0, segment_reader)?),
                ))
            }
            Some(QuickwitAggregations::TantivyAggregations(aggs)) => Some(
                AggregationSegmentCollectors::TantivyAggregationSegmentCollector(
                    AggregationSegmentCollector::from_agg_req_and_reader(
                        aggs,
                        segment_reader,
                        segment_ord,
                        &self.aggregation_limits,
                    )?,
                ),
            ),
            None => None,
        };
        let score_extractor = get_score_extractor(&self.sort_by, segment_reader)?;
        let (order1, order2) = self.sort_by.sort_orders();

        let segment_top_k_collector = if leaf_max_hits == 0 {
            None
        } else {
            let coll: Box<dyn QuickwitSegmentTopKCollector> = specialized_top_k_segment_collector(
                self.split_id.clone(),
                score_extractor,
                leaf_max_hits,
                segment_ord,
                self.search_after.clone(),
                order1,
                order2,
            );
            Some(coll)
        };

        Ok(QuickwitSegmentCollector {
            num_hits: 0,
            segment_top_k_collector,
            aggregation,
        })
    }

    fn requires_scoring(&self) -> bool {
        // We do not need BM25 scoring in Quickwit if it is not opted-in.
        // By returning false, we inform tantivy that it does not need to decompress
        // term frequencies.
        self.sort_by.first.requires_scoring()
            || self
                .sort_by
                .second
                .as_ref()
                .map(|sort_by| sort_by.requires_scoring())
                .unwrap_or(false)
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<tantivy::Result<LeafSearchResponse>>,
    ) -> tantivy::Result<Self::Fruit> {
        let segment_fruits: tantivy::Result<Vec<LeafSearchResponse>> =
            segment_fruits.into_iter().collect();
        // We want the hits in [start_offset..start_offset + max_hits).
        // All leaves will return their top [0..start_offset + max_hits) documents.
        // We compute the overall [0..start_offset + max_hits) documents ...
        let num_hits = self.start_offset + self.max_hits;
        let (sort_order1, sort_order2) = self.sort_by.sort_orders();
        let mut merged_leaf_response = merge_leaf_responses(
            &self.aggregation,
            segment_fruits?,
            sort_order1,
            sort_order2,
            num_hits,
        )?;
        // ... and drop the first [..start_offsets) hits.
        // note that self.start_offset is 0 when merging from leaf_search, and is only set when
        // merging from root_search, so as to remove the firsts elements only once.
        merged_leaf_response.partial_hits.drain(
            0..self
                .start_offset
                .min(merged_leaf_response.partial_hits.len()),
        );
        merged_leaf_response.partial_hits.truncate(self.max_hits);
        Ok(merged_leaf_response)
    }
}

fn map_error(err: postcard::Error) -> TantivyError {
    TantivyError::InternalError(format!("merge result Postcard error: {err}"))
}

/// Merges a set of Leaf Results.
fn merge_intermediate_aggregation_result<'a>(
    aggregations_opt: &Option<QuickwitAggregations>,
    intermediate_aggregation_results: impl Iterator<Item = &'a [u8]>,
) -> tantivy::Result<Option<Vec<u8>>> {
    let merged_intermediate_aggregation_result = match aggregations_opt {
        Some(QuickwitAggregations::FindTraceIdsAggregation(collector)) => {
            let fruits: Vec<
                <<FindTraceIdsCollector as Collector>::Child as SegmentCollector>::Fruit,
            > = intermediate_aggregation_results
                .map(|intermediate_aggregation_result| {
                    postcard::from_bytes(intermediate_aggregation_result).map_err(map_error)
                })
                .collect::<Result<_, _>>()?;
            let merged_fruit: Vec<Span> = collector.merge_fruits(fruits)?;
            let serialized = postcard::to_allocvec(&merged_fruit).map_err(map_error)?;
            Some(serialized)
        }
        Some(QuickwitAggregations::TantivyAggregations(_)) => {
            let fruits: Vec<IntermediateAggregationResults> = intermediate_aggregation_results
                .map(|intermediate_aggregation_result| {
                    postcard::from_bytes(intermediate_aggregation_result).map_err(map_error)
                })
                .collect::<Result<_, _>>()?;

            let mut fruit_iter = fruits.into_iter();
            if let Some(first_fruit) = fruit_iter.next() {
                let mut merged_fruit = first_fruit;
                for fruit in fruit_iter {
                    merged_fruit.merge_fruits(fruit)?;
                }
                let serialized = postcard::to_allocvec(&merged_fruit).map_err(map_error)?;

                Some(serialized)
            } else {
                None
            }
        }
        None => None,
    };

    Ok(merged_intermediate_aggregation_result)
}

/// Merges a set of Leaf Results.
fn merge_leaf_responses(
    aggregations_opt: &Option<QuickwitAggregations>,
    mut leaf_responses: Vec<LeafSearchResponse>,
    sort_order1: SortOrder,
    sort_order2: SortOrder,
    max_hits: usize,
) -> tantivy::Result<LeafSearchResponse> {
    // Optimization: No merging needed if there is only one result.
    if leaf_responses.len() == 1 {
        return Ok(leaf_responses.pop().unwrap());
    }

    let merged_intermediate_aggregation_result: Option<Vec<u8>> =
        merge_intermediate_aggregation_result(
            aggregations_opt,
            leaf_responses.iter().filter_map(|leaf_response| {
                leaf_response.intermediate_aggregation_result.as_deref()
            }),
        )?;
    let num_attempted_splits = leaf_responses
        .iter()
        .map(|leaf_response| leaf_response.num_attempted_splits)
        .sum();
    let num_hits: u64 = leaf_responses
        .iter()
        .map(|leaf_response| leaf_response.num_hits)
        .sum();
    let failed_splits = leaf_responses
        .iter()
        .flat_map(|leaf_response| leaf_response.failed_splits.iter())
        .cloned()
        .collect_vec();
    let all_partial_hits: Vec<PartialHit> = leaf_responses
        .into_iter()
        .flat_map(|leaf_response| leaf_response.partial_hits)
        .collect();
    let top_k_partial_hits: Vec<PartialHit> = top_k_partial_hits(
        all_partial_hits.into_iter(),
        sort_order1,
        sort_order2,
        max_hits,
    );
    Ok(LeafSearchResponse {
        intermediate_aggregation_result: merged_intermediate_aggregation_result,
        num_hits,
        partial_hits: top_k_partial_hits,
        failed_splits,
        num_attempted_splits,
    })
}

/// Mutates partial_hits so that it contains the top-num_hitso hits,
/// and so that these elements are sorted.
///
/// TODO we could possibly optimize the sort away (but I doubt it matters).
fn top_k_partial_hits(
    partial_hits: impl Iterator<Item = PartialHit>,
    order1: SortOrder,
    order2: SortOrder,
    num_hits: usize,
) -> Vec<PartialHit> {
    let sort_key_mapper = HitSortingMapper { order1, order2 };
    let mut top_k_hits = TopK::new(num_hits, sort_key_mapper);

    partial_hits.for_each(|hit| top_k_hits.add_entry(hit));

    top_k_hits.finalize()
}

pub(crate) fn sort_by_from_request(search_request: &SearchRequest) -> SortByPair {
    let to_sort_by_component = |field_name: &str, order| {
        if field_name == "_score" {
            SortByComponent::Score { order }
        } else if field_name == "_shard_doc" || field_name == "_doc" {
            SortByComponent::DocId { order }
        } else {
            SortByComponent::FastField {
                field_name: field_name.to_string(),
                order,
            }
        }
    };

    let num_sort_fields = search_request.sort_fields.len();
    if num_sort_fields == 0 {
        SortByComponent::DocId {
            order: SortOrder::Desc,
        }
        .into()
    } else if num_sort_fields == 1 {
        let sort_field = &search_request.sort_fields[0];
        let order = SortOrder::try_from(sort_field.sort_order).unwrap_or(SortOrder::Desc);
        to_sort_by_component(&sort_field.field_name, order).into()
    } else if num_sort_fields == 2 {
        let sort_field1 = &search_request.sort_fields[0];
        let order1 = SortOrder::try_from(sort_field1.sort_order).unwrap_or(SortOrder::Desc);
        let sort_field2 = &search_request.sort_fields[1];
        let order2 = SortOrder::try_from(sort_field2.sort_order).unwrap_or(SortOrder::Desc);
        SortByPair {
            first: to_sort_by_component(&sort_field1.field_name, order1),
            second: Some(to_sort_by_component(&sort_field2.field_name, order2)),
        }
    } else {
        panic!("Sort by more than 2 fields is not supported yet.")
    }
}

/// Builds the QuickwitCollector, in function of the information that was requested by the user.
pub(crate) fn make_collector_for_split(
    split_id: SplitId,
    search_request: &SearchRequest,
    aggregation_limits: AggregationLimits,
) -> crate::Result<QuickwitCollector> {
    let aggregation = match &search_request.aggregation_request {
        Some(aggregation) => Some(serde_json::from_str(aggregation)?),
        None => None,
    };
    let sort_by = sort_by_from_request(search_request);
    Ok(QuickwitCollector {
        split_id,
        start_offset: search_request.start_offset as usize,
        max_hits: search_request.max_hits as usize,
        sort_by,
        aggregation,
        aggregation_limits,
        search_after: search_request.search_after.clone(),
    })
}

/// Builds a QuickwitCollector that's only useful for merging fruits.
pub(crate) fn make_merge_collector(
    search_request: &SearchRequest,
    aggregation_limits: &AggregationLimits,
) -> crate::Result<QuickwitCollector> {
    let aggregation = match &search_request.aggregation_request {
        Some(aggregation) => Some(serde_json::from_str(aggregation)?),
        None => None,
    };
    let sort_by = sort_by_from_request(search_request);
    Ok(QuickwitCollector {
        split_id: SplitId::default(),
        start_offset: search_request.start_offset as usize,
        max_hits: search_request.max_hits as usize,
        sort_by,
        aggregation,
        aggregation_limits: aggregation_limits.clone(),
        search_after: search_request.search_after.clone(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentPartialHitSortingKey {
    sort_value: Option<u64>,
    sort_value2: Option<u64>,
    doc_id: DocId,
    // TODO This should not be there.
    sort_order: SortOrder,
    // TODO This should not be there.
    sort_order2: SortOrder,
}

impl Ord for SegmentPartialHitSortingKey {
    fn cmp(&self, other: &SegmentPartialHitSortingKey) -> Ordering {
        debug_assert_eq!(
            self.sort_order, other.sort_order,
            "comparing two PartialHitSortingKey of different ordering"
        );
        debug_assert_eq!(
            self.sort_order2, other.sort_order2,
            "comparing two PartialHitSortingKey of different ordering"
        );
        let order = self
            .sort_order
            .compare_opt(&self.sort_value, &other.sort_value);
        let order2 = self
            .sort_order2
            .compare_opt(&self.sort_value2, &other.sort_value2);
        let order_addr = self.sort_order.compare(&self.doc_id, &other.doc_id);
        order.then(order2).then(order_addr)
    }
}

impl PartialOrd for SegmentPartialHitSortingKey {
    fn partial_cmp(&self, other: &SegmentPartialHitSortingKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartialHitSortingKey {
    sort_value: Option<SortValue>,
    sort_value2: Option<SortValue>,
    address: GlobalDocAddress,
    // TODO remove this
    sort_order: SortOrder,
    sort_order2: SortOrder,
}

impl Ord for PartialHitSortingKey {
    fn cmp(&self, other: &PartialHitSortingKey) -> Ordering {
        assert_eq!(
            self.sort_order, other.sort_order,
            "comparing two PartialHitSortingKey of different ordering"
        );
        assert_eq!(
            self.sort_order2, other.sort_order2,
            "comparing two PartialHitSortingKey of different ordering"
        );

        let order = self
            .sort_order
            .compare_opt(&self.sort_value, &other.sort_value);

        let order2 = self
            .sort_order2
            .compare_opt(&self.sort_value2, &other.sort_value2);

        let order_addr = self.sort_order.compare(&self.address, &other.address);

        order.then(order2).then(order_addr)
    }
}

impl PartialOrd for PartialHitSortingKey {
    fn partial_cmp(&self, other: &PartialHitSortingKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub(crate) struct HitSortingMapper {
    pub order1: SortOrder,
    pub order2: SortOrder,
}

impl SortKeyMapper<PartialHit> for HitSortingMapper {
    type Key = PartialHitSortingKey;
    fn get_sort_key(&self, partial_hit: &PartialHit) -> PartialHitSortingKey {
        PartialHitSortingKey {
            sort_value: partial_hit.sort_value.and_then(|v| v.sort_value),
            sort_value2: partial_hit.sort_value2.and_then(|v| v.sort_value),
            address: GlobalDocAddress::from_partial_hit(partial_hit),
            sort_order: self.order1,
            sort_order2: self.order2,
        }
    }
}

impl SortKeyMapper<SegmentPartialHit> for HitSortingMapper {
    type Key = SegmentPartialHitSortingKey;
    fn get_sort_key(&self, partial_hit: &SegmentPartialHit) -> SegmentPartialHitSortingKey {
        SegmentPartialHitSortingKey {
            sort_value: partial_hit.sort_value,
            sort_value2: partial_hit.sort_value2,
            doc_id: partial_hit.doc_id,
            sort_order: self.order1,
            sort_order2: self.order2,
        }
    }
}

/// Incrementally merge segment results.
#[derive(Clone)]
pub(crate) struct IncrementalCollector {
    top_k_hits: TopK<PartialHit, PartialHitSortingKey, HitSortingMapper>,
    incremental_aggregation: QuickwitIncrementalAggregations,
    num_hits: u64,
    failed_splits: Vec<SplitSearchError>,
    num_attempted_splits: u64,
    start_offset: usize,
}

impl IncrementalCollector {
    /// Create a new incremental collector
    pub(crate) fn new(collector: QuickwitCollector) -> Self {
        let incremental_aggregation = collector
            .aggregation
            .as_ref()
            .map(QuickwitAggregations::maybe_incremental_aggregator)
            .unwrap_or(QuickwitIncrementalAggregations::NoAggregation);
        let (order1, order2) = collector.sort_by.sort_orders();
        let sort_key_mapper = HitSortingMapper { order1, order2 };
        IncrementalCollector {
            top_k_hits: TopK::new(collector.max_hits + collector.start_offset, sort_key_mapper),
            start_offset: collector.start_offset,
            incremental_aggregation,
            num_hits: 0,
            failed_splits: Vec::new(),
            num_attempted_splits: 0,
        }
    }

    /// Merge one search result with the current state
    pub(crate) fn add_result(&mut self, leaf_response: LeafSearchResponse) -> tantivy::Result<()> {
        let LeafSearchResponse {
            num_hits,
            partial_hits,
            failed_splits,
            num_attempted_splits,
            intermediate_aggregation_result,
        } = leaf_response;

        self.num_hits += num_hits;
        self.top_k_hits.add_entries(partial_hits.into_iter());
        self.failed_splits.extend(failed_splits);
        self.num_attempted_splits += num_attempted_splits;
        if let Some(intermediate_aggregation_result) = intermediate_aggregation_result {
            self.incremental_aggregation
                .add(intermediate_aggregation_result)?;
        }
        Ok(())
    }

    /// Add a failed split to the state
    pub(crate) fn add_failed_split(&mut self, split_error: SplitSearchError) {
        self.failed_splits.push(split_error)
    }

    /// Get the worst top-hit. Can be used to skip splits if they can't possibly do better.
    ///
    /// Only returns a result if enough hits were recorded already.
    pub(crate) fn peek_worst_hit(&self) -> Option<Cow<PartialHit>> {
        if self.top_k_hits.max_len() == 0 {
            return self
                .incremental_aggregation
                .virtual_worst_hit()
                .map(Cow::Owned);
        }

        if self.top_k_hits.at_capacity() {
            self.top_k_hits.peek_worst().map(Cow::Borrowed)
        } else {
            None
        }
    }

    /// Finalize the merge, creating a LeafSearchResponse.
    pub(crate) fn finalize(self) -> tantivy::Result<LeafSearchResponse> {
        let intermediate_aggregation_result = self.incremental_aggregation.finalize()?;
        let mut partial_hits = self.top_k_hits.finalize();
        if self.start_offset != 0 {
            partial_hits.drain(0..self.start_offset.min(partial_hits.len()));
        }
        Ok(LeafSearchResponse {
            num_hits: self.num_hits,
            partial_hits,
            failed_splits: self.failed_splits,
            num_attempted_splits: self.num_attempted_splits,
            intermediate_aggregation_result,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use quickwit_proto::search::{
        LeafSearchResponse, PartialHit, SearchRequest, SortByValue, SortField, SortOrder,
        SortValue, SplitSearchError,
    };
    use quickwit_proto::types::DocMappingUid;
    use tantivy::collector::Collector;
    use tantivy::TantivyDocument;

    use super::{make_merge_collector, IncrementalCollector};
    use crate::collector::top_k_partial_hits;

    #[test]
    fn test_merge_partial_hits_no_tie() {
        let make_doc = |sort_value: u64| PartialHit {
            sort_value: Some(SortValue::U64(sort_value).into()),
            sort_value2: None,
            split_id: "split1".to_string(),
            segment_ord: 0u32,
            doc_id: 0u32,
        };
        assert_eq!(
            top_k_partial_hits(
                vec![make_doc(1u64), make_doc(3u64), make_doc(2u64),].into_iter(),
                SortOrder::Asc,
                SortOrder::Asc,
                2
            ),
            vec![make_doc(1), make_doc(2)]
        );
    }

    #[test]
    fn test_merge_partial_hits_with_tie() {
        let make_hit_given_split_id = |split_id: u64| PartialHit {
            sort_value: Some(SortValue::U64(0u64).into()),
            sort_value2: None,
            split_id: format!("split_{split_id}"),
            segment_ord: 0u32,
            doc_id: 0u32,
        };
        assert_eq!(
            &top_k_partial_hits(
                vec![
                    make_hit_given_split_id(1u64),
                    make_hit_given_split_id(3u64),
                    make_hit_given_split_id(2u64),
                ]
                .into_iter(),
                SortOrder::Desc,
                SortOrder::Desc,
                2
            ),
            &[make_hit_given_split_id(3), make_hit_given_split_id(2)]
        );
        assert_eq!(
            &top_k_partial_hits(
                vec![
                    make_hit_given_split_id(1u64),
                    make_hit_given_split_id(3u64),
                    make_hit_given_split_id(2u64),
                ]
                .into_iter(),
                SortOrder::Asc,
                SortOrder::Asc,
                2
            ),
            &[make_hit_given_split_id(1), make_hit_given_split_id(2)]
        );
    }

    // TODO figure out a way to remove this boilerplate and use mockall
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct MockDocMapper;

    #[typetag::serde(name = "mock")]
    impl quickwit_doc_mapper::DocMapper for MockDocMapper {
        fn doc_mapping_uid(&self) -> DocMappingUid {
            DocMappingUid::default()
        }

        // Required methods
        fn doc_from_json_obj(
            &self,
            _json_obj: quickwit_doc_mapper::JsonObject,
            _doc_len: u64,
        ) -> Result<(u64, TantivyDocument), quickwit_doc_mapper::DocParsingError> {
            unimplemented!()
        }
        fn doc_to_json(
            &self,
            _named_doc: std::collections::BTreeMap<String, Vec<tantivy::schema::OwnedValue>>,
        ) -> anyhow::Result<quickwit_doc_mapper::JsonObject> {
            unimplemented!()
        }
        fn schema(&self) -> tantivy::schema::Schema {
            unimplemented!()
        }
        fn query(
            &self,
            _split_schema: tantivy::schema::Schema,
            _query_ast: &quickwit_query::query_ast::QueryAst,
            _with_validation: bool,
        ) -> Result<
            (
                Box<dyn tantivy::query::Query>,
                quickwit_doc_mapper::WarmupInfo,
            ),
            quickwit_doc_mapper::QueryParserError,
        > {
            unimplemented!()
        }
        fn default_search_fields(&self) -> &[String] {
            unimplemented!()
        }
        fn max_num_partitions(&self) -> std::num::NonZeroU32 {
            unimplemented!()
        }
        fn tokenizer_manager(&self) -> &quickwit_query::tokenizers::TokenizerManager {
            unimplemented!()
        }

        fn timestamp_field_name(&self) -> Option<&str> {
            None
        }
    }

    fn sort_dataset() -> Vec<(Option<u64>, Option<u64>)> {
        // every combination of 0..=2 + None, in random order.
        // (2, 1) is duplicated to allow testing for DocId sorting with two sort fields
        vec![
            (Some(2), Some(1)),
            (Some(0), Some(1)),
            (Some(1), Some(1)),
            (Some(0), Some(0)),
            (None, Some(1)),
            (None, Some(2)),
            (Some(2), Some(1)),
            (Some(1), Some(2)),
            (Some(0), None),
            (None, Some(0)),
            (Some(2), Some(0)),
            (Some(2), Some(2)),
            (Some(0), Some(2)),
            (Some(2), None),
            (None, None),
            (Some(1), Some(0)),
            (Some(1), None),
        ]
    }

    fn make_request(max_hits: u64, sort_fields: &str) -> SearchRequest {
        SearchRequest {
            max_hits,
            sort_fields: sort_fields
                .split(',')
                .filter(|field| !field.is_empty())
                .map(|field| {
                    if let Some(field) = field.strip_prefix('-') {
                        SortField {
                            field_name: field.to_string(),
                            sort_order: SortOrder::Asc.into(),
                            sort_datetime_format: None,
                        }
                    } else {
                        SortField {
                            field_name: field.to_string(),
                            sort_order: SortOrder::Desc.into(),
                            sort_datetime_format: None,
                        }
                    }
                })
                .collect(),
            ..SearchRequest::default()
        }
    }

    fn make_index() -> tantivy::Index {
        use tantivy::indexer::UserOperation;
        use tantivy::schema::{NumericOptions, Schema};
        use tantivy::Index;

        let dataset = sort_dataset();

        let mut schema_builder = Schema::builder();
        let opts = NumericOptions::default().set_fast();

        schema_builder.add_u64_field("sort1", opts.clone());
        schema_builder.add_u64_field("sort2", opts);
        let schema = schema_builder.build();

        let field1 = schema.get_field("sort1").unwrap();
        let field2 = schema.get_field("sort2").unwrap();

        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer(50_000_000).unwrap();

        index_writer
            .run(
                dataset
                    .into_iter()
                    .map(|(val1, val2)| {
                        let mut doc = TantivyDocument::new();
                        if let Some(val1) = val1 {
                            doc.add_u64(field1, val1);
                        }
                        if let Some(val2) = val2 {
                            doc.add_u64(field2, val2);
                        }
                        doc
                    })
                    .map(UserOperation::Add),
            )
            .unwrap();
        index_writer.commit().unwrap();

        index
    }

    #[test]
    fn test_single_split_sorting() {
        let index = make_index();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // tuple of DocId and sort value
        type Doc = (usize, (Option<u64>, Option<u64>));

        let mut dataset: Vec<Doc> = sort_dataset().into_iter().enumerate().collect();

        let reverse_int = |val: &Option<u64>| val.as_ref().map(|val| u64::MAX - val);
        let cmp_doc_id_desc = |a: &Doc, b: &Doc| b.0.cmp(&a.0);
        let cmp_doc_id_asc = |a: &Doc, b: &Doc| a.0.cmp(&b.0);
        let cmp_1_desc = |a: &Doc, b: &Doc| b.1 .0.cmp(&a.1 .0);
        let cmp_1_asc = |a: &Doc, b: &Doc| reverse_int(&b.1 .0).cmp(&reverse_int(&a.1 .0));
        let cmp_2_desc = |a: &Doc, b: &Doc| b.1 .1.cmp(&a.1 .1);
        let cmp_2_asc = |a: &Doc, b: &Doc| reverse_int(&b.1 .1).cmp(&reverse_int(&a.1 .1));

        {
            // the logic for sorting isn't easy to wrap one's head around. These simple tests are
            // here to convince oneself they do what we want them todo
            let mut data = vec![(1, (None, None)), (0, (None, None))];
            let data_copy = data.clone();
            data.sort_by(cmp_doc_id_desc);
            assert_eq!(data, data_copy);

            let mut data = vec![(0, (None, None)), (1, (None, None))];
            let data_copy = data.clone();
            data.sort_by(cmp_doc_id_asc);
            assert_eq!(data, data_copy);

            let mut data = vec![
                (1, (Some(2), None)),
                (0, (Some(1), None)),
                (2, (None, None)),
            ];
            let data_copy = data.clone();
            data.sort_by(cmp_1_desc);
            assert_eq!(data, data_copy);

            let mut data = vec![
                (1, (Some(1), None)),
                (0, (Some(2), None)),
                (2, (None, None)),
            ];
            let data_copy = data.clone();
            data.sort_by(cmp_1_asc);
            assert_eq!(data, data_copy);

            let mut data = vec![
                (1, (None, Some(2))),
                (0, (None, Some(1))),
                (2, (None, None)),
            ];
            let data_copy = data.clone();
            data.sort_by(cmp_2_desc);
            assert_eq!(data, data_copy);

            let mut data = vec![
                (1, (None, Some(1))),
                (0, (None, Some(2))),
                (2, (None, None)),
            ];
            let data_copy = data.clone();
            data.sort_by(cmp_2_asc);
            assert_eq!(data, data_copy);
        }

        #[allow(clippy::type_complexity)]
        let sort_orders: Vec<(_, Box<dyn Fn(&Doc, &Doc) -> Ordering>)> = vec![
            ("", Box::new(cmp_doc_id_desc)),
            (
                "sort1",
                Box::new(|a, b| cmp_1_desc(a, b).then(cmp_doc_id_desc(a, b))),
            ),
            (
                "-sort1",
                Box::new(|a, b| cmp_1_asc(a, b).then(cmp_doc_id_asc(a, b))),
            ),
            (
                "sort1,sort2",
                Box::new(|a, b| {
                    cmp_1_desc(a, b).then(cmp_2_desc(a, b).then(cmp_doc_id_desc(a, b)))
                }),
            ),
            (
                "-sort1,sort2",
                Box::new(|a, b| {
                    cmp_1_asc(a, b)
                        .then(cmp_2_desc(a, b))
                        .then(cmp_doc_id_asc(a, b))
                }),
            ),
            (
                "sort1,-sort2",
                Box::new(|a, b| cmp_1_desc(a, b).then(cmp_2_asc(a, b).then(cmp_doc_id_desc(a, b)))),
            ),
            (
                "-sort1,-sort2",
                Box::new(|a, b| {
                    cmp_1_asc(a, b)
                        .then(cmp_2_asc(a, b))
                        .then(cmp_doc_id_asc(a, b))
                }),
            ),
        ];

        for (sort_str, sort_function) in sort_orders {
            dataset.sort_by(sort_function);
            // Check increasing slice sizes of the dataset
            for slice_len in 0..dataset.len() {
                let collector = super::make_collector_for_split(
                    "fake_split_id".to_string(),
                    &make_request(slice_len as u64, sort_str),
                    Default::default(),
                )
                .unwrap();
                let res = searcher
                    .search(&tantivy::query::AllQuery, &collector)
                    .unwrap();
                assert_eq!(
                    res.partial_hits.len(),
                    slice_len,
                    "mismatch slice_len for \"{sort_str}\":{slice_len}"
                );
                for (expected, got) in dataset.iter().zip(res.partial_hits.iter()) {
                    if expected.0 as u32 != got.doc_id {
                        let expected_docids = dataset
                            .iter()
                            .map(|(docid, val)| {
                                format!("{} {:?} {:?}", *docid as u32, val.0.clone(), val.1.clone())
                            })
                            .collect::<Vec<_>>();
                        let got_docids = res
                            .partial_hits
                            .iter()
                            .map(|hit| {
                                format!(
                                    "{} {:?} {:?}",
                                    hit.doc_id,
                                    hit.sort_value.and_then(|el| el.sort_value).clone(),
                                    hit.sort_value2.and_then(|el| el.sort_value).clone()
                                )
                            })
                            .collect::<Vec<_>>();
                        eprintln!("expected: {:#?}", expected_docids);
                        eprintln!("got: {:#?}", got_docids);
                        panic!("mismatch ordering for \"{sort_str}\":{slice_len}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_search_after() {
        let index = make_index();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // tuple of DocId and sort value
        type Doc = (usize, (Option<u64>, Option<u64>));

        let mut dataset: Vec<Doc> = sort_dataset().into_iter().enumerate().collect();

        let reverse_int = |val: &Option<u64>| val.as_ref().map(|val| u64::MAX - val);
        let cmp_doc_id_desc = |a: &Doc, b: &Doc| b.0.cmp(&a.0);
        let cmp_1_desc = |a: &Doc, b: &Doc| b.1 .0.cmp(&a.1 .0);
        let cmp_2_asc = |a: &Doc, b: &Doc| reverse_int(&b.1 .1).cmp(&reverse_int(&a.1 .1));

        let sort_function =
            |a: &Doc, b: &Doc| cmp_1_desc(a, b).then(cmp_2_asc(a, b).then(cmp_doc_id_desc(a, b)));
        dataset.sort_by(sort_function);
        let partial_sort_value = dataset
            .iter()
            .map(|(doc_id, (val1, val2))| PartialHit {
                split_id: "fake_split_id".to_string(),
                segment_ord: 0,
                doc_id: *doc_id as u32,
                sort_value: Some(SortByValue {
                    sort_value: val1.map(SortValue::U64),
                }),
                sort_value2: Some(SortByValue {
                    sort_value: val2.map(SortValue::U64),
                }),
            })
            .collect::<Vec<_>>();
        // we eliminate based on sort value
        for (i, search_after) in partial_sort_value.into_iter().enumerate() {
            let request = SearchRequest {
                max_hits: 1000,
                sort_fields: vec![
                    SortField {
                        field_name: "sort1".to_string(),
                        sort_order: SortOrder::Desc.into(),
                        sort_datetime_format: None,
                    },
                    SortField {
                        field_name: "sort2".to_string(),
                        sort_order: SortOrder::Asc.into(),
                        sort_datetime_format: None,
                    },
                ],
                search_after: Some(search_after),
                ..SearchRequest::default()
            };
            let collector = super::make_collector_for_split(
                "fake_split_id".to_string(),
                &request,
                Default::default(),
            )
            .unwrap();
            let res = searcher
                .search(&tantivy::query::AllQuery, &collector)
                .unwrap();
            // we count results even if they were removed due to search_after
            assert_eq!(res.num_hits, dataset.len() as u64);
            // we get as many result as expected
            assert_eq!(res.partial_hits.len(), dataset.len() - i - 1);
            for (expected, got) in dataset[i + 1..].iter().zip(res.partial_hits.iter()) {
                assert_eq!(expected.0 as u32, got.doc_id,);
            }
        }

        // we eliminate based on split id
        {
            let search_after = PartialHit {
                split_id: "fake_split_id2".to_string(),
                segment_ord: 0,
                doc_id: 5,
                sort_value: None,
                sort_value2: None,
            };
            let request = SearchRequest {
                max_hits: 1000,
                sort_fields: vec![SortField {
                    field_name: "_shard_doc".to_string(),
                    sort_order: SortOrder::Desc.into(),
                    sort_datetime_format: None,
                }],
                search_after: Some(search_after),
                ..SearchRequest::default()
            };

            let collector = super::make_collector_for_split(
                "fake_split_id1".to_string(),
                &request,
                Default::default(),
            )
            .unwrap();
            let res = searcher
                .search(&tantivy::query::AllQuery, &collector)
                .unwrap();
            assert_eq!(res.num_hits, dataset.len() as u64);
            // we are searching split id1, and we remove anything before id2 in descending order
            // (i.e. higher than id2 lexicographically), so every document matches
            assert_eq!(res.partial_hits.len(), dataset.len());

            let collector = super::make_collector_for_split(
                "fake_split_id2".to_string(),
                &request,
                Default::default(),
            )
            .unwrap();
            let res = searcher
                .search(&tantivy::query::AllQuery, &collector)
                .unwrap();
            assert_eq!(res.num_hits, dataset.len() as u64);
            // we are searching the limit split, but only doc_id in 0..5
            assert_eq!(res.partial_hits.len(), 5);

            let collector = super::make_collector_for_split(
                "fake_split_id3".to_string(),
                &request,
                Default::default(),
            )
            .unwrap();
            let res = searcher
                .search(&tantivy::query::AllQuery, &collector)
                .unwrap();
            assert_eq!(res.num_hits, dataset.len() as u64);
            // we are searching split id3, and we remove anything before id2 in descending order
            // (i.e. higher than id2 lexicographically), so everything is removed
            assert_eq!(res.partial_hits.len(), 0);
        }
    }

    fn merge_collector_equal_results(
        request: &SearchRequest,
        results: Vec<LeafSearchResponse>,
    ) -> LeafSearchResponse {
        let collector = make_merge_collector(request, &Default::default()).unwrap();
        let mut incremental_collector = IncrementalCollector::new(collector.clone());

        let result = collector
            .merge_fruits(results.iter().cloned().map(Ok).collect())
            .unwrap();

        for split_result in results {
            incremental_collector.add_result(split_result).unwrap();
        }

        let incremental_result = incremental_collector.finalize().unwrap();
        assert_eq!(result, incremental_result);
        result
    }

    #[test]
    fn test_merge_collectors() {
        let result = merge_collector_equal_results(
            &SearchRequest {
                start_offset: 0,
                max_hits: 2,
                sort_fields: vec![SortField {
                    field_name: "timestamp".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: None,
                }],
                aggregation_request: None,
                ..Default::default()
            },
            vec![LeafSearchResponse {
                num_hits: 1234,
                partial_hits: vec![PartialHit {
                    split_id: "1".to_string(),
                    segment_ord: 0,
                    doc_id: 123,
                    sort_value: Some(SortValue::I64(1234).into()),
                    sort_value2: None,
                }],
                failed_splits: Vec::new(),
                num_attempted_splits: 3,
                intermediate_aggregation_result: None,
            }],
        );

        assert_eq!(
            result,
            LeafSearchResponse {
                num_hits: 1234,
                partial_hits: vec![PartialHit {
                    split_id: "1".to_string(),
                    segment_ord: 0,
                    doc_id: 123,
                    sort_value: Some(SortValue::I64(1234).into()),
                    sort_value2: None,
                }],
                failed_splits: Vec::new(),
                num_attempted_splits: 3,
                intermediate_aggregation_result: None
            }
        );

        let result = merge_collector_equal_results(
            &SearchRequest {
                start_offset: 0,
                max_hits: 2,
                sort_fields: vec![SortField {
                    field_name: "timestamp".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: None,
                }],
                aggregation_request: None,
                ..Default::default()
            },
            vec![
                LeafSearchResponse {
                    num_hits: 1234,
                    partial_hits: vec![
                        PartialHit {
                            split_id: "1".to_string(),
                            segment_ord: 0,
                            doc_id: 123,
                            sort_value: Some(SortValue::I64(1234).into()),
                            sort_value2: None,
                        },
                        PartialHit {
                            split_id: "1".to_string(),
                            segment_ord: 0,
                            doc_id: 125,
                            sort_value: Some(SortValue::I64(1236).into()),
                            sort_value2: None,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 3,
                    intermediate_aggregation_result: None,
                },
                LeafSearchResponse {
                    num_hits: 10,
                    partial_hits: vec![PartialHit {
                        split_id: "2".to_string(),
                        segment_ord: 0,
                        doc_id: 3,
                        sort_value: Some(SortValue::I64(1235).into()),
                        sort_value2: None,
                    }],
                    failed_splits: vec![SplitSearchError {
                        error: "fake error".to_string(),
                        split_id: "3".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 2,
                    intermediate_aggregation_result: None,
                },
            ],
        );

        assert_eq!(
            result,
            LeafSearchResponse {
                num_hits: 1244,
                partial_hits: vec![
                    PartialHit {
                        split_id: "1".to_string(),
                        segment_ord: 0,
                        doc_id: 125,
                        sort_value: Some(SortValue::I64(1236).into()),
                        sort_value2: None,
                    },
                    PartialHit {
                        split_id: "2".to_string(),
                        segment_ord: 0,
                        doc_id: 3,
                        sort_value: Some(SortValue::I64(1235).into()),
                        sort_value2: None,
                    },
                ],
                failed_splits: vec![SplitSearchError {
                    error: "fake error".to_string(),
                    split_id: "3".to_string(),
                    retryable_error: true,
                }],
                num_attempted_splits: 5,
                intermediate_aggregation_result: None
            }
        );

        // same request, but we reverse sort order
        let result = merge_collector_equal_results(
            &SearchRequest {
                start_offset: 0,
                max_hits: 2,
                sort_fields: vec![SortField {
                    field_name: "timestamp".to_string(),
                    sort_order: SortOrder::Asc as i32,
                    sort_datetime_format: None,
                }],
                aggregation_request: None,
                ..Default::default()
            },
            vec![
                LeafSearchResponse {
                    num_hits: 1234,
                    partial_hits: vec![
                        PartialHit {
                            split_id: "1".to_string(),
                            segment_ord: 0,
                            doc_id: 123,
                            sort_value: Some(SortValue::I64(1234).into()),
                            sort_value2: None,
                        },
                        PartialHit {
                            split_id: "1".to_string(),
                            segment_ord: 0,
                            doc_id: 125,
                            sort_value: Some(SortValue::I64(1236).into()),
                            sort_value2: None,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 3,
                    intermediate_aggregation_result: None,
                },
                LeafSearchResponse {
                    num_hits: 10,
                    partial_hits: vec![PartialHit {
                        split_id: "2".to_string(),
                        segment_ord: 0,
                        doc_id: 3,
                        sort_value: Some(SortValue::I64(1235).into()),
                        sort_value2: None,
                    }],
                    failed_splits: vec![SplitSearchError {
                        error: "fake error".to_string(),
                        split_id: "3".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 2,
                    intermediate_aggregation_result: None,
                },
            ],
        );

        assert_eq!(
            result,
            LeafSearchResponse {
                num_hits: 1244,
                partial_hits: vec![
                    PartialHit {
                        split_id: "1".to_string(),
                        segment_ord: 0,
                        doc_id: 123,
                        sort_value: Some(SortValue::I64(1234).into()),
                        sort_value2: None,
                    },
                    PartialHit {
                        split_id: "2".to_string(),
                        segment_ord: 0,
                        doc_id: 3,
                        sort_value: Some(SortValue::I64(1235).into()),
                        sort_value2: None,
                    },
                ],
                failed_splits: vec![SplitSearchError {
                    error: "fake error".to_string(),
                    split_id: "3".to_string(),
                    retryable_error: true,
                }],
                num_attempted_splits: 5,
                intermediate_aggregation_result: None
            }
        );
        // TODO would be nice to test aggregation too.
    }
}
