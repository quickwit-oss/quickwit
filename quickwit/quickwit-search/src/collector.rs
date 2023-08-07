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

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

use itertools::Itertools;
use quickwit_common::binary_heap::top_k;
use quickwit_doc_mapper::{DocMapper, WarmupInfo};
use quickwit_proto::{LeafSearchResponse, PartialHit, SearchRequest, SortOrder, SortValue};
use serde::Deserialize;
use tantivy::aggregation::agg_req::{get_fast_field_names, Aggregations};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{AggregationLimits, AggregationSegmentCollector};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{ColumnType, MonotonicallyMappableToU64};
use tantivy::fastfield::Column;
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

use crate::filters::{create_timestamp_filter_builder, TimestampFilter, TimestampFilterBuilder};
use crate::find_trace_ids_collector::{FindTraceIdsCollector, FindTraceIdsSegmentCollector};
use crate::GlobalDocAddress;

#[derive(Clone, Debug)]
pub(crate) enum SortByComponent {
    DocId,
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
    fn to_sorting_field_computer_component(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<SortingFieldComputerComponent> {
        match self {
            SortByComponent::DocId => Ok(SortingFieldComputerComponent::DocId),
            SortByComponent::FastField { field_name, order } => {
                let sort_column_opt: Option<(Column<u64>, ColumnType)> =
                    segment_reader.fast_fields().u64_lenient(field_name)?;
                let (sort_column, column_type) = sort_column_opt.unwrap_or_else(|| {
                    (
                        Column::build_empty_column(segment_reader.max_doc()),
                        ColumnType::U64,
                    )
                });
                let sort_field_type = SortFieldType::try_from(column_type)?;
                Ok(SortingFieldComputerComponent::FastField {
                    sort_column,
                    sort_field_type,
                    order: *order,
                })
            }
            SortByComponent::Score { order } => {
                Ok(SortingFieldComputerComponent::Score { order: *order })
            }
        }
    }
    pub fn requires_scoring(&self) -> bool {
        match self {
            SortByComponent::DocId => false,
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
            SortByComponent::DocId => SortOrder::Desc,
            SortByComponent::FastField { order, .. } => *order,
            SortByComponent::Score { order } => *order,
        }
    }
}

#[derive(Copy, Clone)]
enum SortFieldType {
    U64,
    I64,
    F64,
    DateTime,
    Bool,
}

/// The `SortingFieldComputer` can be seen as the specialization of `SortBy` applied to a specific
/// `SegmentReader`. Its role is to compute the sorting field given a `DocId`.
enum SortingFieldComputerComponent {
    /// If undefined, we simply sort by DocIds.
    DocId,
    FastField {
        sort_column: Column<u64>,
        sort_field_type: SortFieldType,
        order: SortOrder,
    },
    Score {
        order: SortOrder,
    },
}
impl SortingFieldComputerComponent {
    fn recover_typed_sort_value(&self, sort_value: Option<u64>) -> Option<SortValue> {
        let recover_from_fast_field = |sort_value: u64, order: SortOrder, sort_field_type| {
            let sort_value = match order {
                SortOrder::Asc => u64::MAX - sort_value,
                SortOrder::Desc => sort_value,
            };
            match sort_field_type {
                SortFieldType::U64 => SortValue::U64(sort_value),
                SortFieldType::I64 => SortValue::I64(i64::from_u64(sort_value)),
                SortFieldType::F64 => SortValue::F64(f64::from_u64(sort_value)),
                SortFieldType::DateTime => SortValue::I64(i64::from_u64(sort_value)),
                SortFieldType::Bool => SortValue::Boolean(sort_value == 1u64),
            }
        };
        if let Some(sort_value) = sort_value {
            match self {
                SortingFieldComputerComponent::DocId => Some(SortValue::U64(sort_value)),
                SortingFieldComputerComponent::FastField {
                    sort_column: _,
                    sort_field_type,
                    order,
                } => Some(recover_from_fast_field(
                    sort_value,
                    *order,
                    *sort_field_type,
                )),
                SortingFieldComputerComponent::Score { order: _ } => Some(SortValue::F64(
                    MonotonicallyMappableToU64::from_u64(sort_value),
                )),
            }
        } else {
            None
        }
    }

    /// Returns the ranking key for the given element
    ///
    /// The functions return None if the sort key is a fast field, for which we have no value
    /// for the given doc_id.
    /// All value are mapped using a monotonic mapping.
    /// If the sort ord is ascending, we use the mapping `val -> u64::MAX - val` to
    /// artificially invert the order.
    ///
    /// Given the u64 value, it is possible to recover the original value using
    /// `Self::recover_typed_sort_value`.
    fn compute_u64_sort_value_opt(&self, doc_id: DocId, score: Score) -> Option<u64> {
        let apply_sort = |order, field_val| match order {
            SortOrder::Desc => field_val,
            SortOrder::Asc => u64::MAX - field_val,
        };
        match self {
            SortingFieldComputerComponent::DocId => Some(doc_id as u64),
            SortingFieldComputerComponent::FastField {
                sort_column, order, ..
            } => sort_column
                .first(doc_id)
                .map(|field_val| apply_sort(*order, field_val)),
            SortingFieldComputerComponent::Score { order } => {
                let u64_score = (score as f64).to_u64();
                let u64_score = apply_sort(*order, u64_score);
                Some(u64_score)
            }
        }
    }
}
impl From<SortingFieldComputerComponent> for SortingFieldComputerPair {
    fn from(value: SortingFieldComputerComponent) -> Self {
        Self {
            first: value,
            second: None,
        }
    }
}

pub(crate) struct SortingFieldComputerPair {
    first: SortingFieldComputerComponent,
    second: Option<SortingFieldComputerComponent>,
}

impl SortingFieldComputerPair {
    fn recover_typed_sort_value(
        &self,
        sort_value1: Option<u64>,
        sort_value2: Option<u64>,
    ) -> (Option<SortValue>, Option<SortValue>) {
        let first_sort = sort_value1
            .and_then(|sort_value1| self.first.recover_typed_sort_value(Some(sort_value1)));
        let second_sort = sort_value2.and_then(|sort_value2| {
            self.second
                .as_ref()
                .expect("no second sort field, but got second sort value")
                .recover_typed_sort_value(Some(sort_value2))
        });
        (first_sort, second_sort)
    }

    /// Returns the ranking key for the given element
    ///
    /// The functions return None if the sort key is a fast field, for which we have no value
    /// for the given doc_id.
    /// All value are mapped using a monotonic mapping.
    /// If the sort ord is ascending, we use the mapping `val -> u64::MAX - val` to
    /// artificially invert the order.
    ///
    /// Given the u64 value, it is possible to recover the original value using
    /// `Self::recover_typed_sort_value`.
    fn compute_u64_sort_value_opt(
        &self,
        doc_id: DocId,
        score: Score,
    ) -> (Option<u64>, Option<u64>) {
        let first = self.first.compute_u64_sort_value_opt(doc_id, score);
        let second = self
            .second
            .as_ref()
            .and_then(|second| second.compute_u64_sort_value_opt(doc_id, score));
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
/// segment specific `SortFieldComputer`.
fn resolve_sort_by(
    sort_by: &SortByPair,
    segment_reader: &SegmentReader,
) -> tantivy::Result<SortingFieldComputerPair> {
    Ok(SortingFieldComputerPair {
        first: sort_by
            .first
            .to_sorting_field_computer_component(segment_reader)?,
        second: sort_by
            .second
            .as_ref()
            .map(|first| first.to_sorting_field_computer_component(segment_reader))
            .transpose()?,
    })
}

/// PartialHitHeapItem order is the inverse of the natural order
/// so that we actually have a min-heap.
#[derive(Clone, Copy, Debug)]
struct PartialHitHeapItem {
    sort_value_opt1: Option<u64>,
    sort_value_opt2: Option<u64>,
    doc_id: DocId,
}

impl PartialOrd for PartialHitHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartialHitHeapItem {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let by_sorting_field1 = other.sort_value_opt1.cmp(&self.sort_value_opt1);
        let by_sorting_field2 = other.sort_value_opt2.cmp(&self.sort_value_opt2);

        let lazy_order_by_doc_id = || {
            self.doc_id
                .partial_cmp(&other.doc_id)
                .unwrap_or(Ordering::Equal)
        };

        // In case of a tie on the feature, we sort by ascending `DocId`.
        by_sorting_field1
            .then_with(|| by_sorting_field2)
            .then_with(lazy_order_by_doc_id)
    }
}

impl PartialEq for PartialHitHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PartialHitHeapItem {}

enum AggregationSegmentCollectors {
    FindTraceIdsSegmentCollector(Box<FindTraceIdsSegmentCollector>),
    TantivyAggregationSegmentCollector(AggregationSegmentCollector),
}

/// Quickwit collector working at the scale of the segment.
pub struct QuickwitSegmentCollector {
    num_hits: u64,
    split_id: String,
    sort_by: SortingFieldComputerPair,
    hits: BinaryHeap<PartialHitHeapItem>,
    max_hits: usize,
    segment_ord: u32,
    timestamp_filter_opt: Option<TimestampFilter>,
    aggregation: Option<AggregationSegmentCollectors>,
}

impl QuickwitSegmentCollector {
    #[inline]
    fn at_capacity(&self) -> bool {
        self.hits.len() >= self.max_hits
    }

    #[inline]
    fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        let (sorting_field_value_opt1, sorting_field_value_opt2): (Option<u64>, Option<u64>) =
            self.sort_by.compute_u64_sort_value_opt(doc_id, score);
        if self.at_capacity() {
            if let Some(sorting_field_value) = sorting_field_value_opt1 {
                if let Some(limit_sorting_field) =
                    self.hits.peek().and_then(|head| head.sort_value_opt1)
                {
                    // In case of a tie, we keep the document with a lower `DocId`.
                    if limit_sorting_field < sorting_field_value {
                        if let Some(mut head) = self.hits.peek_mut() {
                            head.sort_value_opt1 = Some(sorting_field_value);
                            head.sort_value_opt2 = sorting_field_value_opt2;
                            head.doc_id = doc_id;
                        }
                    }
                }
            }
        } else {
            // we have not reached capacity yet, so we can just push the
            // element.
            self.hits.push(PartialHitHeapItem {
                sort_value_opt1: sorting_field_value_opt1,
                sort_value_opt2: sorting_field_value_opt2,
                doc_id,
            });
        }
    }

    #[inline]
    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(ref timestamp_filter) = self.timestamp_filter_opt {
            return timestamp_filter.is_within_range(doc_id);
        }
        true
    }
}

impl SegmentCollector for QuickwitSegmentCollector {
    type Fruit = tantivy::Result<LeafSearchResponse>;

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if !self.accept_document(doc_id) {
            return;
        }

        self.num_hits += 1;
        self.collect_top_k(doc_id, score);

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
        let segment_ord = self.segment_ord;
        // TODO use into_iter_sorted() once it gets stable.
        let split_id = self.split_id;
        let sort_by = self.sort_by;
        let partial_hits: Vec<PartialHit> = self
            .hits
            .into_sorted_vec()
            .into_iter()
            .map(|hit| {
                let (sort_value1, sort_value2) =
                    sort_by.recover_typed_sort_value(hit.sort_value_opt1, hit.sort_value_opt2);

                PartialHit {
                    sort_value: Some(quickwit_proto::SortByValue {
                        sort_value: sort_value1,
                    }),
                    sort_value2: Some(quickwit_proto::SortByValue {
                        sort_value: sort_value2,
                    }),
                    segment_ord,
                    doc_id: hit.doc_id,
                    split_id: split_id.clone(),
                }
            })
            .collect();

        let intermediate_aggregation_result = match self.aggregation {
            Some(AggregationSegmentCollectors::FindTraceIdsSegmentCollector(collector)) => {
                let fruit = collector.harvest();
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
#[derive(Debug, Clone, Deserialize)]
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
}

/// The quickwit collector is the tantivy Collector used in Quickwit.
///
/// It defines the data that should be accumulated about the documents matching
/// the query.
#[derive(Clone)]
pub(crate) struct QuickwitCollector {
    pub split_id: String,
    pub start_offset: usize,
    pub max_hits: usize,
    pub sort_by: SortByPair,
    timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub aggregation: Option<QuickwitAggregations>,
    pub aggregation_limits: AggregationLimits,
}

impl QuickwitCollector {
    pub fn fast_field_names(&self) -> HashSet<String> {
        let mut fast_field_names = HashSet::default();
        self.sort_by.first.add_fast_field(&mut fast_field_names);
        if let Some(sort_by_second) = &self.sort_by.second {
            sort_by_second.add_fast_field(&mut fast_field_names);
        }
        if let Some(aggregations) = &self.aggregation {
            fast_field_names.extend(aggregations.fast_field_names());
        }
        if let Some(timestamp_filter_builder) = &self.timestamp_filter_builder_opt {
            fast_field_names.insert(timestamp_filter_builder.timestamp_field_name.clone());
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
        let sort_by = resolve_sort_by(&self.sort_by, segment_reader)?;
        // Regardless of the start_offset, we need to collect top-K
        // starting from 0 for every leaves.
        let leaf_max_hits = self.max_hits + self.start_offset;

        let timestamp_filter_opt = match &self.timestamp_filter_builder_opt {
            Some(timestamp_filter_builder) => timestamp_filter_builder.build(segment_reader)?,
            None => None,
        };
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
                        &self.aggregation_limits,
                    )?,
                ),
            ),
            None => None,
        };
        Ok(QuickwitSegmentCollector {
            num_hits: 0u64,
            split_id: self.split_id.clone(),
            sort_by,
            hits: BinaryHeap::with_capacity(leaf_max_hits),
            segment_ord,
            max_hits: leaf_max_hits,
            timestamp_filter_opt,
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
        // All leaves will return their top [0..max_hits) documents.
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
        merged_leaf_response
            .partial_hits
            .drain(
                0..self
                    .start_offset
                    .min(merged_leaf_response.partial_hits.len()),
            )
            .count(); //< we just use count as a way to consume the entire iterator.
        Ok(merged_leaf_response)
    }
}

fn map_error(err: postcard::Error) -> TantivyError {
    TantivyError::InternalError(format!("Merge Result Postcard Error: {err}"))
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
    let merged_intermediate_aggregation_result = match aggregations_opt {
        Some(QuickwitAggregations::FindTraceIdsAggregation(collector)) => {
            let fruits: Vec<
                <<FindTraceIdsCollector as Collector>::Child as SegmentCollector>::Fruit,
            > = leaf_responses
                .iter()
                .filter_map(|leaf_response| {
                    leaf_response.intermediate_aggregation_result.as_ref().map(
                        |intermediate_aggregation_result| {
                            postcard::from_bytes(intermediate_aggregation_result.as_slice())
                                .map_err(map_error)
                        },
                    )
                })
                .collect::<Result<_, _>>()?;
            let merged_fruit = collector.merge_fruits(fruits)?;
            let serialized = postcard::to_allocvec(&merged_fruit).map_err(map_error)?;
            Some(serialized)
        }
        Some(QuickwitAggregations::TantivyAggregations(_)) => {
            let fruits: Vec<IntermediateAggregationResults> = leaf_responses
                .iter()
                .filter_map(|leaf_response| {
                    leaf_response.intermediate_aggregation_result.as_ref().map(
                        |intermediate_aggregation_result| {
                            postcard::from_bytes(intermediate_aggregation_result.as_slice())
                                .map_err(map_error)
                        },
                    )
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
    sort_order1: SortOrder,
    sort_order2: SortOrder,
    num_hits: usize,
) -> Vec<PartialHit> {
    let get_sort_values = |partial_hit: &PartialHit| {
        (
            partial_hit
                .sort_value
                .and_then(|sort_value| sort_value.sort_value),
            partial_hit
                .sort_value2
                .and_then(|sort_value| sort_value.sort_value),
        )
    };
    match (sort_order1, sort_order2) {
        (SortOrder::Asc, SortOrder::Asc) => {
            top_k(partial_hits.into_iter(), num_hits, |partial_hit| {
                // This reverse dance is a little bit complicated.
                // Note that `Option<Reverse<T>>` is very different from `Reverse<Option<T>>`.
                //
                // Since the value is Option<Option<T>>, we have to use and_then (in
                // get_sort_values) to flatten it to Option<T>, or else we would get
                // `Option<Reverse<Option<T>>`.
                //
                // We do want the earlier: documents without any values should always get ranked
                // after documents with a value, regardless of whether we use
                // ascending or descending order.
                let (score, score2) = get_sort_values(partial_hit);
                let score = score.map(Reverse);
                let score2 = score2.map(Reverse);
                let addr = GlobalDocAddress::from_partial_hit(partial_hit);
                (score, score2, Reverse(addr))
            })
        }
        (SortOrder::Asc, SortOrder::Desc) => {
            top_k(partial_hits.into_iter(), num_hits, |partial_hit| {
                let (score, score2) = get_sort_values(partial_hit);
                let score = score.map(Reverse);
                let addr = GlobalDocAddress::from_partial_hit(partial_hit);
                (score, score2, Reverse(addr))
            })
        }
        (SortOrder::Desc, SortOrder::Desc) => {
            top_k(partial_hits.into_iter(), num_hits, |partial_hit| {
                let addr = GlobalDocAddress::from_partial_hit(partial_hit);
                let (score, score2) = get_sort_values(partial_hit);
                (score, score2, addr)
            })
        }
        (SortOrder::Desc, SortOrder::Asc) => {
            top_k(partial_hits.into_iter(), num_hits, |partial_hit| {
                let (score, score2) = get_sort_values(partial_hit);
                let score2 = score2.map(Reverse);
                let addr = GlobalDocAddress::from_partial_hit(partial_hit);
                (score, score2, addr)
            })
        }
    }
}

pub(crate) fn sort_by_from_request(search_request: &SearchRequest) -> SortByPair {
    let to_sort_by_component = |field_name: &str, order| {
        if field_name == "_score" {
            SortByComponent::Score { order }
        } else {
            SortByComponent::FastField {
                field_name: field_name.to_string(),
                order,
            }
        }
    };

    let num_sort_fields = search_request.sort_fields.len();
    if num_sort_fields == 0 {
        SortByComponent::DocId.into()
    } else if num_sort_fields == 1 {
        let sort_field = &search_request.sort_fields[0];
        let order = SortOrder::from_i32(sort_field.sort_order).unwrap_or(SortOrder::Desc);
        to_sort_by_component(&sort_field.field_name, order).into()
    } else if num_sort_fields == 2 {
        let sort_field1 = &search_request.sort_fields[0];
        let order1 = SortOrder::from_i32(sort_field1.sort_order).unwrap_or(SortOrder::Desc);
        let sort_field2 = &search_request.sort_fields[1];
        let order2 = SortOrder::from_i32(sort_field2.sort_order).unwrap_or(SortOrder::Desc);
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
    split_id: String,
    doc_mapper: &dyn DocMapper,
    search_request: &SearchRequest,
    aggregation_limits: AggregationLimits,
) -> crate::Result<QuickwitCollector> {
    let aggregation = match &search_request.aggregation_request {
        Some(aggregation) => Some(serde_json::from_str(aggregation)?),
        None => None,
    };
    let timestamp_filter_builder_opt = create_timestamp_filter_builder(
        doc_mapper.timestamp_field_name(),
        search_request.start_timestamp,
        search_request.end_timestamp,
    );
    let sort_by = sort_by_from_request(search_request);
    Ok(QuickwitCollector {
        split_id,
        start_offset: search_request.start_offset as usize,
        max_hits: search_request.max_hits as usize,
        sort_by,
        timestamp_filter_builder_opt,
        aggregation,
        aggregation_limits,
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
        split_id: String::default(),
        start_offset: search_request.start_offset as usize,
        max_hits: search_request.max_hits as usize,
        sort_by,
        timestamp_filter_builder_opt: None,
        aggregation,
        aggregation_limits: aggregation_limits.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use quickwit_proto::{PartialHit, SortOrder, SortValue};

    use super::PartialHitHeapItem;
    use crate::collector::top_k_partial_hits;

    #[test]
    fn test_partial_hit_ordered_by_sorting_field() {
        let lesser_score = PartialHitHeapItem {
            doc_id: 1u32,
            sort_value_opt1: Some(1u64),
            sort_value_opt2: None,
        };
        let higher_score = PartialHitHeapItem {
            sort_value_opt1: Some(2u64),
            sort_value_opt2: None,
            doc_id: 1u32,
        };
        assert_eq!(lesser_score.cmp(&higher_score), Ordering::Greater);
    }
    #[test]
    fn test_partial_hit_ordered_by_sorting_field_2() {
        let get_el = |val1, val2, docid| PartialHitHeapItem {
            doc_id: docid,
            sort_value_opt1: val1,
            sort_value_opt2: val2,
        };
        let mut data = vec![
            get_el(Some(1u64), None, 1u32),
            get_el(Some(2u64), Some(2u64), 1u32),
            get_el(Some(2u64), Some(1u64), 1u32),
            get_el(Some(2u64), None, 1u32),
            get_el(None, Some(1u64), 1u32),
            get_el(None, None, 1u32),
        ];
        data.sort();
        assert_eq!(
            data,
            vec![
                get_el(Some(2u64), Some(2u64), 1u32),
                get_el(Some(2u64), Some(1u64), 1u32),
                get_el(Some(2u64), None, 1u32),
                get_el(Some(1u64), None, 1u32),
                get_el(None, Some(1u64), 1u32),
                get_el(None, None, 1u32),
            ]
        );
    }

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
}
