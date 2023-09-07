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

use std::cmp::Ordering;
use std::collections::HashSet;

use itertools::Itertools;
use quickwit_common::binary_heap::{SortKeyMapper, TopK};
use quickwit_doc_mapper::{DocMapper, WarmupInfo};
use quickwit_proto::search::{LeafSearchResponse, PartialHit, SearchRequest, SortOrder, SortValue};
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
    fn to_sorting_field_extractor_component(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<SortingFieldExtractorComponent> {
        match self {
            SortByComponent::DocId => Ok(SortingFieldExtractorComponent::DocId),
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

/// The `SortingFieldExtractor` is used to extract a score, which can either be a true score,
/// a value from a fast field, or nothing (sort by DocId).
enum SortingFieldExtractorComponent {
    /// If undefined, we simply sort by DocIds.
    DocId,
    FastField {
        sort_column: Column<u64>,
        sort_field_type: SortFieldType,
    },
    Score,
}

impl SortingFieldExtractorComponent {
    /// Returns the sort value for the given element
    ///
    /// The function returns None if the sort key is a fast field, for which we have no value
    /// for the given doc_id, or we sort by DocId.
    fn extract_typed_sort_value_opt(&self, doc_id: DocId, score: Score) -> Option<SortValue> {
        let map_fast_field_to_value = |fast_field_value, field_type| match field_type {
            SortFieldType::U64 => SortValue::U64(fast_field_value),
            SortFieldType::I64 => SortValue::I64(i64::from_u64(fast_field_value)),
            SortFieldType::F64 => SortValue::F64(f64::from_u64(fast_field_value)),
            SortFieldType::DateTime => SortValue::I64(i64::from_u64(fast_field_value)),
            SortFieldType::Bool => SortValue::Boolean(fast_field_value != 0u64),
        };

        match self {
            SortingFieldExtractorComponent::DocId => None,
            SortingFieldExtractorComponent::FastField {
                sort_column,
                sort_field_type,
                ..
            } => sort_column
                .first(doc_id)
                .map(|field_val| map_fast_field_to_value(field_val, *sort_field_type)),
            SortingFieldExtractorComponent::Score { .. } => Some(SortValue::F64(score as f64)),
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
    first: SortingFieldExtractorComponent,
    second: Option<SortingFieldExtractorComponent>,
}

impl SortingFieldExtractorPair {
    /// Returns the list of sort values for the given element
    ///
    /// See also [`SortingFieldExtractorComponent::extract_typed_sort_value_opt`] for more
    /// information.
    fn extract_typed_sort_value(
        &self,
        doc_id: DocId,
        score: Score,
    ) -> (Option<SortValue>, Option<SortValue>) {
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
    score_extractor: SortingFieldExtractorPair,
    // PartialHits in this heap don't contain a split_id yet.
    top_k_hits: TopK<PartialHit, PartialHitSortingKey, HitSortingMapper>,
    segment_ord: u32,
    timestamp_filter_opt: Option<TimestampFilter>,
    aggregation: Option<AggregationSegmentCollectors>,
}

impl QuickwitSegmentCollector {
    #[inline]
    fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        let (sort_value, sort_value2) =
            self.score_extractor.extract_typed_sort_value(doc_id, score);

        let hit = PartialHit {
            sort_value: sort_value.map(Into::into),
            sort_value2: sort_value2.map(Into::into),
            // we actually know the split_id, but the hit is likely to be discarded, so clonning it
            // would cause a probably useless allocation. TODO use an Arc<str> instead?
            split_id: String::new(),
            segment_ord: self.segment_ord,
            doc_id,
        };
        self.top_k_hits.add_entry(hit);
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
        let split_id = self.split_id;
        let partial_hits: Vec<PartialHit> = self
            .top_k_hits
            .finalize()
            .into_iter()
            .map(|mut hit| {
                hit.split_id = split_id.clone();
                hit
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
        let score_extractor = get_score_extractor(&self.sort_by, segment_reader)?;
        let (order1, order2) = self.sort_by.sort_orders();
        let sort_key_mapper = HitSortingMapper { order1, order2 };
        Ok(QuickwitSegmentCollector {
            num_hits: 0u64,
            split_id: self.split_id.clone(),
            score_extractor,
            top_k_hits: TopK::new(leaf_max_hits, sort_key_mapper),
            segment_ord,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct PartialHitSortingKey {
    sort_value: Option<SortValue>,
    sort_value2: Option<SortValue>,
    address: GlobalDocAddress,
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

struct HitSortingMapper {
    order1: SortOrder,
    order2: SortOrder,
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use quickwit_proto::search::{PartialHit, SearchRequest, SortField, SortOrder, SortValue};

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

    // TODO figure out a way to remove this boilerplate and use mockall
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct MockDocMapper;

    #[typetag::serde(name = "mock")]
    impl quickwit_doc_mapper::DocMapper for MockDocMapper {
        // Required methods
        fn doc_from_json_obj(
            &self,
            _json_obj: quickwit_doc_mapper::JsonObject,
        ) -> Result<(u64, tantivy::Document), quickwit_doc_mapper::DocParsingError> {
            unimplemented!()
        }
        fn doc_to_json(
            &self,
            _named_doc: std::collections::BTreeMap<String, Vec<tantivy::schema::Value>>,
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
        fn tokenizer_manager(&self) -> &tantivy::tokenizer::TokenizerManager {
            unimplemented!()
        }

        fn timestamp_field_name(&self) -> Option<&str> {
            None
        }
    }

    fn sort_dataset() -> Vec<(Option<u64>, Option<u64>)> {
        // every comination of 0..=2 + None, in random order.
        // (2, 1) is dupplicated to allow testing for DocId sorting with two sort fields
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
                        }
                    } else {
                        SortField {
                            field_name: field.to_string(),
                            sort_order: SortOrder::Desc.into(),
                        }
                    }
                })
                .collect(),
            ..SearchRequest::default()
        }
    }

    fn make_index() -> tantivy::Index {
        use tantivy::schema::{Document, NumericOptions, Schema};
        use tantivy::{Index, UserOperation};

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
                        let mut doc = Document::new();
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
            // the logic for sorting isn't easy to wrap one's head arround. These simple tests are
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
            for len in 1..dataset.len() {
                let collector = super::make_collector_for_split(
                    "fake_split_id".to_string(),
                    &MockDocMapper,
                    &make_request(len as u64, sort_str),
                    Default::default(),
                )
                .unwrap();
                let res = dbg!(searcher
                    .search(&tantivy::query::AllQuery, &collector)
                    .unwrap());
                assert_eq!(res.partial_hits.len(), len);
                for (expected, got) in dataset.iter().zip(res.partial_hits.iter()) {
                    assert_eq!(
                        expected.0 as u32, got.doc_id,
                        "missmatch ordering for \"{sort_str}\":{len}"
                    );
                }
            }
        }
    }
}
