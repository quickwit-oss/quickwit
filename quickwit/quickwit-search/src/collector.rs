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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;

use itertools::{Either, Itertools};
use quickwit_common::binary_heap::{SortKeyMapper, TopK};
use quickwit_common::numeric_types::num_proj::{
    ProjectedNumber, f64_to_i64, f64_to_u64, i64_to_f64, i64_to_u64, u64_to_f64, u64_to_i64,
};
use quickwit_doc_mapper::{FastFieldWarmupInfo, WarmupInfo};
use quickwit_proto::search::{
    LeafSearchResponse, PartialHit, ResourceStats, SearchRequest, SortByValue, SortOrder,
    SortValue, SplitSearchError, TypeSortKey,
};
use quickwit_proto::types::SplitId;
use serde::Deserialize;
use tantivy::aggregation::agg_req::{Aggregations, get_fast_field_names};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{AggContextParams, AggregationLimitsGuard, AggregationSegmentCollector};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{
    ColumnIndex, ColumnType, MonotonicallyMappableToU64, StrColumn, TermOrdHit,
};
use tantivy::fastfield::Column;
use tantivy::tokenizer::TokenizerManager;
use tantivy::{
    COLLECT_BLOCK_BUFFER_LEN, DocId, Score, SegmentOrdinal, SegmentReader, TantivyError,
};

use crate::find_trace_ids_collector::{FindTraceIdsCollector, FindTraceIdsSegmentCollector, Span};
use crate::sort_repr::{ElidableU64, InternalSortValueRepr, InternalValueRepr};
use crate::top_k_collector::QuickwitSegmentTopKCollector;
use crate::{GlobalDocAddress, merge_resource_stats, merge_resource_stats_it};

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

impl SortByComponent {
    fn to_sorting_field_extractor_component(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<SortingFieldExtractorComponent> {
        match self {
            SortByComponent::DocId { .. } => Ok(SortingFieldExtractorComponent::DocId),
            SortByComponent::FastField { field_name, .. } => {
                let allowed_column_types = [
                    ColumnType::I64,
                    ColumnType::U64,
                    ColumnType::F64,
                    ColumnType::Str,
                    ColumnType::DateTime,
                    ColumnType::Bool,
                    // ColumnType::IpAddr Unsupported
                    // ColumnType::Bytes Unsupported
                ];
                let fast_fields = segment_reader.fast_fields();
                let mut sort_columns = fast_fields
                    .u64_lenient_for_type_all(Some(&allowed_column_types), field_name)?
                    .into_iter()
                    .map(|(col, col_typ)| match col_typ {
                        ColumnType::U64 => Ok((col, SortFieldType::U64)),
                        ColumnType::I64 => Ok((col, SortFieldType::I64)),
                        ColumnType::F64 => Ok((col, SortFieldType::F64)),
                        ColumnType::DateTime => Ok((col, SortFieldType::DateTime)),
                        ColumnType::Bool => Ok((col, SortFieldType::Bool)),
                        ColumnType::Str => Ok((
                            col,
                            SortFieldType::String(
                                fast_fields
                                    .str(field_name)?
                                    .expect("field with str column type should have str column"),
                            ),
                        )),
                        _ => panic!("unsupported"),
                    })
                    .collect::<tantivy::Result<Vec<_>>>()?;

                sort_columns.sort_by_key(|(_, col_typ)| col_typ.type_sort_key());

                // TODO we could skip the columns that are before the search after

                Ok(SortingFieldExtractorComponent::FastField(
                    FastFieldExtractor {
                        sort_columns,
                        col_scratch: Box::new([None; COLLECT_BLOCK_BUFFER_LEN]),
                    },
                ))
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

#[derive(Clone, Debug)]
pub(crate) enum SortFieldType {
    U64,
    I64,
    F64,
    DateTime,
    Bool,
    String(StrColumn),
}

impl SortFieldType {
    fn type_sort_key(&self) -> TypeSortKey {
        match self {
            SortFieldType::U64 => TypeSortKey::Numeric,
            SortFieldType::I64 => TypeSortKey::Numeric,
            SortFieldType::F64 => TypeSortKey::Numeric,
            SortFieldType::DateTime => TypeSortKey::DateTime,
            SortFieldType::Bool => TypeSortKey::Boolean,
            SortFieldType::String(_) => TypeSortKey::Str,
        }
    }
}

struct FastFieldExtractor {
    /// Sort columns are sorted in the same order as types (TypeSortKey)
    sort_columns: Vec<(Column<u64>, SortFieldType)>,
    col_scratch: Box<[Option<u64>; COLLECT_BLOCK_BUFFER_LEN]>,
}

impl FastFieldExtractor {
    fn fill_batch<V: ElidableU64>(
        &mut self,
        docs: &[DocId],
        order: SortOrder,
        out: &mut [InternalValueRepr<V>],
    ) {
        let n = docs.len();
        let unique_column = &self.sort_columns[0].0;
        if let ColumnIndex::Multivalued(_) = unique_column.index {
            // TODO: first_vals() doesn't enforce zeroing for multivalued
            // columns. It seems like something that should be fixed in Tantivy?
            self.col_scratch[..n].fill(None);
        }
        self.sort_columns[0]
            .0
            .first_vals(docs, &mut self.col_scratch[..n]);
        for (repr, val_opt) in out[..n].iter_mut().zip(self.col_scratch[..n].iter()) {
            *repr = match val_opt {
                Some(val) => InternalValueRepr::new(*val, 0, order),
                None => InternalValueRepr::new_missing(),
            };
        }
    }
}

/// The `SortingFieldExtractor` is used to extract a score, which can either be a true score,
/// a value from a fast field, or nothing (sort by DocId).
enum SortingFieldExtractorComponent {
    /// If undefined, we simply sort by DocIds.
    DocId,
    FastField(FastFieldExtractor),
    Score,
}

impl SortingFieldExtractorComponent {
    pub fn is_doc_id(&self) -> bool {
        matches!(self, SortingFieldExtractorComponent::DocId)
    }

    /// Currently batch extraction only has a fast path for full columns. That
    /// can only happen if there is only one column for the fast field.
    fn extractor_for_batch_if_worthwhile(&mut self) -> Option<&mut FastFieldExtractor> {
        match self {
            SortingFieldExtractorComponent::FastField(extractor)
                if extractor.sort_columns.len() == 1 =>
            {
                Some(extractor)
            }
            _ => None,
        }
    }

    /// Returns the sort value for the given element in its u64 representation.
    /// The returned u64 representation maintains the ordering of the original
    /// value.
    #[inline]
    fn project_to_internal_sort_value<V: ElidableU64>(
        &self,
        doc_id: DocId,
        score: Score,
        order: SortOrder,
    ) -> InternalValueRepr<V> {
        match self {
            SortingFieldExtractorComponent::DocId => {
                // Doc id is handled at the compound sort value level
                debug_assert!(V::is_elided());
                InternalValueRepr::new_missing()
            }
            SortingFieldExtractorComponent::FastField(FastFieldExtractor {
                sort_columns, ..
            }) => {
                for (idx, (sort_column, _)) in sort_columns.iter().enumerate() {
                    if let Some(value) = sort_column.first(doc_id) {
                        return InternalValueRepr::new(value, idx as u8, order);
                    }
                }
                InternalValueRepr::new_missing()
            }
            SortingFieldExtractorComponent::Score => {
                InternalValueRepr::new((score as f64).to_u64(), 0, order)
            }
        }
    }

    fn project_from_internal_sort_value<V: ElidableU64>(
        &self,
        internal_repr: InternalValueRepr<V>,
        order: SortOrder,
    ) -> tantivy::Result<Option<SortByValue>> {
        if V::is_elided() {
            return Ok(None);
        }
        let Some((col_idx, val_as_u64)) = internal_repr.decode(order) else {
            return Ok(Some(SortByValue { sort_value: None }));
        };
        let sort_value = match self {
            SortingFieldExtractorComponent::FastField(FastFieldExtractor {
                sort_columns, ..
            }) => {
                let (_, field_type) = &sort_columns[col_idx as usize];
                match field_type {
                    SortFieldType::U64 => SortValue::U64(val_as_u64),
                    SortFieldType::I64 => SortValue::I64(i64::from_u64(val_as_u64)),
                    SortFieldType::F64 => SortValue::F64(f64::from_u64(val_as_u64)),
                    SortFieldType::DateTime => SortValue::Datetime(i64::from_u64(val_as_u64)),
                    SortFieldType::Bool => SortValue::Boolean(val_as_u64 != 0u64),
                    SortFieldType::String(str_column) => {
                        let term_dict = str_column.dictionary();
                        let mut buffer = Vec::new();
                        term_dict.ord_to_term(val_as_u64, &mut buffer)?;
                        let string_value = String::from_utf8(buffer).map_err(|_| {
                            tantivy::TantivyError::InternalError(
                                "term dictionary contains non-UTF-8 bytes".to_string(),
                            )
                        })?;
                        SortValue::Str(string_value)
                    }
                }
            }
            SortingFieldExtractorComponent::Score => SortValue::F64(f64::from_u64(val_as_u64)),
            SortingFieldExtractorComponent::DocId => {
                return Err(tantivy::TantivyError::InternalError(
                    "value should be elided on doc id sort".to_string(),
                ));
            }
        };
        Ok(Some(SortByValue {
            sort_value: Some(sort_value),
        }))
    }

    fn project_to_internal_search_after<V: ElidableU64>(
        &self,
        sort_by_value: &SortByValue,
        sort_order: SortOrder,
    ) -> tantivy::Result<InternalValueRepr<V>> {
        let SortByValue {
            sort_value: sort_value_opt,
        } = sort_by_value;
        match (self, sort_value_opt) {
            (SortingFieldExtractorComponent::DocId, _) => {
                // Doc id sorts are handled at the compound sort value level
                debug_assert!(V::is_elided());
                Ok(InternalValueRepr::new_missing())
            }
            (SortingFieldExtractorComponent::FastField(_), None) => {
                Ok(InternalValueRepr::new_missing())
            }
            (
                SortingFieldExtractorComponent::FastField(FastFieldExtractor {
                    sort_columns, ..
                }),
                Some(sort_value),
            ) => project_search_after_sort_value(sort_columns, sort_value, sort_order),
            (SortingFieldExtractorComponent::Score, Some(SortValue::F64(val))) => {
                Ok(InternalValueRepr::new(val.to_u64(), 0, sort_order))
            }
            (SortingFieldExtractorComponent::Score, _) => {
                Err(tantivy::TantivyError::InvalidArgument(
                    "got non-F64 sort value for score".to_string(),
                ))
            }
        }
    }
}

fn projected_number_internal_repr<T: MonotonicallyMappableToU64, V: ElidableU64>(
    projected: ProjectedNumber<T>,
    order: SortOrder,
    accessor_idx: u8,
) -> InternalValueRepr<V> {
    match (projected, order) {
        (ProjectedNumber::Exact(val), _) => {
            InternalValueRepr::new(val.to_u64(), accessor_idx, order)
        }
        (ProjectedNumber::AfterLast, SortOrder::Asc) => {
            InternalValueRepr::new_skip_column(accessor_idx, order)
        }
        (ProjectedNumber::AfterLast, SortOrder::Desc) => {
            InternalValueRepr::new_keep_column(accessor_idx, order)
        }
        (ProjectedNumber::Next(val), SortOrder::Asc) => {
            let val_u64 = val.to_u64();
            if val_u64 == 0 {
                InternalValueRepr::new_keep_column(accessor_idx, order)
            } else {
                InternalValueRepr::new(val_u64 - 1, accessor_idx, order)
            }
        }
        (ProjectedNumber::Next(val), SortOrder::Desc) => {
            let val_u64 = val.to_u64();
            if val_u64 == 0 {
                InternalValueRepr::new_skip_column(accessor_idx, order)
            } else {
                InternalValueRepr::new(val_u64, accessor_idx, order)
            }
        }
    }
}

fn project_search_after_sort_value<V: ElidableU64>(
    sort_columns: &[(Column<u64>, SortFieldType)],
    sort_value: &SortValue,
    sort_order: SortOrder,
) -> tantivy::Result<InternalValueRepr<V>> {
    let col_iter = match sort_order {
        SortOrder::Asc => Either::Left(sort_columns.iter().enumerate()),
        SortOrder::Desc => Either::Right(sort_columns.iter().enumerate().rev()),
    };
    for (idx, sort_column) in col_iter {
        let internal_repr = match (&sort_column.1, sort_value) {
            // project to u64 column
            (SortFieldType::U64, SortValue::U64(val)) => {
                InternalValueRepr::new(*val, idx as u8, sort_order)
            }
            (SortFieldType::U64, SortValue::F64(val)) => {
                projected_number_internal_repr(f64_to_u64(*val), sort_order, idx as u8)
            }
            (SortFieldType::U64, SortValue::I64(val)) => {
                projected_number_internal_repr(i64_to_u64(*val), sort_order, idx as u8)
            }
            // project to i64 column
            (SortFieldType::I64, SortValue::I64(val)) => {
                InternalValueRepr::new(val.to_u64(), idx as u8, sort_order)
            }
            (SortFieldType::I64, SortValue::F64(val)) => {
                projected_number_internal_repr(f64_to_i64(*val), sort_order, idx as u8)
            }
            (SortFieldType::I64, SortValue::U64(val)) => {
                projected_number_internal_repr(u64_to_i64(*val), sort_order, idx as u8)
            }
            // project to f64 column
            (SortFieldType::F64, SortValue::F64(val)) => {
                InternalValueRepr::new(val.to_u64(), idx as u8, sort_order)
            }
            (SortFieldType::F64, SortValue::I64(val)) => {
                projected_number_internal_repr(i64_to_f64(*val), sort_order, idx as u8)
            }
            (SortFieldType::F64, SortValue::U64(val)) => {
                projected_number_internal_repr(u64_to_f64(*val), sort_order, idx as u8)
            }
            // other types
            (SortFieldType::DateTime, SortValue::Datetime(val)) => {
                InternalValueRepr::new(val.to_u64(), idx as u8, sort_order)
            }
            (SortFieldType::Bool, SortValue::Boolean(val)) => {
                InternalValueRepr::new(val.to_u64(), idx as u8, sort_order)
            }
            (SortFieldType::String(str_column), SortValue::Str(val)) => {
                let term_dict = str_column.dictionary();
                let hit = term_dict.term_ord_or_next(val.as_str().as_bytes())?;
                match (hit, sort_order) {
                    (TermOrdHit::Exact(ord), _) => {
                        InternalValueRepr::new(ord, idx as u8, sort_order)
                    }
                    (TermOrdHit::Next(ord), SortOrder::Desc) => {
                        InternalValueRepr::new(ord, idx as u8, sort_order)
                    }
                    (TermOrdHit::Next(0), SortOrder::Asc) => {
                        InternalValueRepr::new_keep_column(idx as u8, sort_order)
                    }
                    (TermOrdHit::Next(ord), SortOrder::Asc) => {
                        InternalValueRepr::new(ord - 1, idx as u8, sort_order)
                    }
                }
            }
            // unsupported mixed types
            //
            // TODO: we need a strongly typed pagination API to support JSON
            // fields with datetime and schema evolutions
            (
                SortFieldType::I64 | SortFieldType::U64 | SortFieldType::F64,
                SortValue::Datetime(_),
            ) => {
                return Err(TantivyError::SchemaError(
                    "search after not supported for schema updates to datetime".to_string(),
                ));
            }
            (
                SortFieldType::DateTime,
                SortValue::I64(_) | SortValue::U64(_) | SortValue::F64(_),
            ) => {
                return Err(TantivyError::SchemaError(
                    "search after not supported on multi-typed fields with datetime".to_string(),
                ));
            }
            // supported mixed types
            (sort_field_type, sort_value) => {
                let column_key = sort_field_type.type_sort_key();
                let value_key = sort_value.type_sort_key();
                debug_assert_ne!(column_key, value_key);
                let column_comes_after = match sort_order {
                    SortOrder::Desc => column_key < value_key,
                    SortOrder::Asc => column_key > value_key,
                };
                if column_comes_after {
                    InternalValueRepr::new_keep_column(idx as u8, sort_order)
                } else {
                    continue;
                }
            }
        };
        return Ok(internal_repr);
    }
    Ok(InternalValueRepr::new_skip_all_but_missing())
}

pub(crate) struct SortingFieldExtractorPair<V1: ElidableU64, V2: ElidableU64> {
    first: SortingFieldExtractorComponent,
    second: Option<SortingFieldExtractorComponent>,
    first_order: SortOrder,
    second_order: SortOrder,
    sort1_scratch: Box<[InternalValueRepr<V1>; COLLECT_BLOCK_BUFFER_LEN]>,
    sort2_scratch: Box<[InternalValueRepr<V2>; COLLECT_BLOCK_BUFFER_LEN]>,
}

impl<V1: ElidableU64, V2: ElidableU64> SortingFieldExtractorPair<V1, V2> {
    fn doc_id_sort_order(&self) -> SortOrder {
        if self.first.is_doc_id() {
            self.first_order
        } else if let Some(second) = &self.second
            && second.is_doc_id()
        {
            self.second_order
        } else {
            // TODO this is the current behavior which is weird. QW docs for the
            // native search API advertise that the sort order by default is
            // reverse(doc_id). In ES _shard_doc is supposed to be always ascending.
            self.first_order
        }
    }

    pub(crate) fn search_after_from_partial_hit(
        &self,
        split_id: &SplitId,
        segment_ord: SegmentOrdinal,
        partial_hit: &PartialHit,
    ) -> tantivy::Result<InternalSortValueRepr<V1, V2>> {
        let sort_1 = if let Some(sort_by_value) = &partial_hit.sort_value {
            self.first
                .project_to_internal_search_after(sort_by_value, self.first_order)?
        } else {
            InternalValueRepr::new_missing()
        };
        let sort_2 = if let Some(sort_by_value) = &partial_hit.sort_value2 {
            self.second
                .as_ref()
                .ok_or_else(|| {
                    TantivyError::InvalidArgument(
                        "search after has 2 values but there is only 1 sort dimension".to_string(),
                    )
                })?
                .project_to_internal_search_after(sort_by_value, self.second_order)?
        } else {
            InternalValueRepr::new_missing()
        };

        let internal_repr = if partial_hit.split_id.is_empty() {
            // When split_id is empty, the search_after is a pure sort-value
            // boundary (no doc position), any doc with the same sort value must be
            // excluded otherwise we risk iterating over an over through the same
            // documents.
            InternalSortValueRepr::new_skip_doc_ids(sort_1, sort_2)
        } else {
            let split_cmp = split_id
                .as_str()
                .cmp(partial_hit.split_id.as_str())
                .then(segment_ord.cmp(&partial_hit.segment_ord));
            match (split_cmp, self.doc_id_sort_order()) {
                (Ordering::Less, SortOrder::Asc) | (Ordering::Greater, SortOrder::Desc) => {
                    InternalSortValueRepr::new_skip_doc_ids(sort_1, sort_2)
                }
                (Ordering::Less, SortOrder::Desc) | (Ordering::Greater, SortOrder::Asc) => {
                    InternalSortValueRepr::new_keep_doc_ids(sort_1, sort_2)
                }
                (Ordering::Equal, doc_id_order) => {
                    InternalSortValueRepr::new(sort_1, sort_2, partial_hit.doc_id, doc_id_order)
                }
            }
        };
        Ok(internal_repr)
    }

    pub(crate) fn internal_to_partial_hit(
        &self,
        split_id: &SplitId,
        segment_ord: SegmentOrdinal,
        internal_repr: InternalSortValueRepr<V1, V2>,
    ) -> tantivy::Result<PartialHit> {
        let sort_1 = self
            .first
            .project_from_internal_sort_value(internal_repr.sort_1(), self.first_order)?;
        let sort_2 = self
            .second
            .as_ref()
            .map(|second| {
                second.project_from_internal_sort_value(internal_repr.sort_2(), self.second_order)
            })
            .transpose()?
            .unwrap_or_default();
        Ok(PartialHit {
            sort_value: sort_1,
            sort_value2: sort_2,
            doc_id: internal_repr.doc_id(self.doc_id_sort_order()),
            split_id: split_id.clone(),
            segment_ord,
        })
    }

    /// Returns the list of sort values for the given element
    ///
    /// See also [`SortingFieldExtractorComponent::extract_typed_sort_value_opt`] for more
    /// information.
    #[inline]
    pub(crate) fn project_to_internal_sort_value(
        &self,
        doc_id: DocId,
        score: Score,
    ) -> InternalSortValueRepr<V1, V2> {
        let first = self
            .first
            .project_to_internal_sort_value(doc_id, score, self.first_order);
        let second = self
            .second
            .as_ref()
            .map(|second| second.project_to_internal_sort_value(doc_id, score, self.second_order))
            .unwrap_or_else(InternalValueRepr::new_missing);
        InternalSortValueRepr::new(first, second, doc_id, self.doc_id_sort_order())
    }

    pub(crate) fn project_to_internal_sort_value_block(
        &mut self,
        docs: &[DocId],
        mut f: impl FnMut(InternalSortValueRepr<V1, V2>),
    ) {
        let doc_id_order = self.doc_id_sort_order();
        let first_order = self.first_order;
        let second_order = self.second_order;

        let n = docs.len();

        let SortingFieldExtractorPair {
            first,
            second,
            sort1_scratch,
            sort2_scratch,
            ..
        } = self;

        let first_extractor_opt = first.extractor_for_batch_if_worthwhile();
        let second_extractor_opt = second
            .as_mut()
            .and_then(|s| s.extractor_for_batch_if_worthwhile());
        match (first_extractor_opt, second_extractor_opt) {
            (Some(fst_batch_extr), Some(sec_batch_extr)) => {
                fst_batch_extr.fill_batch(docs, first_order, &mut sort1_scratch[..n]);
                sec_batch_extr.fill_batch(docs, second_order, &mut sort2_scratch[..n]);
                for i in 0..n {
                    f(InternalSortValueRepr::new(
                        sort1_scratch[i],
                        sort2_scratch[i],
                        docs[i],
                        doc_id_order,
                    ));
                }
            }
            (Some(fst_batch_extr), None) => {
                fst_batch_extr.fill_batch(docs, first_order, &mut sort1_scratch[..n]);
                for i in 0..n {
                    let sort2 = second
                        .as_ref()
                        .map(|s| s.project_to_internal_sort_value(docs[i], 0.0, second_order))
                        .unwrap_or_else(InternalValueRepr::new_missing);
                    f(InternalSortValueRepr::new(
                        sort1_scratch[i],
                        sort2,
                        docs[i],
                        doc_id_order,
                    ));
                }
            }
            (None, Some(sec_batch_extr)) => {
                sec_batch_extr.fill_batch(docs, second_order, &mut sort2_scratch[..n]);
                for i in 0..n {
                    let sort1 = first.project_to_internal_sort_value(docs[i], 0.0, first_order);
                    f(InternalSortValueRepr::new(
                        sort1,
                        sort2_scratch[i],
                        docs[i],
                        doc_id_order,
                    ));
                }
            }
            (None, None) => {
                for &doc_id in docs {
                    let first = self
                        .first
                        .project_to_internal_sort_value(doc_id, 0.0, first_order);
                    let second = self
                        .second
                        .as_ref()
                        .map(|s| s.project_to_internal_sort_value(doc_id, 0.0, second_order))
                        .unwrap_or_else(InternalValueRepr::new_missing);
                    f(InternalSortValueRepr::new(
                        first,
                        second,
                        doc_id,
                        doc_id_order,
                    ));
                }
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum AggregationSegmentCollectors {
    FindTraceIdsSegmentCollector(Box<FindTraceIdsSegmentCollector>),
    TantivyAggregationSegmentCollector(AggregationSegmentCollector),
}

/// Quickwit collector working at the scale of the segment.
pub struct QuickwitSegmentCollector {
    segment_top_k_collector: Option<QuickwitSegmentTopKCollector>,
    aggregation: Option<AggregationSegmentCollectors>,
    num_hits: u64,
}

/// Takes a user-defined sorting criteria and resolves it to a
/// segment specific `SortingFieldExtractorPair`.
#[allow(clippy::type_complexity)]
fn get_sorting_field_extractors<V1: ElidableU64, V2: ElidableU64>(
    sort_by: &SortByPair,
    segment_reader: &SegmentReader,
    split_id: &SplitId,
    segment_ord: SegmentOrdinal,
    search_after: &Option<PartialHit>,
) -> tantivy::Result<(
    SortingFieldExtractorPair<V1, V2>,
    Option<InternalSortValueRepr<V1, V2>>,
)> {
    let extractor = SortingFieldExtractorPair {
        first: sort_by
            .first
            .to_sorting_field_extractor_component(segment_reader)?,
        second: sort_by
            .second
            .as_ref()
            .map(|first| first.to_sorting_field_extractor_component(segment_reader))
            .transpose()?,
        first_order: sort_by.first.sort_order(),
        second_order: sort_by
            .second
            .as_ref()
            .map(|second| second.sort_order())
            // value irrelevant?
            .unwrap_or(SortOrder::Desc),
        sort1_scratch: Box::new([InternalValueRepr::new_missing(); COLLECT_BLOCK_BUFFER_LEN]),
        sort2_scratch: Box::new([InternalValueRepr::new_missing(); COLLECT_BLOCK_BUFFER_LEN]),
    };
    let search_after_opt = search_after
        .as_ref()
        .map(|search_after| {
            extractor.search_after_from_partial_hit(split_id, segment_ord, search_after)
        })
        .transpose()?;
    Ok((extractor, search_after_opt))
}

impl SegmentCollector for QuickwitSegmentCollector {
    type Fruit = tantivy::Result<LeafSearchResponse>;

    #[inline]
    fn collect_block(&mut self, filtered_docs: &[DocId]) {
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
            partial_hits = segment_top_k_collector.get_top_k()?;
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
            num_successful_splits: 1,
            resource_stats: None,
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
    /// Returns the list of fast fields that should be loaded for the aggregation.
    pub fn fast_field_names(&self) -> HashSet<String> {
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
            QuickwitIncrementalAggregations::FindTraceIdsAggregation(collector, state) => {
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
                if let Some(first) = state.first()
                    && first.len() >= collector.num_traces
                    && let Some(last_elem) = first.last()
                {
                    let timestamp = last_elem.span_timestamp.into_timestamp_nanos();
                    return Some(PartialHit {
                        sort_value: Some(SortByValue {
                            sort_value: Some(SortValue::Datetime(timestamp)),
                        }),
                        sort_value2: None,
                        split_id: SplitId::new(),
                        segment_ord: 0,
                        doc_id: 0,
                    });
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
    pub agg_context_params: AggContextParams,
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
            fast_fields: self
                .fast_field_names()
                .into_iter()
                .map(|name| FastFieldWarmupInfo {
                    name,
                    with_subfields: false,
                })
                .collect(),
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
                        &self.agg_context_params,
                    )?,
                ),
            ),
            None => None,
        };

        let segment_top_k_collector = if leaf_max_hits == 0 {
            None
        } else {
            let segment_top_k_collector = match self.sort_by {
                SortByPair {
                    first: SortByComponent::DocId { .. },
                    second: None,
                } => {
                    let (extractor, search_after_opt) = get_sorting_field_extractors(
                        &self.sort_by,
                        segment_reader,
                        &self.split_id,
                        segment_ord,
                        &self.search_after,
                    )?;
                    QuickwitSegmentTopKCollector::new_with_doc_id_sort(
                        self.split_id.clone(),
                        segment_ord,
                        extractor,
                        leaf_max_hits,
                        search_after_opt,
                    )
                }
                SortByPair {
                    first: _,
                    second: None | Some(SortByComponent::DocId { .. }),
                } => {
                    let (extractor, search_after_opt) = get_sorting_field_extractors(
                        &self.sort_by,
                        segment_reader,
                        &self.split_id,
                        segment_ord,
                        &self.search_after,
                    )?;
                    QuickwitSegmentTopKCollector::new_with_one_dim_sort(
                        self.split_id.clone(),
                        segment_ord,
                        extractor,
                        leaf_max_hits,
                        search_after_opt,
                    )
                }
                SortByPair {
                    first: _,
                    second: Some(_),
                } => {
                    let (extractor, search_after_opt) = get_sorting_field_extractors(
                        &self.sort_by,
                        segment_reader,
                        &self.split_id,
                        segment_ord,
                        &self.search_after,
                    )?;
                    QuickwitSegmentTopKCollector::new_with_two_dim_sort(
                        self.split_id.clone(),
                        segment_ord,
                        extractor,
                        leaf_max_hits,
                        search_after_opt,
                    )
                }
            };
            Some(segment_top_k_collector)
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

fn map_error(error: postcard::Error) -> TantivyError {
    TantivyError::InternalError(format!(
        "failed to merge intermediate aggregation results: Postcard error: {error}"
    ))
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
            let merged_opt = intermediate_aggregation_results
                .map(|bytes| postcard::from_bytes(bytes).map_err(map_error))
                .try_fold::<_, _, Result<_, TantivyError>>(
                    None,
                    |acc: Option<IntermediateAggregationResults>, fruits_res| {
                        let fruits = fruits_res?;
                        match acc {
                            Some(mut merged_fruits) => {
                                merged_fruits.merge_fruits(fruits)?;
                                Ok(Some(merged_fruits))
                            }
                            None => Ok(Some(fruits)),
                        }
                    },
                )?;
            let serialized =
                postcard::to_allocvec(&merged_opt.unwrap_or_default()).map_err(map_error)?;
            Some(serialized)
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

    let resource_stats_it = leaf_responses
        .iter()
        .map(|leaf_response| &leaf_response.resource_stats);
    let merged_resource_stats = merge_resource_stats_it(resource_stats_it);

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
    let num_successful_splits = leaf_responses
        .iter()
        .map(|leaf_response| leaf_response.num_successful_splits)
        .sum::<u64>();
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
        num_successful_splits,
        resource_stats: merged_resource_stats,
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
        SortByPair {
            first: SortByComponent::DocId {
                order: SortOrder::Desc,
            },
            second: None,
        }
    } else if num_sort_fields == 1 {
        let sort_field = &search_request.sort_fields[0];
        let order = SortOrder::try_from(sort_field.sort_order).unwrap_or(SortOrder::Desc);
        let first = to_sort_by_component(&sort_field.field_name, order);
        SortByPair {
            first,
            second: None,
        }
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
    agg_context_params: AggContextParams,
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
        agg_context_params,
        search_after: search_request.search_after.clone(),
    })
}

/// Builds a QuickwitCollector that's only useful for merging fruits.
pub(crate) fn make_merge_collector(
    search_request: &SearchRequest,
    agg_limits: AggregationLimitsGuard,
) -> crate::Result<QuickwitCollector> {
    // Note: at this point the tokenizer manager is not used anymore by aggregations (filter query),
    // so we can create an empty one. So if it will ever be used, it would panic.
    let agg_context_params = AggContextParams {
        limits: agg_limits,
        tokenizers: TokenizerManager::new(),
    };

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
        agg_context_params,
        search_after: search_request.search_after.clone(),
    })
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
            sort_value: partial_hit.sort_value.clone().and_then(|v| v.sort_value),
            sort_value2: partial_hit.sort_value2.clone().and_then(|v| v.sort_value),
            address: GlobalDocAddress::from_partial_hit(partial_hit),
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
    num_successful_splits: u64,
    start_offset: usize,
    resource_stats: Option<ResourceStats>,
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
            num_successful_splits: 0,
            resource_stats: None,
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
            num_successful_splits,
            resource_stats,
        } = leaf_response;

        merge_resource_stats(&resource_stats, &mut self.resource_stats);

        self.num_hits += num_hits;
        self.top_k_hits.add_entries(partial_hits.into_iter());
        self.failed_splits.extend(failed_splits);
        self.num_attempted_splits += num_attempted_splits;
        self.num_successful_splits += num_successful_splits;
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
    pub(crate) fn peek_worst_hit(&self) -> Option<Cow<'_, PartialHit>> {
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
            num_successful_splits: self.num_successful_splits,
            intermediate_aggregation_result,
            resource_stats: self.resource_stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use quickwit_proto::search::{
        LeafSearchResponse, PartialHit, ResourceStats, SearchRequest, SortByValue, SortField,
        SortOrder, SortValue, SplitSearchError,
    };
    use tantivy::TantivyDocument;
    use tantivy::aggregation::agg_req::Aggregations;
    use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
    use tantivy::collector::Collector;

    use super::{
        IncrementalCollector, QuickwitAggregations, make_merge_collector,
        merge_intermediate_aggregation_result, top_k_partial_hits,
    };

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

    /// Create a list of SortField from a comma-separated list of field names.
    /// Field names can be prefixed with - to indicate ascending order.
    fn make_sort_fields(sort_fields: &str) -> Vec<SortField> {
        sort_fields
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
            .collect()
    }

    /// Build a tantivy index from a JSON dataset. Each element must be a JSON
    /// object whose keys match field names in the pre-determined schema.
    fn make_index(dataset: &[serde_json::Value]) -> tantivy::Index {
        use tantivy::Index;
        use tantivy::indexer::UserOperation;
        use tantivy::schema::{FAST, NumericOptions, Schema};

        let mut schema_builder = Schema::builder();
        let opts = NumericOptions::default().set_fast();
        schema_builder.add_u64_field("sort_u64_1", opts.clone());
        schema_builder.add_u64_field("sort_u64_2", opts);
        schema_builder.add_json_field("kv", FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50_000_000).unwrap();

        index_writer
            .run(
                dataset
                    .iter()
                    .map(|obj| TantivyDocument::parse_json(&schema, &obj.to_string()).unwrap())
                    .map(UserOperation::Add),
            )
            .unwrap();
        index_writer.commit().unwrap();

        index
    }

    #[test]
    fn test_single_split_sorting_single_type() {
        let raw_dataset = sort_dataset();
        let json_dataset: Vec<serde_json::Value> = raw_dataset
            .iter()
            .map(|(v1, v2)| {
                let mut obj = serde_json::Map::new();
                if let Some(v) = v1 {
                    obj.insert("sort_u64_1".to_string(), (*v).into());
                }
                if let Some(v) = v2 {
                    obj.insert("sort_u64_2".to_string(), (*v).into());
                }
                serde_json::Value::Object(obj)
            })
            .collect();
        let index = make_index(&json_dataset);

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // tuple of DocId and sort value
        type Doc = (usize, (Option<u64>, Option<u64>));

        let mut dataset: Vec<Doc> = raw_dataset.into_iter().enumerate().collect();

        let reverse_int = |val: &Option<u64>| val.as_ref().map(|val| u64::MAX - val);
        let cmp_doc_id_desc = |a: &Doc, b: &Doc| b.0.cmp(&a.0);
        let cmp_doc_id_asc = |a: &Doc, b: &Doc| a.0.cmp(&b.0);
        let cmp_1_desc = |a: &Doc, b: &Doc| b.1.0.cmp(&a.1.0);
        let cmp_1_asc = |a: &Doc, b: &Doc| reverse_int(&b.1.0).cmp(&reverse_int(&a.1.0));
        let cmp_2_desc = |a: &Doc, b: &Doc| b.1.1.cmp(&a.1.1);
        let cmp_2_asc = |a: &Doc, b: &Doc| reverse_int(&b.1.1).cmp(&reverse_int(&a.1.1));

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

        // The implicit doc_id tiebreaker is always ascending, matching Elasticsearch's
        // behavior where _shard_doc is always ascending regardless of primary sort direction.
        #[allow(clippy::type_complexity)]
        let sort_orders: Vec<(_, Box<dyn Fn(&Doc, &Doc) -> Ordering>)> = vec![
            ("", Box::new(cmp_doc_id_desc)),
            (
                "sort_u64_1",
                Box::new(|a, b| cmp_1_desc(a, b).then(cmp_doc_id_desc(a, b))),
            ),
            (
                "-sort_u64_1",
                Box::new(|a, b| cmp_1_asc(a, b).then(cmp_doc_id_asc(a, b))),
            ),
            (
                "sort_u64_1,sort_u64_2",
                Box::new(|a, b| {
                    cmp_1_desc(a, b).then(cmp_2_desc(a, b).then(cmp_doc_id_desc(a, b)))
                }),
            ),
            (
                "-sort_u64_1,sort_u64_2",
                Box::new(|a, b| {
                    cmp_1_asc(a, b)
                        .then(cmp_2_desc(a, b))
                        .then(cmp_doc_id_asc(a, b))
                }),
            ),
            (
                "sort_u64_1,-sort_u64_2",
                Box::new(|a, b| cmp_1_desc(a, b).then(cmp_2_asc(a, b).then(cmp_doc_id_desc(a, b)))),
            ),
            (
                "-sort_u64_1,-sort_u64_2",
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
                    &SearchRequest {
                        max_hits: slice_len as u64,
                        sort_fields: make_sort_fields(sort_str),
                        ..SearchRequest::default()
                    },
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
                                    hit.sort_value.clone().and_then(|el| el.sort_value),
                                    hit.sort_value2.clone().and_then(|el| el.sort_value)
                                )
                            })
                            .collect::<Vec<_>>();
                        eprintln!("expected: {expected_docids:#?}");
                        eprintln!("got: {got_docids:#?}");
                        panic!("mismatch ordering for \"{sort_str}\":{slice_len}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_search_after_single_type() {
        let raw_dataset = sort_dataset();
        let json_dataset: Vec<serde_json::Value> = raw_dataset
            .iter()
            .map(|(v1, v2)| {
                let mut obj = serde_json::Map::new();
                if let Some(v) = v1 {
                    obj.insert("sort_u64_1".to_string(), (*v).into());
                }
                if let Some(v) = v2 {
                    obj.insert("sort_u64_2".to_string(), (*v).into());
                }
                serde_json::Value::Object(obj)
            })
            .collect();
        let index = make_index(&json_dataset);

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // tuple of DocId and sort value
        type Doc = (usize, (Option<u64>, Option<u64>));

        let mut dataset: Vec<Doc> = raw_dataset.into_iter().enumerate().collect();

        let reverse_int = |val: &Option<u64>| val.as_ref().map(|val| u64::MAX - val);
        let cmp_doc_id_desc = |a: &Doc, b: &Doc| b.0.cmp(&a.0);
        let cmp_1_desc = |a: &Doc, b: &Doc| b.1.0.cmp(&a.1.0);
        let cmp_2_asc = |a: &Doc, b: &Doc| reverse_int(&b.1.1).cmp(&reverse_int(&a.1.1));

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
                        field_name: "sort_u64_1".to_string(),
                        sort_order: SortOrder::Desc.into(),
                        sort_datetime_format: None,
                    },
                    SortField {
                        field_name: "sort_u64_2".to_string(),
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

    fn assert_search_after_results(
        searcher: &tantivy::Searcher,
        index_len: usize,
        sort_str: &str,
        search_after: PartialHit,
        expected_doc_ids: impl AsRef<[u32]>,
        label: &str,
    ) {
        let expected_doc_ids = expected_doc_ids.as_ref();
        let request = SearchRequest {
            max_hits: 1000,
            sort_fields: make_sort_fields(sort_str),
            search_after: Some(search_after.clone()),
            ..SearchRequest::default()
        };
        let collector = super::make_collector_for_split(
            "fake_split_id".to_string(),
            &request,
            Default::default(),
        )
        .unwrap();
        let Ok(res) = searcher.search(&tantivy::query::AllQuery, &collector) else {
            panic!("search failed for {label} with search_after {search_after:?}");
        };
        // num_hits counts every doc regardless of search_after.
        assert_eq!(
            res.num_hits, index_len as u64,
            "num_hits mismatch for {label}"
        );
        assert_eq!(
            res.partial_hits.len(),
            expected_doc_ids.len(),
            "result count mismatch for {label}"
        );
        for (expected_doc_id, got) in expected_doc_ids.iter().zip(res.partial_hits.iter()) {
            assert_eq!(
                *expected_doc_id, got.doc_id,
                "doc order mismatch for {label} after {search_after:?}"
            );
        }
    }

    #[test]
    fn test_single_split_search_after_multitype() {
        let dataset: Vec<serde_json::Value> = vec![
            serde_json::json!({"kv": {"sort1": false, "sort2": "b"}}), // doc 0
            serde_json::json!({"kv": {"sort1": true, "sort2": "a"}}),  // doc 1
            serde_json::json!({"kv": {"sort1": "apple", "sort2": "a"}}), // doc 2
            serde_json::json!({"kv": {"sort1": "banana", "sort2": "b"}}), // doc 3
            serde_json::json!({"kv": {"sort1": 1, "sort2": "b"}}),     // doc 4
            serde_json::json!({"kv": {"sort1": 5, "sort2": "a"}}),     // doc 5
            serde_json::json!({}),                                     // doc 6: missing
        ];

        let index = make_index(&dataset);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        for (sort_str, expected_order) in [
            // Desc: booleans (true first) > strings (lex desc) > numbers (largest first) > missing
            ("kv.sort1", &[1, 0, 3, 2, 5, 4, 6]),
            // Asc: numbers (smallest first) > strings (lex asc) > booleans (false first) >
            // missing
            ("-kv.sort1", &[4, 5, 2, 3, 0, 1, 6]),
            ("", &[6, 5, 4, 3, 2, 1, 0]),
            ("_doc", &[6, 5, 4, 3, 2, 1, 0]),
            ("-_doc", &[0, 1, 2, 3, 4, 5, 6]),
            // sort2 with "b" first then "a"
            ("kv.sort2,kv.sort1", &[0, 3, 4, 1, 2, 5, 6]),
            // sort2 with "a" first then "b"
            ("-kv.sort2,kv.sort1", &[1, 2, 5, 0, 3, 4, 6]),
        ] {
            // Step 1: full search to collect PartialHits carrying the correct typed SortValues.
            let collector = super::make_collector_for_split(
                "fake_split_id".to_string(),
                &SearchRequest {
                    max_hits: 1000,
                    sort_fields: make_sort_fields(sort_str),
                    ..Default::default()
                },
                Default::default(),
            )
            .unwrap();
            let full_res = searcher
                .search(&tantivy::query::AllQuery, &collector)
                .unwrap();
            assert_eq!(full_res.partial_hits.len(), dataset.len());
            for (expected_doc_id, got) in expected_order.iter().zip(full_res.partial_hits.iter()) {
                assert_eq!(
                    *expected_doc_id, got.doc_id,
                    "sort order mismatch for \"{sort_str}\""
                );
            }

            // Step 2: use each PartialHit as a search_after fence and verify the returned tail.
            for (i, search_after) in full_res.partial_hits.iter().enumerate() {
                assert_search_after_results(
                    &searcher,
                    dataset.len(),
                    sort_str,
                    search_after.clone(),
                    &expected_order[i + 1..],
                    &format!("\"{sort_str}\" search_after position {i}"),
                );
            }
        }
    }

    #[test]
    fn test_single_split_search_after_exogeneous_type() {
        let dataset: Vec<serde_json::Value> = vec![
            serde_json::json!({"kv": {"mixed": false, "integer": 1}}), // doc 0
            serde_json::json!({"kv": {"mixed": true, "integer": 4}}),  // doc 1
            serde_json::json!({"kv": {"mixed": "banana", "integer": 3}}), // doc 2
            serde_json::json!({"kv": {"mixed": "plum", "integer": 4}}), // doc 3
        ];

        let index = make_index(&dataset);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let str_sort_val = |s: &str| SortValue::Str(s.to_string());
        for (sort_str, search_after_value, expected_order) in [
            // Desc: booleans (true first) > strings (lex desc) > numbers (search after) > missing
            ("kv.mixed", SortValue::I64(-10), vec![]),
            // Asc:  numbers (search after) > strings (lex asc) > booleans (false first) > missing
            ("-kv.mixed", SortValue::I64(-10), vec![2, 3, 0, 1]),
            // project f64 to i64
            ("kv.integer", SortValue::F64(3.5), vec![2, 0]),
            ("-kv.integer", SortValue::F64(3.5), vec![1, 3]),
            // str not in columns dict, check all possible relative position
            ("kv.mixed", str_sort_val("c"), vec![2]),
            ("-kv.mixed", str_sort_val("c"), vec![3, 0, 1]),
            ("kv.mixed", str_sort_val("a"), vec![]),
            ("-kv.mixed", str_sort_val("a"), vec![2, 3, 0, 1]),
            ("kv.mixed", str_sort_val("z"), vec![3, 2]),
            ("-kv.mixed", str_sort_val("z"), vec![0, 1]),
        ] {
            assert_search_after_results(
                &searcher,
                dataset.len(),
                sort_str,
                PartialHit {
                    sort_value: Some(search_after_value.clone().into()),
                    sort_value2: None,
                    ..Default::default()
                },
                expected_order,
                &format!("\"{sort_str}\""),
            );
        }
    }

    #[test]
    fn test_single_split_search_after_exogeneous_type_with_null() {
        let dataset: Vec<serde_json::Value> = vec![
            serde_json::json!({"kv": {"sort": false}}),    // doc 0
            serde_json::json!({"kv": {"sort": true}}),     // doc 1
            serde_json::json!({"kv": {"sort": "apple"}}),  // doc 2
            serde_json::json!({"kv": {"sort": "banana"}}), // doc 3
            serde_json::json!({}),                         // doc 4: missing
        ];

        let index = make_index(&dataset);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let search_after_value = SortValue::I64(-10);

        // Desc: booleans (true first) > strings (lex desc) > numbers (search after) > missing
        let desc_order: &[u32] = &[4];
        // Asc:  numbers (search after) > strings (lex asc) > booleans (false first) > missing
        let asc_order: &[u32] = &[2, 3, 0, 1, 4];

        for (sort_str, expected_order) in [("kv.sort", desc_order), ("-kv.sort", asc_order)] {
            assert_search_after_results(
                &searcher,
                dataset.len(),
                sort_str,
                PartialHit {
                    sort_value: Some(search_after_value.clone().into()),
                    sort_value2: None,
                    ..Default::default()
                },
                expected_order,
                &format!("\"{sort_str}\""),
            );
        }
    }

    #[test]
    fn test_single_split_default_sort() {
        let dataset: Vec<serde_json::Value> = vec![
            serde_json::json!({"sort_u64_1": 15}), // doc 0
            serde_json::json!({"sort_u64_1": 13}), // doc 1
            serde_json::json!({"sort_u64_1": 10}), // doc 2
            serde_json::json!({"sort_u64_1": 12}), // doc 3
            serde_json::json!({"sort_u64_1": 9}),  // doc 4
        ];

        let index = make_index(&dataset);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let request = SearchRequest {
            max_hits: 3,
            sort_fields: vec![],
            search_after: None,
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
        // assert the exact hits where in other tests we mostly focus on the order
        assert_eq!(
            res.partial_hits,
            vec![
                PartialHit {
                    split_id: "fake_split_id".to_string(),
                    segment_ord: 0,
                    doc_id: 4,
                    sort_value: None,
                    sort_value2: None,
                },
                PartialHit {
                    split_id: "fake_split_id".to_string(),
                    segment_ord: 0,
                    doc_id: 3,
                    sort_value: None,
                    sort_value2: None,
                },
                PartialHit {
                    split_id: "fake_split_id".to_string(),
                    segment_ord: 0,
                    doc_id: 2,
                    sort_value: None,
                    sort_value2: None,
                },
            ]
        );
    }

    /// Merge intermediate results, asserting that both the regular and
    /// incremental merge produce the same output.
    fn merge_on_both_collectors(
        request: &SearchRequest,
        results: Vec<LeafSearchResponse>,
    ) -> LeafSearchResponse {
        let collector = make_merge_collector(request, Default::default()).unwrap();
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
        let result = merge_on_both_collectors(
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
                num_successful_splits: 3,
                intermediate_aggregation_result: None,
                resource_stats: None,
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
                num_successful_splits: 3,
                intermediate_aggregation_result: None,
                resource_stats: None,
            }
        );

        let result = merge_on_both_collectors(
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
                    num_successful_splits: 3,
                    intermediate_aggregation_result: None,
                    resource_stats: None,
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
                    num_successful_splits: 1,
                    intermediate_aggregation_result: None,
                    resource_stats: None,
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
                num_successful_splits: 4,
                intermediate_aggregation_result: None,
                resource_stats: None,
            }
        );

        // same request, but we reverse sort order
        let result = merge_on_both_collectors(
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
                    num_successful_splits: 3,
                    intermediate_aggregation_result: None,
                    resource_stats: Some(ResourceStats {
                        cpu_microsecs: 100,
                        ..Default::default()
                    }),
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
                    num_successful_splits: 1,
                    intermediate_aggregation_result: None,
                    resource_stats: Some(ResourceStats {
                        cpu_microsecs: 50,
                        ..Default::default()
                    }),
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
                num_successful_splits: 4,
                intermediate_aggregation_result: None,
                resource_stats: Some(ResourceStats {
                    cpu_microsecs: 150,
                    ..Default::default()
                }),
            }
        );
        // TODO would be nice to test aggregation too.
    }

    #[test]
    fn test_merge_empty_intermediate_aggregation_result() {
        let merged = merge_intermediate_aggregation_result(&None, std::iter::empty()).unwrap();
        assert!(merged.is_none());

        let aggregations_json = r#"{
            "avg_price": { "avg": { "field": "price" } }
        }"#;
        let ttv_aggregations: Aggregations = serde_json::from_str(aggregations_json).unwrap();
        let qw_aggregations = QuickwitAggregations::TantivyAggregations(ttv_aggregations);
        let serialized =
            merge_intermediate_aggregation_result(&Some(qw_aggregations), std::iter::empty())
                .unwrap()
                .unwrap();
        let _merged: IntermediateAggregationResults = postcard::from_bytes(&serialized).unwrap();
        // Hopefully `_merged` is empty but the API does not allow us to assert that.
    }
}
