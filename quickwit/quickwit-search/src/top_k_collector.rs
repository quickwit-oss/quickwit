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

use std::cmp::{Ordering, Reverse};
use std::fmt::Debug;
use std::marker::PhantomData;

use quickwit_common::binary_heap::TopK;
use quickwit_proto::search::{PartialHit, SortOrder};
use quickwit_proto::types::SplitId;
use tantivy::{DocId, Score};

use crate::collector::{
    HitSortingMapper, SegmentPartialHit, SegmentPartialHitSortingKey,
    SortingFieldExtractorComponent, SortingFieldExtractorPair,
};

pub trait QuickwitSegmentTopKCollector {
    fn collect_top_k_block(&mut self, docs: &[DocId]);
    fn collect_top_k(&mut self, doc_id: DocId, score: Score);
    fn get_top_k(&self) -> Vec<PartialHit>;
}

trait IntoOptionU64 {
    #[inline]
    fn is_unit_type() -> bool {
        false
    }
    fn into_option_u64(self) -> Option<u64>;
    fn from_option_u64(value: Option<u64>) -> Self;
}
trait MinValue {
    fn min_value() -> Self;
}

impl IntoOptionU64 for Option<u64> {
    #[inline]
    fn into_option_u64(self) -> Option<u64> {
        self
    }
    #[inline]
    fn from_option_u64(value: Option<u64>) -> Self {
        value
    }
}

impl MinValue for Option<u64> {
    #[inline]
    fn min_value() -> Self {
        None
    }
}

impl IntoOptionU64 for Option<Reverse<u64>> {
    #[inline]
    fn into_option_u64(self) -> Option<u64> {
        self.map(|el| el.0)
    }
    #[inline]
    fn from_option_u64(value: Option<u64>) -> Self {
        value.map(Reverse)
    }
}
impl MinValue for Option<Reverse<u64>> {
    #[inline]
    fn min_value() -> Self {
        None
    }
}

impl IntoOptionU64 for () {
    #[inline]
    fn is_unit_type() -> bool {
        true
    }
    #[inline]
    fn into_option_u64(self) -> Option<u64> {
        None
    }
    #[inline]
    fn from_option_u64(_: Option<u64>) -> Self {}
}
impl MinValue for () {
    #[inline]
    fn min_value() -> Self {}
}

/// Generic hit struct for top k collector.
/// V1 and V2 are the types of the two values to sort by.
/// They are either Option<u64> or _statically_ disabled via unit type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Hit<V1, V2, const REVERSE_DOCID: bool> {
    doc_id: DocId,
    value1: V1,
    value2: V2,
}

impl<V1, V2, const REVERSE_DOCID: bool> MinValue for Hit<V1, V2, REVERSE_DOCID>
where
    V1: MinValue,
    V2: MinValue,
{
    #[inline]
    fn min_value() -> Self {
        let doc_id = if REVERSE_DOCID {
            DocId::MAX
        } else {
            DocId::MIN
        };
        Hit {
            doc_id,
            value1: V1::min_value(),
            value2: V2::min_value(),
        }
    }
}

impl<V1, V2, const REVERSE_DOCID: bool> std::fmt::Display for Hit<V1, V2, REVERSE_DOCID>
where
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + Debug,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hit(doc_id: {}, value1: {:?}, value2: {:?})",
            self.doc_id, self.value1, self.value2
        )
    }
}

impl<V1, V2, const REVERSE_DOCID: bool> Ord for Hit<V1, V2, REVERSE_DOCID>
where
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + Debug + MinValue,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + Debug + MinValue,
{
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.value1.cmp(&other.value1);
        order
            .then_with(|| self.value2.cmp(&other.value2))
            .then_with(|| {
                if REVERSE_DOCID {
                    other.doc_id.cmp(&self.doc_id)
                } else {
                    self.doc_id.cmp(&other.doc_id)
                }
            })
    }
}

impl<V1, V2, const REVERSE_DOCID: bool> PartialOrd for Hit<V1, V2, REVERSE_DOCID>
where
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + Debug + MinValue,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + Debug + MinValue,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    const REVERSE_DOCID: bool,
> Hit<V1, V2, REVERSE_DOCID>
{
    #[inline]
    fn into_segment_partial_hit(self) -> SegmentPartialHit {
        SegmentPartialHit {
            sort_value: self.value1.into_option_u64(),
            sort_value2: self.value2.into_option_u64(),
            doc_id: self.doc_id,
        }
    }
}

pub fn specialized_top_k_segment_collector(
    split_id: SplitId,
    score_extractor: SortingFieldExtractorPair,
    leaf_max_hits: usize,
    segment_ord: u32,
    search_after_option: Option<PartialHit>,
    order1: SortOrder,
    order2: SortOrder,
) -> Box<dyn QuickwitSegmentTopKCollector> {
    // TODO: Add support for search_after to the specialized collector.
    // Eventually we may want to remove the generic collector to reduce complexity.
    if search_after_option.is_some() || score_extractor.is_score() {
        return Box::new(GenericQuickwitSegmentTopKCollector::new(
            split_id,
            score_extractor,
            leaf_max_hits,
            segment_ord,
            search_after_option,
            order1,
            order2,
        ));
    }

    let sort_first_by_ff = score_extractor.first.is_fast_field();
    let sort_second_by_ff = score_extractor
        .second
        .as_ref()
        .map(|extr| extr.is_fast_field())
        .unwrap_or(false);

    #[derive(Debug)]
    enum SortType {
        DocId,
        OneFFSort,
        TwoFFSorts,
    }
    let sort_type = match (sort_first_by_ff, sort_second_by_ff) {
        (false, false) => SortType::DocId,
        (true, false) => SortType::OneFFSort,
        (true, true) => SortType::TwoFFSorts,
        (false, true) => panic!("Internal error: Got second sort, but no first sort"),
    };
    // only check order1 for OneFFSort and DocId, as it's the only sort
    //
    // REVERSE_DOCID is only used for SortType::DocId and SortType::OneFFSort
    match (sort_type, order1, order2) {
        (SortType::DocId, SortOrder::Desc, _) => {
            Box::new(SpecializedSegmentTopKCollector::<(), (), false>::new(
                split_id,
                score_extractor,
                leaf_max_hits,
                segment_ord,
            ))
        }
        (SortType::DocId, SortOrder::Asc, _) => {
            Box::new(SpecializedSegmentTopKCollector::<(), (), true>::new(
                split_id,
                score_extractor,
                leaf_max_hits,
                segment_ord,
            ))
        }
        (SortType::OneFFSort, SortOrder::Asc, SortOrder::Asc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<Reverse<u64>>,
                (),
                true,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
        (SortType::OneFFSort, SortOrder::Desc, SortOrder::Asc) => Box::new(
            SpecializedSegmentTopKCollector::<Option<u64>, (), false>::new(
                split_id,
                score_extractor,
                leaf_max_hits,
                segment_ord,
            ),
        ),
        (SortType::OneFFSort, SortOrder::Asc, SortOrder::Desc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<Reverse<u64>>,
                (),
                true,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
        (SortType::OneFFSort, SortOrder::Desc, SortOrder::Desc) => Box::new(
            SpecializedSegmentTopKCollector::<Option<u64>, (), false>::new(
                split_id,
                score_extractor,
                leaf_max_hits,
                segment_ord,
            ),
        ),
        (SortType::TwoFFSorts, SortOrder::Asc, SortOrder::Asc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<Reverse<u64>>,
                Option<Reverse<u64>>,
                true,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
        (SortType::TwoFFSorts, SortOrder::Asc, SortOrder::Desc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<Reverse<u64>>,
                Option<u64>,
                true,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
        (SortType::TwoFFSorts, SortOrder::Desc, SortOrder::Asc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<u64>,
                Option<Reverse<u64>>,
                false,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
        (SortType::TwoFFSorts, SortOrder::Desc, SortOrder::Desc) => {
            Box::new(SpecializedSegmentTopKCollector::<
                Option<u64>,
                Option<u64>,
                false,
            >::new(
                split_id, score_extractor, leaf_max_hits, segment_ord
            ))
        }
    }
}

/// Fast Top K Computation
///
/// The buffer is truncated to the top_n elements when it reaches the capacity of the Vec.
/// That means capacity has special meaning and should be carried over when cloning or serializing.
///
/// For TopK == 0, it will be relative expensive.
struct TopKComputer<D> {
    /// Reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<Reverse<D>>,
    top_n: usize,
    pub(crate) threshold: D,
}

// Custom clone to keep capacity
impl<D: Clone> Clone for TopKComputer<D> {
    fn clone(&self) -> Self {
        let mut buffer_clone = Vec::with_capacity(self.buffer.capacity());
        buffer_clone.extend(self.buffer.iter().cloned());

        TopKComputer {
            buffer: buffer_clone,
            top_n: self.top_n,
            threshold: self.threshold.clone(),
        }
    }
}

impl<D> TopKComputer<D>
where D: Ord + Copy + Debug + MinValue
{
    /// Create a new `TopKComputer`.
    pub fn new(top_n: usize) -> Self {
        // Vec cap can't be 0, since it would panic in push
        let vec_cap = top_n.max(1) * 10;
        TopKComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: D::min_value(),
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    #[inline]
    pub fn push(&mut self, doc: D) {
        if doc < self.threshold {
            return;
        }
        if self.buffer.len() == self.buffer.capacity() {
            let median = self.truncate_top_n();
            self.threshold = median;
        }

        // This is faster since it avoids the buffer resizing to be inlined from vec.push()
        // (this is in the hot path)
        // TODO: Replace with `push_within_capacity` when it's stabilized
        let uninit = self.buffer.spare_capacity_mut();
        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is larger than 2.
        uninit[0].write(Reverse(doc));
        // This is safe because it would panic in the line above
        unsafe {
            self.buffer.set_len(self.buffer.len() + 1);
        }
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) -> D {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable(self.top_n);

        let median_score = *median_el;
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        median_score.0
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<D> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.sort_unstable();
        self.buffer.into_iter().map(|el| el.0).collect()
    }

    /// Returns the top n elements in stored order.
    /// Useful if you do not need the elements in sorted order,
    /// for example when merging the results of multiple segments.
    #[allow(dead_code)]
    pub fn into_vec(mut self) -> Vec<D> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.into_iter().map(|el| el.0).collect()
    }
}

pub use tantivy::COLLECT_BLOCK_BUFFER_LEN;
struct SpecSortingFieldExtractor<V1, V2> {
    _phantom: std::marker::PhantomData<(V1, V2)>,
    sort_values1: Box<[Option<u64>; COLLECT_BLOCK_BUFFER_LEN]>,
    sort_values2: Box<[Option<u64>; COLLECT_BLOCK_BUFFER_LEN]>,

    pub first: SortingFieldExtractorComponent,
    pub second: Option<SortingFieldExtractorComponent>,
}

impl<
    V1: Copy + PartialEq + PartialOrd + Ord + IntoOptionU64 + Debug,
    V2: Copy + PartialEq + PartialOrd + Ord + IntoOptionU64 + Debug,
> SpecSortingFieldExtractor<V1, V2>
{
    fn new(
        first: SortingFieldExtractorComponent,
        second: Option<SortingFieldExtractorComponent>,
    ) -> Self {
        Self {
            _phantom: PhantomData,
            sort_values1: vec![None; COLLECT_BLOCK_BUFFER_LEN]
                .into_boxed_slice()
                .try_into()
                .unwrap(),
            sort_values2: vec![None; COLLECT_BLOCK_BUFFER_LEN]
                .into_boxed_slice()
                .try_into()
                .unwrap(),
            first,
            second,
        }
    }
    /// Fetches the sort values for the given docs.
    /// Does noting when sorting by docid.
    fn fetch_data(&mut self, docs: &[DocId]) {
        self.first
            .extract_typed_sort_values_block(docs, &mut self.sort_values1[..docs.len()]);
        if let Some(second) = self.second.as_ref() {
            second.extract_typed_sort_values_block(docs, &mut self.sort_values2[..docs.len()]);
        }
    }
    #[inline]
    fn iter_hits<'a, const REVERSE_DOCID: bool>(
        &'a self,
        docs: &'a [DocId],
    ) -> impl Iterator<Item = Hit<V1, V2, REVERSE_DOCID>> + 'a {
        SpecSortingFieldIter::<V1, V2, REVERSE_DOCID>::new(
            docs,
            &self.sort_values1,
            &self.sort_values2,
        )
    }
}

struct SpecSortingFieldIter<'a, V1, V2, const REVERSE_DOCID: bool> {
    docs: std::slice::Iter<'a, DocId>,
    sort_values1: std::slice::Iter<'a, Option<u64>>,
    sort_values2: std::slice::Iter<'a, Option<u64>>,
    _phantom: PhantomData<(V1, V2)>,
}

impl<'a, V1, V2, const REVERSE_DOCID: bool> SpecSortingFieldIter<'a, V1, V2, REVERSE_DOCID>
where
    V1: Copy + PartialEq + PartialOrd + Ord + IntoOptionU64,
    V2: Copy + PartialEq + PartialOrd + Ord + IntoOptionU64,
{
    #[inline]
    pub fn new(
        docs: &'a [DocId],
        sort_values1: &'a [Option<u64>; COLLECT_BLOCK_BUFFER_LEN],
        sort_values2: &'a [Option<u64>; COLLECT_BLOCK_BUFFER_LEN],
    ) -> Self {
        Self {
            docs: docs.iter(),
            sort_values1: sort_values1.iter(),
            sort_values2: sort_values2.iter(),
            _phantom: PhantomData,
        }
    }
}

impl<V1, V2, const REVERSE_DOCID: bool> Iterator for SpecSortingFieldIter<'_, V1, V2, REVERSE_DOCID>
where
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug,
{
    type Item = Hit<V1, V2, REVERSE_DOCID>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let doc_id = *self.docs.next()?;

        let value1 = if !V1::is_unit_type() {
            V1::from_option_u64(*self.sort_values1.next()?)
        } else {
            V1::from_option_u64(None)
        };

        let value2 = if !V2::is_unit_type() {
            V2::from_option_u64(*self.sort_values2.next()?)
        } else {
            V2::from_option_u64(None)
        };

        Some(Hit {
            doc_id,
            value1,
            value2,
        })
    }
}

/// No search after handling
/// Quickwit collector working at the scale of the segment.
struct SpecializedSegmentTopKCollector<
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    const REVERSE_DOCID: bool,
> {
    split_id: SplitId,
    hit_fetcher: SpecSortingFieldExtractor<V1, V2>,
    top_k_hits: TopKComputer<Hit<V1, V2, REVERSE_DOCID>>,
    segment_ord: u32,
}

impl<
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue + 'static,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue + 'static,
    const REVERSE_DOCID: bool,
> SpecializedSegmentTopKCollector<V1, V2, REVERSE_DOCID>
{
    pub fn new(
        split_id: SplitId,
        score_extractor: SortingFieldExtractorPair,
        leaf_max_hits: usize,
        segment_ord: u32,
    ) -> Self {
        let hit_fetcher =
            SpecSortingFieldExtractor::new(score_extractor.first, score_extractor.second);
        let top_k_hits = TopKComputer::new(leaf_max_hits);
        Self {
            split_id,
            hit_fetcher,
            top_k_hits,
            segment_ord,
        }
    }
}
impl<
    V1: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    V2: Copy + PartialEq + Eq + PartialOrd + Ord + IntoOptionU64 + Debug + MinValue,
    const REVERSE_DOCID: bool,
> QuickwitSegmentTopKCollector for SpecializedSegmentTopKCollector<V1, V2, REVERSE_DOCID>
{
    fn collect_top_k_block(&mut self, docs: &[DocId]) {
        self.hit_fetcher.fetch_data(docs);
        let iter = self.hit_fetcher.iter_hits::<REVERSE_DOCID>(docs);
        for doc_id in iter {
            self.top_k_hits.push(doc_id);
        }
    }

    #[inline]
    fn collect_top_k(&mut self, _doc_id: DocId, _score: Score) {
        panic!("Internal Error: This collector does not support collect_top_k");
    }

    fn get_top_k(&self) -> Vec<PartialHit> {
        self.top_k_hits
            .clone()
            .into_sorted_vec()
            .into_iter()
            .map(|el| el.into_segment_partial_hit())
            .map(|segment_partial_hit: SegmentPartialHit| {
                segment_partial_hit.into_partial_hit(
                    self.split_id.clone(),
                    self.segment_ord,
                    &self.hit_fetcher.first,
                    &self.hit_fetcher.second,
                )
            })
            .collect()
    }
}

/// Quickwit collector working at the scale of the segment.
pub(crate) struct GenericQuickwitSegmentTopKCollector {
    split_id: SplitId,
    score_extractor: SortingFieldExtractorPair,
    // PartialHits in this heap don't contain a split_id yet.
    top_k_hits: TopK<SegmentPartialHit, SegmentPartialHitSortingKey, HitSortingMapper>,
    segment_ord: u32,
    search_after: Option<SearchAfterSegment>,
    // Precomputed order for search_after for split_id and segment_ord
    precomp_search_after_order: Ordering,
    sort_values1: Box<[Option<u64>; COLLECT_BLOCK_BUFFER_LEN]>,
    sort_values2: Box<[Option<u64>; COLLECT_BLOCK_BUFFER_LEN]>,
}

impl GenericQuickwitSegmentTopKCollector {
    pub fn new(
        split_id: SplitId,
        score_extractor: SortingFieldExtractorPair,
        leaf_max_hits: usize,
        segment_ord: u32,
        search_after_option: Option<PartialHit>,
        order1: SortOrder,
        order2: SortOrder,
    ) -> Self {
        let sort_key_mapper = HitSortingMapper { order1, order2 };
        let precomp_search_after_order = match &search_after_option {
            Some(search_after) if !search_after.split_id.is_empty() => order1
                .compare(&split_id, &search_after.split_id)
                .then_with(|| order1.compare(&segment_ord, &search_after.segment_ord)),
            // This value isn't actually used.
            _ => Ordering::Equal,
        };
        let search_after =
            SearchAfterSegment::new(search_after_option, order1, order2, &score_extractor);

        GenericQuickwitSegmentTopKCollector {
            split_id,
            score_extractor,
            top_k_hits: TopK::new(leaf_max_hits, sort_key_mapper), // Adjusted for context
            segment_ord,
            search_after,
            precomp_search_after_order,
            sort_values1: vec![None; COLLECT_BLOCK_BUFFER_LEN]
                .into_boxed_slice()
                .try_into()
                .unwrap(),
            sort_values2: vec![None; COLLECT_BLOCK_BUFFER_LEN]
                .into_boxed_slice()
                .try_into()
                .unwrap(),
        }
    }
    #[inline]
    /// Generic top k collection, that includes search_after handling
    ///
    /// Outside of the collector to circumvent lifetime issues.
    fn collect_top_k_vals(
        doc_id: DocId,
        sort_value: Option<u64>,
        sort_value2: Option<u64>,
        search_after: &Option<SearchAfterSegment>,
        precomp_search_after_order: Ordering,
        top_k_hits: &mut TopK<SegmentPartialHit, SegmentPartialHitSortingKey, HitSortingMapper>,
    ) {
        if let Some(search_after) = &search_after {
            let search_after_value1 = search_after.sort_value;
            let search_after_value2 = search_after.sort_value2;
            let orders = &top_k_hits.sort_key_mapper;
            let mut cmp_result = orders
                .order1
                .compare_opt(&sort_value, &search_after_value1)
                .then_with(|| {
                    orders
                        .order2
                        .compare_opt(&sort_value2, &search_after_value2)
                });
            if search_after.compare_on_equal {
                // TODO actually it's not first, it should be what's in _shard_doc then first then
                // default
                let order = orders.order1;
                cmp_result = cmp_result
                    .then(precomp_search_after_order)
                    // We compare doc_id only if sort_value1, sort_value2, split_id and segment_ord
                    // are equal.
                    .then_with(|| order.compare(&doc_id, &search_after.doc_id))
            }

            if cmp_result != Ordering::Less {
                return;
            }
        }

        let hit = SegmentPartialHit {
            sort_value,
            sort_value2,
            doc_id,
        };
        top_k_hits.add_entry(hit);
    }
}
impl QuickwitSegmentTopKCollector for GenericQuickwitSegmentTopKCollector {
    fn collect_top_k_block(&mut self, docs: &[DocId]) {
        self.score_extractor.extract_typed_sort_values(
            docs,
            &mut self.sort_values1[..],
            &mut self.sort_values2[..],
        );
        if self.search_after.is_some() {
            // Search after not optimized for block collection yet
            for ((doc_id, sort_value), sort_value2) in docs
                .iter()
                .cloned()
                .zip(self.sort_values1.iter().cloned())
                .zip(self.sort_values2.iter().cloned())
            {
                Self::collect_top_k_vals(
                    doc_id,
                    sort_value,
                    sort_value2,
                    &self.search_after,
                    self.precomp_search_after_order,
                    &mut self.top_k_hits,
                );
            }
        } else {
            // Probably would make sense to check the fence against e.g. sort_values1 earlier,
            // before creating the SegmentPartialHit.
            //
            // Below are different versions to avoid iterating the caches if they are unused.
            //
            // No sort values loaded. Sort only by doc_id.
            if !self.score_extractor.first.is_fast_field() {
                for doc_id in docs.iter().cloned() {
                    let hit = SegmentPartialHit {
                        sort_value: None,
                        sort_value2: None,
                        doc_id,
                    };
                    self.top_k_hits.add_entry(hit);
                }
                return;
            }
            let has_no_second_sort = !self
                .score_extractor
                .second
                .as_ref()
                .map(|extr| extr.is_fast_field())
                .unwrap_or(false);
            // No second sort values => We can skip iterating the second sort values cache.
            if has_no_second_sort {
                for (doc_id, sort_value) in
                    docs.iter().cloned().zip(self.sort_values1.iter().cloned())
                {
                    let hit = SegmentPartialHit {
                        sort_value,
                        sort_value2: None,
                        doc_id,
                    };
                    self.top_k_hits.add_entry(hit);
                }
                return;
            }

            for ((doc_id, sort_value), sort_value2) in docs
                .iter()
                .cloned()
                .zip(self.sort_values1.iter().cloned())
                .zip(self.sort_values2.iter().cloned())
            {
                let hit = SegmentPartialHit {
                    sort_value,
                    sort_value2,
                    doc_id,
                };
                self.top_k_hits.add_entry(hit);
            }
        }
    }

    #[inline]
    fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        let (sort_value, sort_value2): (Option<u64>, Option<u64>) =
            self.score_extractor.extract_typed_sort_value(doc_id, score);
        Self::collect_top_k_vals(
            doc_id,
            sort_value,
            sort_value2,
            &self.search_after,
            self.precomp_search_after_order,
            &mut self.top_k_hits,
        );
    }

    fn get_top_k(&self) -> Vec<PartialHit> {
        self.top_k_hits
            .clone()
            .finalize()
            .into_iter()
            .map(|segment_partial_hit: SegmentPartialHit| {
                segment_partial_hit.into_partial_hit(
                    self.split_id.clone(),
                    self.segment_ord,
                    &self.score_extractor.first,
                    &self.score_extractor.second,
                )
            })
            .collect()
    }
}

/// Search After, but the sort values are converted to the u64 fast field representation.
pub(crate) struct SearchAfterSegment {
    sort_value: Option<u64>,
    sort_value2: Option<u64>,
    compare_on_equal: bool,
    doc_id: DocId,
}
impl SearchAfterSegment {
    pub fn new(
        search_after_opt: Option<PartialHit>,
        sort_order1: SortOrder,
        sort_order2: SortOrder,
        score_extractor: &SortingFieldExtractorPair,
    ) -> Option<Self> {
        let search_after = search_after_opt?;
        let mut sort_value = None;
        if let Some(search_after_sort_value) = search_after
            .sort_value
            .and_then(|sort_value| sort_value.sort_value)
        {
            if let Some(new_value) = score_extractor
                .first
                .convert_to_u64_ff_val(search_after_sort_value, sort_order1)
            {
                sort_value = Some(new_value);
            } else {
                // Value is out of bounds, we ignore sort_value2 and disable the whole
                // search_after
                return None;
            }
        }
        let mut sort_value2 = None;
        if let Some(search_after_sort_value) = search_after
            .sort_value2
            .and_then(|sort_value2| sort_value2.sort_value)
        {
            let extractor = score_extractor
                .second
                .as_ref()
                .expect("Internal error: Got sort_value2, but no sort extractor");
            if let Some(new_value) =
                extractor.convert_to_u64_ff_val(search_after_sort_value, sort_order2)
            {
                sort_value2 = Some(new_value);
            }
        }
        Some(Self {
            sort_value,
            sort_value2,
            compare_on_equal: !search_after.split_id.is_empty(),
            doc_id: search_after.doc_id,
        })
    }
}
