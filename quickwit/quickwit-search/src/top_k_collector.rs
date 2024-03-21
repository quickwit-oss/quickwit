use std::cmp::Ordering;

use quickwit_common::binary_heap::TopK;
use quickwit_proto::search::{PartialHit, SortOrder};
use tantivy::{DocId, Score};

use crate::collector::{
    HitSortingMapper, SegmentPartialHit, SegmentPartialHitSortingKey, SortingFieldExtractorPair,
};

/// Quickwit collector working at the scale of the segment.
pub(crate) struct QuickwitSegmentTopKCollector {
    pub(crate) split_id: String,
    pub(crate) score_extractor: SortingFieldExtractorPair,
    // PartialHits in this heap don't contain a split_id yet.
    pub(crate) top_k_hits: TopK<SegmentPartialHit, SegmentPartialHitSortingKey, HitSortingMapper>,
    pub(crate) segment_ord: u32,
    pub(crate) search_after: Option<SearchAfterSegment>,
    // Precomputed order for search_after for split_id and segment_ord
    pub(crate) precomp_search_after_order: Ordering,
    pub(crate) sort_values1: Box<[Option<u64>; 64]>,
    pub(crate) sort_values2: Box<[Option<u64>; 64]>,
}

impl QuickwitSegmentTopKCollector {
    pub(crate) fn collect_top_k_block(&mut self, docs: &[DocId]) {
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
            // Probaly would make sense to check the fence against e.g. sort_values1 earlier,
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
    /// Generic top k collection, that includes search_after handling
    ///
    /// Outside of the collector to circumvent lifetime issues.
    pub(crate) fn collect_top_k_vals(
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

    #[inline]
    pub(crate) fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
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
        search_after: Option<PartialHit>,
        sort_order1: SortOrder,
        sort_order2: SortOrder,
        score_extractor: &SortingFieldExtractorPair,
    ) -> Option<Self> {
        let Some(search_after) = search_after else {
            return None;
        };

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
