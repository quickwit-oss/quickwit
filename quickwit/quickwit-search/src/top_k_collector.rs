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

use std::cmp::Ordering;

use quickwit_proto::search::PartialHit;
use quickwit_proto::types::SplitId;
use tantivy::{DocId, Score, SegmentOrdinal};

use crate::collector::SortingFieldExtractorPair;
use crate::sort_repr::{ElidableU64, InternalSortValueRepr};
use crate::top_k_computer::TopKComputer;

pub struct QuickwitSegmentTopKCollectorTemplate<V1: ElidableU64, V2: ElidableU64> {
    split_id: SplitId,
    // We track the segment ordinal here, but splits only have 1 segment so this
    // should always be 0.
    segment_ord: SegmentOrdinal,
    hit_fetcher: SortingFieldExtractorPair<V1, V2>,
    top_k_hits: TopKComputer<InternalSortValueRepr<V1, V2>>,
    search_after_opt: Option<InternalSortValueRepr<V1, V2>>,
}

impl<V1: ElidableU64, V2: ElidableU64> QuickwitSegmentTopKCollectorTemplate<V1, V2> {
    pub(crate) fn collect_top_k_block(&mut self, docs: &[DocId]) {
        let search_after_opt = self.search_after_opt;
        let top_k_hits = &mut self.top_k_hits;
        self.hit_fetcher
            .project_to_internal_sort_value_block(docs, |repr| {
                if let Some(search_after) = search_after_opt
                    && repr.cmp(&search_after) != Ordering::Less
                {
                    return;
                }
                top_k_hits.push(repr);
            });
    }

    pub(crate) fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        let internal_repr = self
            .hit_fetcher
            .project_to_internal_sort_value(doc_id, score);
        if let Some(search_after) = self.search_after_opt
            && internal_repr.cmp(&search_after) != Ordering::Less
        {
            return;
        }
        self.top_k_hits.push(internal_repr);
    }

    pub(crate) fn get_top_k(&self) -> tantivy::Result<Vec<PartialHit>> {
        self.top_k_hits
            .clone()
            .into_sorted_vec()
            .into_iter()
            .map(|internal_repr| {
                self.hit_fetcher.internal_to_partial_hit(
                    &self.split_id,
                    self.segment_ord,
                    internal_repr,
                )
            })
            .collect()
    }
}

pub enum QuickwitSegmentTopKCollector {
    DocIdSort(QuickwitSegmentTopKCollectorTemplate<(), ()>),
    OneDimSort(QuickwitSegmentTopKCollectorTemplate<u64, ()>),
    TwoDimSort(QuickwitSegmentTopKCollectorTemplate<u64, u64>),
    Noop,
}

impl QuickwitSegmentTopKCollector {
    pub fn new_with_doc_id_sort(
        split_id: SplitId,
        segment_ord: SegmentOrdinal,
        hit_fetcher: SortingFieldExtractorPair<(), ()>,
        top_k: usize,
        search_after_opt: Option<InternalSortValueRepr<(), ()>>,
    ) -> Self {
        if let Some(search_after) = &search_after_opt
            && search_after.is_skip_all()
        {
            QuickwitSegmentTopKCollector::Noop
        } else {
            QuickwitSegmentTopKCollector::DocIdSort(QuickwitSegmentTopKCollectorTemplate {
                split_id,
                segment_ord,
                top_k_hits: TopKComputer::new(top_k),
                hit_fetcher,
                search_after_opt,
            })
        }
    }

    pub fn new_with_one_dim_sort(
        split_id: SplitId,
        segment_ord: SegmentOrdinal,
        hit_fetcher: SortingFieldExtractorPair<u64, ()>,
        top_k: usize,
        search_after_opt: Option<InternalSortValueRepr<u64, ()>>,
    ) -> Self {
        if let Some(search_after) = &search_after_opt
            && search_after.is_skip_all()
        {
            QuickwitSegmentTopKCollector::Noop
        } else {
            QuickwitSegmentTopKCollector::OneDimSort(QuickwitSegmentTopKCollectorTemplate {
                split_id,
                segment_ord,
                top_k_hits: TopKComputer::new(top_k),
                hit_fetcher,
                search_after_opt,
            })
        }
    }

    pub fn new_with_two_dim_sort(
        split_id: SplitId,
        segment_ord: SegmentOrdinal,
        hit_fetcher: SortingFieldExtractorPair<u64, u64>,
        top_k: usize,
        search_after_opt: Option<InternalSortValueRepr<u64, u64>>,
    ) -> Self {
        if let Some(search_after) = &search_after_opt
            && search_after.is_skip_all()
        {
            QuickwitSegmentTopKCollector::Noop
        } else {
            QuickwitSegmentTopKCollector::TwoDimSort(QuickwitSegmentTopKCollectorTemplate {
                split_id,
                segment_ord,
                top_k_hits: TopKComputer::new(top_k),
                hit_fetcher,
                search_after_opt,
            })
        }
    }

    pub(crate) fn collect_top_k_block(&mut self, docs: &[DocId]) {
        match self {
            QuickwitSegmentTopKCollector::DocIdSort(collector) => {
                collector.collect_top_k_block(docs)
            }
            QuickwitSegmentTopKCollector::OneDimSort(collector) => {
                collector.collect_top_k_block(docs)
            }
            QuickwitSegmentTopKCollector::TwoDimSort(collector) => {
                collector.collect_top_k_block(docs)
            }
            QuickwitSegmentTopKCollector::Noop => {}
        }
    }

    pub(crate) fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        match self {
            QuickwitSegmentTopKCollector::DocIdSort(collector) => {
                collector.collect_top_k(doc_id, score)
            }
            QuickwitSegmentTopKCollector::OneDimSort(collector) => {
                collector.collect_top_k(doc_id, score)
            }
            QuickwitSegmentTopKCollector::TwoDimSort(collector) => {
                collector.collect_top_k(doc_id, score)
            }
            QuickwitSegmentTopKCollector::Noop => {}
        }
    }

    pub(crate) fn get_top_k(&self) -> tantivy::Result<Vec<PartialHit>> {
        match self {
            QuickwitSegmentTopKCollector::DocIdSort(collector) => collector.get_top_k(),
            QuickwitSegmentTopKCollector::OneDimSort(collector) => collector.get_top_k(),
            QuickwitSegmentTopKCollector::TwoDimSort(collector) => collector.get_top_k(),
            QuickwitSegmentTopKCollector::Noop => Ok(vec![]),
        }
    }
}
