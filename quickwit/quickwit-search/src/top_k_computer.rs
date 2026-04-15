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

use std::cmp::Reverse;
use std::fmt::Debug;

pub(crate) trait MinValue {
    fn min_value() -> Self;
}

/// Fast Top K Computation
///
/// The buffer is truncated to the top_n elements when it reaches the capacity of the Vec.
/// That means capacity has special meaning and should be carried over when cloning or serializing.
///
/// For TopK == 0, it will be relative expensive.
pub(crate) struct TopKComputer<D> {
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
        let vec_cap = top_n.max(1) * 10;
        TopKComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: D::min_value(),
        }
    }
}

impl<D> TopKComputer<D>
where D: Ord + Copy + Debug
{
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
        // This cannot panic, because truncate_top_n will at least remove one element, since
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
}
