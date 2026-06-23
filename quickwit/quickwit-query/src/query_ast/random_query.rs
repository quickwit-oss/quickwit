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

use std::hash::Hasher;

use fnv::FnvHasher;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Geometric};
use serde::{Deserialize, Serialize};
use tantivy::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED};

use super::{BuildTantivyAst, BuildTantivyAstContext, TantivyQueryAst};
use crate::InvalidQuery;
use crate::query_ast::QueryAst;

/// A query that samples documents with a given probability, seeded deterministically from the
/// split ID.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RandomQuery {
    pub probability: f64,
}

impl PartialEq for RandomQuery {
    fn eq(&self, other: &Self) -> bool {
        self.probability.to_bits() == other.probability.to_bits()
    }
}

impl Eq for RandomQuery {}

impl From<RandomQuery> for QueryAst {
    fn from(random_query: RandomQuery) -> Self {
        QueryAst::Random(random_query)
    }
}

impl BuildTantivyAst for RandomQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let seed = seed_from_split_id(context.split_id);
        Ok(TantivyRandomQuery {
            probability: self.probability,
            seed,
        }
        .into())
    }
}

fn seed_from_split_id(split_id: &str) -> u64 {
    let mut hasher = FnvHasher::default();
    hasher.write(split_id.as_bytes());
    hasher.finish()
}

#[derive(Clone, Debug)]
struct TantivyRandomQuery {
    probability: f64,
    seed: u64,
}

impl Query for TantivyRandomQuery {
    fn weight(
        &self,
        _enable_scoring: EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(RandomWeight {
            probability: self.probability,
            seed: self.seed,
        }))
    }
}

struct RandomWeight {
    probability: f64,
    seed: u64,
}

impl Weight for RandomWeight {
    fn scorer(&self, reader: &SegmentReader, _boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        Ok(Box::new(RandomDocSet::new(
            self.probability,
            reader.max_doc(),
            self.seed,
        )))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = RandomDocSet::new(self.probability, reader.max_doc(), self.seed);
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("random match", 1.0))
        } else {
            Err(tantivy::TantivyError::InvalidArgument(
                "document not matched by random query".to_string(),
            ))
        }
    }
}

/// Selects documents with probability p using a geometric distribution (first success of a Bernoulli trials)
///
/// Two modes are used depending on the probability:
/// - Selecting (p <= 0.5): Geometric(p) gaps between *selected* docs.
///   E[gap] = (1-p)/p -- efficient when few docs are selected.
/// - Rejecting (p > 0.5): Geometric(1-p) gaps between *rejected* docs.
///   E[gap] = p/(1-p) -- efficient when few docs are rejected.
enum SamplingMode {
    Selecting {
        geo: Geometric,
    },
    Rejecting {
        geo: Geometric,
        /// The next doc that will NOT be returned (TERMINATED when no more rejections remain).
        next_rejection: DocId,
    },
}

struct RandomDocSet {
    current_doc: DocId,
    max_doc: DocId,
    probability: f64,
    rng: SmallRng,
    mode: SamplingMode,
}

impl RandomDocSet {
    fn new(probability: f64, max_doc: DocId, seed: u64) -> Self {
        let probability = probability.clamp(0.0, 1.0);
        if probability == 0.0 || max_doc == 0 {
            return RandomDocSet {
                current_doc: TERMINATED,
                max_doc,
                probability,
                rng: SmallRng::seed_from_u64(seed),
                mode: SamplingMode::Selecting {
                    geo: Geometric::new(1.0).expect("1.0 is valid"),
                },
            };
        }
        let mut rng = SmallRng::seed_from_u64(seed);

        if probability <= 0.5 {
            // Geometric(p): gap = number of docs to skip before the next selected doc.
            let geo = Geometric::new(probability).expect("probability clamped to (0,1]");
            let first_gap: u64 = geo.sample(&mut rng);
            let current_doc = if first_gap < max_doc as u64 {
                first_gap as DocId
            } else {
                TERMINATED
            };
            RandomDocSet {
                current_doc,
                max_doc,
                probability,
                rng,
                mode: SamplingMode::Selecting { geo },
            }
        } else {
            // Geometric(1-p): gap = number of docs to skip before the next *rejected* doc.
            let rejection_prob = 1.0 - probability;
            let geo = Geometric::new(rejection_prob).expect("1-probability clamped to (0,1]");
            let first_gap: u64 = geo.sample(&mut rng);
            let first_rejection = if first_gap < max_doc as u64 {
                first_gap as DocId
            } else {
                TERMINATED
            };
            let (first_doc, next_rejection) =
                advance_past_rejections(0, first_rejection, max_doc, &geo, &mut rng);
            RandomDocSet {
                current_doc: first_doc,
                max_doc,
                probability,
                rng,
                mode: SamplingMode::Rejecting { geo, next_rejection },
            }
        }
    }
}

/// Starting from `candidate`, advances past any consecutive rejections.
///
/// Returns `(first_selected_doc, updated_next_rejection)`.
/// `first_selected_doc` is TERMINATED when the segment is exhausted.
fn advance_past_rejections(
    mut candidate: DocId,
    mut next_rejection: DocId,
    max_doc: DocId,
    geo: &Geometric,
    rng: &mut SmallRng,
) -> (DocId, DocId) {
    while candidate < max_doc && next_rejection != TERMINATED && candidate == next_rejection {
        candidate += 1;
        let gap: u64 = geo.sample(rng);
        next_rejection = if (candidate as u64) + gap < max_doc as u64 {
            candidate + gap as u32
        } else {
            TERMINATED
        };
    }
    let selected = if candidate < max_doc { candidate } else { TERMINATED };
    (selected, next_rejection)
}

impl DocSet for RandomDocSet {
    fn advance(&mut self) -> DocId {
        if self.current_doc == TERMINATED {
            return TERMINATED;
        }

        match &mut self.mode {
            SamplingMode::Selecting { geo } => {
                let gap: u64 = geo.sample(&mut self.rng);
                // gap is failures-before-success (support {0,1,...}), so gap=0 means next doc.
                let next = self.current_doc as u64 + gap + 1;
                self.current_doc = if next < self.max_doc as u64 {
                    next as DocId
                } else {
                    TERMINATED
                };
            }
            SamplingMode::Rejecting { geo, next_rejection } => {
                let candidate = self.current_doc + 1;
                let (new_doc, new_next_rej) =
                    advance_past_rejections(candidate, *next_rejection, self.max_doc, &geo, &mut self.rng);
                self.current_doc = new_doc;
                if let SamplingMode::Rejecting { next_rejection, .. } = &mut self.mode {
                    *next_rejection = new_next_rej;
                }
            }
        }

        self.current_doc
    }

    fn doc(&self) -> DocId {
        self.current_doc
    }

    fn size_hint(&self) -> u32 {
        ((self.max_doc as f64 * self.probability) as u64).min(u32::MAX as u64) as u32
    }
}

impl Scorer for RandomDocSet {
    fn score(&mut self) -> Score {
        1.0
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{DocSet, TERMINATED};

    use super::{RandomDocSet, seed_from_split_id};

    fn collect_docs(mut docset: RandomDocSet) -> Vec<u32> {
        let mut docs = Vec::new();
        let mut doc = docset.doc();
        while doc != TERMINATED {
            docs.push(doc);
            doc = docset.advance();
        }
        docs
    }

    #[test]
    fn test_random_query_determinism() {
        let docs1 = collect_docs(RandomDocSet::new(0.1, 10_000, 42));
        let docs2 = collect_docs(RandomDocSet::new(0.1, 10_000, 42));
        assert_eq!(docs1, docs2);
    }

    #[test]
    fn test_random_query_different_seeds_differ() {
        let docs1 = collect_docs(RandomDocSet::new(0.1, 10_000, 1));
        let docs2 = collect_docs(RandomDocSet::new(0.1, 10_000, 2));
        assert_ne!(docs1, docs2);
    }

    #[test]
    fn test_random_query_selecting_mode_fraction() {
        let docs = collect_docs(RandomDocSet::new(0.1, 100_000, 0));
        let count = docs.len();
        // E[count] = 10_000; allow +/-10% tolerance
        assert!(count > 9_000 && count < 11_000, "count={count}");
    }

    #[test]
    fn test_random_query_rejecting_mode_fraction() {
        let docs = collect_docs(RandomDocSet::new(0.999, 10_000, 0));
        let count = docs.len();
        // E[count] = 9_990; allow +/-1% tolerance
        assert!(count > 9_890 && count < 10_000, "count={count}");
    }

    #[test]
    fn test_random_query_probability_zero() {
        let docset = RandomDocSet::new(0.0, 10_000, 0);
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_random_query_probability_one() {
        let docs = collect_docs(RandomDocSet::new(1.0, 100, 0));
        let expected: Vec<u32> = (0..100).collect();
        assert_eq!(docs, expected);
    }

    #[test]
    fn test_random_query_empty_segment() {
        let docset = RandomDocSet::new(0.5, 0, 0);
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_random_query_docs_in_bounds() {
        let max_doc = 1000u32;
        let docs = collect_docs(RandomDocSet::new(0.3, max_doc, 7));
        for &doc in &docs {
            assert!(doc < max_doc, "doc {doc} >= max_doc {max_doc}");
        }
    }

    #[test]
    fn test_random_query_docs_in_bounds_rejecting_mode() {
        let max_doc = 1000u32;
        let docs = collect_docs(RandomDocSet::new(0.8, max_doc, 7));
        for &doc in &docs {
            assert!(doc < max_doc, "doc {doc} >= max_doc {max_doc}");
        }
    }

    #[test]
    fn test_random_query_docs_strictly_increasing() {
        let docs = collect_docs(RandomDocSet::new(0.5, 10_000, 99));
        for window in docs.windows(2) {
            assert!(window[0] < window[1], "docs not strictly increasing: {:?}", window);
        }
    }

    #[test]
    fn test_seed_from_split_id_deterministic() {
        assert_eq!(seed_from_split_id("split-abc"), seed_from_split_id("split-abc"));
        assert_ne!(seed_from_split_id("split-abc"), seed_from_split_id("split-abd"));
    }
}
