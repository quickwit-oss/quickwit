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

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{BuildTantivyAst, BuildTantivyAstContext, TantivyQueryAst};
use crate::InvalidQuery;
use crate::query_ast::QueryAst;

/// A node caching the result of an inner query.
///
/// This can be used when it's known that some sub-ast might appear in many queries,
/// or that the same query might be run, with various aggregations.
///
/// /!\ Sprinkling this everywhere can lead to performance degradations: the whole posting
/// list of the underlying query will need to be evaluated to build the cache, whereas it could
/// have been largely skipped if some other part of the query is very selective.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheNode {
    pub inner: Box<QueryAst>,
    #[serde(skip)]
    pub state: CacheState,
}

#[derive(Default, Clone)]
pub enum CacheState {
    // This is the state a CacheNode should be before
    #[default]
    Uninitialized,
    CacheHit(CacheEntry),
    CacheMiss(CacheFiller),
}

type Cache = Arc<
    std::sync::Mutex<
        std::collections::HashMap<(String, String), Vec<(SegmentId, tantivy_common::BitSet)>>,
    >,
>;

impl std::fmt::Debug for CacheState {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheState::Uninitialized => fmt.debug_tuple("Uninitialized").finish(),
            CacheState::CacheHit(_) => fmt.debug_tuple("CacheHit").finish_non_exhaustive(),
            CacheState::CacheMiss(_) => fmt.debug_tuple("CacheMiss").finish_non_exhaustive(),
        }
    }
}

// cache state shouldn't impact a CacheNode equality
impl Eq for CacheNode {}
impl PartialEq for CacheNode {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl From<CacheNode> for QueryAst {
    fn from(cache_node: CacheNode) -> Self {
        QueryAst::Cache(cache_node)
    }
}

impl CacheNode {
    pub fn fill_cache_state(&mut self, cache: &Cache, split_id: String) {
        let Ok(query) = serde_json::to_string(&self.inner) else {
            return;
        };
        if let Some(entries) = cache
            .lock()
            .unwrap()
            .get(&(split_id.clone(), query.clone()))
        {
            self.state = CacheState::CacheHit(CacheEntry {
                entries: entries.clone(),
            });
        } else {
            self.state = CacheState::CacheMiss(CacheFiller {
                cache: cache.clone(),
                split_id,
                query,
            });
        }
    }
}

impl BuildTantivyAst for CacheNode {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        match &self.state {
            CacheState::Uninitialized => self.inner.build_tantivy_ast_call(context),
            CacheState::CacheHit(cache_entry) => Ok(CacheHitQuery {
                cache_entry: cache_entry.clone(),
            }
            .into()),
            CacheState::CacheMiss(cache_filler) => {
                let tantivy_query: Box<dyn Query> = self
                    .inner
                    .build_tantivy_ast_call(context)?
                    .simplify()
                    .into();
                Ok(CacheFillerQuery {
                    inner_query: Arc::new(tantivy_query),
                    cache_filler: cache_filler.clone(),
                }
                .into())
            }
        }
    }
}

use tantivy::index::SegmentId;
use tantivy::query::{BitSetDocSet, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TantivyError};

#[derive(Clone, Debug)]
pub struct CacheHitQuery {
    cache_entry: CacheEntry,
}

impl Query for CacheHitQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        if enable_scoring.is_scoring_enabled() {
            Err(tantivy::TantivyError::InternalError(
                "Predicate cache doesn't support scoring yet".to_string(),
            ))
        } else {
            Ok(Box::new(CacheHitWeight {
                cache_entry: self.cache_entry.clone(),
            }))
        }
    }
}

/// Weight associated with the `AllQuery` query.
pub struct CacheHitWeight {
    cache_entry: CacheEntry,
}

impl Weight for CacheHitWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        // we could try to run the query if for some reason we don't actually find an entry in
        // cache, but that would have required loading stuff during warmup which we skipped.
        // An error is the best we can do
        let bitset = self
            .cache_entry
            .for_segment(reader.segment_id())
            .ok_or_else(|| TantivyError::InternalError("Segment not found in cache".to_string()))?;
        let scorer = CacheHitScorer {
            bitset: bitset.into(),
            boost,
        };
        Ok(Box::new(scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("CacheHitScorer", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

pub struct CacheHitScorer {
    bitset: BitSetDocSet,
    boost: Score,
}

impl DocSet for CacheHitScorer {
    fn advance(&mut self) -> DocId {
        self.bitset.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.bitset.seek(target)
    }

    fn doc(&self) -> DocId {
        self.bitset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.bitset.size_hint()
    }
}

impl Scorer for CacheHitScorer {
    fn score(&mut self) -> f32 {
        self.boost
    }
}

#[derive(Clone)]
pub struct CacheEntry {
    // TODO we probably want to use something more compact than a bitset in the future
    entries: Vec<(SegmentId, tantivy_common::BitSet)>,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_map()
            .entries(self.entries.iter().map(|entry| (&entry.0, "<..>")))
            .finish()
    }
}

impl CacheEntry {
    fn for_segment(&self, segment_id: SegmentId) -> Option<tantivy_common::BitSet> {
        self.entries
            .iter()
            .find(|entry| entry.0 == segment_id)
            .map(|entry| entry.1.clone())
    }
}

#[derive(Clone)]
pub struct CacheFiller {
    cache: Cache,
    split_id: String,
    query: String,
}

impl std::fmt::Debug for CacheFiller {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("CacheFiller")
            .field("split_id", &self.split_id)
            .finish_non_exhaustive()
    }
}

impl CacheFiller {
    fn fill_segment(&self, segment_id: SegmentId, value: tantivy_common::BitSet) {
        let mut cache = self.cache.lock().unwrap();
        let entry = cache
            .entry((self.split_id.clone(), self.query.clone()))
            .or_default();
        if !entry
            .iter()
            .any(|(entry_segment, _)| *entry_segment == segment_id)
        {
            entry.push((segment_id, value));
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheFillerQuery {
    inner_query: Arc<dyn Query>,
    cache_filler: CacheFiller,
}

impl Query for CacheFillerQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        if enable_scoring.is_scoring_enabled() {
            Err(tantivy::TantivyError::InternalError(
                "Predicate cache doesn't support scoring yet".to_string(),
            ))
        } else {
            Ok(Box::new(CacheFillerWeight {
                inner_weight: self.inner_query.weight(enable_scoring)?,
                cache_filler: self.cache_filler.clone(),
            }))
        }
    }
}

/// Weight associated with the `AllQuery` query.
pub struct CacheFillerWeight {
    inner_weight: Box<dyn Weight>,
    cache_filler: CacheFiller,
}

impl Weight for CacheFillerWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let mut bitset = tantivy_common::BitSet::with_max_value(reader.max_doc());
        let mut scorer = self.inner_weight.scorer(reader, boost)?;
        while let doc_id @ 0..tantivy::TERMINATED = scorer.advance() {
            bitset.insert(doc_id);
        }
        self.cache_filler
            .fill_segment(reader.segment_id(), bitset.clone());
        CacheHitScorer {
            bitset: bitset.into(),
            boost,
        };
        Ok(Box::new(scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.inner_weight.explain(reader, doc)
    }
}
