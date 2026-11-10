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

use std::collections::HashSet;
use std::sync::Arc;

use bitpacking::{BitPacker, BitPacker1x};
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
    pub fn new(ast: QueryAst) -> Self {
        CacheNode {
            inner: Box::new(ast),
            state: CacheState::Uninitialized,
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
                let tantivy_query_ast = self.inner.build_tantivy_ast_call(context)?.simplify();
                let mut required_terms = HashSet::new();
                super::required_terms::collect_required_terms(
                    &tantivy_query_ast,
                    context.schema,
                    &mut required_terms,
                );
                let tantivy_query: Box<dyn Query> = tantivy_query_ast.into();
                Ok(CacheFillerQuery {
                    inner_query: Box::new(tantivy_query),
                    cache_filler: cache_filler.clone(),
                    required_terms,
                }
                .into())
            }
        }
    }
}

use tantivy::directory::OwnedBytes;
use tantivy::index::SegmentId;
use tantivy::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TantivyError, Term};

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
        let mut hit_set = self
            .cache_entry
            .for_segment(reader.segment_id())
            .ok_or_else(|| TantivyError::InternalError("Segment not found in cache".to_string()))?;
        hit_set.boost = boost;
        Ok(Box::new(hit_set))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("HitSet", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

#[derive(Clone)]
pub struct CacheEntry {
    segment_id: SegmentId,
    hits: HitSet,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("CacheEntry")
            .field("segment_id", &self.segment_id)
            .finish_non_exhaustive()
    }
}

impl CacheEntry {
    fn for_segment(&self, segment_id: SegmentId) -> Option<HitSet> {
        if segment_id == self.segment_id {
            Some(self.hits.clone())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct HitSet {
    buffer: OwnedBytes,
    buffer_pos: usize,
    previous_last_val: Option<u32>,
    current_block: [u32; BitPacker1x::BLOCK_LEN],
    block_pos: usize,
    boost: Score,
}

const INCOMPLETE_BLOCK_MARKER: u8 = 0x80;

impl HitSet {
    /// An empty hit set, matching no document.
    pub fn empty() -> Self {
        Self::from_buffer(OwnedBytes::new(vec![0, 0, 0, 0]))
    }

    /// Returns true if this hit set matches no document.
    pub fn is_empty(&self) -> bool {
        self.size_hint() == 0
    }

    /// Build a HitSet from its serialized form.
    ///
    /// The provided buffer must come from `HitSet::into_buffer`
    pub fn from_buffer(buffer: OwnedBytes) -> Self {
        let mut this = Self {
            buffer,
            // skip count
            buffer_pos: 4,
            previous_last_val: None,
            current_block: [0; BitPacker1x::BLOCK_LEN],
            // we set this to block_len minus 1 so we can call advance() once to initialize
            // everything
            block_pos: BitPacker1x::BLOCK_LEN - 1,
            boost: 1.0,
        };
        this.advance();
        this
    }

    /// Build a hit set after checking block lengths, markers, and the encoded document count.
    /// Returns `None` for malformed encodings. Document IDs are assumed to be strictly sorted.
    pub fn try_from_buffer(buffer: OwnedBytes) -> Option<Self> {
        let num_docs = u32::from_ne_bytes(buffer.get(..4)?.try_into().ok()?) as usize;
        let mut remaining = &buffer[4..];
        for _ in 0..num_docs / BitPacker1x::BLOCK_LEN {
            let (&num_bits, block) = remaining.split_first()?;
            if num_bits > 32 {
                return None;
            }
            remaining = block.get(BitPacker1x::compressed_block_size(num_bits)..)?;
        }
        let tail_len = num_docs % BitPacker1x::BLOCK_LEN;
        if tail_len > 0 {
            let (&marker, tail) = remaining.split_first()?;
            if marker != INCOMPLETE_BLOCK_MARKER {
                return None;
            }
            remaining = tail.get(tail_len * 4..)?;
        }
        if !remaining.is_empty() {
            return None;
        }
        Some(Self::from_buffer(buffer))
    }

    /// Return a buffer representing the underlying data.
    ///
    /// This does not preserve where in the DocSet you are.
    pub fn into_buffer(self) -> OwnedBytes {
        self.buffer
    }

    fn load_new_block(&mut self) {
        let Some(num_bits) = self.buffer.get(self.buffer_pos) else {
            // we ended iteration: simply fill the current_block full of TERMINATED
            self.current_block = [tantivy::TERMINATED; 32];
            return;
        };
        self.buffer_pos += 1;
        if *num_bits == INCOMPLETE_BLOCK_MARKER {
            // final block, decode as many ids as possible
            let mut i = 0;
            for chunk in self.buffer[self.buffer_pos..].as_chunks().0 {
                self.current_block[i] = u32::from_ne_bytes(*chunk);
                i += 1;
            }
            // pad with TERMINATED
            while i < BitPacker1x::BLOCK_LEN {
                self.current_block[i] = tantivy::TERMINATED;
                i += 1;
            }
            self.buffer_pos = self.buffer.len();
        } else {
            self.buffer_pos += BitPacker1x.decompress_strictly_sorted(
                self.previous_last_val,
                &self.buffer[self.buffer_pos..],
                &mut self.current_block,
                *num_bits,
            );
            self.previous_last_val = self.current_block.last().copied();
        }
    }
}

impl DocSet for HitSet {
    fn advance(&mut self) -> DocId {
        self.block_pos += 1;
        if let Some(doc_id) = self.current_block.get(self.block_pos) {
            return *doc_id;
        }
        self.load_new_block();
        self.block_pos = 0;
        self.current_block[0]
    }

    // fn seek(&mut self, target: DocId) -> DocId {
    // }

    #[inline(always)]
    fn doc(&self) -> DocId {
        self.current_block[self.block_pos]
    }

    fn size_hint(&self) -> u32 {
        u32::from_ne_bytes(self.buffer[0..4].try_into().unwrap())
    }
}

impl Scorer for HitSet {
    fn score(&mut self) -> f32 {
        self.boost
    }
}

pub struct HitSetBuilder {
    count: u32,
    current_block: [u32; BitPacker1x::BLOCK_LEN],
    previous_last_val: Option<u32>,
    buffer: Vec<u8>,
}

impl HitSetBuilder {
    pub fn new() -> Self {
        HitSetBuilder {
            count: 0,
            current_block: [0; BitPacker1x::BLOCK_LEN],
            previous_last_val: None,
            buffer: vec![0; 4],
        }
    }

    fn in_block_pos(&self) -> usize {
        (self.count % BitPacker1x::BLOCK_LEN as u32) as usize
    }

    fn end_of_block(&self) -> bool {
        self.in_block_pos() == (BitPacker1x::BLOCK_LEN - 1)
    }

    fn flush_block(&mut self) {
        let num_bits =
            BitPacker1x.num_bits_strictly_sorted(self.previous_last_val, &self.current_block);
        self.buffer.push(num_bits);
        let current_buffer_pos = self.buffer.len();
        let new_end = current_buffer_pos + (BitPacker1x::BLOCK_LEN * num_bits as usize) / 8;
        self.buffer.resize(new_end, 0);
        BitPacker1x.compress_strictly_sorted(
            self.previous_last_val,
            &self.current_block,
            &mut self.buffer[current_buffer_pos..],
            num_bits,
        );
        self.previous_last_val = self.current_block.last().copied();
    }

    pub fn insert(&mut self, value: u32) {
        self.current_block[self.in_block_pos()] = value;
        if self.end_of_block() {
            self.flush_block();
        }
        self.count += 1;
    }

    pub fn build(mut self) -> HitSet {
        if self.in_block_pos() != 0 {
            self.buffer.push(INCOMPLETE_BLOCK_MARKER);
            for elem in &self.current_block[..self.in_block_pos()] {
                self.buffer.extend_from_slice(&elem.to_ne_bytes());
            }
        }
        // write back the count of items
        self.buffer[0..4].copy_from_slice(&self.count.to_ne_bytes());
        HitSet::from_buffer(OwnedBytes::new(self.buffer))
    }
}

#[derive(Clone)]
pub struct CacheFiller {
    cache: Arc<dyn PredicateCache>,
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
    fn fill_segment(&self, segment_id: SegmentId, value: HitSet) {
        self.cache
            .put(self.split_id.clone(), self.query.clone(), segment_id, value);
    }
}

#[derive(Debug)]
pub struct CacheFillerQuery {
    inner_query: Box<dyn Query>,
    cache_filler: CacheFiller,
    required_terms: HashSet<Term>,
}

impl Clone for CacheFillerQuery {
    fn clone(&self) -> Self {
        Self {
            inner_query: self.inner_query.box_clone(),
            cache_filler: self.cache_filler.clone(),
            required_terms: self.required_terms.clone(),
        }
    }
}

impl CacheFillerQuery {
    pub(crate) fn required_terms(&self) -> &HashSet<Term> {
        &self.required_terms
    }
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
    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.inner_query.query_terms(visitor)
    }
}

/// Weight associated with the `AllQuery` query.
pub struct CacheFillerWeight {
    inner_weight: Box<dyn Weight>,
    cache_filler: CacheFiller,
}

impl Weight for CacheFillerWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let mut hit_set_builder = HitSetBuilder::new();
        let mut scorer = self.inner_weight.scorer(reader, 1.0)?;
        let mut doc_id = scorer.doc();
        while doc_id < tantivy::TERMINATED {
            hit_set_builder.insert(doc_id);
            doc_id = scorer.advance();
        }
        let mut hit_set = hit_set_builder.build();
        self.cache_filler
            .fill_segment(reader.segment_id(), hit_set.clone());
        hit_set.boost = boost;
        Ok(Box::new(hit_set))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.inner_weight.explain(reader, doc)
    }
}

/// Resolves cache nodes asynchronously before synchronous query construction and warmup planning.
/// Uninitialized nodes execute their inner query without caching.
pub struct PredicateCacheInjector {
    pub cache: Arc<dyn PredicateCache>,
    pub split_id: String,
}

impl PredicateCacheInjector {
    pub async fn resolve(&self, query_ast: &mut QueryAst) {
        let mut pending = vec![query_ast];
        let mut lookups = Vec::new();
        while let Some(query_ast) = pending.pop() {
            match query_ast {
                QueryAst::Bool(bool_query) => pending.extend(
                    bool_query
                        .must
                        .iter_mut()
                        .chain(bool_query.should.iter_mut())
                        .chain(bool_query.must_not.iter_mut())
                        .chain(bool_query.filter.iter_mut())
                        .rev(),
                ),
                QueryAst::Boost { underlying, .. } => pending.push(underlying),
                QueryAst::Cache(cache_node) => {
                    if let Ok(query) = serde_json::to_string(&cache_node.inner) {
                        // Borrow only the state so nested cache nodes can be resolved concurrently.
                        let state = &mut cache_node.state;
                        lookups.push(async move {
                            *state = if let Some((segment_id, hits)) =
                                self.cache.get(self.split_id.clone(), query.clone()).await
                            {
                                CacheState::CacheHit(CacheEntry { segment_id, hits })
                            } else {
                                CacheState::CacheMiss(CacheFiller {
                                    cache: self.cache.clone(),
                                    split_id: self.split_id.clone(),
                                    query,
                                })
                            };
                        });
                    }
                    pending.push(&mut cache_node.inner);
                }
                QueryAst::Term(_)
                | QueryAst::TermSet(_)
                | QueryAst::FullText(_)
                | QueryAst::PhrasePrefix(_)
                | QueryAst::Range(_)
                | QueryAst::MatchAll
                | QueryAst::MatchNone
                | QueryAst::UserInput(_)
                | QueryAst::FieldPresence(_)
                | QueryAst::Wildcard(_)
                | QueryAst::Regex(_)
                | QueryAst::CalcField(_) => {}
            }
        }
        futures::future::join_all(lookups).await;
    }
}

// we use a trait to dodge circular dependancies with quickwit-storage
#[async_trait::async_trait]
pub trait PredicateCache: Send + Sync + 'static {
    async fn get(&self, split_id: String, query_ast_json: String) -> Option<(SegmentId, HitSet)>;

    // Called during synchronous query execution on a CPU thread.
    fn put(&self, split_id: String, query_ast_json: String, segment: SegmentId, results: HitSet);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tantivy::DocSet;
    use tantivy::query::Query as TantivyQuery;
    use tantivy::schema::{Schema, TEXT};

    use super::*;
    use crate::query_ast::{
        BoolQuery, BuildTantivyAstContext, QueryAstTransformer, QueryAstVisitor, TermQuery,
    };

    #[async_trait::async_trait]
    impl PredicateCache for Mutex<HashMap<(String, String), (SegmentId, HitSet)>> {
        async fn get(
            &self,
            split_id: String,
            query_ast_json: String,
        ) -> Option<(SegmentId, HitSet)> {
            // Exercise preparation with a lookup that yields before returning.
            tokio::task::yield_now().await;
            self.lock()
                .unwrap()
                .get(&(split_id, query_ast_json))
                .cloned()
        }

        fn put(
            &self,
            split_id: String,
            query_ast_json: String,
            segment: SegmentId,
            results: HitSet,
        ) {
            self.lock()
                .unwrap()
                .insert((split_id, query_ast_json), (segment, results));
        }
    }

    #[track_caller]
    fn test_hit_set_roundtrip_helper<I: Iterator<Item = u32> + Clone>(iter: I) {
        let mut hitset_builder = HitSetBuilder::new();
        for i in iter.clone() {
            hitset_builder.insert(i);
        }
        let mut hitset = HitSet::try_from_buffer(hitset_builder.build().into_buffer()).unwrap();

        for val in iter {
            assert_eq!(hitset.doc(), val);
            hitset.advance();
        }
        for _ in 0..96 {
            assert_eq!(hitset.doc(), tantivy::TERMINATED);
            hitset.advance();
        }
    }

    #[test]
    fn test_hit_set_rejects_malformed_encodings() {
        for len in 0..4 {
            assert!(HitSet::try_from_buffer(OwnedBytes::new(vec![0; len])).is_none());
        }
        for (num_docs, blocks) in [
            // Trailing data in an empty hit set.
            (0u32, vec![0]),
            // Partial blocks: missing data, wrong marker, truncated ID, or extra bytes.
            (1, vec![]),
            (1, vec![0]),
            (1, vec![INCOMPLETE_BLOCK_MARKER, 0, 0, 0]),
            (1, vec![INCOMPLETE_BLOCK_MARKER, 0, 0, 0, 0, 0]),
            (1, [vec![INCOMPLETE_BLOCK_MARKER], vec![0; 132]].concat()),
            // Full blocks: invalid bit width, wrong marker, or truncated packed data.
            (32, vec![33]),
            (32, vec![INCOMPLETE_BLOCK_MARKER]),
            (32, vec![1, 0, 0, 0]),
            // Corruption after a valid first block.
            (33, vec![0, INCOMPLETE_BLOCK_MARKER]),
            (64, vec![0]),
            (64, vec![0, 33]),
        ] {
            let mut buffer = num_docs.to_ne_bytes().to_vec();
            buffer.extend(blocks);
            assert!(HitSet::try_from_buffer(OwnedBytes::new(buffer)).is_none());
        }
    }

    #[test]
    fn test_hit_set_roundtrip() {
        test_hit_set_roundtrip_helper(0..64);
        test_hit_set_roundtrip_helper((u32::MAX - 64)..u32::MAX);
        // this generate a pseurorandom strictrly increasing sequence
        let generator = std::iter::successors(Some(0u32), |x| Some(x + x.trailing_ones() + 1));

        // empty
        test_hit_set_roundtrip_helper(generator.clone().take(0));
        // one item
        test_hit_set_roundtrip_helper(generator.clone().take(1));
        test_hit_set_roundtrip_helper(generator.clone().skip(10).take(1));
        // partial block
        test_hit_set_roundtrip_helper(generator.clone().take(24));
        test_hit_set_roundtrip_helper(generator.clone().skip(10).take(24));

        // one block
        test_hit_set_roundtrip_helper(generator.clone().take(32));
        test_hit_set_roundtrip_helper(generator.clone().skip(10).take(32));
        // two blocks
        test_hit_set_roundtrip_helper(generator.clone().take(64));
        test_hit_set_roundtrip_helper(generator.clone().skip(10).take(64));

        // many blocks, partial last block
        test_hit_set_roundtrip_helper(generator.clone().take(1024 + 6));
        test_hit_set_roundtrip_helper(generator.clone().skip(10).take(1024 + 6));
    }

    #[test]
    fn test_built_tantivy_ast() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let term_query: QueryAst = TermQuery {
            field: "body".to_string(),
            value: "val".to_string(),
        }
        .into();
        let tantivy_term_query: Box<dyn TantivyQuery> = term_query
            .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
            .unwrap()
            .into();

        {
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::Uninitialized,
            };
            let uninit_cache_query: Box<dyn TantivyQuery> = ast
                .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
                .unwrap()
                .into();
            assert_eq!(
                format!("{uninit_cache_query:?}"),
                format!("{tantivy_term_query:?}")
            );
        }

        {
            let cache_entry = CacheEntry {
                segment_id: SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a")
                    .unwrap(),
                hits: HitSet::empty(),
            };
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::CacheHit(cache_entry),
            };
            let cache_hit_query: Box<dyn TantivyQuery> = ast
                .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
                .unwrap()
                .into();

            let debug_query = format!("{cache_hit_query:?}");
            assert!(debug_query.contains("CacheHitQuery"));
            assert!(!debug_query.contains("TermQuery"));
        }
        {
            let cache_filler = CacheFiller {
                cache: Arc::new(Mutex::new(HashMap::new())),
                split_id: "split_id".to_string(),
                query: "{}".to_string(),
            };
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::CacheMiss(cache_filler),
            };
            let cache_miss_query: Box<dyn TantivyQuery> = ast
                .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
                .unwrap()
                .into();

            let debug_query = format!("{cache_miss_query:?}");
            assert!(debug_query.contains("CacheFillerQuery"));
            assert!(debug_query.contains(&format!("{tantivy_term_query:?}")));
        }
    }

    struct FoundATermVisitor(bool);
    impl QueryAstVisitor<'_> for FoundATermVisitor {
        type Err = std::convert::Infallible;
        fn visit_term(&mut self, _term: &TermQuery) -> Result<(), Self::Err> {
            self.0 = true;
            Ok(())
        }
    }

    impl QueryAstTransformer for FoundATermVisitor {
        type Err = std::convert::Infallible;
        fn transform_term(&mut self, term: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
            self.0 = true;
            Ok(Some(term.into()))
        }
    }

    #[test]
    fn test_default_visitor_ignore_cached_node() {
        let term_query: QueryAst = TermQuery {
            field: "body".to_string(),
            value: "val".to_string(),
        }
        .into();
        {
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::Uninitialized,
            }
            .into();

            let mut visitor = FoundATermVisitor(false);
            visitor.visit(&ast).unwrap();
            assert!(visitor.0);
            let mut visitor = FoundATermVisitor(false);
            visitor.transform(ast).unwrap();
            assert!(visitor.0);
        }
        {
            let cache_entry = CacheEntry {
                segment_id: SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a")
                    .unwrap(),
                hits: HitSet::empty(),
            };
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::CacheHit(cache_entry),
            }
            .into();

            let mut visitor = FoundATermVisitor(false);
            visitor.visit(&ast).unwrap();
            assert!(!visitor.0);
            let mut visitor = FoundATermVisitor(false);
            visitor.transform(ast).unwrap();
            assert!(!visitor.0);
        }
        {
            let cache_filler = CacheFiller {
                cache: Arc::new(Mutex::new(HashMap::new())),
                split_id: "split_id".to_string(),
                query: "{}".to_string(),
            };
            let ast = CacheNode {
                inner: Box::new(term_query.clone()),
                state: CacheState::CacheMiss(cache_filler),
            }
            .into();

            let mut visitor = FoundATermVisitor(false);
            visitor.visit(&ast).unwrap();
            assert!(visitor.0);
            let mut visitor = FoundATermVisitor(false);
            visitor.transform(ast).unwrap();
            assert!(visitor.0);
        }
    }

    #[tokio::test]
    async fn test_cache_filler_preserves_required_terms() {
        let mut schema_builder = Schema::builder();
        let body_field = schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let context = BuildTantivyAstContext::for_test(&schema);
        let term_query: QueryAst = TermQuery {
            field: "body".to_string(),
            value: "val".to_string(),
        }
        .into();
        let cache_node = CacheNode::new(term_query);
        let query_json = serde_json::to_string(&cache_node.inner).unwrap();
        let ast: QueryAst = cache_node.into();
        let cache = Arc::new(Mutex::new(HashMap::new()));

        let mut cache_miss_ast = ast.clone();
        PredicateCacheInjector {
            cache: cache.clone(),
            split_id: "split".to_string(),
        }
        .resolve(&mut cache_miss_ast)
        .await;
        let (_query, required_terms) = cache_miss_ast
            .build_tantivy_query_and_required_terms(&context)
            .unwrap();
        assert_eq!(
            required_terms,
            HashSet::from([Term::from_field_text(body_field, "val")])
        );

        cache.put(
            "split".to_string(),
            query_json,
            SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap(),
            HitSet::empty(),
        );
        let mut cache_hit_ast = ast;
        PredicateCacheInjector {
            cache,
            split_id: "split".to_string(),
        }
        .resolve(&mut cache_hit_ast)
        .await;
        let (_query, required_terms) = cache_hit_ast
            .build_tantivy_query_and_required_terms(&context)
            .unwrap();
        assert!(required_terms.is_empty());
    }

    #[tokio::test]
    async fn test_cache_injector_resolves_hits_and_misses() {
        let term_query: QueryAst = TermQuery {
            field: "body".to_string(),
            value: "val".to_string(),
        }
        .into();
        let cache_node = CacheNode {
            inner: Box::new(term_query.clone()),
            state: CacheState::Uninitialized,
        };
        let query_json = serde_json::to_string(&cache_node.inner).unwrap();
        let ast: QueryAst = cache_node.into();
        let cache = Arc::new(Mutex::new(HashMap::new()));
        cache.put(
            "split_2".to_string(),
            query_json,
            SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap(),
            HitSet::empty(),
        );

        {
            let mut filled = ast.clone();
            PredicateCacheInjector {
                cache: cache.clone(),
                split_id: "split_1".to_string(),
            }
            .resolve(&mut filled)
            .await;
            assert!(matches!(
                filled,
                QueryAst::Cache(CacheNode {
                    state: CacheState::CacheMiss(_),
                    ..
                })
            ));
        }

        {
            let mut filled = ast.clone();
            PredicateCacheInjector {
                cache: cache.clone(),
                split_id: "split_2".to_string(),
            }
            .resolve(&mut filled)
            .await;
            assert!(matches!(
                filled,
                QueryAst::Cache(CacheNode {
                    state: CacheState::CacheHit(_),
                    ..
                })
            ));
        }
    }

    #[async_trait::async_trait]
    impl PredicateCache for tokio::sync::Barrier {
        async fn get(&self, _: String, _: String) -> Option<(SegmentId, HitSet)> {
            self.wait().await;
            None
        }

        fn put(&self, _: String, _: String, _: SegmentId, _: HitSet) {
            unreachable!("preparation only reads the cache");
        }
    }

    #[tokio::test]
    async fn test_cache_injector_resolves_concurrently() {
        let cached_all = QueryAst::from(CacheNode::new(QueryAst::MatchAll));
        let mut ast = QueryAst::from(BoolQuery {
            must: vec![CacheNode::new(cached_all.clone()).into(), cached_all],
            ..Default::default()
        });
        let injector = PredicateCacheInjector {
            cache: Arc::new(tokio::sync::Barrier::new(3)),
            split_id: "split".to_string(),
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            injector.resolve(&mut ast),
        )
        .await
        .expect("sibling and nested lookups must run concurrently");
    }

    #[tokio::test]
    async fn test_cache_injector_resolves_nested_nodes() {
        let cached_all = QueryAst::from(CacheNode::new(QueryAst::MatchAll));
        let mut ast = QueryAst::from(BoolQuery {
            must: vec![CacheNode::new(cached_all.clone()).into()],
            should: vec![cached_all.clone().boost(Some(2.0f32.try_into().unwrap()))],
            must_not: vec![cached_all.clone()],
            filter: vec![cached_all],
            ..Default::default()
        });
        let query_json = serde_json::to_string(&ast).unwrap();
        let cache = Arc::new(Mutex::new(HashMap::new()));
        cache.put(
            "split".to_string(),
            serde_json::to_string(&QueryAst::MatchAll).unwrap(),
            SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap(),
            HitSet::empty(),
        );
        PredicateCacheInjector {
            cache,
            split_id: "split".to_string(),
        }
        .resolve(&mut ast)
        .await;
        assert_eq!(serde_json::to_string(&ast).unwrap(), query_json);

        #[derive(Default)]
        struct CacheStates {
            hits: usize,
            misses: usize,
        }
        impl<'a> QueryAstVisitor<'a> for CacheStates {
            type Err = std::convert::Infallible;

            fn visit_cache_node(&mut self, node: &'a CacheNode) -> Result<(), Self::Err> {
                match node.state {
                    CacheState::CacheHit(_) => self.hits += 1,
                    CacheState::CacheMiss(_) => self.misses += 1,
                    CacheState::Uninitialized => panic!("cache node was not resolved"),
                }
                self.visit(&node.inner)
            }
        }
        let mut states = CacheStates::default();
        states.visit(&ast).unwrap();
        assert_eq!(states.hits, 4);
        assert_eq!(states.misses, 1);
    }

    #[test]
    fn test_cache_hit_returns_correct_docs() {
        let mut schema_builder = Schema::builder();
        let host_field = schema_builder.add_text_field("host", TEXT);
        let schema = schema_builder.build();
        let index = tantivy::IndexBuilder::new()
            .schema(schema.clone())
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for count in 1..13 {
            let mut doc = tantivy::TantivyDocument::default();
            doc.add_text(host_field, format!("host_{count}"));
            for _ in 0..count {
                index_writer.add_document(doc.clone()).unwrap();
            }
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_id = searcher.segment_readers()[0].segment_id();

        let generator =
            std::iter::successors(Some(0u32), |x| Some(x + x.trailing_ones() + 1)).take(500);
        let mut hitset_builder = HitSetBuilder::new();
        for i in generator {
            hitset_builder.insert(i);
        }
        let hitset = hitset_builder.build();

        // this query isn't even valid for that split, but that's not relevant as it won't get run
        let term_query: QueryAst = TermQuery {
            field: "body".to_string(),
            value: "val".to_string(),
        }
        .into();
        let cache_entry = CacheEntry {
            segment_id,
            hits: hitset,
        };
        let ast = CacheNode {
            inner: Box::new(term_query.clone()),
            state: CacheState::CacheHit(cache_entry),
        };
        let cache_hit_query: Box<dyn TantivyQuery> = ast
            .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
            .unwrap()
            .into();

        assert_eq!(cache_hit_query.count(&searcher).unwrap(), 500);
    }

    #[tokio::test]
    async fn test_cache_miss_returns_correct_docs_and_fill_cache() {
        let mut schema_builder = Schema::builder();
        let host_field = schema_builder.add_text_field("host", TEXT);
        let schema = schema_builder.build();
        let index = tantivy::IndexBuilder::new()
            .schema(schema.clone())
            .create_in_ram()
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for count in 1..13 {
            let mut doc = tantivy::TantivyDocument::default();
            doc.add_text(host_field, format!("host_{count}"));
            for _ in 0..count {
                index_writer.add_document(doc.clone()).unwrap();
            }
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_id = searcher.segment_readers()[0].segment_id();

        let term_query: QueryAst = TermQuery {
            field: "host".to_string(),
            value: "11".to_string(),
        }
        .into();
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let cache_filler = CacheFiller {
            cache: cache.clone(),
            split_id: "split_id".to_string(),
            query: "{some_query}".to_string(),
        };
        let ast = CacheNode {
            inner: Box::new(term_query.clone()),
            state: CacheState::CacheMiss(cache_filler),
        };
        let cache_hit_query: Box<dyn TantivyQuery> = ast
            .build_tantivy_ast_impl(&BuildTantivyAstContext::for_test(&schema))
            .unwrap()
            .into();

        assert_eq!(cache_hit_query.count(&searcher).unwrap(), 11);
        let mut cache_entry = cache
            .get("split_id".to_string(), "{some_query}".to_string())
            .await
            .unwrap();
        assert_eq!(cache_entry.0, segment_id);
        let expected = (10 * 11 / 2)..(11 * 12 / 2);
        for doc_id in expected {
            assert_eq!(cache_entry.1.doc(), doc_id);
            cache_entry.1.advance();
        }
    }
}
