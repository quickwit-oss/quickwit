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

use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use quickwit_common::temp_dir::TempDirectory;
use quickwit_directories::write_hotcache;
use quickwit_doc_mapper::DocMapperBuilder;
use quickwit_query::query_ast::{BoolQuery, CalcFieldQuery, RegexQuery};
use quickwit_storage::{
    DownloadCounters, MockStorageCache, PutPayload, RamStorageBuilder, SplitPayloadBuilder,
    StorageResolver,
};
use tantivy::TantivyDocument;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{FAST, STRING, Schema};

use super::*;

const SPLIT_ID: &str = "predicate-warmup";
type Entries = HashMap<(String, String), (SegmentId, HitSet)>;

#[derive(Default)]
struct TestCache {
    admit: bool,
    entries: Mutex<Entries>,
    writes: AtomicUsize,
}

impl PredicateCache for TestCache {
    fn get(&self, split: String, key: String) -> Option<(SegmentId, HitSet)> {
        self.entries.lock().unwrap().get(&(split, key)).cloned()
    }

    fn put(&self, split: String, key: String, segment: SegmentId, hits: HitSet) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self.admit {
            self.entries
                .lock()
                .unwrap()
                .insert((split, key), (segment, hits));
        }
    }
}

struct Fixture {
    searcher: Searcher,
    predicate_searcher: Searcher,
    byte_cache: ByteRangeCache,
    downloads: Arc<DownloadCounters>,
    body_cache_accesses: Arc<AtomicUsize>,
    mapper: DocMapper,
    _split_cache_dir: TempDirectory,
}

impl Fixture {
    async fn new() -> Self {
        let directory = TempDirectory::for_test();
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", STRING | FAST);
        let json = schema.add_json_field("json", STRING);
        let index = Index::create_in_dir(directory.path(), schema.build()).unwrap();
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 20_000_000)
            .unwrap();
        for values in [vec!["api"], vec!["other"], vec!["other", "api"]] {
            let mut document = TantivyDocument::default();
            for value in &values {
                document.add_text(body, value);
            }
            document.add_object(
                json,
                std::collections::BTreeMap::from([(
                    "path".to_string(),
                    tantivy::schema::OwnedValue::Str(values[0].to_string()),
                )]),
            );
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();
        drop(writer);
        let mut hotcache = Vec::new();
        write_hotcache(
            MmapDirectory::open(directory.path()).unwrap(),
            &mut hotcache,
        )
        .unwrap();
        let mut builder = SplitPayloadBuilder::default();
        for entry in std::fs::read_dir(directory.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                builder.add_payload(
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    Box::new(std::fs::read(path).unwrap()),
                );
            }
        }
        let payload = builder.finalize(&hotcache).unwrap();
        let footer = payload.footer_range.clone();
        let bytes = payload.read_all().await.unwrap();
        let ram = RamStorageBuilder::default()
            .put(&format!("{SPLIT_ID}.split"), bytes.as_slice())
            .build();
        let (storage, downloads) = CountingStorage::instrument_storage(Arc::new(ram));
        let split_cache_dir = TempDirectory::for_test();
        std::fs::write(
            split_cache_dir.path().join(format!("{SPLIT_ID}.split")),
            &bytes,
        )
        .unwrap();
        let mut context = SearcherContext::for_test();
        context.split_cache_opt = Some(
            SearchSplitCache::with_root_path(
                split_cache_dir.path().to_path_buf(),
                StorageResolver::for_test(),
                serde_json::from_value(serde_json::json!({"max_num_bytes": "10MB"})).unwrap(),
            )
            .unwrap(),
        );
        // Observe every lookup and admission in the persistent body cache. The whole-split
        // cache below it is already populated, so ordinary warmup needs no remote body reads.
        let body_cache_accesses = Arc::new(AtomicUsize::new(0));
        let mut body_cache = MockStorageCache::new();
        let accesses = body_cache_accesses.clone();
        body_cache.expect_get().returning(move |_, _| {
            accesses.fetch_add(1, Ordering::Relaxed);
            None
        });
        let accesses = body_cache_accesses.clone();
        body_cache.expect_put().returning(move |_, _, _| {
            accesses.fetch_add(1, Ordering::Relaxed);
        });
        context.fast_fields_cache = Arc::new(body_cache);
        let byte_cache = ByteRangeCache::with_infinite_capacity();
        let (normal, _, predicate) = open_index_with_predicate_directory(
            &context,
            storage,
            &SplitIdAndFooterOffsets {
                split_id: SPLIT_ID.to_string(),
                split_footer_start: footer.start,
                split_footer_end: footer.end,
                ..Default::default()
            },
            None,
            Some(byte_cache.clone()),
        )
        .await
        .unwrap();
        let searcher = normal.reader().unwrap().searcher();
        let predicate_searcher = Index::open(predicate).unwrap().reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(
            downloads.snapshot().1,
            1,
            "both readers share the footer fetch"
        );
        assert_eq!(body_cache_accesses.load(Ordering::Relaxed), 0);
        Self {
            searcher,
            predicate_searcher,
            byte_cache,
            downloads,
            body_cache_accesses,
            mapper: serde_json::from_str::<DocMapperBuilder>("{}")
                .unwrap()
                .try_build()
                .unwrap(),
            _split_cache_dir: split_cache_dir,
        }
    }

    async fn prepare(
        &self,
        ast: QueryAst,
        cache: Arc<TestCache>,
    ) -> Box<dyn tantivy::query::Query> {
        let (_, info, ast) = self
            .mapper
            .query_and_ast(
                self.searcher.schema().clone(),
                ast,
                false,
                Some((cache.clone(), SPLIT_ID.into())),
            )
            .unwrap();
        let WarmupOutcome::Ready(hits) = warmup(
            &self.searcher,
            Some(&self.predicate_searcher),
            &info,
            Priority::default(),
            &|_, _| {},
            cache,
            SPLIT_ID,
        )
        .await
        .unwrap() else {
            panic!("expected prepared hitsets");
        };
        assert!(!hits.is_empty());
        let ast = WarmedHitsInjector(hits).transform(ast).unwrap().unwrap();
        self.mapper
            .query(self.searcher.schema().clone(), ast, false, None)
            .unwrap()
            .0
    }
}

fn regex() -> QueryAst {
    RegexQuery {
        field: "body".into(),
        regex: "api".into(),
    }
    .into()
}

#[tokio::test]
async fn predicate_preparation_bypasses_body_caches_and_retains_hits() {
    for (ast, expected) in [
        (regex(), vec![0, 2]),
        (
            serde_json::from_value(serde_json::json!({
                "type": "wildcard", "field": "body", "value": "a*",
                "lenient": false, "case_insensitive": false,
            }))
            .unwrap(),
            vec![0, 2],
        ),
        (
            RegexQuery {
                field: "json.path".into(),
                regex: "api".into(),
            }
            .into(),
            vec![0],
        ),
        (
            RegexQuery {
                field: "body".into(),
                regex: "missing".into(),
            }
            .into(),
            vec![],
        ),
    ] {
        for admit in [false, true] {
            let fixture = Fixture::new().await;
            let cache = Arc::new(TestCache {
                admit,
                ..Default::default()
            });
            let bytes_before = fixture.byte_cache.get_num_bytes();
            let downloads_before = fixture.downloads.snapshot();
            let query = fixture.prepare(ast.clone(), cache.clone()).await;
            assert!(
                fixture.downloads.snapshot().1 > downloads_before.1,
                "predicate reads bypass even the populated whole-split cache"
            );
            assert_eq!(fixture.byte_cache.get_num_bytes(), bytes_before);
            assert_eq!(fixture.body_cache_accesses.load(Ordering::Relaxed), 0);
            assert_eq!(cache.writes.load(Ordering::Relaxed), 1);
            assert_eq!(cache.entries.lock().unwrap().len(), usize::from(admit));
            let downloads = fixture.downloads.snapshot();
            if admit {
                let (cached_query, info) = fixture
                    .mapper
                    .query(
                        fixture.searcher.schema().clone(),
                        ast.clone(),
                        false,
                        Some((cache.clone(), SPLIT_ID.into())),
                    )
                    .unwrap();
                assert!(info.automatons_grouped_by_field.is_empty());
                assert_eq!(
                    cached_query.count(&fixture.searcher).unwrap(),
                    expected.len()
                );
                assert_eq!(fixture.downloads.snapshot(), downloads);
            }
            cache.entries.lock().unwrap().clear();
            for _ in 0..2 {
                assert_eq!(
                    fixture
                        .searcher
                        .search(&query, &tantivy::collector::DocSetCollector)
                        .unwrap(),
                    expected
                        .iter()
                        .map(|&doc| tantivy::DocAddress::new(0, doc))
                        .collect()
                );
            }
            assert_eq!(fixture.downloads.snapshot(), downloads);
            assert_eq!(fixture.byte_cache.get_num_bytes(), bytes_before);
            assert_eq!(fixture.body_cache_accesses.load(Ordering::Relaxed), 0);
            assert_eq!(cache.writes.load(Ordering::Relaxed), 1);
        }
    }
}

#[tokio::test]
async fn shared_prefilter_retains_ordinary_postings_warmup() {
    let fixture = Fixture::new().await;
    let cache = Arc::new(TestCache::default());
    let ast: QueryAst = BoolQuery {
        filter: vec![
            regex(),
            CalcFieldQuery {
                expression: tantivy::jitexpr::ast::deserialize(
                    r#"(EQ (REGEXP_EXTRACT body "^([a-z]+)$" 1u64) "api")"#,
                )
                .unwrap(),
            }
            .into(),
        ],
        ..Default::default()
    }
    .into();
    let bytes_before = fixture.byte_cache.get_num_bytes();
    let query = fixture.prepare(ast, cache.clone()).await;
    assert!(fixture.byte_cache.get_num_bytes() > bytes_before);
    assert!(fixture.body_cache_accesses.load(Ordering::Relaxed) > 0);
    let downloads = fixture.downloads.snapshot();
    // The regex matches documents 0 and 2. The calculated field checks only the first value.
    assert_eq!(query.count(&fixture.searcher).unwrap(), 1);
    assert_eq!(fixture.downloads.snapshot(), downloads);
    assert_eq!(cache.writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn ordinary_regex_keeps_cached_warmup_for_scored_and_disabled_paths() {
    for scored in [false, true] {
        let fixture = Fixture::new().await;
        // These are the two callers that pass no predicate-cache context in leaf search.
        let (query, info) = fixture
            .mapper
            .query(fixture.searcher.schema().clone(), regex(), false, None)
            .unwrap();
        assert!(
            info.automatons_grouped_by_field
                .values()
                .flat_map(|map| map.values())
                .all(|requirement| requirement.needs_postings
                    && requirement.predicate_keys.is_empty())
        );
        let bytes_before = fixture.byte_cache.get_num_bytes();
        let downloads = fixture.downloads.snapshot();
        let outcome = warmup(
            &fixture.searcher,
            None,
            &info,
            Priority::default(),
            &|_, _| {},
            Arc::new(TestCache::default()),
            SPLIT_ID,
        )
        .await
        .unwrap();
        assert!(matches!(outcome, WarmupOutcome::Ready(hits) if hits.is_empty()));
        assert!(fixture.byte_cache.get_num_bytes() > bytes_before);
        assert!(fixture.body_cache_accesses.load(Ordering::Relaxed) > 0);
        if scored {
            let hits = fixture
                .searcher
                .search(
                    &query,
                    &tantivy::collector::TopDocs::with_limit(3).order_by_score(),
                )
                .unwrap();
            assert_eq!(hits.len(), 2);
            assert!(
                hits.iter()
                    .all(|(score, _)| score.is_finite() && *score > 0.0)
            );
        } else {
            assert_eq!(query.count(&fixture.searcher).unwrap(), 2);
        }
        assert_eq!(
            fixture.downloads.snapshot(),
            downloads,
            "ordinary warmup reads the populated split cache and search reads warmed bytes"
        );
    }
}
