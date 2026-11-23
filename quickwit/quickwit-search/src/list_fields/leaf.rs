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

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use futures::stream::{self, StreamExt, TryStreamExt};
use quickwit_common::shared_consts::{FIELD_PRESENCE_FIELD_NAME, SPLIT_FIELDS_FILE_NAME};
use quickwit_common::uri::Uri;
use quickwit_proto::search::{
    ListFieldsEntry, ListFieldsMetadata, ListFieldsResponse, SplitIdAndFooterOffsets,
};
use quickwit_proto::types::IndexId;
use quickwit_storage::{AsyncDebouncer, OwnedBytes, Storage};
use tracing::instrument;

use crate::leaf::open_split_bundle;
use crate::list_fields::patterns::FieldPatterns;
use crate::list_fields::split_counts::SplitCountsByField;
use crate::list_fields::{field_list_size_limit, sort_and_dedup};
use crate::search_thread_pool;
use crate::service::SearcherContext;

const DYNAMIC_FIELD_PREFIX: &str = "_dynamic.";

/// Cap on the number of per-split fields-metadata downloads in flight at once within a single
/// `leaf_list_fields` call.
const MAX_CONCURRENT_DOWNLOADS: usize = 500;

/// A decoded split's field list can be much larger than its compressed metadata blob.
const MAX_CONCURRENT_DECODES: usize = 4;

/// Key identifying equivalent concurrent [`leaf_list_fields`] requests.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeafListFieldsKey {
    index_id: IndexId,
    index_uri: Uri,
    field_patterns: Vec<String>,
    split_ids: Vec<String>,
    limit: usize,
}

impl LeafListFieldsKey {
    fn new(
        index_id: &IndexId,
        index_uri: &Uri,
        field_patterns: &[String],
        split_footers: &[SplitIdAndFooterOffsets],
        limit: usize,
    ) -> Self {
        let mut field_patterns = field_patterns.to_vec();
        field_patterns.sort_unstable();
        field_patterns.dedup();
        let mut split_ids: Vec<String> = split_footers
            .iter()
            .map(|split_footer| split_footer.split_id.clone())
            .collect();
        split_ids.sort_unstable();
        Self {
            index_id: index_id.clone(),
            index_uri: index_uri.clone(),
            field_patterns,
            split_ids,
            limit,
        }
    }
}

/// Debounces identical concurrent [`leaf_list_fields`] requests so parallel callers share one
/// execution.
pub type ListFieldsDebouncer = AsyncDebouncer<LeafListFieldsKey, crate::Result<ListFieldsResponse>>;

/// `leaf` step of list fields.
///
/// Returns field metadata from the assigned splits.
#[instrument(skip_all, fields(index_id = %index_id, num_splits = split_footers.len()))]
pub async fn leaf_list_fields(
    index_id: IndexId,
    field_patterns_strs: &[String],
    split_footers: Vec<SplitIdAndFooterOffsets>,
    limit: Option<u32>,
    searcher_ctx: Arc<SearcherContext>,
    storage: Arc<dyn Storage>,
) -> crate::Result<ListFieldsResponse> {
    if split_footers.is_empty() {
        return Ok(ListFieldsResponse::default());
    }
    let field_patterns = FieldPatterns::from_strs(field_patterns_strs)?;
    let limit = field_list_size_limit(limit);
    if limit == 0 {
        return Ok(ListFieldsResponse::default());
    }
    let key = LeafListFieldsKey::new(
        &index_id,
        storage.uri(),
        field_patterns_strs,
        &split_footers,
        limit,
    );
    searcher_ctx
        .list_fields_debouncer
        .get_or_create(key, || {
            let searcher_ctx = searcher_ctx.clone();
            execute_leaf_list_fields(
                index_id,
                field_patterns,
                split_footers,
                limit,
                searcher_ctx,
                storage,
            )
        })
        .await
}

async fn execute_leaf_list_fields(
    index_id: IndexId,
    field_patterns: FieldPatterns,
    split_footers: Vec<SplitIdAndFooterOffsets>,
    limit: usize,
    searcher_ctx: Arc<SearcherContext>,
    storage: Arc<dyn Storage>,
) -> crate::Result<ListFieldsResponse> {
    let mut fields_by_split = stream::iter(split_footers)
        .map(async |split_footer| {
            let bytes = get_fields_metadata(&split_footer, &searcher_ctx, storage.clone()).await?;
            anyhow::Ok((split_footer.split_id, bytes))
        })
        .buffer_unordered(MAX_CONCURRENT_DOWNLOADS)
        .map_ok(async |(split_id, bytes)| {
            let patterns = field_patterns.clone();
            search_thread_pool()
                .run_cpu_intensive(move || deserialize_matching_fields(bytes, &split_id, &patterns))
                .await
                .context("failed to decode split fields")?
        })
        .try_buffer_unordered(MAX_CONCURRENT_DECODES);

    let mut split_counts = SplitCountsByField::default();
    while let Some(fields) = fields_by_split.try_next().await? {
        split_counts = search_thread_pool()
            .run_cpu_intensive(move || split_counts.count_split(fields, limit))
            .await
            .context("failed to count splits containing each field")?;
        if split_counts.len() == limit {
            break;
        }
    }
    drop(fields_by_split);
    let entries = search_thread_pool()
        .run_cpu_intensive(move || split_counts.into_entries(&index_id))
        .await
        .context("failed to build list fields response")?;
    Ok(ListFieldsResponse { entries })
}

/// Gets the raw serialized fields metadata blob for a single split, either from the cache or,
/// on miss, from storage (populating the cache as a side effect). The cache stores the bytes
/// exactly as fetched (proto + zstd).
#[instrument(skip_all, fields(split_id = %split_footer.split_id))]
async fn get_fields_metadata(
    split_footer: &SplitIdAndFooterOffsets,
    searcher_ctx: &SearcherContext,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<OwnedBytes> {
    if let Some(serialized_entries) = searcher_ctx.list_fields_cache.get(&split_footer.split_id) {
        return Ok(serialized_entries);
    }
    let serialized_entries = fetch_fields_metadata(searcher_ctx, storage, split_footer).await?;

    searcher_ctx
        .list_fields_cache
        .put(split_footer.split_id.clone(), serialized_entries.clone());

    Ok(serialized_entries)
}

/// Downloads the serialized fields metadata blob for one split.
#[instrument(skip_all, fields(split_id = %split_footer.split_id))]
async fn fetch_fields_metadata(
    searcher_ctx: &SearcherContext,
    storage: Arc<dyn Storage>,
    split_footer: &SplitIdAndFooterOffsets,
) -> anyhow::Result<OwnedBytes> {
    let (_, split_bundle) = open_split_bundle(searcher_ctx, storage, split_footer).await?;
    let serialized_entries = split_bundle
        .get_all(Path::new(SPLIT_FIELDS_FILE_NAME))
        .await
        .with_context(|| {
            format!(
                "failed to fetch fields metadata from split `{}`",
                split_footer.split_id
            )
        })?;
    Ok(serialized_entries)
}

/// Normalize field names before matching patterns. Return one entry per (name, type) in the split.
#[instrument(skip_all, fields(split_id))]
fn deserialize_matching_fields(
    serialized_entries: OwnedBytes,
    split_id: &str,
    field_patterns: &FieldPatterns,
) -> anyhow::Result<Vec<ListFieldsEntry>> {
    let mut entries = ListFieldsMetadata::deserialize(serialized_entries)
        .with_context(|| format!("failed to deserialize fields metadata for split `{split_id}`"))?
        .entries;

    entries.retain_mut(|list_field_entry| {
        if list_field_entry.field_name == FIELD_PRESENCE_FIELD_NAME {
            return false;
        }
        if list_field_entry
            .field_name
            .starts_with(DYNAMIC_FIELD_PREFIX)
        {
            list_field_entry
                .field_name
                .drain(..DYNAMIC_FIELD_PREFIX.len());
        }
        true
    });
    // Removing the dynamic prefix may have broken the original ordering, so we re-sort and
    // defensively dedup.
    sort_and_dedup(&mut entries);
    if !field_patterns.is_empty() {
        entries.retain(|entry| field_patterns.matches_any(&entry.field_name));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::ListFieldsType;

    use super::*;

    fn entry(field_name: &str) -> ListFieldsEntry {
        entry_typed(field_name, ListFieldsType::Str)
    }

    fn entry_typed(field_name: &str, field_type: ListFieldsType) -> ListFieldsEntry {
        ListFieldsEntry {
            field_name: field_name.to_string(),
            field_type: field_type as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: Vec::new(),
            num_splits: 0,
        }
    }

    fn serialize(entries: Vec<ListFieldsEntry>) -> OwnedBytes {
        OwnedBytes::new(ListFieldsMetadata { entries }.serialize())
    }

    fn matching_fields(entries: Vec<ListFieldsEntry>, patterns: &[String]) -> Vec<ListFieldsEntry> {
        let patterns = FieldPatterns::from_strs(patterns).unwrap();
        deserialize_matching_fields(serialize(entries), "test-split", &patterns).unwrap()
    }

    #[test]
    fn deserialize_fields_metadata_strips_field_presence() {
        let fields = matching_fields(
            vec![entry(FIELD_PRESENCE_FIELD_NAME), entry("user.name")],
            &[],
        );
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_name, "user.name");
    }

    #[test]
    fn deserialize_fields_metadata_removes_dynamic_prefix() {
        // The serialized blob is sorted by (name, type); `_dynamic.foo` < `user.name`.
        let fields = matching_fields(vec![entry("user.name"), entry("_dynamic.foo")], &[]);
        let field_names: Vec<&str> = fields
            .iter()
            .map(|field| field.field_name.as_str())
            .collect();
        assert_eq!(field_names, ["foo", "user.name"]);
    }

    #[tokio::test]
    async fn leaf_list_fields_counts_all_splits_below_limit() {
        let searcher = Arc::new(SearcherContext::for_test());
        let mut splits = Vec::new();
        for i in 0..24 {
            let mut common = entry("_dynamic.match.z");
            common.searchable = i % 2 == 0;
            common.aggregatable = false;
            let mut other = if i % 2 == 0 {
                entry_typed("match.z", ListFieldsType::U64)
            } else {
                entry("match.a")
            };
            other.aggregatable = i % 3 == 0;
            let split_id = format!("split-{i}");
            let bytes = serialize(vec![entry(&format!("ignored.{i}")), common, other]);
            searcher.list_fields_cache.put(split_id.clone(), bytes);
            splits.push(SplitIdAndFooterOffsets {
                split_id,
                ..Default::default()
            });
        }
        let response = leaf_list_fields(
            "index".into(),
            &["match.*".into()],
            splits,
            Some(4),
            searcher,
            Arc::new(quickwit_storage::RamStorage::default()),
        )
        .await
        .unwrap();
        let mut first = entry("match.a");
        first.num_splits = 12;
        first.index_ids = vec!["index".into()];
        first.non_aggregatable_index_ids = first.index_ids.clone();
        let mut second = entry("match.z");
        second.num_splits = 24;
        second.index_ids = first.index_ids.clone();
        second.non_searchable_index_ids = first.index_ids.clone();
        second.aggregatable = false;
        let mut third = entry_typed("match.z", ListFieldsType::U64);
        third.num_splits = 12;
        third.index_ids = first.index_ids.clone();
        third.non_aggregatable_index_ids = first.index_ids.clone();
        assert_eq!(response.entries, [first, second, third]);
    }

    #[tokio::test]
    async fn leaf_list_fields_stops_at_limit() {
        let searcher = Arc::new(SearcherContext::for_test());
        let mut splits = Vec::new();
        for split_id in ["one", "two"] {
            let fields = vec![entry("a"), entry("b"), entry("c")];
            searcher
                .list_fields_cache
                .put(split_id.into(), serialize(fields));
            splits.push(SplitIdAndFooterOffsets {
                split_id: split_id.into(),
                ..Default::default()
            });
        }
        let response = leaf_list_fields(
            "index".into(),
            &[],
            splits,
            Some(2),
            searcher,
            Arc::new(quickwit_storage::RamStorage::default()),
        )
        .await
        .unwrap();
        // Whichever split finishes first fills the response by itself.
        let fields: Vec<_> = response
            .entries
            .iter()
            .map(|field| (field.field_name.as_str(), field.num_splits))
            .collect();
        assert_eq!(fields, [("a", 1), ("b", 1)]);
    }

    #[test]
    fn counts_retained_fields_through_end_of_last_split() {
        let mut last_z = entry("z");
        last_z.searchable = false;
        let fields = SplitCountsByField::default()
            .count_split(vec![entry("z")], 2)
            .count_split(vec![entry("a"), entry("b"), last_z], 2)
            .into_entries("index");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field_name, "a");
        assert_eq!(fields[1].field_name, "z");
        assert_eq!(fields[1].num_splits, 2);
        assert_eq!(fields[1].non_searchable_index_ids, ["index"]);
    }

    #[tokio::test]
    async fn leaf_list_fields_skips_storage_for_zero_limit() {
        let response = leaf_list_fields(
            "index".into(),
            &[],
            vec![SplitIdAndFooterOffsets {
                split_id: "missing".into(),
                ..Default::default()
            }],
            Some(0),
            Arc::new(SearcherContext::for_test()),
            Arc::new(quickwit_storage::RamStorage::default()),
        )
        .await
        .unwrap();
        assert!(response.entries.is_empty());
    }

    #[test]
    fn deserialize_fields_metadata_dedups_after_prefix_rewrite() {
        // Both `_dynamic.foo` and `foo` collapse to `foo` after stripping the prefix.
        let fields = matching_fields(vec![entry("_dynamic.foo"), entry("foo")], &[]);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_name, "foo");
    }

    #[test]
    fn deserialize_fields_metadata_dedups_by_name_and_type() {
        // Same field name with different types must NOT be deduped; the sort key is (name, type).
        let fields = matching_fields(
            vec![
                entry_typed("status", ListFieldsType::U64),
                entry_typed("status", ListFieldsType::Bool),
                entry("user.name"),
                entry_typed("status", ListFieldsType::Str),
            ],
            &[],
        );
        let field_pairs: Vec<(&str, i32)> = fields
            .iter()
            .map(|field| (field.field_name.as_str(), field.field_type))
            .collect();
        assert_eq!(
            field_pairs,
            [
                ("status", ListFieldsType::Str as i32),
                ("status", ListFieldsType::U64 as i32),
                ("status", ListFieldsType::Bool as i32),
                ("user.name", ListFieldsType::Str as i32),
            ]
        );
    }

    #[test]
    fn deserialize_fields_metadata_sorts_by_name_then_type() {
        // `_dynamic.b` collapses to `b`, so after the rewrite we should see, in order:
        // (`a`, Str), (`b`, Str), (`b`, U64). Build the input out of name-sort order to
        // make sure the post-rewrite re-sort is actually doing the work.
        let fields = matching_fields(
            vec![
                entry_typed("_dynamic.b", ListFieldsType::U64),
                entry_typed("_dynamic.b", ListFieldsType::Str),
                entry_typed("a", ListFieldsType::Str),
            ],
            &[],
        );
        let field_pairs: Vec<(&str, i32)> = fields
            .iter()
            .map(|field| (field.field_name.as_str(), field.field_type))
            .collect();
        assert_eq!(
            field_pairs,
            [
                ("a", ListFieldsType::Str as i32),
                ("b", ListFieldsType::Str as i32),
                ("b", ListFieldsType::U64 as i32),
            ]
        );
    }

    #[test]
    fn filter_fields_metadata_returns_all_when_empty_patterns() {
        let fields = vec![entry("a"), entry("b")];
        assert_eq!(matching_fields(fields.clone(), &[]), fields);
    }

    #[test]
    fn filter_fields_metadata_keeps_only_matching_fields() {
        let fields = vec![
            entry("_dynamic.service.name"),
            entry("user.name"),
            entry("service.id"),
        ];
        assert_eq!(
            matching_fields(fields, &["service.*".into()]),
            [entry("service.id"), entry("service.name")]
        );
    }

    async fn build_test_split(
        split_id: &str,
        fields: Vec<ListFieldsEntry>,
    ) -> (SplitIdAndFooterOffsets, OwnedBytes) {
        let serialized = ListFieldsMetadata { entries: fields }.serialize();
        let split_payload =
            quickwit_storage::SplitPayloadBuilder::get_split_payload(&[], &serialized, None, &[])
                .unwrap();
        let footer_range = split_payload.footer_range.clone();
        let ram_storage = quickwit_storage::RamStorage::default();
        let path = Path::new(&format!("{split_id}.split")).to_path_buf();
        ram_storage
            .put(&path, Box::new(split_payload))
            .await
            .unwrap();
        let bytes = ram_storage.get_all(&path).await.unwrap();
        let footer = SplitIdAndFooterOffsets {
            split_id: split_id.to_string(),
            split_footer_start: footer_range.start,
            split_footer_end: footer_range.end,
            num_docs: 1,
            ..Default::default()
        };
        (footer, bytes)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leaf_list_fields_debounces_identical_concurrent_requests() {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (split_one, bytes_one) =
            build_test_split("split-1", vec![entry("a"), entry("b")]).await;
        let (split_two, bytes_two) =
            build_test_split("split-2", vec![entry("b"), entry("c")]).await;
        let split_files: HashMap<String, OwnedBytes> = HashMap::from([
            ("split-1.split".to_string(), bytes_one),
            ("split-2.split".to_string(), bytes_two),
        ]);

        let get_slice_calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started_tx = std::sync::Mutex::new(Some(started_tx));
        let release_rx = std::sync::Mutex::new(release_rx);

        let mut mock_storage = quickwit_storage::MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///debounce-test"));
        {
            let get_slice_calls = get_slice_calls.clone();
            mock_storage
                .expect_get_slice()
                .returning(move |path, range| {
                    if get_slice_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        if let Some(tx) = started_tx.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                        tokio::task::block_in_place(|| {
                            let _ = release_rx.lock().unwrap().recv();
                        });
                    }
                    let file_name = path.to_str().unwrap();
                    Ok(split_files[file_name].slice(range))
                });
        }
        let storage: Arc<dyn Storage> = Arc::new(mock_storage);
        let searcher = Arc::new(SearcherContext::for_test());

        let first_task = {
            let searcher = searcher.clone();
            let storage = storage.clone();
            let splits = vec![split_one.clone(), split_two.clone()];
            tokio::spawn(async move {
                leaf_list_fields(
                    "index".into(),
                    &["b*".into(), "a*".into()],
                    splits,
                    Some(10),
                    searcher,
                    storage,
                )
                .await
            })
        };

        started_rx.await.unwrap();

        // Concurrent callers with reordered splits and reordered/duplicated patterns normalize to
        // the same key and join the in-flight execution.
        let mut joined_tasks = Vec::new();
        for _ in 0..5 {
            let searcher = searcher.clone();
            let storage = storage.clone();
            let splits = vec![split_two.clone(), split_one.clone()];
            joined_tasks.push(tokio::spawn(async move {
                leaf_list_fields(
                    "index".into(),
                    &["a*".into(), "b*".into(), "a*".into()],
                    splits,
                    Some(10),
                    searcher,
                    storage,
                )
                .await
            }));
        }

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(searcher.list_fields_debouncer.len(), 1);
        assert!(get_slice_calls.load(Ordering::SeqCst) <= 3);

        release_tx.send(()).unwrap();

        let first_response = first_task.await.unwrap().unwrap();
        let field_names: Vec<&str> = first_response
            .entries
            .iter()
            .map(|field| field.field_name.as_str())
            .collect();
        assert_eq!(field_names, ["a", "b"]);

        for task in joined_tasks {
            assert_eq!(task.await.unwrap().unwrap(), first_response);
        }
        // 2 splits × (footer + split_fields) = 4 reads total across all 6 concurrent callers.
        assert_eq!(get_slice_calls.load(Ordering::SeqCst), 4);
    }
}
