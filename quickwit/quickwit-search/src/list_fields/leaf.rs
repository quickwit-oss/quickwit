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
use quickwit_proto::search::{
    ListFieldsEntry, ListFieldsMetadata, ListFieldsResponse, SplitIdAndFooterOffsets,
};
use quickwit_proto::types::{IndexId, SplitId};
use quickwit_storage::{OwnedBytes, Storage};
use tracing::{Span, instrument};

use crate::leaf::open_split_bundle;
use crate::list_fields::patterns::FieldPatterns;
use crate::list_fields::{merge_entries, sort_and_dedup};
use crate::search_thread_pool;
use crate::service::SearcherContext;

const DYNAMIC_FIELD_PREFIX: &str = "_dynamic.";

/// Cap on the number of per-split fields-metadata downloads in flight at once within a single
/// `leaf_list_fields` call.
const MAX_CONCURRENT_DOWNLOADS: usize = 500;

/// `leaf` step of list fields.
///
/// Returns field metadata from the assigned splits.
#[instrument(skip_all, fields(index_id = %index_id, num_splits = split_footers.len()))]
pub async fn leaf_list_fields(
    index_id: IndexId,
    field_patterns_strs: &[String],
    split_footers: Vec<SplitIdAndFooterOffsets>,
    searcher_ctx: Arc<SearcherContext>,
    storage: Arc<dyn Storage>,
) -> crate::Result<ListFieldsResponse> {
    if split_footers.is_empty() {
        return Ok(ListFieldsResponse::default());
    }
    let field_patterns = FieldPatterns::from_strs(field_patterns_strs)?;

    let all_entries: Vec<Vec<ListFieldsEntry>> = get_and_process_fields_metadata(
        &index_id,
        &field_patterns,
        split_footers,
        searcher_ctx,
        storage,
    )
    .await?;

    let merged_entries: Vec<ListFieldsEntry> = merge_fields_metadata(all_entries).await?;

    let response = ListFieldsResponse {
        entries: merged_entries,
    };
    Ok(response)
}

/// Two-stage pipeline: download blobs with bounded I/O concurrency, then hand each one off to the
/// CPU pool as soon as it's ready. The CPU stage is left unbounded here because `run_cpu_intensive`
/// already serializes work through the search thread pool.
async fn get_and_process_fields_metadata(
    index_id: &str,
    field_patterns: &FieldPatterns,
    split_footers: Vec<SplitIdAndFooterOffsets>,
    searcher_ctx: Arc<SearcherContext>,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<Vec<Vec<ListFieldsEntry>>> {
    stream::iter(split_footers)
        .map(|split_footer| {
            let searcher_ctx = searcher_ctx.clone();
            let storage = storage.clone();
            async move {
                let serialized_entries: OwnedBytes =
                    get_fields_metadata(&split_footer, &searcher_ctx, storage).await?;
                anyhow::Ok((split_footer.split_id, serialized_entries))
            }
        })
        .buffer_unordered(MAX_CONCURRENT_DOWNLOADS)
        .map_ok(|(split_id, serialized_entries)| {
            process_fields_metadata(
                serialized_entries,
                index_id.to_string(),
                split_id,
                field_patterns.clone(),
            )
        })
        // `usize::MAX` is fine here: the upstream `buffer_unordered` already bounds the number of
        // items that can have completed and be waiting for CPU work to `MAX_CONCURRENT_DOWNLOADS`,
        // and `run_cpu_intensive` inside `process_fields_metadata` queues onto the search thread
        // pool, so the effective CPU parallelism is the pool size — not this value.
        .try_buffer_unordered(usize::MAX)
        .try_collect()
        .await
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

/// Dispatches deserialization + filtering onto the CPU-intensive thread pool. With one task per
/// split fanning in here, the pool's workers process splits in parallel.
async fn process_fields_metadata(
    serialized_entries: OwnedBytes,
    index_id: IndexId,
    split_id: SplitId,
    field_patterns: FieldPatterns,
) -> anyhow::Result<Vec<ListFieldsEntry>> {
    let parent_span = Span::current();
    search_thread_pool()
        .run_cpu_intensive(move || {
            parent_span.in_scope(|| {
                let entries: Vec<ListFieldsEntry> =
                    deserialize_fields_metadata(serialized_entries, &index_id, &split_id)?;
                let filtered_entries = filter_fields_metadata(entries, &field_patterns);
                Ok(filtered_entries)
            })
        })
        .await
        .context("failed to deserialize and filter fields metadata")?
}

/// Deserialize the fields metadata blob, strip the field presence entry,
/// rewrite dynamic field names, and enforce the strictly-sorted-by-(name, type) and deduplication
/// invariants.
#[instrument(skip_all, fields(split_id))]
fn deserialize_fields_metadata(
    serialized_entries: OwnedBytes,
    index_id: &str,
    split_id: &str,
) -> anyhow::Result<Vec<ListFieldsEntry>> {
    let mut entries = ListFieldsMetadata::deserialize(serialized_entries)
        .with_context(|| format!("failed to deserialize fields metadata for split `{split_id}`"))?
        .entries;

    entries.retain_mut(|list_field_entry| {
        if list_field_entry.field_name == FIELD_PRESENCE_FIELD_NAME {
            return false;
        }
        list_field_entry.index_ids = vec![index_id.to_string()];

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
    Ok(entries)
}

/// Filters the list of fields against the given patterns. Order is preserved, so the
/// strictly-sorted-by-(name, type) invariant is maintained. Patterns match against the
/// user-visible field names (i.e. after `DYNAMIC_FIELD_PREFIX` has been stripped).
#[instrument(skip_all)]
fn filter_fields_metadata(
    list_fields_entries: Vec<ListFieldsEntry>,
    field_patterns: &FieldPatterns,
) -> Vec<ListFieldsEntry> {
    if field_patterns.is_empty() {
        return list_fields_entries;
    }
    list_fields_entries
        .into_iter()
        .filter(|entry| field_patterns.matches_any(&entry.field_name))
        .collect()
}

#[instrument(skip_all, fields(num_splits = all_entries.len()))]
async fn merge_fields_metadata(
    all_entries: Vec<Vec<ListFieldsEntry>>,
) -> crate::Result<Vec<ListFieldsEntry>> {
    let parent_span = Span::current();
    search_thread_pool()
        .run_cpu_intensive(move || parent_span.in_scope(|| merge_entries(all_entries)))
        .await
        .context("failed to merge single split list fields")?
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
        }
    }

    fn serialize(entries: Vec<ListFieldsEntry>) -> OwnedBytes {
        OwnedBytes::new(ListFieldsMetadata { entries }.serialize())
    }

    #[test]
    fn deserialize_fields_metadata_strips_field_presence() {
        let bytes = serialize(vec![entry(FIELD_PRESENCE_FIELD_NAME), entry("user.name")]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_name, "user.name");
    }

    #[test]
    fn deserialize_fields_metadata_removes_dynamic_prefix() {
        // The serialized blob is sorted by (name, type); `_dynamic.foo` < `user.name`.
        let bytes = serialize(vec![entry("user.name"), entry("_dynamic.foo")]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
        let field_names: Vec<&str> = fields
            .iter()
            .map(|field| field.field_name.as_str())
            .collect();
        assert_eq!(field_names, ["foo", "user.name"]);
    }

    #[test]
    fn deserialize_fields_metadata_inserts_index_id() {
        let bytes = serialize(vec![entry("user.name")]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
        assert_eq!(fields[0].index_ids, ["test-index"]);
    }

    #[test]
    fn deserialize_fields_metadata_dedups_after_prefix_rewrite() {
        // Both `_dynamic.foo` and `foo` collapse to `foo` after stripping the prefix.
        let bytes = serialize(vec![entry("_dynamic.foo"), entry("foo")]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_name, "foo");
    }

    #[test]
    fn deserialize_fields_metadata_dedups_by_name_and_type() {
        // Same field name with different types must NOT be deduped; the sort key is (name, type).
        let bytes = serialize(vec![
            entry_typed("status", ListFieldsType::U64),
            entry_typed("status", ListFieldsType::Bool),
            entry("user.name"),
            entry_typed("status", ListFieldsType::Str),
        ]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
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
        let bytes = serialize(vec![
            entry_typed("_dynamic.b", ListFieldsType::U64),
            entry_typed("_dynamic.b", ListFieldsType::Str),
            entry_typed("a", ListFieldsType::Str),
        ]);
        let fields = deserialize_fields_metadata(bytes, "test-index", "test-split").unwrap();
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
        let patterns = FieldPatterns::from_strs(&[]).unwrap();
        let filtered = filter_fields_metadata(fields.clone(), &patterns);
        assert_eq!(filtered, fields);
    }

    #[test]
    fn filter_fields_metadata_keeps_only_matching_fields() {
        let fields = vec![
            entry("service.name"),
            entry("user.name"),
            entry("service.id"),
        ];
        let patterns = FieldPatterns::from_strs(&["service.*".to_string()]).unwrap();
        let filtered = filter_fields_metadata(fields, &patterns);
        let field_names: Vec<&str> = filtered
            .iter()
            .map(|field| field.field_name.as_str())
            .collect();
        assert_eq!(field_names, ["service.name", "service.id"]);
    }
}
