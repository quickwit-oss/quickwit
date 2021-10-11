// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::iter::FromIterator;
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Mailbox, QueueCapacity, SyncActor};
use quickwit_common::split_file;
use quickwit_directories::{BundleDirectory, UnionDirectory};
use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::SplitMetadata;
use tantivy::directory::{DirectoryClone, MmapDirectory, RamDirectory};
use tantivy::fastfield::{DynamicFastFieldReader, FastFieldReader, FastValue};
use tantivy::{
    demux, DemuxMapping, Directory, DocIdToSegmentOrdinal, Index, IndexMeta, SegmentId,
    SegmentReader, TantivyError,
};
use tracing::{debug, info};

use crate::merge_policy::MergeOperation;
use crate::models::{IndexedSplit, MergeScratch, ScratchDirectory};
use crate::new_split_id;

pub struct MergeExecutor {
    index_id: String,
    merge_packager_mailbox: Mailbox<IndexedSplit>,
    demux_field_name: Option<String>,
}

impl Actor for MergeExecutor {
    type Message = MergeScratch;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }
}

fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta> {
    let mut union_index_meta = index_metas.pop().with_context(|| "Only one IndexMeta")?;
    for index_meta in index_metas {
        union_index_meta.segments.extend(index_meta.segments);
    }
    Ok(union_index_meta)
}

fn open_split_directories(
    // Directory containing the splits to merge
    split_path: &Path,
    // Splits metadata
    split_ids: &[String],
) -> anyhow::Result<(IndexMeta, Vec<Box<dyn Directory>>)> {
    let mmap_directory = MmapDirectory::open(split_path)?;
    let mut directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas = Vec::new();
    for split_id in split_ids {
        let split_filename = split_file(split_id);
        let split_fileslice = mmap_directory.open_read(Path::new(&split_filename))?;
        let split_directory = BundleDirectory::open_split(split_fileslice)?;
        directories.push(split_directory.box_clone());
        let index_meta = Index::open(split_directory)?.load_metas()?;
        index_metas.push(index_meta);
    }
    let union_index_meta = combine_index_meta(index_metas)?;
    Ok((union_index_meta, directories))
}

/// Creates a directory with a single `meta.json` file describe in `index_meta`
fn create_shadowing_meta_json_directory(index_meta: IndexMeta) -> anyhow::Result<RamDirectory> {
    let union_index_meta_json = serde_json::to_string_pretty(&index_meta)?;
    let ram_directory = RamDirectory::default();
    ram_directory.atomic_write(Path::new("meta.json"), union_index_meta_json.as_bytes())?;
    Ok(ram_directory)
}

impl SyncActor for MergeExecutor {
    fn process_message(
        &mut self,
        merge_scratch: MergeScratch,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match merge_scratch.merge_operation {
            MergeOperation::Merge {
                merge_split_id: split_id,
                splits,
            } => {
                self.process_merge(
                    split_id,
                    splits,
                    merge_scratch.merge_scratch_directory,
                    merge_scratch.downloaded_splits_directory,
                    ctx,
                )?;
            }
            MergeOperation::Demux { splits } => {
                self.process_demux(
                    splits,
                    merge_scratch.merge_scratch_directory,
                    merge_scratch.downloaded_splits_directory,
                    ctx,
                )?;
            }
        }
        Ok(())
    }
}

fn merge_time_range(splits: &[SplitMetadata]) -> Option<RangeInclusive<i64>> {
    splits
        .iter()
        .flat_map(|split| split.time_range.clone())
        .flat_map(|time_range| vec![*time_range.start(), *time_range.end()].into_iter())
        .minmax()
        .into_option()
        .map(|(min_timestamp, max_timestamp)| min_timestamp..=max_timestamp)
}

fn sum_doc_sizes_in_bytes(splits: &[SplitMetadata]) -> u64 {
    splits.iter().map(|split| split.size_in_bytes).sum::<u64>()
}

fn sum_num_docs(splits: &[SplitMetadata]) -> u64 {
    splits.iter().map(|split| split.num_records as u64).sum()
}

fn merge_all_segments(index: &Index) -> anyhow::Result<()> {
    let segment_ids: Vec<SegmentId> = index
        .searchable_segment_metas()?
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect();
    if segment_ids.len() <= 1 {
        return Ok(());
    }
    debug!(segment_ids=?segment_ids,"merging-segments");
    let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    // TODO it would be nice if tantivy could let us run the merge in the current thread.
    futures::executor::block_on(index_writer.merge(&segment_ids))?;
    Ok(())
}

fn merge_split_directories(
    union_index_meta: IndexMeta,
    split_directories: Vec<Box<dyn Directory>>,
    output_path: &Path,
) -> anyhow::Result<MmapDirectory> {
    let shadowing_meta_json_directory = create_shadowing_meta_json_directory(union_index_meta)?;
    // This directory is here to receive the merged split, as well as the final meta.json file.
    let output_directory = MmapDirectory::open(output_path)?;
    let mut directory_stack: Vec<Box<dyn Directory>> = vec![
        output_directory.box_clone(),
        Box::new(shadowing_meta_json_directory),
    ];
    directory_stack.extend(split_directories.into_iter());
    let union_directory = UnionDirectory::union_of(directory_stack);
    let union_index = Index::open(union_directory)?;
    merge_all_segments(&union_index)?;
    Ok(output_directory)
}

impl MergeExecutor {
    pub fn new(index_id: String, merge_packager_mailbox: Mailbox<IndexedSplit>) -> Self {
        MergeExecutor {
            index_id,
            merge_packager_mailbox,
            demux_field_name: None,
        }
    }

    fn process_merge(
        &mut self,
        split_merge_id: String,
        splits: Vec<SplitMetadata>,
        merge_scratch_directory: ScratchDirectory,
        downloaded_splits_directory: ScratchDirectory,
        ctx: &ActorContext<MergeScratch>,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        info!(split_merge_id=%split_merge_id, "merge-start");
        let replaced_split_ids: Vec<String> =
            splits.iter().map(|split| split.split_id.clone()).collect();
        let (union_index_meta, split_directories) =
            open_split_directories(downloaded_splits_directory.path(), &replaced_split_ids)?;
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        let merged_directory = {
            let _protected_zone_guard = ctx.protect_zone();
            merge_split_directories(
                union_index_meta,
                split_directories,
                merge_scratch_directory.path(),
            )?
        };
        // This will have the side effect of deleting the directory containing the downloaded
        // splits.
        let time_range = merge_time_range(&splits);
        let docs_size_in_bytes = sum_doc_sizes_in_bytes(&splits);
        let num_docs = sum_num_docs(&splits);

        let merged_index = Index::open(merged_directory)?;
        let index_writer = merged_index.writer_with_num_threads(1, 3_000_000)?;
        info!(split_merge_id=%split_merge_id, elapsed_secs=start.elapsed().as_secs_f32(), "merge-stop");
        let indexed_split = IndexedSplit {
            split_id: split_merge_id,
            index_id: self.index_id.clone(),
            replaced_split_ids,

            time_range,
            num_docs,
            docs_size_in_bytes,
            // start_time is not very interesting here.
            split_date_of_birth: Instant::now(),
            checkpoint_delta: CheckpointDelta::default(), //< TODO fixme
            index: merged_index,
            index_writer,
            split_scratch_directory: merge_scratch_directory,
        };
        ctx.send_message_blocking(&self.merge_packager_mailbox, indexed_split)?;
        Ok(())
    }

    fn process_demux(
        &mut self,
        splits: Vec<SplitMetadata>,
        merge_scratch_directory: ScratchDirectory,
        downloaded_splits_directory: ScratchDirectory,
        _ctx: &ActorContext<MergeScratch>,
    ) -> anyhow::Result<()> {
        let _start = Instant::now();
        info!("demux-start");
        let num_splits = splits.len();
        let replaced_split_ids = splits.iter().map(|split| split.split_id.clone());
        let mut index_metas = Vec::new();
        let mmap_directory = MmapDirectory::open(downloaded_splits_directory.path())?;
        let demux_field_name = self
            .demux_field_name
            .clone()
            .expect("Process demux cannot be called without a demux field.");
        let mut split_with_all_docs = VirtualSplit::new(HashMap::new());
        let mut fast_field_values_vec = Vec::new();
        let mut replaced_segments = Vec::new();
        for split_id in replaced_split_ids.into_iter() {
            let split_filename = split_file(&split_id);
            let split_fileslice = mmap_directory.open_read(Path::new(&split_filename))?;
            let split_directory = BundleDirectory::open_split(split_fileslice)?;
            let index = Index::open(split_directory)?;
            index_metas.push(index.load_metas()?);
            let searchable_segments = index.searchable_segments()?;
            let segment = searchable_segments
                .into_iter()
                .next()
                .expect("Split must have at least one segment.");
            let segment_reader = SegmentReader::open(&segment)?;
            replaced_segments.push(segment);
            let num_docs = segment_reader.num_docs() as usize;
            let reader = make_fast_field_reader::<i64>(&segment_reader, &demux_field_name)?;
            let mut fast_field_values: Vec<i64> = Vec::new();
            fast_field_values.reserve_exact(num_docs);
            reader.get_range(0, &mut fast_field_values);
            for fast_field_value in fast_field_values.iter() {
                split_with_all_docs.add_docs(*fast_field_value, 1);
            }
            fast_field_values_vec.push(fast_field_values);
        }
        // TODO: set min / max as executor parameters.
        let demuxed_splits_num_docs_by_value =
            demux_values(split_with_all_docs, 10_000_000, 20_000_000, num_splits);
        let demux_mapping =
            build_demux_mapping(demuxed_splits_num_docs_by_value, fast_field_values_vec);
        let demuxed_scratched_directories: Vec<ScratchDirectory> = (0..num_splits)
            .map(|_| merge_scratch_directory.temp_child())
            .try_collect()?;
        let demuxed_split_directories: Vec<MmapDirectory> = demuxed_scratched_directories
            .iter()
            .map(|directory| MmapDirectory::open(directory.path()))
            .try_collect()?;
        let union_index_meta = combine_index_meta(index_metas)?;
        let indexes = demux(
            &replaced_segments,
            &demux_mapping,
            union_index_meta.index_settings,
            demuxed_split_directories,
        )?;

        // TODO: add log with time.
        // TODO: Send a demux operation.
        for (index, scratched_directory) in indexes
            .into_iter()
            .zip(demuxed_scratched_directories.into_iter())
        {
            // TODO: get time range and num_docs, docs_size_in_bytes
            let index_writer = index.writer_with_num_threads(1, 3_000_000)?;
            let _indexed_split = IndexedSplit {
                split_id: new_split_id(),
                index_id: self.index_id.clone(),
                replaced_split_ids: vec![],
                time_range: None,
                num_docs: 0,
                docs_size_in_bytes: 0,
                // start_time is not very interesting here.
                split_date_of_birth: Instant::now(),
                checkpoint_delta: CheckpointDelta::default(), //< TODO fixme
                index,
                index_writer,
                split_scratch_directory: scratched_directory,
            };
            // ctx.send_message_blocking(&self.merge_packager_mailbox, indexed_split)?
        }
        Ok(())
    }
}

struct SegmentNumDocs {
    ordinal: u32,
    num_docs: usize,
}

pub fn build_demux_mapping(
    split_maps: Vec<VirtualSplit>,
    fast_field_values_vec: Vec<Vec<i64>>,
) -> DemuxMapping {
    let mut num_docs_by_demux_value = HashMap::new();
    for (ordinal, split_map) in split_maps.iter().enumerate() {
        for (demux_value, &num_docs) in split_map.0.iter() {
            num_docs_by_demux_value
                .entry(*demux_value)
                .or_insert_with(Vec::new)
                .push(SegmentNumDocs {
                    ordinal: ordinal as u32,
                    num_docs,
                });
        }
    }
    let mut doc_id_to_segment_ordinals = fast_field_values_vec
        .iter()
        .cloned()
        .map(|values| DocIdToSegmentOrdinal::with_max_doc(values.len()))
        .collect_vec();

    for ordinal in 0..fast_field_values_vec.len() {
        let num_docs = fast_field_values_vec[ordinal].len();
        for doc_id in 0..num_docs {
            let demux_value = fast_field_values_vec[ordinal][doc_id];
            let segment_num_docs_vec = num_docs_by_demux_value
                .get_mut(&demux_value)
                .expect("Demux value must be present.");
            segment_num_docs_vec[0].num_docs -= 1;
            doc_id_to_segment_ordinals[ordinal].set(doc_id as u32, segment_num_docs_vec[0].ordinal);
        }
    }

    let mut mapping = DemuxMapping::default();
    for doc_id_to_segment_ord in doc_id_to_segment_ordinals.into_iter() {
        mapping.add(doc_id_to_segment_ord);
    }
    mapping
}

#[derive(Debug)]
pub struct VirtualSplit(HashMap<i64, usize>);

impl VirtualSplit {
    pub fn new(map: HashMap<i64, usize>) -> Self {
        Self(map)
    }
    pub fn total_num_docs(&self) -> usize {
        self.0.values().sum()
    }

    pub fn sorted_demux_values(&self) -> Vec<i64> {
        self.0.keys().cloned().sorted().collect_vec()
    }

    pub fn remove_docs(&mut self, demux_value: &i64, num_docs: usize) {
        *self.0.get_mut(demux_value).expect("msg") -= num_docs;
    }

    pub fn add_docs(&mut self, demux_value: i64, num_docs: usize) {
        *self.0.entry(demux_value).or_insert(0) += num_docs;
    }

    pub fn num_docs(&self, demux_value: &i64) -> &usize {
        self.0.get(demux_value).unwrap_or(&0usize)
    }
}

/// Naive demuxing of a virtual split into `output_num_splits` virtual splits,
/// a virtual split being defined by a map of (demux value, number of docs).
/// The naive demuxing creates groups of demux values following the 
/// [`Next-Fit` bin packing logic](https://en.wikipedia.org/wiki/Next-fit_bin_packing)
/// AND such that following constraints are satisfied:
/// - exactly `output_num_splits` splits are created
/// - each produced split satifies the constraints [min_split_num_docs, max_split_num_docs]
/// The algorithm logic follows these steps:
/// - Open a virtual split with 0 docs.
/// - Iterate on each demux value in natural order and put corresponding num docs in the open split
///   with the following rules:
///     - If `open split num docs + demux value num docs <= split_upper_bound`, docs are added to
///       the split.
///     - If `open split num docs + demux value num docs > `split_upper_bound`, we put all the
///       docs we can in the open split, the remaining docs will be added in the next one.
///     - If after adding docs, the current split num docs >= `split_lower_bound`, the split is
///       closed and we open a new split.
///
/// The split bounds must be carefully chosen to ensure the contraints [min_split_num_docs,
/// max_split_num_docs] are satisfied for all splits. To ensure that, it is sufficient to define
/// these bounds starting from the last split and backpropagating them until the first split as
/// follows:
/// - We know the last split `n` must have num docs in [min_split_num_docs, max_split_num_docs]. To
///   ensure that, it is sufficient to have remaining_num_docs in [min_split_num_docs,
///   max_split_num_docs] when we open this last split.q
/// - When opening the `n - 1` split, we must put at least `max(min_split_num_docs,
///   remaining_num_docs - max_split_num_docs)` docs in order to have less than `max_split_num_docs`
///   in the `n` split. Similarly, we must put at most `min(max_split_num_docs, remaining_num_docs -
///   min_split_num_docs)` in order to have more than `min_split_num_docs` in the `n` split.
/// - ...
/// - On the first split, we need to have bounds between [max(min_split_num_docs, total_num_docs -
///   (n-1) * max_split_num_docs), min(max_split_num_docs, total_num_docs - (n-1) *
///   min_split_num_docs)].
pub fn demux_values(
    mut input_split: VirtualSplit,
    min_split_num_docs: usize,
    max_split_num_docs: usize,
    output_num_splits: usize,
) -> Vec<VirtualSplit> {
    let total_num_docs = input_split.total_num_docs();
    assert!(
        max_split_num_docs * output_num_splits >= total_num_docs,
        "Input split num docs must be `<= max_split_num_docs * output_num_splits`."
    );
    assert!(
        min_split_num_docs * output_num_splits <= total_num_docs,
        "Input split num docs must be `>= min_split_num_docs * output_num_splits`."
    );
    let demux_values = input_split.sorted_demux_values();
    let mut splits = Vec::new();
    let mut current_split = VirtualSplit::new(HashMap::new());
    let mut num_docs_split_bounds = compute_split_bounds(
        total_num_docs,
        output_num_splits,
        min_split_num_docs,
        max_split_num_docs,
    );
    for demux_value in demux_values.into_iter() {
        while *input_split.num_docs(&demux_value) > 0 {
            let num_docs_to_add = if current_split.total_num_docs()
                + *input_split.num_docs(&demux_value)
                <= *num_docs_split_bounds.end()
            {
                *input_split.num_docs(&demux_value)
            } else {
                num_docs_split_bounds.end() - current_split.total_num_docs()
            };
            current_split.add_docs(demux_value, num_docs_to_add);
            input_split.remove_docs(&demux_value, num_docs_to_add);
            if current_split.total_num_docs() >= *num_docs_split_bounds.start() {
                splits.push(VirtualSplit::new(HashMap::from_iter(
                    current_split.0.drain(),
                )));
                num_docs_split_bounds = compute_split_bounds(
                    input_split.total_num_docs(),
                    output_num_splits - splits.len(),
                    min_split_num_docs,
                    max_split_num_docs,
                );
            }
        }
    }
    assert!(
        splits
            .iter()
            .map(|split| split.total_num_docs())
            .sum::<usize>()
            == total_num_docs,
        "Demuxing must keep the same number of docs."
    );
    assert!(
        splits
            .iter()
            .map(|split| split.total_num_docs())
            .min()
            .unwrap_or(min_split_num_docs)
            >= min_split_num_docs,
        "Demuxing must satisfy the min contraint on split num docs."
    );
    assert!(
        splits
            .iter()
            .map(|split| split.total_num_docs())
            .max()
            .unwrap_or(max_split_num_docs)
            <= max_split_num_docs,
        "Demuxing must satisfy the max contraint on split num docs."
    );
    assert!(
        splits.len() == output_num_splits,
        "Demuxing must return exactly the requested output splits number."
    );
    splits
}

/// Compute num docs bounds for the current split that is going to be filled.
pub fn compute_split_bounds(
    remaining_num_docs: usize,
    remaining_num_splits: usize,
    min_split_num_docs: usize,
    max_split_num_docs: usize,
) -> RangeInclusive<usize> {
    if remaining_num_splits == 0 {
        return RangeInclusive::new(min_split_num_docs, max_split_num_docs);
    }
    let num_docs_lower_bound =
        if remaining_num_docs > (remaining_num_splits - 1) * max_split_num_docs {
            std::cmp::max(
                min_split_num_docs,
                remaining_num_docs - (remaining_num_splits - 1) * max_split_num_docs,
            )
        } else {
            min_split_num_docs
        };
    let num_docs_upper_bound = std::cmp::min(
        max_split_num_docs,
        remaining_num_docs - (remaining_num_splits - 1) * min_split_num_docs,
    );
    assert!(
        num_docs_lower_bound <= num_docs_upper_bound,
        "num docs lower bound must be <= num docs upper bound."
    );
    RangeInclusive::new(num_docs_lower_bound, num_docs_upper_bound)
}

// TODO: refactor that as such a function is already present in quickwit-search.
pub fn make_fast_field_reader<T: FastValue>(
    segment_reader: &SegmentReader,
    fast_field_to_collect: &str,
) -> tantivy::Result<DynamicFastFieldReader<T>> {
    let field = segment_reader
        .schema()
        .get_field(fast_field_to_collect)
        .ok_or_else(|| TantivyError::SchemaError("field does not exist".to_owned()))?;
    let fast_field_slice = segment_reader.fast_fields().fast_field_data(field, 0)?;
    DynamicFastFieldReader::open(fast_field_slice)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::sample::select;
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_index_config::DefaultIndexConfigBuilder;
    use quickwit_metastore::SplitMetadata;

    use super::*;
    use crate::merge_policy::MergeOperation;
    use crate::models::ScratchDirectory;
    use crate::TestSandbox;

    #[tokio::test]
    async fn test_merge_executor() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "tag_fields": [],
            "field_mappings": [
                { "name": "body", "type": "text" },
                { "name": "ts", "type": "i64", "fast": true }
            ]
        }"#;
        let index_config =
            Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
        let index_id = "test-index";
        let test_index_builder = TestSandbox::create(index_id, index_config).await?;
        for split_id in 0..4 {
            let docs = vec![
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713 + split_id }),
            ];
            test_index_builder.add_documents(docs).await?;
        }
        let metastore = test_index_builder.metastore();
        let splits_with_footer_offsets = metastore.list_all_splits(index_id).await?;
        let splits: Vec<SplitMetadata> = splits_with_footer_offsets
            .into_iter()
            .map(|split_and_footer_offsets| split_and_footer_offsets.split_metadata)
            .collect();
        assert_eq!(splits.len(), 4);
        let merge_scratch_directory = ScratchDirectory::try_new_temp()?;
        let downloaded_splits_directory = merge_scratch_directory.temp_child()?;
        let storage = test_index_builder.index_storage(index_id)?;
        for split in &splits {
            let split_filename = split_file(&split.split_id);
            let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
            storage
                .copy_to_file(Path::new(&split_filename), &dest_filepath)
                .await?;
        }
        let merge_scratch = MergeScratch {
            merge_operation: MergeOperation::Merge {
                merge_split_id: crate::new_split_id(),
                splits,
            },
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let merge_executor = MergeExecutor::new(index_id.to_string(), merge_packager_mailbox);
        let universe = Universe::new();
        let (merge_executor_mailbox, merge_executor_handle) =
            universe.spawn_actor(merge_executor).spawn_sync();
        universe
            .send_message(&merge_executor_mailbox, merge_scratch)
            .await?;
        merge_executor_handle.process_pending_and_observe().await;
        let mut packager_msgs = merge_packager_inbox.drain_available_message_for_test();
        assert_eq!(packager_msgs.len(), 1);
        let packager_msg = packager_msgs.pop().unwrap();
        assert_eq!(packager_msg.num_docs, 4);
        assert_eq!(packager_msg.docs_size_in_bytes, 136);

        let reader = packager_msg.index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Ok(())
    }

    #[test]
    fn test_demux_with_same_num_docs() {
        let mut num_docs_map = HashMap::new();
        num_docs_map.insert(0, 100);
        num_docs_map.insert(1, 100);
        num_docs_map.insert(2, 100);
        let splits = demux_values(VirtualSplit::new(num_docs_map), 100, 200, 3);

        assert_eq!(splits.len(), 3);
        assert_eq!(*splits[0].num_docs(&0), 100);
        assert_eq!(*splits[1].num_docs(&1), 100);
        assert_eq!(*splits[2].num_docs(&2), 100);
    }

    #[test]
    fn test_demux_distribution_with_huge_diff_in_num_docs() {
        let mut num_docs_map = HashMap::new();
        num_docs_map.insert(0, 1);
        num_docs_map.insert(1, 200);
        num_docs_map.insert(2, 200);
        num_docs_map.insert(3, 1);
        let splits = demux_values(VirtualSplit::new(num_docs_map), 100, 200, 3);

        assert_eq!(splits.len(), 3);
        assert_eq!(*splits[0].num_docs(&0), 1);
        assert_eq!(*splits[0].num_docs(&1), 199);
        assert_eq!(*splits[1].num_docs(&1), 1);
        assert_eq!(*splits[1].num_docs(&2), 101);
        assert_eq!(*splits[2].num_docs(&2), 99);
        assert_eq!(*splits[2].num_docs(&3), 1);
    }

    #[test]
    fn test_demux_not_cutting_tenants_docs_into_two_splits_thanks_nice_min_max() {
        let mut num_docs_map = HashMap::new();
        num_docs_map.insert(0, 1);
        num_docs_map.insert(1, 50);
        num_docs_map.insert(2, 75);
        num_docs_map.insert(3, 100);
        num_docs_map.insert(4, 50);
        num_docs_map.insert(5, 150);
        let splits = demux_values(VirtualSplit::new(num_docs_map), 100, 200, 3);

        assert_eq!(splits.len(), 3);
        assert_eq!(*splits[0].num_docs(&0), 1);
        assert_eq!(*splits[0].num_docs(&1), 50);
        assert_eq!(*splits[0].num_docs(&2), 75);
        assert_eq!(*splits[1].num_docs(&3), 100);
        assert_eq!(*splits[2].num_docs(&4), 50);
        assert_eq!(*splits[2].num_docs(&5), 150);
    }

    #[test]
    fn test_demux_should_not_cut_tenant_with_small_tenants_with_same_num_docs() {
        let mut num_docs_map = HashMap::new();
        for i in 0..1000 {
            num_docs_map.insert(i as i64, 20_001);
        }
        let splits = demux_values(VirtualSplit::new(num_docs_map), 10_000_000, 20_000_000, 2);
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].total_num_docs(), 10_000_500);
        assert_eq!(splits[1].total_num_docs(), 10_000_500);
    }

    #[test]
    #[should_panic(
        expected = "Input split num docs must be `<= max_split_num_docs * output_num_splits`."
    )]
    fn test_demux_should_panic_when_one_split_has_too_many_docs() {
        let mut num_docs_map = HashMap::new();
        num_docs_map.insert(0, 1);
        num_docs_map.insert(1, 201);
        num_docs_map.insert(2, 201);
        num_docs_map.insert(3, 201);
        demux_values(VirtualSplit::new(num_docs_map), 100, 200, 3);
    }

    use proptest::prelude::*;

    fn proptest_config() -> ProptestConfig {
        let mut proptest_config = ProptestConfig::with_cases(20);
        proptest_config.max_shrink_iters = 600;
        proptest_config
    }

    proptest! {
        #![proptest_config(proptest_config())]
        #[test]
        fn test_proptest_simulate_demux_with_huge_tenants(tenants_num_docs in proptest::collection::vec(select(&[10_001, 100_001, 1_000_001, 10_000_001, 19_999_999, 20_000_000][..]), 1..1000)) {
            let num_splits_out = tenants_num_docs.iter().sum::<usize>() / 20_000_000 + 1;
            let mut num_docs_map = HashMap::new();
            for (i, num_docs) in tenants_num_docs.iter().enumerate() {
                num_docs_map.insert(i as i64, *num_docs);
            }
            let splits = demux_values(VirtualSplit::new(num_docs_map), 10_000_000, 20_000_000, num_splits_out);
            assert_eq!(splits.len(), num_splits_out);
        }
    }
}
