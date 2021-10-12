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
    demux, DemuxMapping, Directory, DocIdToSegmentOrdinal, Index, IndexMeta, Segment, SegmentId,
    SegmentReader, TantivyError,
};
use tracing::{debug, info};

use crate::merge_policy::MergeOperation;
use crate::models::{IndexedSplit, IndexedSplitBatch, MergeScratch, ScratchDirectory};
use crate::new_split_id;

pub struct MergeExecutor {
    index_id: String,
    merge_packager_mailbox: Mailbox<IndexedSplitBatch>,
    min_split_num_docs: usize,
    max_split_num_docs: usize,
    timestamp_field_name: Option<String>,
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
    pub fn new(
        index_id: String,
        merge_packager_mailbox: Mailbox<IndexedSplitBatch>,
        demux_field_name: Option<String>,
        timestamp_field_name: Option<String>,
    ) -> Self {
        MergeExecutor {
            index_id,
            merge_packager_mailbox,
            min_split_num_docs: 10_000_000,
            max_split_num_docs: 20_000_000,
            timestamp_field_name,
            demux_field_name,
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
        ctx.send_message_blocking(
            &self.merge_packager_mailbox,
            IndexedSplitBatch {
                splits: vec![indexed_split],
            },
        )?;
        Ok(())
    }

    fn process_demux(
        &mut self,
        splits: Vec<SplitMetadata>,
        merge_scratch_directory: ScratchDirectory,
        downloaded_splits_directory: ScratchDirectory,
        ctx: &ActorContext<MergeScratch>,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        info!("demux-start");
        assert!(
            self.demux_field_name.is_some(),
            "`process_demux` cannot be called without a demux field."
        );
        let demux_field_name = self.demux_field_name.clone().unwrap();
        let num_splits = splits.len();
        let total_docs_size_in_bytes = splits.iter().map(|split| split.size_in_bytes).sum::<u64>();
        let replaced_split_ids = splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec();
        let (index_metas, replaced_segments) =
            load_metas_and_segments(downloaded_splits_directory.path(), &replaced_split_ids)?;
        let replaced_segments_demux_values =
            read_segments_demux_values(&replaced_segments, &demux_field_name)?;
        // Build virtual split for all replaced splits = counting demux values in all replaced
        // segments.
        let mut split_with_all_docs = VirtualSplit::new(HashMap::new());
        for fast_field_values in replaced_segments_demux_values.iter() {
            for fast_field_value in fast_field_values {
                split_with_all_docs.add_docs(*fast_field_value, 1);
            }
        }
        let total_num_docs = split_with_all_docs.total_num_docs();
        let demuxed_splits = demux_values(
            split_with_all_docs,
            self.min_split_num_docs,
            self.max_split_num_docs,
            num_splits,
        );
        ctx.record_progress();
        let demux_mapping = build_demux_mapping(replaced_segments_demux_values, demuxed_splits);
        let demuxed_scratched_directories: Vec<ScratchDirectory> = (0..num_splits)
            .map(|_| merge_scratch_directory.temp_child())
            .try_collect()?;
        let demuxed_split_directories: Vec<MmapDirectory> = demuxed_scratched_directories
            .iter()
            .map(|directory| MmapDirectory::open(directory.path()))
            .try_collect()?;
        let union_index_meta = combine_index_meta(index_metas)?;
        ctx.record_progress();
        let indexes = {
            let _protected_zone_guard = ctx.protect_zone();
            demux(
                &replaced_segments,
                &demux_mapping,
                union_index_meta.index_settings,
                demuxed_split_directories,
            )?
        };
        info!(elapsed_secs = start.elapsed().as_secs_f32(), "demux-stop");
        let mut indexed_splits = Vec::new();
        for (index, scratched_directory) in indexes
            .into_iter()
            .zip(demuxed_scratched_directories.into_iter())
        {
            let searchable_segments = index.searchable_segments()?;
            let segment = searchable_segments
                .into_iter()
                .next()
                .expect("Split must have at least one segment.");
            let segment_reader = SegmentReader::open(&segment)?;
            // We cannot compute `docs_size_in_bytes` for demuxed splits as it
            // is obtained by summing the docs json bytes at indexing. So we do a simple estimation.
            // TODO: could be interesting to use the split size or some other proxy to have
            // a better estimate.
            let num_docs = segment_reader.num_docs() as usize;
            let docs_size_in_bytes =
                (num_docs as f32 * total_docs_size_in_bytes as f32 / total_num_docs as f32) as u64;
            let time_range = if let Some(ref timestamp_field) = self.timestamp_field_name {
                let reader = make_fast_field_reader::<i64>(&segment_reader, timestamp_field)?;
                Some(RangeInclusive::new(reader.min_value(), reader.max_value()))
            } else {
                None
            };
            let index_writer = index.writer_with_num_threads(1, 3_000_000)?;
            let indexed_split = IndexedSplit {
                split_id: new_split_id(),
                index_id: self.index_id.clone(),
                replaced_split_ids: replaced_split_ids.clone(),
                time_range,
                num_docs: num_docs as u64,
                docs_size_in_bytes,
                // start_time is not very interesting here.
                split_date_of_birth: Instant::now(),
                checkpoint_delta: CheckpointDelta::default(), //< TODO fixme
                index,
                index_writer,
                split_scratch_directory: scratched_directory,
            };
            indexed_splits.push(indexed_split);
        }
        ctx.send_message_blocking(
            &self.merge_packager_mailbox,
            IndexedSplitBatch {
                splits: indexed_splits,
            },
        )?;
        Ok(())
    }
}

// Open indexes and return metas and segments for each split.
// Note: only the first segment of each split is taken.
pub fn load_metas_and_segments(
    directory_path: &Path,
    split_ids: &[String],
) -> anyhow::Result<(Vec<IndexMeta>, Vec<Segment>)> {
    let mmap_directory = MmapDirectory::open(directory_path)?;
    let mut replaced_segments = Vec::new();
    let mut index_metas = Vec::new();
    for split_id in split_ids.iter() {
        let split_filename = split_file(split_id);
        let split_fileslice = mmap_directory.open_read(Path::new(&split_filename))?;
        let split_directory = BundleDirectory::open_split(split_fileslice)?;
        let index = Index::open(split_directory)?;
        index_metas.push(index.load_metas()?);
        let searchable_segments = index.searchable_segments()?;
        assert_eq!(
            searchable_segments.len(),
            1,
            "Only one segment is expected for a split that is going to be demuxed."
        );
        let segment = searchable_segments.into_iter().next().unwrap();
        replaced_segments.push(segment);
    }
    Ok((index_metas, replaced_segments))
}

// Read fast values of demux field for each segment and return them.
pub fn read_segments_demux_values(
    segments: &[Segment],
    demux_field_name: &str,
) -> anyhow::Result<Vec<Vec<i64>>> {
    let mut fast_field_values_vec = Vec::new();
    for segment in segments {
        let segment_reader = SegmentReader::open(segment)?;
        let num_docs = segment_reader.num_docs() as usize;
        let reader = make_fast_field_reader::<i64>(&segment_reader, demux_field_name)?;
        let mut fast_field_values: Vec<i64> = Vec::new();
        fast_field_values.resize(num_docs, 0);
        reader.get_range(0, &mut fast_field_values);
        fast_field_values_vec.push(fast_field_values);
    }
    Ok(fast_field_values_vec)
}

/// Build tantivy `DemuxMapping` from input (or old) segments demux values and target
/// demuxed segments defined as virtual splits.
pub fn build_demux_mapping(
    input_segments_demux_values: Vec<Vec<i64>>,
    target_demuxed_splits: Vec<VirtualSplit>,
) -> DemuxMapping {
    assert_eq!(
        input_segments_demux_values.len(),
        target_demuxed_splits.len(),
        "Demuxed splits must have the same size as number of segments to demux."
    );
    assert_eq!(
        input_segments_demux_values
            .iter()
            .map(|values| values.len())
            .sum::<usize>(),
        target_demuxed_splits
            .iter()
            .map(|split| split.total_num_docs())
            .sum::<usize>(),
        "Num docs must be equal between input segments and target splits."
    );
    // Create a hash map of list of `SegmentNumDocs` for each demux value.
    // A `SegmentNumDocs` item can be seen as a docs stock that will be
    // consumed when filling `DocIdToSegmentOrdinal`, each time a demux value is seen,
    // we decrement the segment stock until emptying it and passing to the next segment stock.
    let mut num_docs_segment_stocks_by_demux_value = HashMap::new();
    for (ordinal, split_map) in target_demuxed_splits.iter().enumerate() {
        for (demux_value, &num_docs) in split_map.0.iter() {
            num_docs_segment_stocks_by_demux_value
                .entry(*demux_value)
                .or_insert_with(Vec::new)
                .push(SegmentNumDocs {
                    ordinal: ordinal as u32,
                    num_docs,
                });
        }
    }
    let mut mapping = DemuxMapping::default();
    for segment_demux_values in input_segments_demux_values.iter() {
        // Fill `DocIdToSegmentOrdinal` with available `SegmentNumDocs`.
        let mut doc_id_to_segment_ordinal =
            DocIdToSegmentOrdinal::with_max_doc(segment_demux_values.len());
        for (doc_id, demux_value) in segment_demux_values.iter().enumerate() {
            let segment_num_docs_vec = num_docs_segment_stocks_by_demux_value
                .get_mut(demux_value)
                .expect("Demux value must be present.");
            segment_num_docs_vec[0].num_docs -= 1;
            doc_id_to_segment_ordinal.set(doc_id as u32, segment_num_docs_vec[0].ordinal);
            // When a `SegmentNumDocs` is empty, remove it.
            if segment_num_docs_vec[0].num_docs == 0 {
                segment_num_docs_vec.remove(0);
            }
        }
        mapping.add(doc_id_to_segment_ordinal);
    }
    assert!(
        num_docs_segment_stocks_by_demux_value
            .values()
            .all(|stocks| stocks.is_empty()),
        "All docs must be placed in new segments."
    );
    mapping
}

// A virtual split is a view on a split that contains only the information of
// docs count per demux value in the split. The demux algorithm use a virtual
// split as input and produced demuxed virtual splits. The virtual splits are
// then used for the real demux that will create real split.
// Thus the usage of `virtual`.
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

    pub fn num_docs(&self, demux_value: &i64) -> usize {
        *self.0.get(demux_value).unwrap_or(&0usize)
    }
}

struct SegmentNumDocs {
    ordinal: u32,
    num_docs: usize,
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
///     - If `open split num docs + demux value num docs > `split_upper_bound`, we put all the docs
///       we can in the open split, the remaining docs will be added in the next one.
///     - If after adding docs, the current split num docs >= `split_lower_bound`, the split is
///       closed and we open a new split.
///
/// The split bounds must be carefully chosen to ensure the contraints [min_split_num_docs,
/// max_split_num_docs] are satisfied for all splits. To ensure that, it is sufficient to define
/// these bounds starting from the last split and backpropagating them to the first split.
///
/// The rationale is as follows: when there is `k` splits to fill, we need to have remaining
/// num docs in [k * min_split_num_docs, k * max_split_num_docs] to satisfy the min/max constraint
/// on all the remaining `k` splits.
/// When starting filling the `n - k - 1`, this translates to the following constraint:
///   1. If `remaining_num_docs - k * max_split_num_docs > min_split_num_docs', we must put at
///      least `remaining_num_docs - max_split_num_docs` in the `n - k - 1` split otherwise we will
///      have too much docs in the `k` remaining splits.
///   2. If `remaining_num_docs - k * max_split_num_docs <= min_split_num_docs', we just need
///      to satisfy the min constraint `min_split_num_docs` for the split `n - k - 1`, we will never
///      have too much docs in the next splits.
///   3. If `remaining_num_docs - k * min_split_num_docs < max_split_num_docs', we must put at
///      most `remaining_num_docs - (k - 1) * min_split_num_docs` docs otherwise we will have not
///      enough docs in the `k` remaining splits.
///   4. If `remaining_num_docs - k * min_split_num_docs >= max_split_num_docs', we just need
///      to satisfy the max constraint `max_split_num_docs` for the split `n - k`. We will always
///      have enough docs for the next splits.
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
    let input_split_demux_values = input_split.sorted_demux_values();
    let mut demuxed_splits = Vec::new();
    let mut current_split = VirtualSplit::new(HashMap::new());
    let mut num_docs_split_bounds = compute_current_split_bounds(
        total_num_docs,
        output_num_splits - 1,
        min_split_num_docs,
        max_split_num_docs,
    );
    for demux_value in input_split_demux_values.into_iter() {
        while input_split.num_docs(&demux_value) > 0 {
            let num_docs_to_add = if current_split.total_num_docs()
                + input_split.num_docs(&demux_value)
                <= *num_docs_split_bounds.end()
            {
                input_split.num_docs(&demux_value)
            } else {
                num_docs_split_bounds.end() - current_split.total_num_docs()
            };
            current_split.add_docs(demux_value, num_docs_to_add);
            input_split.remove_docs(&demux_value, num_docs_to_add);
            if current_split.total_num_docs() >= *num_docs_split_bounds.start() {
                demuxed_splits.push(VirtualSplit::new(HashMap::from_iter(
                    current_split.0.drain(),
                )));
                // No more split to fill.
                if output_num_splits - demuxed_splits.len() == 0 {
                    break;
                }
                num_docs_split_bounds = compute_current_split_bounds(
                    input_split.total_num_docs(),
                    output_num_splits - demuxed_splits.len() - 1,
                    min_split_num_docs,
                    max_split_num_docs,
                );
            }
        }
    }
    assert_eq!(
        demuxed_splits
            .iter()
            .map(|split| split.total_num_docs())
            .sum::<usize>(),
        total_num_docs,
        "Demuxing must keep the same number of docs."
    );
    assert!(
        demuxed_splits
            .iter()
            .map(|split| split.total_num_docs())
            .min()
            .unwrap_or(min_split_num_docs)
            >= min_split_num_docs,
        "Demuxing must satisfy the min contraint on split num docs."
    );
    assert!(
        demuxed_splits
            .iter()
            .map(|split| split.total_num_docs())
            .max()
            .unwrap_or(max_split_num_docs)
            <= max_split_num_docs,
        "Demuxing must satisfy the max contraint on split num docs."
    );
    assert!(
        demuxed_splits.len() == output_num_splits,
        "Demuxing must return exactly the requested output splits number."
    );
    demuxed_splits
}

/// Compute split bounds for the current split that is going to be filled
/// knowing that there are `remaining_num_splits` splits that needs to be filled
/// and to satisfiy [min_split_num_docs, max_split_num_docs] constraint.
/// See description of [demux_values] algorithm for more details.
pub fn compute_current_split_bounds(
    remaining_num_docs: usize,
    remaining_num_splits: usize,
    min_split_num_docs: usize,
    max_split_num_docs: usize,
) -> RangeInclusive<usize> {
    // When there are no more splits to fill, we return `remaining_num_docs` as
    // the lower bound as we just want to put all remaining docs in the current split.
    if remaining_num_splits == 0 {
        return RangeInclusive::new(remaining_num_docs, max_split_num_docs);
    }
    let num_docs_lower_bound = if remaining_num_docs > remaining_num_splits * max_split_num_docs {
        std::cmp::max(
            min_split_num_docs,
            remaining_num_docs - remaining_num_splits * max_split_num_docs,
        )
    } else {
        min_split_num_docs
    };
    let num_docs_upper_bound = std::cmp::min(
        max_split_num_docs,
        remaining_num_docs - remaining_num_splits * min_split_num_docs,
    );
    assert!(
        num_docs_lower_bound <= num_docs_upper_bound,
        "Num docs lower bound must be <= num docs upper bound."
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
    use std::mem;
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
        let merge_executor =
            MergeExecutor::new(index_id.to_string(), merge_packager_mailbox, None, None);
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
        assert_eq!(packager_msg.splits[0].num_docs, 4);
        assert_eq!(packager_msg.splits[0].docs_size_in_bytes, 136);

        let reader = packager_msg.splits[0].index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_demux_execution() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "demux_field": "tenant_id",
            "tag_fields": [],
            "field_mappings": [
                { "name": "body", "type": "text" },
                { "name": "ts", "type": "i64", "fast": true },
                { "name": "tenant_id", "type": "i64", "fast": true }
            ]
        }"#;
        let index_config =
            Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
        let index_id = "test-index-demux";
        let test_index_builder = TestSandbox::create(index_id, index_config).await?;
        for split_id in 0..4 {
            let docs = vec![
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713 + split_id, "tenant_id": split_id * 10 }),
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713 + split_id, "tenant_id": (split_id + 1) * 10 }),
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713 + split_id, "tenant_id": (split_id + 2) * 10 }),
            ];
            test_index_builder.add_documents(docs).await?;
        }
        let metastore = test_index_builder.metastore();
        let splits_with_footer_offsets = metastore.list_all_splits(index_id).await?;
        let splits: Vec<SplitMetadata> = splits_with_footer_offsets
            .into_iter()
            .map(|split_and_footer_offsets| split_and_footer_offsets.split_metadata)
            .collect();
        let total_num_bytes_docs = splits.iter().map(|split| split.size_in_bytes).sum::<u64>();
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
            merge_operation: MergeOperation::Demux { splits },
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let mut merge_executor = MergeExecutor::new(
            index_id.to_string(),
            merge_packager_mailbox,
            Some("tenant_id".to_string()),
            None,
        );
        merge_executor.min_split_num_docs = 1;
        merge_executor.max_split_num_docs = 3;
        let universe = Universe::new();
        let (merge_executor_mailbox, merge_executor_handle) =
            universe.spawn_actor(merge_executor).spawn_sync();
        universe
            .send_message(&merge_executor_mailbox, merge_scratch)
            .await?;
        mem::drop(merge_executor_mailbox);
        let _ = merge_executor_handle.join().await;
        let mut packager_msgs = merge_packager_inbox.drain_available_message_for_test();
        assert_eq!(packager_msgs.len(), 1);
        let mut splits = packager_msgs.pop().unwrap().splits;
        assert_eq!(splits.len(), 4);
        let first_indexed_split = splits.pop().unwrap();
        assert_eq!(first_indexed_split.num_docs, 3);
        assert_eq!(
            first_indexed_split.docs_size_in_bytes,
            total_num_bytes_docs / 4
        );
        let reader = first_indexed_split.index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Ok(())
    }

    #[test]
    fn test_build_demux_mapping() {
        let segments_demux_values: Vec<Vec<i64>> =
            vec![vec![1, 5, 10, 5, 10, 10], vec![1, 5, 10, 20]];
        let split_1 = VirtualSplit::new(vec![(1, 2), (5, 3), (10, 1)].into_iter().collect());
        let split_2 = VirtualSplit::new(vec![(10, 3), (20, 1)].into_iter().collect());
        let splits = vec![split_1, split_2];
        let demux_mapping = build_demux_mapping(segments_demux_values, splits);
        assert_eq!(demux_mapping.get_old_num_segments(), 2);
    }

    #[test]
    fn test_demux_with_same_num_docs() {
        let mut num_docs_map = HashMap::new();
        num_docs_map.insert(0, 100);
        num_docs_map.insert(1, 100);
        num_docs_map.insert(2, 100);
        let splits = demux_values(VirtualSplit::new(num_docs_map), 100, 200, 3);

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].num_docs(&0), 100);
        assert_eq!(splits[1].num_docs(&1), 100);
        assert_eq!(splits[2].num_docs(&2), 100);
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
        assert_eq!(splits[0].num_docs(&0), 1);
        assert_eq!(splits[0].num_docs(&1), 199);
        assert_eq!(splits[1].num_docs(&1), 1);
        assert_eq!(splits[1].num_docs(&2), 101);
        assert_eq!(splits[2].num_docs(&2), 99);
        assert_eq!(splits[2].num_docs(&3), 1);
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
        assert_eq!(splits[0].num_docs(&0), 1);
        assert_eq!(splits[0].num_docs(&1), 50);
        assert_eq!(splits[0].num_docs(&2), 75);
        assert_eq!(splits[1].num_docs(&3), 100);
        assert_eq!(splits[2].num_docs(&4), 50);
        assert_eq!(splits[2].num_docs(&5), 150);
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
