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

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::Context;
use fail::fail_point;
use quickwit_actors::{Actor, ActorContext, Mailbox, QueueCapacity, SyncActor};
use quickwit_directories::write_hotcache;
use quickwit_storage::{BundleStorageBuilder, BUNDLE_FILENAME};
use tantivy::common::CountingWriter;
use tantivy::schema::Field;
use tantivy::{ReloadPolicy, SegmentId, SegmentMeta};
use tracing::*;

use crate::models::{IndexedSplit, MergePlannerMessage, PackagedSplit, ScratchDirectory};

/// The role of the packager is to get an index writer and
/// produce a split file.
///
/// This includes the following steps:
/// - commit: this step is CPU heavy
/// - indentifying the list of tags for the splits, and labelling it accordingly
/// - creating a bundle file
/// - computing the hotcache
/// - appending it to the split file.
///
/// The split format is described in `internals/split-format.md`
pub struct Packager {
    uploader_mailbox: Mailbox<PackagedSplit>,
    merge_planner_mailbox_opt: Option<Mailbox<MergePlannerMessage>>,
    /// The special field for extracting tags.
    tags_field: Field,
}

impl Packager {
    pub fn new(
        tags_field: Field,
        uploader_mailbox: Mailbox<PackagedSplit>,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlannerMessage>>,
    ) -> Packager {
        Packager {
            uploader_mailbox,
            merge_planner_mailbox_opt,
            tags_field,
        }
    }
}

impl Actor for Packager {
    type Message = IndexedSplit;

    type ObservableState = ();

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }
}

/// returns true iff merge is required to reach a state where
/// we have zero, or a single segment with no deletes segment.
fn is_merge_required(segment_metas: &[SegmentMeta]) -> bool {
    match &segment_metas {
        // there are no segment to merge
        [] => false,
        // if there is only segment but it has deletes, it
        // still makes sense to merge it alone in order to remove deleted documents.
        [segment_meta] => segment_meta.has_deletes(),
        _ => true,
    }
}

/// Commits the tantivy Index.
/// Tantivy will serialize all of its remaining internal in RAM
/// datastructure and write them on disk.
///
/// It consists in several sequentials phases mixing both
/// CPU and IO, the longest once being the serialization of
/// the inverted index. This phase is CPU bound.
fn commit_split(split: &mut IndexedSplit, ctx: &ActorContext<IndexedSplit>) -> anyhow::Result<()> {
    info!(index=%split.index_id, split=?split, "commit-split");
    let _protected_zone_guard = ctx.protect_zone();
    split
        .index_writer
        .commit()
        .with_context(|| format!("Commit split `{:?}` failed", &split))?;
    Ok(())
}

fn list_split_files(
    segment_metas: &[SegmentMeta],
    scratch_directory: &ScratchDirectory,
) -> Vec<PathBuf> {
    let mut index_files = vec![scratch_directory.path().join("meta.json")];

    // list the segment files
    for segment_meta in segment_metas {
        for relative_path in segment_meta.list_files() {
            let filepath = scratch_directory.path().join(&relative_path);
            if filepath.exists() {
                // If the file is missing, this is fine.
                // segment_meta.list_files() may actually returns files that
                // may not exist.
                index_files.push(filepath);
            }
        }
    }
    index_files
}

/// Create the file bundle.
///
/// Returns the range of the file offsets metadata (required to open the bundle.)
fn create_file_bundle(
    segment_metas: &[SegmentMeta],
    scratch_dir: &ScratchDirectory,
    split_file: &mut impl io::Write,
    ctx: &ActorContext<IndexedSplit>,
) -> anyhow::Result<Range<u64>> {
    let _protected_zone_guard = ctx.protect_zone();
    // List the split files that will be packaged into the bundle.
    let mut bundle_storage_builder = BundleStorageBuilder::new(split_file)?;

    for split_file in list_split_files(segment_metas, scratch_dir) {
        bundle_storage_builder.add_file(&split_file)?;
    }

    let bundle_storage_offsets = bundle_storage_builder.finalize()?;
    Ok(bundle_storage_offsets)
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_if_required(
    split: &mut IndexedSplit,
    ctx: &ActorContext<IndexedSplit>,
) -> anyhow::Result<Vec<SegmentMeta>> {
    debug!(split = ?split, "merge-segments-if-required");
    let segment_metas_before_merge = split.index.searchable_segment_metas()?;
    if is_merge_required(&segment_metas_before_merge[..]) {
        let segment_ids: Vec<SegmentId> = segment_metas_before_merge
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();

        info!(segment_ids=?segment_ids,"merging-segments");
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        let _protected_zone_guard = ctx.protect_zone();
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let segment_metas_after_merge: Vec<SegmentMeta> = split.index.searchable_segment_metas()?;
    Ok(segment_metas_after_merge)
}

fn build_hotcache<W: io::Write>(split_path: &Path, split_file: &mut W) -> anyhow::Result<()> {
    let mmap_directory = tantivy::directory::MmapDirectory::open(split_path)?;
    write_hotcache(mmap_directory, split_file)?;
    Ok(())
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
    tags_field: Field,
    ctx: &ActorContext<IndexedSplit>,
) -> anyhow::Result<PackagedSplit> {
    info!(split = ?split, "create-packaged-split");

    let split_filepath = split.split_scratch_directory.path().join(BUNDLE_FILENAME); // TODO rename <split_id>.split
    let mut split_file = CountingWriter::wrap(File::create(split_filepath)?);

    debug!(split = ?split, "create-file-bundle");
    let Range {
        start: footer_start,
        end: _,
    } = create_file_bundle(
        segment_metas,
        &split.split_scratch_directory,
        &mut split_file,
        ctx,
    )?;

    let num_docs = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.num_docs() as u64)
        .sum();

    // Extracts tag values from `_tags` special fields.
    let mut tags = HashSet::default();
    let index_reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    for reader in index_reader.searcher().segment_readers() {
        let inv_index = reader.inverted_index(tags_field)?;
        let mut terms_streamer = inv_index.terms().stream()?;
        while let Some((term_data, _)) = terms_streamer.next() {
            tags.insert(String::from_utf8_lossy(term_data).to_string());
        }
    }
    ctx.record_progress();

    debug!(split = ?split, "build-hotcache");
    let hotcache_offset_start = split_file.written_bytes();
    build_hotcache(split.split_scratch_directory.path(), &mut split_file)?;
    let hotcache_offset_end = split_file.written_bytes();
    let hotcache_num_bytes = hotcache_offset_end - hotcache_offset_start;
    ctx.record_progress();

    debug!(split = ?split, "split-write-all");
    split_file.write_all(&hotcache_num_bytes.to_le_bytes())?;
    split_file.flush()?;

    let footer_end = split_file.written_bytes();

    let packaged_split = PackagedSplit {
        split_id: split.split_id.to_string(),
        replaced_split_ids: split.replaced_split_ids,
        index_id: split.index_id,
        checkpoint_deltas: vec![split.checkpoint_delta],
        split_scratch_directory: split.split_scratch_directory,
        num_docs,
        time_range: split.time_range,
        size_in_bytes: split.docs_size_in_bytes,
        tags,
        footer_offsets: footer_start..footer_end,
        split_date_of_birth: split.split_date_of_birth,
    };
    Ok(packaged_split)
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        mut split: IndexedSplit,
        ctx: &ActorContext<IndexedSplit>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        fail_point!("packager:before");
        commit_split(&mut split, ctx)?;
        let segment_metas = merge_segments_if_required(&mut split, ctx)?;
        let packaged_split =
            create_packaged_split(&segment_metas[..], split, self.tags_field, ctx)?;
        ctx.send_message_blocking(&self.uploader_mailbox, packaged_split)?;
        fail_point!("packager:after");
        Ok(())
    }

    fn finalize(
        &mut self,
        _exit_status: &quickwit_actors::ActorExitStatus,
        ctx: &ActorContext<Self::Message>,
    ) -> anyhow::Result<()> {
        if let Some(merge_planner_mailbox) = self.merge_planner_mailbox_opt.as_ref() {
            // We are trying to stop the merge planner.
            // If the merge planner is already dead, this is not an error.
            let _ = ctx.send_exit_with_success_blocking(merge_planner_mailbox);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::ops::RangeInclusive;
    use std::time::Instant;

    use quickwit_actors::{
        create_test_mailbox, Command, CommandOrMessage, ObservationType, Universe,
    };
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use tantivy::schema::{Schema, FAST, STRING, TEXT};
    use tantivy::{doc, Index};

    use super::*;
    use crate::models::ScratchDirectory;

    fn make_indexed_split_for_test(segments_timestamps: &[&[i64]]) -> anyhow::Result<IndexedSplit> {
        let split_scratch_directory = ScratchDirectory::for_test()?;
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let timestamp_field = schema_builder.add_u64_field("timestamp", FAST);
        let _tags_field =
            schema_builder.add_text_field(quickwit_index_config::TAGS_FIELD_NAME, STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(split_scratch_directory.path(), schema)?;
        let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
        let mut timerange_opt: Option<RangeInclusive<i64>> = None;
        let mut num_docs = 0;
        for (segment_num, segment_timestamps) in segments_timestamps.iter().enumerate() {
            if segment_num > 0 {
                index_writer.commit()?;
            }
            for &timestamp in segment_timestamps.iter() {
                let doc = doc!(
                    text_field => format!("timestamp is {}", timestamp),
                    timestamp_field => timestamp
                );
                index_writer.add_document(doc);
                num_docs += 1;
                timerange_opt = Some(
                    timerange_opt
                        .map(|timestamp_range| {
                            let start = timestamp.min(*timestamp_range.start());
                            let end = timestamp.max(*timestamp_range.end());
                            RangeInclusive::new(start, end)
                        })
                        .unwrap_or_else(|| RangeInclusive::new(timestamp, timestamp)),
                )
            }
        }
        // We don't commit, that's the job of the packager.
        //
        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_id: "test-split".to_string(),
            index_id: "test-index".to_string(),
            time_range: timerange_opt,
            num_docs,
            docs_size_in_bytes: num_docs * 15, //< bogus number
            split_date_of_birth: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::from(10..20),
            replaced_split_ids: Vec::new(),
        };
        Ok(indexed_split)
    }

    #[tokio::test]
    async fn test_packager_no_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();

        let indexed_split = make_indexed_split_for_test(&[&[1628203589, 1628203640]])?;
        let tags_field = indexed_split
            .index
            .schema()
            .get_field(quickwit_index_config::TAGS_FIELD_NAME)
            .unwrap();
        let packager = Packager::new(tags_field, mailbox, None);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        universe
            .send_message(&packager_mailbox, indexed_split)
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_packager_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        let tags_field = indexed_split
            .index
            .schema()
            .get_field(quickwit_index_config::TAGS_FIELD_NAME)
            .unwrap();
        let packager = Packager::new(tags_field, mailbox, None);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        universe
            .send_message(&packager_mailbox, indexed_split)
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_packager_stop_merge_planner_on_finalize() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, _inbox) = create_test_mailbox();
        let (merge_planner_mailbox, merge_planner_inbox) = create_test_mailbox();

        let packager = Packager::new(
            Field::from_field_id(0u32),
            mailbox,
            Some(merge_planner_mailbox),
        );
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        // That way the packager will terminate
        mem::drop(packager_mailbox);
        packager_handle.join().await;
        let merge_planner_msgs = merge_planner_inbox.drain_available_message_or_command_for_test();
        assert_eq!(merge_planner_msgs.len(), 1);
        let merge_planner_msg = merge_planner_msgs.into_iter().next().unwrap();
        assert!(matches!(
            merge_planner_msg,
            CommandOrMessage::Command(Command::ExitWithSuccess)
        ));
        Ok(())
    }
}
