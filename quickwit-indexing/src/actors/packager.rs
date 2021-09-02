// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::fs::File;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;

use crate::models::IndexedSplit;
use crate::models::PackagedSplit;
use crate::models::ScratchDirectory;
use anyhow::Context;
use fail::fail_point;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SyncActor;
use quickwit_directories::write_hotcache;
use quickwit_storage::BundleStorageBuilder;
use quickwit_storage::BUNDLE_FILENAME;
use tantivy::common::CountingWriter;
use tantivy::SegmentId;
use tantivy::SegmentMeta;
use tracing::*;

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
}

impl Packager {
    pub fn new(uploader_mailbox: Mailbox<PackagedSplit>) -> Self {
        Packager { uploader_mailbox }
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
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        info!(segment_ids=?segment_ids, "merge");
        let _protected_zone_guard = ctx.protect_zone();
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let segment_metas_after_merge: Vec<SegmentMeta> = split.index.searchable_segment_metas()?;
    Ok(segment_metas_after_merge)
}

fn build_hotcache<W: io::Write>(
    scratch_dir: &ScratchDirectory,
    split_file: &mut W,
) -> anyhow::Result<()> {
    let mmap_directory = tantivy::directory::MmapDirectory::open(scratch_dir.path())?;
    write_hotcache(mmap_directory, split_file)?;
    Ok(())
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
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

    let segment_ids: Vec<SegmentId> = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.id())
        .collect();

    // Extracts tag values from `_tags` special fields.
    let mut tags = vec![];
    let index_reader = split.index.reader()?;
    for reader in index_reader.searcher().segment_readers() {
        let inv_index = reader.inverted_index(split.tags_field)?;
        let mut terms_streamer = inv_index.terms().stream()?;
        while let Some((term_data, _)) = terms_streamer.next() {
            tags.push(String::from_utf8_lossy(term_data).to_string());
        }
    }
    ctx.record_progress();

    debug!(split = ?split, "build-hotcache");
    let hotcache_offset_start = split_file.written_bytes();
    build_hotcache(&split.split_scratch_directory, &mut split_file)?;
    let hotcache_offset_end = split_file.written_bytes();
    let hotcache_num_bytes = hotcache_offset_end - hotcache_offset_start;
    ctx.record_progress();

    info!(split = ?split, "split-write-all");
    split_file.write_all(&hotcache_num_bytes.to_le_bytes())?;
    split_file.flush()?;

    let footer_end = split_file.written_bytes();

    let packaged_split = PackagedSplit {
        index_id: split.index_id,
        split_id: split.split_id.to_string(),
        checkpoint_delta: split.checkpoint_delta,
        split_scratch_directory: split.split_scratch_directory,
        num_docs,
        segment_ids,
        time_range: split.time_range,
        size_in_bytes: split.docs_size_in_bytes,
        tags,
        footer_offsets: footer_start..footer_end,
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
        let packaged_split = create_packaged_split(&segment_metas[..], split, ctx)?;
        ctx.send_message_blocking(&self.uploader_mailbox, packaged_split)?;
        fail_point!("packager:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::time::Instant;

    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::ObservationType;
    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use tantivy::doc;
    use tantivy::schema::Schema;
    use tantivy::schema::FAST;
    use tantivy::schema::TEXT;
    use tantivy::Index;

    use crate::models::ScratchDirectory;

    use super::*;

    fn make_indexed_split_for_test(segments_timestamps: &[&[i64]]) -> anyhow::Result<IndexedSplit> {
        let split_scratch_directory = ScratchDirectory::try_new_temp()?;
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let timestamp_field = schema_builder.add_u64_field("timestamp", FAST);
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
            start_time: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::from(10..20),
            tags_field: tantivy::schema::Field::from_field_id(0),
        };
        Ok(indexed_split)
    }

    #[tokio::test]
    async fn test_packager_no_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let packager = Packager::new(mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_sync_actor(packager);
        let indexed_split = make_indexed_split_for_test(&[&[1628203589, 1628203640]])?;
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
        let packager = Packager::new(mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_sync_actor(packager);
        let indexed_split = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
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
}
