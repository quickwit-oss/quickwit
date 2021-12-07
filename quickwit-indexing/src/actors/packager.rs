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

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, Mailbox, QueueCapacity, SyncActor};
use quickwit_directories::write_hotcache;
use quickwit_index_config::{make_tag_value, make_too_many_tag_value, MAX_VALUES_PER_TAG_FIELD};
use tantivy::schema::Field;
use tantivy::{ReloadPolicy, SegmentId, SegmentMeta};
use tracing::*;

use crate::models::{
    IndexedSplit, IndexedSplitBatch, PackagedSplit, PackagedSplitBatch, ScratchDirectory,
};

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
    actor_name: &'static str,
    uploader_mailbox: Mailbox<PackagedSplitBatch>,
    /// A list of tag fields specified in the index config and their
    /// corresponding schema field.
    tag_fields_list: Vec<(String, Field)>,
}

impl Packager {
    pub fn new(
        actor_name: &'static str,
        tag_fields_list: Vec<(String, Field)>,
        uploader_mailbox: Mailbox<PackagedSplitBatch>,
    ) -> Packager {
        Packager {
            actor_name,
            uploader_mailbox,
            tag_fields_list,
        }
    }

    pub fn process_indexed_split(
        &self,
        mut split: IndexedSplit,
        ctx: &ActorContext<IndexedSplitBatch>,
    ) -> anyhow::Result<PackagedSplit> {
        commit_split(&mut split, ctx)?;
        let segment_metas = merge_segments_if_required(&mut split, ctx)?;
        let packaged_split =
            create_packaged_split(&segment_metas[..], split, &self.tag_fields_list, ctx)?;
        Ok(packaged_split)
    }
}

impl Actor for Packager {
    type Message = IndexedSplitBatch;

    type ObservableState = ();

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    fn name(&self) -> String {
        self.actor_name.to_string()
    }

    fn message_span(&self, msg_id: u64, batch: &IndexedSplitBatch) -> Span {
        info_span!("", msg_id=&msg_id, num_splits=%batch.splits.len())
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
fn commit_split(
    split: &mut IndexedSplit,
    ctx: &ActorContext<IndexedSplitBatch>,
) -> anyhow::Result<()> {
    info!(split_id=%split.split_id, "commit-split");
    let _protect_guard = ctx.protect_zone();
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
    index_files.sort();
    index_files
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_if_required(
    split: &mut IndexedSplit,
    ctx: &ActorContext<IndexedSplitBatch>,
) -> anyhow::Result<Vec<SegmentMeta>> {
    debug!(
        split_id = split.split_id.as_str(),
        "merge-segments-if-required"
    );
    let segment_metas_before_merge = split.index.searchable_segment_metas()?;
    if is_merge_required(&segment_metas_before_merge[..]) {
        let segment_ids: Vec<SegmentId> = segment_metas_before_merge
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();

        info!(split_id=split.split_id.as_str(), segment_ids=?segment_ids, "merging-segments");
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        let _protected_zone_guard = ctx.protect_zone();
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let segment_metas_after_merge: Vec<SegmentMeta> = split.index.searchable_segment_metas()?;
    Ok(segment_metas_after_merge)
}

fn build_hotcache<W: io::Write>(split_path: &Path, out: &mut W) -> anyhow::Result<()> {
    let mmap_directory = tantivy::directory::MmapDirectory::open(split_path)?;
    write_hotcache(mmap_directory, out)?;
    Ok(())
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
    tag_fields_list: &[(String, Field)],
    ctx: &ActorContext<IndexedSplitBatch>,
) -> anyhow::Result<PackagedSplit> {
    info!(split_id = split.split_id.as_str(), "create-packaged-split");
    let split_files = list_split_files(segment_metas, &split.split_scratch_directory);
    debug!(split_id = split.split_id.as_str(), "create-file-bundle");
    let num_docs = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.num_docs() as u64)
        .sum();

    // Extracts tag values from `_tags` special fields.
    let index_reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let mut tags = BTreeSet::default();
    for (tag_field_name, tag_field) in tag_fields_list {
        let inverted_indexes = index_reader
            .searcher()
            .segment_readers()
            .iter()
            .map(|segment| segment.inverted_index(*tag_field))
            .collect::<Result<Vec<_>, _>>()?;

        if inverted_indexes
            .iter()
            .map(|inv_index| inv_index.terms().num_terms())
            .sum::<usize>()
            >= MAX_VALUES_PER_TAG_FIELD
        {
            tags.insert(make_too_many_tag_value(tag_field_name));
            continue;
        }

        for inv_index in inverted_indexes {
            let mut terms_streamer = inv_index.terms().stream()?;
            while let Some((term_data, _)) = terms_streamer.next() {
                let tag_value = String::from_utf8_lossy(term_data).to_string();
                tags.insert(make_tag_value(tag_field_name, &tag_value));
            }
        }
    }
    ctx.record_progress();

    debug!(split_id = split.split_id.as_str(), "build-hotcache");
    let mut hotcache_bytes = vec![];
    build_hotcache(split.split_scratch_directory.path(), &mut hotcache_bytes)?;
    ctx.record_progress();

    let packaged_split = PackagedSplit {
        split_id: split.split_id.to_string(),
        replaced_split_ids: split.replaced_split_ids,
        index_id: split.index_id,
        checkpoint_deltas: vec![split.checkpoint_delta],
        split_scratch_directory: split.split_scratch_directory,
        num_docs,
        demux_num_ops: split.demux_num_ops,
        time_range: split.time_range,
        size_in_bytes: split.docs_size_in_bytes,
        tags,
        split_date_of_birth: split.split_date_of_birth,
        split_files,
        hotcache_bytes,
    };
    Ok(packaged_split)
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        batch: IndexedSplitBatch,
        ctx: &ActorContext<IndexedSplitBatch>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        info!(split_ids=?batch.splits.iter().map(|split| split.split_id.clone()).collect_vec(), "start-packaging-splits");
        for split in &batch.splits {
            if let Some(controlled_directory) = split.controlled_directory_opt.as_ref() {
                controlled_directory.set_progress_and_kill_switch(
                    ctx.progress().clone(),
                    ctx.kill_switch().clone(),
                );
            }
        }
        fail_point!("packager:before");
        let packaged_splits = batch
            .splits
            .into_iter()
            .map(|split| self.process_indexed_split(split, ctx))
            .try_collect()?;
        ctx.send_message_blocking(
            &self.uploader_mailbox,
            PackagedSplitBatch::new(packaged_splits),
        )?;
        fail_point!("packager:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
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
        let tag_field_1 = schema_builder.add_text_field("tag_field_1", TEXT);
        let tag_field_2 = schema_builder.add_text_field("tag_field_2", TEXT);
        let tags_field =
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
                for num in 1..10 {
                    let doc = doc!(
                        text_field => format!("timestamp is {}", timestamp),
                        timestamp_field => timestamp,
                        tag_field_1 => "value",
                        tag_field_2 => format!("value-{}", num),
                        tags_field => "tag_field_1:value",
                        tags_field => format!("tag_field_2:value-{}", num),
                    );
                    index_writer.add_document(doc)?;
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
        }
        // We don't commit, that's the job of the packager.
        //
        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_id: "test-split".to_string(),
            index_id: "test-index".to_string(),
            time_range: timerange_opt,
            demux_num_ops: 0,
            num_docs,
            docs_size_in_bytes: num_docs * 15, //< bogus number
            split_date_of_birth: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::from(10..20),
            replaced_split_ids: Vec::new(),
            controlled_directory_opt: None,
        };
        Ok(indexed_split)
    }

    fn get_tag_fields_map(schema: Schema, field_names: &[&str]) -> Vec<(String, Field)> {
        field_names
            .iter()
            .map(|field_name| {
                (
                    field_name.to_string(),
                    schema.get_field(field_name).unwrap(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn test_packager_no_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[&[1628203589, 1628203640]])?;
        let tag_fields_map = get_tag_fields_map(
            indexed_split.index.schema(),
            &["tag_field_1", "tag_field_2"],
        );
        let packager = Packager::new("TestPackager", tag_fields_map, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split],
                },
            )
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);

        let split = &packaged_splits[0].splits[0];
        assert_eq!(split.tags.len(), 2);
        assert!(split.tags.contains("tag_field_1:value"));
        assert!(split.tags.contains("tag_field_2:*"));
        Ok(())
    }

    #[tokio::test]
    async fn test_packager_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        let tag_fields_map = get_tag_fields_map(
            indexed_split.index.schema(),
            &["tag_field_1", "tag_field_2"],
        );
        let packager = Packager::new("TestPackager", tag_fields_map, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split],
                },
            )
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
    async fn test_package_two_indexed_split_and_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split_1 = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        let indexed_split_2 = make_indexed_split_for_test(&[&[1628204589], &[1629203640]])?;
        let tag_fields_map = get_tag_fields_map(
            indexed_split_1.index.schema(),
            &["tag_field_1", "tag_field_2"],
        );
        let packager = Packager::new("TestPackager", tag_fields_map, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn_sync();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split_1, indexed_split_2],
                },
            )
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        assert_eq!(packaged_splits.pop().unwrap().into_iter().count(), 2);
        Ok(())
    }
}
