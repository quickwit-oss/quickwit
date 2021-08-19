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

use std::io;
use std::path::Path;

use crate::models::IndexedSplit;
use crate::models::PackagedSplit;
use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SyncActor;
use quickwit_common::HOTCACHE_FILENAME;
use quickwit_directories::write_hotcache;
use quickwit_storage::BundleStorageBuilder;
use quickwit_storage::BundleStorageOffsets;
use quickwit_storage::FileStatistics;
use quickwit_storage::BUNDLE_FILENAME;
use tantivy::SegmentId;
use tantivy::SegmentMeta;
use tracing::{debug, info};

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
        QueueCapacity::Bounded(1)
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

fn create_file_bundle(
    segment_metas: &[SegmentMeta],
    scratch_dir: &Path,
) -> anyhow::Result<(BundleStorageOffsets, FileStatistics)> {
    let mut files_to_upload = Vec::new();
    // list the segment files
    for segment_meta in segment_metas {
        for relative_path in segment_meta.list_files() {
            let filepath = scratch_dir.join(&relative_path);
            match std::fs::metadata(&filepath) {
                Ok(metadata) => {
                    files_to_upload.push((filepath, metadata.len()));
                }
                Err(io_err) => {
                    // If the file is missing, this is fine.
                    // segment_meta.list_files() may actually returns files that are
                    // do not exist.
                    if io_err.kind() != io::ErrorKind::NotFound {
                        return Err(io_err).with_context(|| {
                            format!(
                                "Failed to read metadata of segment file `{}`",
                                relative_path.display(),
                            )
                        })?;
                    }
                }
            }
        }
    }

    // create bundle
    let bundle_path = scratch_dir.join(BUNDLE_FILENAME);
    debug!("Creating Bundle {:?}", bundle_path,);

    let mut create_bundle = BundleStorageBuilder::new(&bundle_path)?;

    // We also need the meta.json file and the hotcache. Contrary to segment files,
    // we return an error here if they are missing.
    for relative_path in [Path::new("meta.json"), Path::new(HOTCACHE_FILENAME)]
        .iter()
        .cloned()
    {
        let filepath = scratch_dir.join(&relative_path);
        create_bundle.add_file(&filepath)?;
    }
    let file_statistics = create_bundle.file_statistics();

    let bundle_storage_offsets = create_bundle.finalize()?;
    Ok((bundle_storage_offsets, file_statistics))
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_if_required(
    split: &mut IndexedSplit,
    ctx: &ActorContext<IndexedSplit>,
) -> anyhow::Result<Vec<SegmentMeta>> {
    info!("merge-segments-if-required");
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

/// Extracts tags from the split.
///
/// Tags are constructed by combining field name and terms of the field in the form `field_name:term`.
/// For example: a split containing the terms [tokio, london, paris] for a field named `city`,
/// the list of extracted tags will be: [city:tokio, city:london, city:paris]  
fn extract_tags(split: &mut IndexedSplit) -> anyhow::Result<Vec<String>> {
    info!("extract-tags");
    let mut tags = vec![];
    let index_reader = split.index.reader()?;
    for reader in index_reader.searcher().segment_readers() {
        for (field_name, field) in split.tag_fields.iter() {
            let inv_index = reader.inverted_index(*field)?;
            let mut terms_streamer = inv_index.terms().stream()?;
            while let Some((term_data, _)) = terms_streamer.next() {
                tags.push(format!(
                    "{}:{}",
                    field_name,
                    String::from_utf8_lossy(term_data)
                ));
            }
        }
    }
    Ok(tags)
}

fn build_hotcache(path: &Path) -> anyhow::Result<()> {
    info!("build-hotcache");
    let hotcache_path = path.join(HOTCACHE_FILENAME);
    let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
    let mmap_directory = tantivy::directory::MmapDirectory::open(path)?;
    write_hotcache(mmap_directory, &mut hotcache_file)?;
    Ok(())
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
    tags: Vec<String>,
) -> anyhow::Result<PackagedSplit> {
    info!("create-packaged-split");
    let num_docs = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.num_docs() as u64)
        .sum();
    let segment_ids: Vec<SegmentId> = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.id())
        .collect();
    let (bundle_offsets, file_statistics) =
        create_file_bundle(segment_metas, split.split_scratch_directory.path()).with_context(
            || {
                format!(
                    "Failed to identify files for upload in packaging for split `{}`.",
                    split.split_id
                )
            },
        )?;
    let packaged_split = PackagedSplit {
        index_id: split.index_id,
        split_id: split.split_id.to_string(),
        checkpoint_delta: split.checkpoint_delta,
        split_scratch_directory: split.split_scratch_directory,
        num_docs,
        segment_ids,
        time_range: split.time_range,
        size_in_bytes: split.docs_size_in_bytes,
        bundle_offsets,
        file_statistics,
        tags,
    };
    Ok(packaged_split)
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        mut split: IndexedSplit,
        ctx: &ActorContext<IndexedSplit>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        commit_split(&mut split, ctx)?;
        let segment_metas = merge_segments_if_required(&mut split, ctx)?;
        let tags = extract_tags(&mut split)?;
        build_hotcache(split.split_scratch_directory.path())?;
        let packaged_split = create_packaged_split(&segment_metas[..], split, tags)?;
        ctx.send_message_blocking(&self.uploader_mailbox, packaged_split)?;
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
            tag_fields: vec![],
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
