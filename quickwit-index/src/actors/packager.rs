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
use std::path::PathBuf;

use crate::models::IndexedSplit;
use crate::models::PackagedSplit;
use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SyncActor;
use quickwit_directories::write_hotcache;
use quickwit_directories::HOTCACHE_FILENAME;
use tantivy::SegmentId;
use tantivy::SegmentMeta;
use tracing::info;

pub struct Packager {
    sink: Mailbox<PackagedSplit>,
}

impl Packager {
    pub fn new(sink: Mailbox<PackagedSplit>) -> Self {
        Packager { sink }
    }
}

impl Actor for Packager {
    type Message = IndexedSplit;

    type ObservableState = ();

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
fn commit_split(split: &mut IndexedSplit) -> anyhow::Result<()> {
    info!("commit-split");
    split
        .index_writer
        .commit()
        .with_context(|| format!("Commit split `{:?}` failed", &split))?;
    Ok(())
}

fn list_files_to_upload(
    segment_metas: &[SegmentMeta],
    scratch_dir: &Path,
) -> anyhow::Result<Vec<(PathBuf, u64)>> {
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
    // We also need the meta.json file and the hotcache. Contrary to segment files,
    // we return an error here if they are missing.
    for relative_path in [Path::new("meta.json"), Path::new(HOTCACHE_FILENAME)]
        .iter()
        .cloned()
    {
        let filepath = scratch_dir.join(&relative_path);
        let metadata = std::fs::metadata(&filepath).with_context(|| {
            format!(
                "Failed to read metadata of mandatory file `{}`.",
                relative_path.display(),
            )
        })?;
        files_to_upload.push((filepath, metadata.len()));
    }
    Ok(files_to_upload)
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_if_required(split: &mut IndexedSplit) -> anyhow::Result<Vec<SegmentMeta>> {
    info!("merge-segments-if-required");
    let segment_metas_before_merge = split.index.searchable_segment_metas()?;
    if is_merge_required(&segment_metas_before_merge[..]) {
        let segment_ids: Vec<SegmentId> = segment_metas_before_merge
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        info!(segment_ids=?segment_ids, "merge");
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let segment_metas_after_merge: Vec<SegmentMeta> = split.index.searchable_segment_metas()?;
    Ok(segment_metas_after_merge)
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
    let files_to_upload = list_files_to_upload(&segment_metas[..], split.temp_dir.path())
        .with_context(|| {
            format!(
                "Failed to identify files for upload in packaging for split `{}`.",
                split.split_id
            )
        })?;
    let packaged_split = PackagedSplit {
        index_id: split.index_id.clone(),
        split_id: split.split_id.to_string(),
        split_scratch_dir: split.temp_dir,
        num_docs,
        segment_ids,
        time_range: split.time_range.clone(),
        size_in_bytes: split.size_in_bytes,
        files_to_upload,
    };
    Ok(packaged_split)
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        mut split: IndexedSplit,
        _context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        commit_split(&mut split)?;
        let segment_metas = merge_segments_if_required(&mut split)?;
        build_hotcache(split.temp_dir.path())?;
        let packaged_split = create_packaged_split(&segment_metas[..], split)?;
        self.sink.send_blocking(packaged_split)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::time::Instant;

    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;
    use quickwit_actors::Observation;
    use tantivy::doc;
    use tantivy::schema::Schema;
    use tantivy::schema::FAST;
    use tantivy::schema::TEXT;
    use tantivy::Index;

    use super::*;

    fn make_indexed_split_for_test(segments_timestamps: &[&[i64]]) -> anyhow::Result<IndexedSplit> {
        let temp_dir = tempfile::tempdir()?;
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let timestamp_field = schema_builder.add_u64_field("timestamp", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(temp_dir.path(), schema)?;
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
        // We don't emit the last segment, that's the job of the packager.
        // In reality, the indexer does not commit at all, but it may emit segments
        // if its memory budget is reached before the commit policy triggers.
        //
        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_id: "test-split".to_string(),
            index_id: "test-index".to_string(),
            time_range: timerange_opt,
            num_docs,
            size_in_bytes: num_docs * 15, //< bogus number
            start_time: Instant::now(),
            index,
            index_writer,
            temp_dir,
        };
        Ok(indexed_split)
    }

    #[tokio::test]
    async fn test_packager_no_merge_required() -> anyhow::Result<()> {
        crate::test_util::setup_logging_for_tests();
        let (mailbox, inbox) = create_test_mailbox();
        let packager = Packager::new(mailbox);
        let packager_handle = packager.spawn(KillSwitch::default());
        let indexed_split = make_indexed_split_for_test(&[&[1628203589, 1628203640]])?;
        packager_handle.mailbox().send_async(indexed_split).await?;
        assert_eq!(
            packager_handle.process_and_observe().await,
            Observation::Running(())
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_packager_merge_required() -> anyhow::Result<()> {
        crate::test_util::setup_logging_for_tests();
        let (mailbox, inbox) = create_test_mailbox();
        let packager = Packager::new(mailbox);
        let packager_handle = packager.spawn(KillSwitch::default());
        let indexed_split = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        packager_handle.mailbox().send_async(indexed_split).await?;
        assert_eq!(
            packager_handle.process_and_observe().await,
            Observation::Running(())
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        Ok(())
    }
}
