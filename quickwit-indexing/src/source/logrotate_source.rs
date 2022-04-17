// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::BTreeMap;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use glob::Pattern as GlobPattern;
use itertools::Either;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::LogRotateSourceParams;
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position};
use serde::Serialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeekExt, BufReader};
use tracing::{debug, trace, warn};

use crate::actors::Indexer;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, TypedSourceFactory};

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64;

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogRotatePosition {
    pub filename: String,
    pub is_next: bool,
    pub offset: u64,
    pub archived_dir: PathBuf,
    pub latest_file: PathBuf,
    #[serde(serialize_with = "serialize_glob")]
    pub name_pattern: GlobPattern,
}

fn serialize_glob<S>(pattern: &GlobPattern, s: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    s.serialize_str(pattern.as_str())
}

impl LogRotatePosition {
    /// Build a LogRotatePosition from the config and last recorded position.
    async fn from_position_and_config(
        position: &Position,
        config: &LogRotateSourceParams,
    ) -> Result<Self, anyhow::Error> {
        // @ is after :, so that filename@:0000123 (123B offset in the file after "filename", which
        // final name may not be know yet), is lexicographicaly after filename:0000999 (999B offset
        // in "filename")
        let position = if let Position::Offset(pos) = position {
            pos
        } else {
            return LogRotatePosition::init_from_config(config).await;
        };
        let (mut filename, offset) = position
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("Failed to parse LogRotatePosition"))?;
        let is_next = filename.ends_with('@');
        if is_next {
            // this won't panic because we know filename ends with an ascii char '+'
            filename = &filename[..filename.len() - 1];
        }
        let offset = offset.parse()?;
        let name_pattern = GlobPattern::new(&config.name_pattern)?;

        Ok(LogRotatePosition {
            filename: filename.to_owned(),
            is_next,
            offset,
            archived_dir: config.archive_dir(),
            latest_file: config.current_file.clone(),
            name_pattern,
        })
    }

    /// Build a LogRotatePosition from its config, assuming no previous document was recorded from
    /// this source.
    async fn init_from_config(config: &LogRotateSourceParams) -> Result<Self, anyhow::Error> {
        let name_pattern = GlobPattern::new(&config.name_pattern)?;

        let mut res = LogRotatePosition {
            filename: "".to_owned(),
            is_next: false,
            offset: 0,
            archived_dir: config.archive_dir(),
            latest_file: config.current_file.clone(),
            name_pattern,
        };
        res.next_file().await?;
        Ok(res)
    }

    /// Build the Position this has reached.
    fn to_position(&self) -> Position {
        let next_marker = if self.is_next { "@" } else { "" };
        let pos_string = format!("{}{}:{:0>20}", self.filename, next_marker, self.offset);
        Position::Offset(Arc::new(pos_string))
    }

    /// Update the position to the next file. Return Ok(true) if this function think you can
    /// read again, whether it's because it found a new file, or current file got changed. Returns
    /// Ok(false) if it has not changed the file currently being read because no new suitable file
    /// was found, and current file has already been read entirely.
    async fn next_file(&mut self) -> Result<bool, anyhow::Error> {
        debug!(
            "received request for finding file after {:?}",
            self.filepath()
        );
        if self.is_next {
            // we are already on self.latest_file, but maybe its name changed?
            trace!("we are operating on non-rotated file");
            if self.resolve_next().await? {
                trace!("the non-rotated file changed name, using it now");
                // its name changed, continue reading this file under it's new name.
                // Report as it being a new file so caller knows it should try reading,
                // but don't update offset so it reads from the same starting point.
                return Ok(true);
            } else {
                trace!("the non-rotated file hasn't changed name yet");
                // no new file available. Returns whether the file grew.
                return Ok(fs::metadata(&self.filepath()).await?.len() != self.offset);
            }
        }
        let next_file = find_next_file(
            &self.archived_dir,
            &self.filename,
            &self.name_pattern,
            &self.latest_file,
        )
        .await?;

        // if current file was updated between receiving the EOF which caused a call to next_file
        // and now, we don't actually want to go to the next file. However we do want to report
        // there is something to read.
        let meta = fs::metadata(&self.filepath()).await?;
        if meta.len() != self.offset && meta.is_file() {
            let len = fs::metadata(&self.filepath()).await?.len();
            trace!(
                "current file was modified, keeping it for the moment {} != {}",
                len,
                self.offset
            );
            return Ok(true);
        }

        // If find_next_file returned anything but Right(false), we know no new update can happen
        // to the file we were reading, so if its len has not changed, it won't in the future. We
        // can safely go to a new file.

        match next_file {
            Either::Left(filename) => {
                trace!("new file {} was found", filename);
                self.filename = filename;
                self.is_next = false;
                self.offset = 0;
                Ok(true)
            }
            Either::Right(true) => {
                trace!("no new file found, but non-rotated file isn't empty, reading from it");
                self.offset = 0;
                self.is_next = true;
                Ok(true)
            }
            Either::Right(false) => {
                trace!("all newer files are empty, don't switch to a newer fail yet");
                // we don't know for sure the daemon won't write here, as it did not start writing
                // in any other file yet. Assume this is still the last file, but that sleeping is
                // fine as no new data has arrived yet.
                Ok(false)
            }
        }
    }

    /// If the position has is_next set, try to resolve the name of the file if it's now available.
    /// Returns Ok(true) if the file transitioned from having an unknown path to a known path.
    async fn resolve_next(&mut self) -> Result<bool, anyhow::Error> {
        if !self.is_next {
            return Ok(false);
        }
        let next_file = find_next_file(
            &self.archived_dir,
            &self.filename,
            &self.name_pattern,
            &self.latest_file,
        )
        .await?;
        match next_file {
            Either::Left(filename) => {
                // we found the final name
                self.filename = filename;
                self.is_next = false;
                Ok(true)
            }
            Either::Right(true) => Ok(false),
            Either::Right(false) => {
                anyhow::bail!(
                    "find_next_file said it was fine to read {:?} after {}, but now it says it \
                     isn't",
                    self.latest_file,
                    self.filename
                );
            }
        }
    }

    /// verify if we are reading the file we should. Returns true if we are.
    /// If !is_next, it's necessarily the right file. Else it's the right file only if no
    /// other file appeared that we should read. This function must be called after opening
    /// self.latest_file in case log got rotated between the decision of resolve_next and
    /// the call to File::open.
    async fn revalidate_next_file(&self) -> Result<bool, anyhow::Error> {
        if !self.is_next {
            // this is definitely the right file
            return Ok(true);
        }
        let next_file = find_next_file(
            &self.archived_dir,
            &self.filename,
            &self.name_pattern,
            &self.latest_file,
        )
        .await?;
        let ok = matches!(next_file, Either::Right(true));
        Ok(ok)
    }

    /// The path of the file to read
    fn filepath(&self) -> PathBuf {
        if self.is_next {
            self.latest_file.clone()
        } else {
            let mut res = self.archived_dir.clone();
            res.push(&self.filename);
            res
        }
    }
}

/// Try to find the next file to read from.
/// Returns Ok(Left(filename)) if a filename was found, Ok(Right(true)) if no file was found,
/// and it's safe to read from latest_log_file, or Ok(Right(false)) when no file was found and
/// reading from latest_log_file could cause skipping some files.
async fn find_next_file(
    dir: &Path,
    current_file: &str,
    pattern: &GlobPattern,
    latest_log_file: &Path,
) -> Result<Either<String, bool>, io::Error> {
    // first check len of latest log file. Then read data about all file which could interest us.
    let len = fs::metadata(latest_log_file)
        .await
        .map(|f| f.len())
        .unwrap_or(0);
    let mut list_dir = fs::read_dir(dir).await?;
    let mut files = BTreeMap::new();
    let mut found_current_file = false;
    while let Some(entry) = list_dir.next_entry().await? {
        let filename = if let Ok(name) = entry.file_name().into_string() {
            name
        } else {
            continue;
        };
        if filename == current_file {
            found_current_file = true;
        }
        if pattern.matches(&filename) && filename.as_str() > current_file {
            // this is a log file which is newer than current file
            let len = entry.metadata().await?.len();
            files.insert(filename, len);
        }
    }

    if !found_current_file && !current_file.is_empty() {
        warn!(
            "File after '{}' was requested, but '{}' wasn't found, it's possible some files got \
             skipped",
            current_file, current_file
        );
    }

    let first_non_empty = files.into_iter().find(|(_, v)| *v > 0).map(|(k, _)| k);

    // If there is a non-empty file, we can skip right to it as any file before this one won't be
    // appended to.
    if let Some(first) = first_non_empty {
        Ok(Either::Left(first))
    } else {
        // if there is only empty file (including no file), and latest_log_file is non-empty,
        // none of the file will be appended to. It's fine to jump to latest_log_file. However if
        // everything is empty, we can't know which one the daemon might have kept open.
        // If logs are rotated right between fs::metadata and fs::read_dir, either
        // - latest_log_file was empty (and every new log file too), so we can't know which
        // log file the daemon might be writing to.
        // - latest_log_file wasn't empty, it is present in files, so files contains at least one
        // non-empty file, and we don't reach this branch.
        //
        // It this suggest opening latest_log_file, it's possible logs get rotated between call to
        // this function and File::open. The caller **must** revalidate that no file got moved
        // after calling open, but before reading anything.
        Ok(Either::Right(len > 0))
    }
}

pub struct LogRotateSource {
    source_id: String,
    params: LogRotateSourceParams,
    position: LogRotatePosition,
    reader: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    first_round_correction: bool,
}

impl LogRotateSource {
    async fn correct_first_round(
        &mut self,
        batch_sink: &Mailbox<Indexer>,
        ctx: &SourceContext,
    ) -> Result<(), anyhow::Error> {
        let start_pos = self.position.to_position();

        if !self.position.revalidate_next_file().await? {
            self.position.next_file().await?;
            // if revalidate returned false/invalid, there **must** be a new file,
            // so is_next must be unset.
            assert!(!self.position.is_next);
            let mut file = File::open(&self.position.filepath())
                .await
                .with_context(|| {
                    format!(
                        "Failed to open source file `{}`.",
                        self.position.filepath().display()
                    )
                })?;
            file.seek(SeekFrom::Start(self.position.offset))
                .await
                .context("failed to seek in file")?;
            self.reader = BufReader::new(Box::new(file));

            let partition_id =
                PartitionId::from(self.params.current_file.to_string_lossy().to_string());
            let checkpoint_delta = CheckpointDelta::from_partition_delta(
                partition_id,
                start_pos,
                self.position.to_position(),
            );
            trace!("sending empty delta to correct file name");
            let raw_doc_batch = RawDocBatch {
                docs: Vec::new(),
                checkpoint_delta,
            };
            ctx.send_message(batch_sink, raw_doc_batch).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Source for LogRotateSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<Indexer>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        if self.first_round_correction {
            self.first_round_correction = false;
            self.correct_first_round(batch_sink, ctx).await?;
        }
        // We collect batches of documents before sending them to the indexer.
        let start_pos = self.position.to_position();
        let mut docs = Vec::new();
        let mut read = 0;
        while read < BATCH_NUM_BYTES_THRESHOLD {
            let mut doc_line = String::new();
            let num_bytes = self
                .reader
                .read_line(&mut doc_line)
                .await
                .map_err(|io_err: io::Error| anyhow::anyhow!(io_err))?;
            if num_bytes == 0 {
                let should_read = self.position.next_file().await?;
                if should_read {
                    let mut file =
                        File::open(&self.position.filepath())
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to open source file `{}`.",
                                    self.position.filepath().display()
                                )
                            })?;
                    if !self.position.revalidate_next_file().await? {
                        self.position.next_file().await?;
                        // if revalidate returned false/invalid, there **must** be a new file,
                        // so is_next must be unset.
                        assert!(!self.position.is_next);
                        file = File::open(&self.position.filepath())
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to open source file `{}`.",
                                    self.position.filepath().display()
                                )
                            })?;
                    }
                    file.seek(SeekFrom::Start(self.position.offset))
                        .await
                        .context("failed to seek in file")?;
                    self.reader = BufReader::new(Box::new(file));
                    continue;
                } else {
                    break;
                }
            }
            trace!("some doc was read");
            docs.push(doc_line);
            read += num_bytes as u64;
            self.position.offset += num_bytes as u64;
        }

        if !docs.is_empty() {
            let partition_id =
                PartitionId::from(self.params.current_file.to_string_lossy().to_string());
            let checkpoint_delta = CheckpointDelta::from_partition_delta(
                partition_id,
                start_pos,
                self.position.to_position(),
            );
            trace!("sending {} new docs", docs.len());
            let raw_doc_batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            ctx.send_message(batch_sink, raw_doc_batch).await?;
        }

        if read >= BATCH_NUM_BYTES_THRESHOLD {
            Ok(Duration::default())
        } else {
            Ok(Duration::from_secs(5))
        }
    }

    fn name(&self) -> String {
        format!("LogRotateSource{{source_id={}}}", self.source_id)
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.position).unwrap()
    }
}

pub struct LogRotateSourceFactory;

#[async_trait]
impl TypedSourceFactory for LogRotateSourceFactory {
    type Source = LogRotateSource;
    type Params = LogRotateSourceParams;

    async fn typed_create_source(
        source_id: String,
        params: LogRotateSourceParams,
        checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
    ) -> anyhow::Result<LogRotateSource> {
        let partition_id = PartitionId::from(params.current_file.to_string_lossy().to_string());
        let position = checkpoint
            .position_for_partition(&partition_id)
            .unwrap_or(&Position::Beginning);

        let position = LogRotatePosition::from_position_and_config(position, &params).await?;

        let mut file = File::open(&position.filepath()).await.with_context(|| {
            format!(
                "Failed to open source file `{}`.",
                position.filepath().display()
            )
        })?;

        // if reading a is_next, and if logs got rotated between now and when last checkpoint,
        // it's possible that the file changed. This can't be corrected here as we have to send
        // a checkpoint delta to make position before and after log rotation match.
        let first_round_correction = !position.revalidate_next_file().await?;

        if !first_round_correction {
            file.seek(SeekFrom::Start(position.offset)).await?;
        }
        let reader = Box::new(file);

        let log_source = LogRotateSource {
            source_id,
            position,
            reader: BufReader::new(reader),
            params,
            first_round_correction,
        };
        Ok(log_source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // XXX add tests
}
