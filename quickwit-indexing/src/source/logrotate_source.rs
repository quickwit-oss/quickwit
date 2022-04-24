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
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::LogRotateSourceParams;
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position, SourceCheckpoint};
use serde::Serialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeekExt, BufReader};
use tracing::{debug, trace, warn};

use crate::actors::Indexer;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, TypedSourceFactory};

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64;

/// Default name when no log file has been rotated yet. It must be before any name
/// any log file may have. This works under the assumption only printable name will
/// be used. Alternatively we could use \0 but it may mess with some debug print.
const START_FILE: &str = "        ";

/// Time to sleep when EOF was reached and there is no newer file to read yet.
const SLEEP_DURATION: Duration = Duration::from_secs(5);

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogRotatePosition {
    /// The name of the file being read from [`archived_dir`]
    pub filename: String,
    /// Define if we are reading [`filename`], or we are reading the file coming after it, but
    /// which final name is still unknown until next log rotation. If `is_next` is set, we are
    /// actually reading from [`latest_file`], and checkpoints contain a marker to show the file
    /// being processed is the one after [`filename`]
    pub is_next: bool,
    /// Offset in the file being read
    pub offset: u64,
    /// Path of the directory containing archived (rotated) logs
    pub archived_dir: PathBuf,
    /// Path of the current log file being writen to by a daemon. May or may not be in
    /// [`archived_dir`].
    pub latest_file: PathBuf,
    /// Pattern for files from [`archived_dir`] which should be considered as archived log files
    /// to be read. It shouldn't match [`latest_file`] if it is in [`archived_dir`].
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
            archived_dir: config
                .archive_dir()
                .ok_or_else(|| anyhow::anyhow!("Invalid archive path"))?,
            latest_file: config.current_file.clone(),
            name_pattern,
        })
    }

    /// Build a LogRotatePosition from its config, assuming no previous document was recorded from
    /// this source.
    async fn init_from_config(config: &LogRotateSourceParams) -> Result<Self, anyhow::Error> {
        let name_pattern = GlobPattern::new(&config.name_pattern)?;

        let mut res = LogRotatePosition {
            filename: START_FILE.to_owned(),
            is_next: false,
            offset: 0,
            archived_dir: config
                .archive_dir()
                .ok_or_else(|| anyhow::anyhow!("Invalid archive path"))?,
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

        if self.filename != START_FILE {
            // if current file was updated between receiving the EOF which caused a call to
            // next_file and now, we don't actually want to go to the next file. However
            // we do want to report there is something to read.
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
        }

        // If find_next_file returned anything but Right(false), we know no new update can happen
        // to the file we were reading, so if its len has not changed, it won't in the future. We
        // can safely go to a new file.

        match next_file {
            NextFile::Name(filename) => {
                trace!("new file {} was found", filename);
                self.filename = filename;
                self.is_next = false;
                self.offset = 0;
                Ok(true)
            }
            NextFile::CurrentFile => {
                trace!("no new file found, but non-rotated file isn't empty, reading from it");
                self.offset = 0;
                self.is_next = true;
                Ok(true)
            }
            NextFile::Unknown => {
                trace!("all newer files are empty, don't switch to a newer fail yet");
                // we don't know for sure the daemon won't write here, as it did not start writing
                // in any other file yet. Assume this is still the last file, but that sleeping is
                // fine as no new data has arrived yet.
                Ok(false)
            }
        }
    }

    /// Try to resolve the name of the file if it's now available. Returns Ok(true) if the file
    /// name is now known.
    ///
    /// # Panics
    ///
    /// Panics if the name of the file being processed was already known before.
    async fn resolve_next(&mut self) -> Result<bool, anyhow::Error> {
        assert!(self.is_next);

        let next_file = find_next_file(
            &self.archived_dir,
            &self.filename,
            &self.name_pattern,
            &self.latest_file,
        )
        .await?;
        match next_file {
            NextFile::Name(filename) => {
                // we found the final name
                self.filename = filename;
                self.is_next = false;
                Ok(true)
            }
            NextFile::CurrentFile => Ok(false),
            NextFile::Unknown => {
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
        let ok = matches!(next_file, NextFile::CurrentFile);
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

enum NextFile {
    Name(String),
    CurrentFile,
    Unknown,
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
) -> Result<NextFile, io::Error> {
    // first check len of latest log file. Then read data about all file which could interest us.
    let len = fs::metadata(latest_log_file)
        .await
        .map(|f| f.len())
        .unwrap_or(0);
    let mut list_dir = match fs::read_dir(dir).await {
        Ok(list_dir) => list_dir,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // log hasn't been rotated yet (archive dir wasn't created), reading from
            // latest_log_file is safe
            return Ok(NextFile::CurrentFile);
        }
        e => e?,
    };
    let mut files = BTreeMap::new();
    let mut found_current_file = false;
    let mut found_any_log = false;
    while let Some(entry) = list_dir.next_entry().await? {
        let filename = if let Ok(name) = entry.file_name().into_string() {
            name
        } else {
            continue;
        };
        if filename == current_file {
            found_current_file = true;
        }
        if pattern.matches(&filename) {
            found_any_log = true;
            if filename.as_str() > current_file {
                // this is a log file which is newer than current file
                let len = entry.metadata().await?.len();
                files.insert(filename, len);
            }
        }
    }

    if !found_current_file && current_file != START_FILE {
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
        Ok(NextFile::Name(first))
    } else {
        // log has never been rotated yet, reading from latest_log_file is safe
        if current_file == START_FILE && !found_any_log {
            return Ok(NextFile::CurrentFile);
        }
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
        if len != 0 {
            Ok(NextFile::CurrentFile)
        } else {
            Ok(NextFile::Unknown)
        }
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
            if num_bytes != 0 {
                trace!("some doc was read");
                docs.push(doc_line);
                read += num_bytes as u64;
                self.position.offset += num_bytes as u64;
                continue;
            }
            // we reached EOF.
            let should_read = self.position.next_file().await?;
            if !should_read {
                break;
            }
            // there is a new file to read, open it and continue from there.
            let mut file = File::open(&self.position.filepath())
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
            Ok(SLEEP_DURATION)
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
        checkpoint: SourceCheckpoint,
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
    use quickwit_actors::{create_test_mailbox, Command, ObservationType, Universe};
    use quickwit_metastore::checkpoint::SourceCheckpoint;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::source::SourceActor;

    async fn position_round_trip(pos: &LogRotatePosition, params: &LogRotateSourceParams) {
        let position = pos.to_position();
        let parsed = LogRotatePosition::from_position_and_config(&position, params)
            .await
            .unwrap();
        assert_eq!(*pos, parsed);
    }

    #[tokio::test]
    async fn test_partition_ordering() {
        let mut pos_list = Vec::new();

        let params = LogRotateSourceParams {
            // no I/O is made for this test, we can use any path
            current_file: "/tmp/file.log".into(),
            archive_dir: None,
            name_pattern: "file.*.log".to_owned(),
        };

        let mut pos = LogRotatePosition {
            filename: "file.1.log".to_string(),
            is_next: false,
            offset: 0,
            archived_dir: params.archive_dir().unwrap(),
            latest_file: params.current_file.clone(),
            name_pattern: GlobPattern::new(&params.name_pattern).unwrap(),
        };

        position_round_trip(&pos, &params).await;
        pos_list.push(pos.to_position());
        for i in 0..63 {
            pos.offset = 1 << i;
            pos_list.push(pos.to_position());
            position_round_trip(&pos, &params).await;
        }
        pos.offset = u64::MAX;
        pos_list.push(pos.to_position());
        position_round_trip(&pos, &params).await;

        pos.offset = 0;
        pos.is_next = true;
        position_round_trip(&pos, &params).await;
        pos_list.push(pos.to_position());
        for i in 0..63 {
            pos.offset = 1 << i;
            pos_list.push(pos.to_position());
            position_round_trip(&pos, &params).await;
        }
        pos.offset = u64::MAX;
        pos_list.push(pos.to_position());
        position_round_trip(&pos, &params).await;

        pos.offset = 0;
        pos.filename = "file.2.log".to_owned();
        pos.is_next = false;
        pos_list.push(pos.to_position());
        position_round_trip(&pos, &params).await;
        pos.offset = 1;
        pos_list.push(pos.to_position());
        position_round_trip(&pos, &params).await;

        pos.offset = 0;
        pos.is_next = true;
        pos_list.push(pos.to_position());
        position_round_trip(&pos, &params).await;

        let sorted = pos_list[1..]
            .iter()
            .try_fold(pos_list[0].clone(), |previous, current| {
                if &previous > current {
                    Err(())
                } else {
                    Ok(current.clone())
                }
            })
            .is_ok();
        assert!(sorted);
    }

    enum Operation {
        AppendDoc(usize),
        Rotate,
        NoticeRotation,
        Step,
        Start,
        AssertState {
            file_id: usize,
            is_next: bool,
            offset: u64,
        },
    }

    // 20 for the digits, one for line break
    const DOC_SIZE: u64 = 21;

    struct LogWriter {
        params: LogRotateSourceParams,
        current_writer: fs::File,
        doc_id: usize,
        next_file_id: usize,
    }

    impl LogWriter {
        async fn new(params: LogRotateSourceParams) -> Result<Self, io::Error> {
            let current_writer = File::create(&params.current_file).await?;
            Ok(LogWriter {
                params,
                current_writer,
                doc_id: 0,
                next_file_id: 0,
            })
        }

        async fn append(&mut self, num_doc: usize) -> Result<(), io::Error> {
            for _ in 0..num_doc {
                self.current_writer
                    .write_all(format!("{:020}\n", self.doc_id).as_bytes())
                    .await?;
                self.doc_id += 1;
            }
            self.current_writer.flush().await
        }

        async fn rotate(&mut self) -> Result<(), io::Error> {
            fs::create_dir_all(self.params.archive_dir().unwrap()).await?;
            fs::rename(
                &self.params.current_file,
                self.params
                    .archive_dir()
                    .unwrap()
                    .join(format!("file.{}.log", self.next_file_id)),
            )
            .await?;
            self.next_file_id += 1;
            File::create(&self.params.current_file).await?;
            Ok(())
        }

        async fn notice_rotation(&mut self) -> Result<(), io::Error> {
            self.current_writer.flush().await?;
            self.current_writer = fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.params.current_file)
                .await?;
            Ok(())
        }
    }

    // run a scenario, returning the docs that got indexed, whose content is a single sequential
    // id on 20 digits.
    async fn run_scenario(
        scenario: &[Operation],
        initial_pos: Option<Position>,
        separate_archive: bool,
    ) -> anyhow::Result<Vec<String>> {
        quickwit_common::setup_logging_for_tests();

        let mut scenario = scenario.iter();

        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        use tempfile::TempDir;
        let temp_dir = TempDir::new()?;

        let archive_dir = if separate_archive {
            Some(temp_dir.path().join("archive"))
        } else {
            None
        };

        let params = LogRotateSourceParams {
            current_file: temp_dir.path().join("file.log"),
            archive_dir,
            name_pattern: "file.*.log".to_owned(),
        };

        let mut log_writer = LogWriter::new(params.clone()).await?;

        let mut checkpoint = SourceCheckpoint::default();
        if let Some(pos) = initial_pos {
            let partition_id = PartitionId::from(params.current_file.to_string_lossy().to_string());
            let checkpoint_delta =
                CheckpointDelta::from_partition_delta(partition_id, Position::Beginning, pos);
            checkpoint.try_apply_delta(checkpoint_delta)?;
        }

        for action in scenario.by_ref() {
            use Operation::*;
            match action {
                Start => break,
                AppendDoc(n) => log_writer.append(*n).await?,
                Rotate => log_writer.rotate().await?,
                NoticeRotation => log_writer.notice_rotation().await?,
                Step => anyhow::bail!("Step must not be used before Start"),
                AssertState { .. } => anyhow::bail!("AssertStarte must not be used before Start"),
            }
        }

        let source = LogRotateSourceFactory::typed_create_source(
            "my-logrotate-source".to_string(),
            params.clone(),
            checkpoint.clone(),
        )
        .await?;

        let logrotate_source_actor = SourceActor {
            source: Box::new(source),
            batch_sink: mailbox,
        };

        let (_logrotate_source_mailbox, logrotate_source_handle) =
            universe.spawn_actor(logrotate_source_actor).spawn();

        logrotate_source_handle.pause().await;

        for action in scenario {
            use Operation::*;
            match action {
                Start => anyhow::bail!("Start must not be used multiple times"),
                AppendDoc(n) => log_writer.append(*n).await?,
                Rotate => log_writer.rotate().await?,
                NoticeRotation => log_writer.notice_rotation().await?,
                Step => {
                    universe.simulate_time_shift(SLEEP_DURATION).await;
                    logrotate_source_handle.resume().await;
                    let mut observation =
                        logrotate_source_handle.process_pending_and_observe().await;
                    assert_eq!(observation.obs_type, ObservationType::Alive);
                    let mut count = 0;
                    loop {
                        // when testing with many docs, a single call to process_pending_and_observe
                        // isn't enough, and lead to spurious failures because there is still
                        // processing to be done.
                        let new_observation =
                            logrotate_source_handle.process_pending_and_observe().await;
                        if observation == new_observation {
                            break;
                        }
                        observation = new_observation;
                        assert_eq!(observation.obs_type, ObservationType::Alive);
                        count += 1;
                        assert!(count < 1000, "test looped");
                    }
                    logrotate_source_handle.pause().await;
                }
                AssertState {
                    file_id,
                    is_next,
                    offset,
                } => {
                    let observation = logrotate_source_handle.observe().await;
                    let expected = serde_json::json!({
                        "filename": format!("file.{}.log", file_id),
                        "is_next": is_next,
                        "offset": offset,
                        "archived_dir": params.archive_dir(),
                        "latest_file": params.current_file,
                        "name_pattern": params.name_pattern,
                    });
                    assert_eq!(observation.obs_type, ObservationType::Alive);
                    assert_eq!(observation.state, expected);
                }
            }
        }

        let mut result = Vec::new();
        let indexer_msgs = inbox.drain_for_test();
        for msg in indexer_msgs {
            if let Some(doc_batch) = msg.downcast_ref::<RawDocBatch>() {
                checkpoint.try_apply_delta(doc_batch.checkpoint_delta.clone())?;
                result.extend_from_slice(&doc_batch.docs);
                continue;
            }
            if msg.downcast_ref::<Command>().is_some() {
                continue;
            }
            anyhow::bail!("Received unexpected message");
        }
        Ok(result)
    }

    #[tokio::test]
    async fn test_logrotate_source_preexisting_files() -> anyhow::Result<()> {
        use Operation::*;
        let scenario = vec![
            AppendDoc(5),
            Rotate,
            NoticeRotation,
            AppendDoc(5),
            Rotate,
            NoticeRotation,
            AppendDoc(5),
            Start,
            Step,
            AssertState {
                file_id: 1,
                is_next: true,
                offset: DOC_SIZE * 5,
            },
        ];

        let res = run_scenario(&scenario, None, false).await?;
        assert_eq!(res.len(), 15);

        let res = run_scenario(&scenario, None, true).await?;
        assert_eq!(res.len(), 15);
        Ok(())
    }

    #[tokio::test]
    async fn test_logrotate_source_preexisting_files_long() -> anyhow::Result<()> {
        use Operation::*;
        let scenario = vec![
            AppendDoc(500_000),
            Rotate,
            NoticeRotation,
            AppendDoc(500_000),
            Rotate,
            NoticeRotation,
            AppendDoc(500_000),
            Start,
            Step,
            AssertState {
                file_id: 1,
                is_next: true,
                offset: DOC_SIZE * 500_000,
            },
        ];

        let res = run_scenario(&scenario, None, false).await?;
        assert_eq!(res.len(), 500_000 * 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_logrotate_source_appearing_content() -> anyhow::Result<()> {
        use Operation::*;
        let scenario = vec![
            Start, // start while no file got rotated yet
            Step,
            AppendDoc(2), // push 2 doc to #0
            Rotate,       // rotate #0 (2 docs)
            Step,
            Rotate, // rotate #1 (empty)
            Step,
            NoticeRotation,
            Step,
            AppendDoc(2), // push 2 doc to #2
            Step,
            AppendDoc(2), // push 2 doc to #2
            Step,
            Rotate, // rotate #2 (4 docs)
            Step,
            AppendDoc(2), // push 2 doc to #2
            Step,
            NoticeRotation, // finaly stop writing to #2 (6 docs)
            Step,
            AppendDoc(2), // push 2 doc to #3
            Step,
        ];

        let res = run_scenario(&scenario, None, false).await?;
        assert_eq!(res.len(), 10);

        let res = run_scenario(&scenario, None, true).await?;
        assert_eq!(res.len(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_logrotate_source_appearing_contenat_long() -> anyhow::Result<()> {
        use Operation::*;
        let scenario = vec![
            Start,
            Step,
            AppendDoc(200_000),
            Rotate,
            Step,
            Rotate,
            Step,
            NoticeRotation,
            Step,
            AppendDoc(200_000),
            Step,
            AppendDoc(200_000),
            Step,
            Rotate,
            Step,
            AppendDoc(200_000),
            Step,
            NoticeRotation,
            Step,
            AppendDoc(200_000),
            Step,
        ];

        let res = run_scenario(&scenario, None, false).await?;
        assert_eq!(res.len(), 200_000 * 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_logrotate_source_checkpoint() -> anyhow::Result<()> {
        use Operation::*;
        let scenario = vec![
            AppendDoc(3), // prefill #0 with 3 docs
            Rotate,
            NoticeRotation,
            AppendDoc(3), // prefill #1 with 3 docs
            Rotate,
            NoticeRotation,
            AppendDoc(2), // prefill #2 (aka 1+is_next) with 2 docs
            // checkpoint here
            AppendDoc(1), // push 1 doc to #2 (aka 1+is_next)
            Rotate,
            NoticeRotation,
            Rotate,
            NoticeRotation, // empty #3
            AppendDoc(2),   // push 2 doc to #4
            Start,
            Step,
            Rotate,
            NoticeRotation, // empty #3
            AppendDoc(2),   // push 2 doc to #5
            Step,
        ];

        // try with is_next = true
        let start_pos = LogRotatePosition {
            filename: "file.1.log".to_string(),
            is_next: true,
            offset: DOC_SIZE * 2,
            archived_dir: PathBuf::new(),
            latest_file: PathBuf::new(),
            name_pattern: GlobPattern::new("").unwrap(),
        };
        let res = run_scenario(&scenario, Some(start_pos.to_position()), false).await?;
        assert_eq!(res.len(), 5);

        // try with is_next = false
        let start_pos = LogRotatePosition {
            filename: "file.2.log".to_string(),
            is_next: false,
            offset: DOC_SIZE * 2,
            archived_dir: PathBuf::new(),
            latest_file: PathBuf::new(),
            name_pattern: GlobPattern::new("").unwrap(),
        };
        let res = run_scenario(&scenario, Some(start_pos.to_position()), true).await?;
        assert_eq!(res.len(), 5);
        Ok(())
    }
}
