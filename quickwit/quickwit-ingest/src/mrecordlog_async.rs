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

use std::io;
use std::ops::RangeBounds;
use std::path::Path;

use bytes::Buf;
use mrecordlog::error::*;
use mrecordlog::{MultiRecordLog, PersistAction, PersistPolicy, Record, ResourceUsage};
use tokio::task::JoinError;
use tracing::error;

/// A light wrapper to allow async operation in mrecordlog.
pub struct MultiRecordLogAsync {
    mrecordlog_opt: Option<MultiRecordLog>,
}

impl MultiRecordLogAsync {
    fn take(&mut self) -> MultiRecordLog {
        let Some(mrecordlog) = self.mrecordlog_opt.take() else {
            error!("wal is poisoned (on write), aborting process");
            std::process::abort();
        };
        mrecordlog
    }

    fn mrecordlog_ref(&self) -> &MultiRecordLog {
        let Some(mrecordlog) = &self.mrecordlog_opt else {
            error!("wal is poisoned (on read), aborting process");
            std::process::abort();
        };
        mrecordlog
    }

    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(directory_path, PersistPolicy::Always(PersistAction::Flush)).await
    }

    pub async fn open_with_prefs(
        directory_path: &Path,
        persist_policy: PersistPolicy,
    ) -> Result<Self, ReadRecordError> {
        let directory_path = directory_path.to_path_buf();
        let mrecordlog = tokio::task::spawn(async move {
            MultiRecordLog::open_with_prefs(&directory_path, persist_policy)
        })
        .await
        .map_err(|join_err| {
            error!(error=?join_err, "failed to load WAL");
            ReadRecordError::IoError(io::Error::other("loading wal from directory failed"))
        })??;
        Ok(Self {
            mrecordlog_opt: Some(mrecordlog),
        })
    }

    async fn run_operation<F, T>(&mut self, operation: F) -> T
    where
        F: FnOnce(&mut MultiRecordLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let mut mrecordlog = self.take();
        let join_res: Result<(T, MultiRecordLog), JoinError> =
            tokio::task::spawn_blocking(move || {
                let res = operation(&mut mrecordlog);
                (res, mrecordlog)
            })
            .await;
        match join_res {
            Ok((operation_result, mrecordlog)) => {
                self.mrecordlog_opt = Some(mrecordlog);
                operation_result
            }
            Err(join_error) => {
                // This could be caused by a panic
                error!(error=?join_error, "failed to run mrecordlog operation");
                panic!("failed to run mrecordlog operation");
            }
        }
    }

    pub async fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        let queue = queue.to_string();
        self.run_operation(move |mrecordlog| mrecordlog.create_queue(&queue))
            .await
    }

    pub async fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        let queue = queue.to_string();
        self.run_operation(move |mrecordlog| mrecordlog.delete_queue(&queue))
            .await
    }

    pub async fn append_records<T: Iterator<Item = impl Buf> + Send + 'static>(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payloads: T,
    ) -> Result<Option<u64>, AppendError> {
        let queue = queue.to_string();
        self.run_operation(move |mrecordlog| {
            mrecordlog.append_records(&queue, position_opt, payloads)
        })
        .await
    }

    #[track_caller]
    #[cfg(test)]
    pub fn assert_records_eq<R>(
        &self,
        queue_id: &str,
        range: R,
        expected_records: &[(u64, [u8; 2], &str)],
    ) where
        R: RangeBounds<u64> + 'static,
    {
        let records = self
            .range(queue_id, range)
            .unwrap()
            .map(|Record { position, payload }| {
                let header: [u8; 2] = payload[..2].try_into().unwrap();
                let payload = String::from_utf8(payload[2..].to_vec()).unwrap();
                (position, header, payload)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            records.len(),
            expected_records.len(),
            "expected {} records, got {}",
            expected_records.len(),
            records.len()
        );
        for ((position, header, payload), (expected_position, expected_header, expected_payload)) in
            records.iter().zip(expected_records.iter())
        {
            assert_eq!(
                position, expected_position,
                "expected record at position `{expected_position}`, got `{position}`",
            );
            assert_eq!(
                header, expected_header,
                "expected record header, `{expected_header:?}`, got `{header:?}`",
            );
            assert_eq!(
                payload, expected_payload,
                "expected record payload, `{expected_payload}`, got `{payload}`",
            );
        }
    }

    pub async fn truncate(&mut self, queue: &str, position: u64) -> Result<usize, TruncateError> {
        let queue = queue.to_string();
        self.run_operation(move |mrecordlog| mrecordlog.truncate(&queue, position))
            .await
    }

    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Result<impl Iterator<Item = Record<'_>> + '_, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        self.mrecordlog_ref().range(queue, range)
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.mrecordlog_ref().queue_exists(queue)
    }

    pub fn list_queues(&self) -> impl Iterator<Item = &str> {
        self.mrecordlog_ref().list_queues()
    }

    pub fn last_record(&self, queue: &str) -> Result<Option<Record<'_>>, MissingQueue> {
        self.mrecordlog_ref().last_record(queue)
    }

    pub fn resource_usage(&self) -> ResourceUsage {
        self.mrecordlog_ref().resource_usage()
    }

    pub fn summary(&self) -> mrecordlog::QueuesSummary {
        self.mrecordlog_ref().summary()
    }
}
