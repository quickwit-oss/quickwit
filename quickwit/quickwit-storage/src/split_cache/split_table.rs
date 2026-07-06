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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use quickwit_proto::types::SplitId;

use crate::metrics::SEARCHER_SPLIT_CACHE;

type LastAccessDate = u64;

/// Maximum number of splits to track.
const MAX_NUM_CANDIDATES: usize = 1_000;

/// Splits that are freshly reported get a last access time of `now - NEWLY_REPORT_SPLIT_LAST_TIME`.
const NEWLY_REPORTED_SPLIT_LAST_TIME: Duration = Duration::from_secs(60 * 10); // 10mn

#[derive(Clone)]
pub(crate) struct SplitKey {
    pub last_accessed: LastAccessDate,
    pub split_id: SplitId,
}

impl PartialOrd for SplitKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SplitKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.last_accessed, &self.split_id).cmp(&(other.last_accessed, &other.split_id))
    }
}

impl PartialEq for SplitKey {
    fn eq(&self, other: &Self) -> bool {
        (self.last_accessed, &self.split_id) == (other.last_accessed, &other.split_id)
    }
}

impl Eq for SplitKey {}

#[derive(Clone, Debug)]
enum Status {
    Candidate(CandidateSplit),
    Downloading { alive_token: Weak<()> },
    OnDisk { num_bytes: u64 },
}

impl PartialEq for Status {
    fn eq(&self, other: &Status) -> bool {
        match (self, other) {
            (Status::Candidate(candidate_split), Status::Candidate(other_candidate_split)) => {
                candidate_split == other_candidate_split
            }
            (Status::Downloading { .. }, Status::Downloading { .. }) => true,
            (
                Status::OnDisk { num_bytes },
                Status::OnDisk {
                    num_bytes: other_num_bytes,
                },
            ) => num_bytes == other_num_bytes,
            _ => false,
        }
    }
}

pub struct SplitInfo {
    pub(crate) split_key: SplitKey,
    status: Status,
}

/// The split table keeps track of splits we know about (regardless of whether they have already
/// been downloaded or not).
///
/// Invariant:
/// Each split appearing into split_to_status, should be listed 1 and exactly once in the
/// either
/// - on_disk_splits
/// - downloading_splits
/// - candidate_splits.
///
/// It is possible for the split table size in bytes to exceed its limits, by at
/// most one split.
pub struct SplitTable {
    on_disk_splits: BTreeSet<SplitKey>,
    downloading_splits: BTreeSet<SplitKey>,
    candidate_splits: BTreeSet<SplitKey>,
    split_to_status: HashMap<SplitId, SplitInfo>,
    origin_time: Instant,
    limits: SplitCacheLimits,
    on_disk_bytes: u64,
}

impl SplitTable {
    pub(crate) fn with_limits_and_existing_splits(
        limits: SplitCacheLimits,
        existing_filepaths: BTreeMap<SplitId, u64>,
    ) -> SplitTable {
        let origin_time = Instant::now() - NEWLY_REPORTED_SPLIT_LAST_TIME;
        let mut split_table = SplitTable {
            on_disk_splits: BTreeSet::default(),
            candidate_splits: BTreeSet::default(),
            downloading_splits: BTreeSet::default(),
            split_to_status: HashMap::default(),
            origin_time,
            limits,
            on_disk_bytes: 0u64,
        };
        split_table.acknowledge_on_disk_splits(existing_filepaths);
        split_table
    }

    fn acknowledge_on_disk_splits(&mut self, existing_filepaths: BTreeMap<SplitId, u64>) {
        for (split_id, num_bytes) in existing_filepaths {
            let split_info = SplitInfo {
                split_key: SplitKey {
                    last_accessed: 0,
                    split_id,
                },
                status: Status::OnDisk { num_bytes },
            };
            self.insert(split_info);
        }
    }
}

fn compute_timestamp(start: Instant) -> LastAccessDate {
    start.elapsed().as_micros() as u64
}

impl SplitTable {
    fn remove(&mut self, split_id: &SplitId) -> Option<SplitInfo> {
        let split_info = self.split_to_status.remove(split_id)?;
        let split_queue: &mut BTreeSet<SplitKey> = match split_info.status {
            Status::Candidate { .. } => &mut self.candidate_splits,
            Status::Downloading { .. } => &mut self.downloading_splits,
            Status::OnDisk { num_bytes } => {
                self.on_disk_bytes -= num_bytes;
                SEARCHER_SPLIT_CACHE.cache_metrics.in_cache_count.dec();
                SEARCHER_SPLIT_CACHE
                    .cache_metrics
                    .in_cache_num_bytes
                    .dec_by(num_bytes as f64);
                SEARCHER_SPLIT_CACHE.cache_metrics.evict_num_items.inc();
                SEARCHER_SPLIT_CACHE
                    .cache_metrics
                    .evict_num_bytes
                    .inc_by(num_bytes);
                &mut self.on_disk_splits
            }
        };
        let is_in_queue = split_queue.remove(&split_info.split_key);
        assert!(is_in_queue);
        if let Status::Downloading { alive_token } = &split_info.status
            && alive_token.strong_count() == 0
        {
            return None;
        }
        Some(split_info)
    }

    fn gc_downloading_splits_if_necessary(&mut self) {
        if self.downloading_splits.len()
            < (self.limits.num_concurrent_downloads.get() as usize + 10)
        {
            return;
        }
        let mut splits_to_remove = Vec::new();
        for split in &self.downloading_splits {
            if let Some(split_info) = self.split_to_status.get(&split.split_id)
                && let Status::Downloading { alive_token } = &split_info.status
                && alive_token.strong_count() == 0
            {
                splits_to_remove.push(split.split_id.clone());
            }
        }
        for split in splits_to_remove {
            self.remove(&split);
        }
    }

    /// Insert a `split_info`. This methods assumes the split was not present in the split table
    /// to begin with. It will panic if the split was already present.
    ///
    /// Keep this method private.
    fn insert(&mut self, split_info: SplitInfo) {
        let was_not_in_queue = match split_info.status {
            Status::Candidate { .. } => {
                // we truncate *before* inserting, otherwise way may end up in an inconsistent
                // state which make truncate_candidate_list loop indefinitely
                self.truncate_candidate_list();
                self.candidate_splits.insert(split_info.split_key.clone())
            }
            Status::Downloading { .. } => {
                self.downloading_splits.insert(split_info.split_key.clone())
            }
            Status::OnDisk { num_bytes } => {
                self.on_disk_bytes += num_bytes;
                SEARCHER_SPLIT_CACHE.cache_metrics.in_cache_count.inc();
                SEARCHER_SPLIT_CACHE
                    .cache_metrics
                    .in_cache_num_bytes
                    .inc_by(num_bytes as f64);
                self.on_disk_splits.insert(split_info.split_key.clone())
            }
        };
        // this is fine to do in an inconsistent state, the last entry will just be ignored while
        // gcing
        self.gc_downloading_splits_if_necessary();
        assert!(was_not_in_queue);
        let split_id_was_absent = self
            .split_to_status
            .insert(split_info.split_key.split_id.clone(), split_info)
            .is_none();
        assert!(split_id_was_absent);
    }

    /// Touch the file, updating its last access time, possibly extending its life in the
    /// cache (if in cache).
    ///
    /// If the file is already on the disk cache, return `Some(num_bytes)`.
    /// If the file is not in cache, return `None`, and register the file in the candidate for
    /// download list.
    pub fn touch(&mut self, split_id: SplitId, storage_uri: &Uri) -> Option<u64> {
        let timestamp = compute_timestamp(self.origin_time);
        let status = self.mutate_split(split_id, |old_split_info, split_id| {
            if let Some(mut split_info) = old_split_info {
                split_info.split_key.last_accessed = timestamp;
                split_info
            } else {
                SplitInfo {
                    split_key: SplitKey {
                        split_id: split_id.clone(),
                        last_accessed: timestamp,
                    },
                    status: Status::Candidate(CandidateSplit {
                        storage_uri: storage_uri.clone(),
                        split_id,
                        living_token: Arc::new(()),
                    }),
                }
            }
        });
        if let Status::OnDisk { num_bytes } = status {
            Some(num_bytes)
        } else {
            None
        }
    }

    /// Mutates the split with the given id.
    ///
    /// By design this function maintains the invariant.
    /// It removes the split with the given id, modifies it, and re-inserts it.
    /// The owned `split_id` is handed to `mutate_fn` so it can be reused when building a fresh
    /// `SplitInfo` (avoiding an extra allocation).
    fn mutate_split(
        &mut self,
        split_id: SplitId,
        mutate_fn: impl FnOnce(Option<SplitInfo>, SplitId) -> SplitInfo,
    ) -> Status {
        let split_info_opt = self.remove(&split_id);
        let new_split: SplitInfo = mutate_fn(split_info_opt, split_id);
        let new_status = new_split.status.clone();
        self.insert(new_split);
        new_status
    }

    fn change_split_status(&mut self, split_id: SplitId, status: Status) {
        let start_time = self.origin_time;
        self.mutate_split(split_id, move |split_info_opt, split_id| {
            if let Some(mut split_info) = split_info_opt {
                split_info.status = status;
                split_info
            } else {
                SplitInfo {
                    split_key: SplitKey {
                        last_accessed: compute_timestamp(start_time),
                        split_id,
                    },
                    status,
                }
            }
        });
    }

    pub(crate) fn report(&mut self, split_id: SplitId, storage_uri: Uri) {
        let origin_time = self.origin_time;
        self.mutate_split(split_id, move |split_info_opt, split_id| {
            if let Some(split_info) = split_info_opt {
                return split_info;
            }
            SplitInfo {
                split_key: SplitKey {
                    last_accessed: compute_timestamp(origin_time)
                        .saturating_sub(NEWLY_REPORTED_SPLIT_LAST_TIME.as_micros() as u64),
                    split_id: split_id.clone(),
                },
                status: Status::Candidate(CandidateSplit {
                    storage_uri,
                    split_id,
                    living_token: Arc::new(()),
                }),
            }
        });
    }

    /// Make sure we have at most `MAX_CANDIDATES` candidate splits.
    fn truncate_candidate_list(&mut self) {
        // we remove one more to make place for one candidate about to be inserted
        while self.candidate_splits.len() >= MAX_NUM_CANDIDATES {
            let worst_candidate = self.candidate_splits.first().unwrap().split_id.clone();
            self.remove(&worst_candidate);
        }
    }

    pub(crate) fn register_as_downloaded(&mut self, split_id: SplitId, num_bytes: u64) {
        self.change_split_status(split_id, Status::OnDisk { num_bytes });
    }

    /// Change the state of the given split from candidate to downloading state,
    /// and returns its URI.
    ///
    /// This function does NOT trigger the download itself. It is up to
    /// the caller to actually initiate the download.
    pub(crate) fn start_download(&mut self, split_id: &SplitId) -> Option<CandidateSplit> {
        let split_info = self.remove(split_id)?;
        let Status::Candidate(candidate_split) = split_info.status else {
            self.insert(split_info);
            return None;
        };
        let alive_token = Arc::downgrade(&candidate_split.living_token);
        self.insert(SplitInfo {
            split_key: split_info.split_key,
            status: Status::Downloading { alive_token },
        });
        Some(candidate_split)
    }

    fn best_candidate(&self) -> Option<SplitKey> {
        self.candidate_splits.last().cloned()
    }

    fn is_out_of_limits(&self) -> bool {
        if self.on_disk_splits.is_empty() {
            return false;
        }
        if self.on_disk_splits.len() + self.downloading_splits.len()
            >= self.limits.max_num_splits.get() as usize
        {
            return true;
        }
        if self.on_disk_bytes > self.limits.max_num_bytes.as_u64() {
            return true;
        }
        false
    }

    /// Evicts splits to reach the target limits.
    ///
    /// Returns false if the first candidate for eviction is
    /// fresher that the candidate split. (Note this is suboptimal.
    ///
    /// Returns `None` if this would mean evicting splits that
    /// have been accessed more recently than the candidate split.
    pub(crate) fn make_room_for_split_if_necessary(
        &mut self,
        last_access_date: LastAccessDate,
    ) -> Result<Vec<SplitId>, NoRoomAvailable> {
        let mut split_infos = Vec::new();
        while self.is_out_of_limits() {
            // We clone the oldest split's key so we can drop the immutable borrow on
            // `on_disk_splits` before calling `remove`, which needs `&mut self`.
            let oldest_split_key_opt: Option<SplitKey> = self.on_disk_splits.first().cloned();
            if let Some(oldest_split_key) = oldest_split_key_opt {
                if oldest_split_key.last_accessed > last_access_date {
                    // This is not worth doing the eviction.
                    break;
                }
                split_infos.extend(self.remove(&oldest_split_key.split_id));
            } else {
                break;
            }
        }
        if self.is_out_of_limits() {
            // We are still out of limits.
            // Let's not go through with the eviction, and reinsert the splits.
            for split_info in split_infos {
                self.insert(split_info);
            }
            Err(NoRoomAvailable)
        } else {
            Ok(split_infos
                .into_iter()
                .map(|split_info| split_info.split_key.split_id)
                .collect())
        }
    }

    pub(crate) fn find_download_opportunity(&mut self) -> Option<DownloadOpportunity> {
        let best_candidate_split_key = self.best_candidate()?;
        let splits_to_delete: Vec<SplitId> = self
            .make_room_for_split_if_necessary(best_candidate_split_key.last_accessed)
            .ok()?;
        let split_to_download: CandidateSplit =
            self.start_download(&best_candidate_split_key.split_id)?;
        Some(DownloadOpportunity {
            splits_to_delete,
            split_to_download,
        })
    }

    #[cfg(test)]
    pub fn num_bytes(&self) -> u64 {
        self.on_disk_bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NoRoomAvailable;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateSplit {
    pub storage_uri: Uri,
    pub split_id: SplitId,
    pub living_token: Arc<()>,
}

pub(crate) struct DownloadOpportunity {
    // At this point, the split have already been removed from the split table.
    // The file however need to be deleted.
    pub splits_to_delete: Vec<SplitId>,
    pub split_to_download: CandidateSplit,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use bytesize::ByteSize;
    use quickwit_common::uri::Uri;
    use quickwit_config::SplitCacheLimits;
    use quickwit_proto::types::SplitId;
    use ulid::Ulid;

    use crate::split_cache::split_table::{
        CandidateSplit, DownloadOpportunity, SplitInfo, SplitKey, SplitTable, Status,
    };

    const TEST_STORAGE_URI: &str = "s3://test";

    /// Generates split ids backed by ULIDs. We keep using ULIDs in tests because the cache's
    /// ordering relies on lexicographic ordering of the ids, and ULID strings happen to be
    /// time-sortable — which makes for readable, deterministic tests.
    fn new_test_split_id() -> SplitId {
        SplitId::from(Ulid::new().to_string())
    }

    fn sorted_split_ids(num_splits: usize) -> Vec<SplitId> {
        let mut split_ids: Vec<SplitId> = std::iter::repeat_with(new_test_split_id)
            .take(num_splits)
            .collect();
        split_ids.sort();
        split_ids
    }

    #[test]
    fn test_split_table() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(1),
                max_num_splits: NonZeroU32::new(1).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        let split_id1 = split_ids[0].clone();
        let split_id2 = split_ids[1].clone();
        split_table.report(split_id1, Uri::for_test(TEST_STORAGE_URI));
        split_table.report(split_id2.clone(), Uri::for_test(TEST_STORAGE_URI));
        let candidate = split_table.best_candidate().unwrap();
        assert_eq!(candidate.split_id, split_id2);
    }

    #[test]
    fn test_split_table_prefer_last_touched() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(1),
                max_num_splits: NonZeroU32::new(1).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        let split_id1 = split_ids[0].clone();
        let split_id2 = split_ids[1].clone();
        split_table.report(split_id1.clone(), Uri::for_test(TEST_STORAGE_URI));
        split_table.report(split_id2, Uri::for_test(TEST_STORAGE_URI));
        let num_bytes_opt = split_table.touch(split_id1.clone(), &Uri::for_test("s3://test1/"));
        assert!(num_bytes_opt.is_none());
        let candidate = split_table.best_candidate().unwrap();
        assert_eq!(candidate.split_id, split_id1);
    }

    #[test]
    fn test_split_table_prefer_start_download_prevent_new_report() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(1),
                max_num_splits: NonZeroU32::new(1).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_id1 = new_test_split_id();
        split_table.report(split_id1.clone(), Uri::for_test(TEST_STORAGE_URI));
        assert_eq!(split_table.num_bytes(), 0);
        let download = split_table.start_download(&split_id1);
        assert!(download.is_some());
        assert!(split_table.start_download(&split_id1).is_none());
        split_table.register_as_downloaded(split_id1.clone(), 10_000_000);
        assert_eq!(split_table.num_bytes(), 10_000_000);
        assert_eq!(
            split_table.touch(split_id1, &Uri::for_test(TEST_STORAGE_URI)),
            Some(10_000_000)
        );
        let split_id2 = new_test_split_id();
        split_table.report(split_id2.clone(), Uri::for_test("s3://test`/"));
        let download = split_table.start_download(&split_id2);
        assert!(download.is_some());
        assert!(split_table.start_download(&split_id2).is_none());
        assert_eq!(split_table.num_bytes(), 10_000_000);
        split_table.register_as_downloaded(split_id2, 3_000_000);
        assert_eq!(split_table.num_bytes(), 13_000_000);
    }

    #[test]
    fn test_eviction_due_to_size() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(6);
        let splits = [
            (split_ids[0].clone(), 10_000),
            (split_ids[1].clone(), 20_000),
            (split_ids[2].clone(), 300_000),
            (split_ids[3].clone(), 400_000),
            (split_ids[4].clone(), 100_000),
            (split_ids[5].clone(), 300_000),
        ];
        for (split_id, num_bytes) in &splits {
            split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI));
            split_table.register_as_downloaded(split_id.clone(), *num_bytes);
        }
        let new_split_id = new_test_split_id();
        split_table.report(new_split_id.clone(), Uri::for_test(TEST_STORAGE_URI));
        let DownloadOpportunity {
            splits_to_delete,
            split_to_download,
        } = split_table.find_download_opportunity().unwrap();
        assert_eq!(
            &splits_to_delete[..],
            &[
                splits[0].0.clone(),
                splits[1].0.clone(),
                splits[2].0.clone()
            ][..]
        );
        assert_eq!(split_to_download.split_id, new_split_id);
    }

    #[test]
    fn test_eviction_due_to_num_splits() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(10),
                max_num_splits: NonZeroU32::new(5).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(6);
        let splits = [
            (split_ids[0].clone(), 10_000),
            (split_ids[1].clone(), 20_000),
            (split_ids[2].clone(), 300_000),
            (split_ids[3].clone(), 400_000),
            (split_ids[4].clone(), 100_000),
            (split_ids[5].clone(), 300_000),
        ];
        for (split_id, num_bytes) in &splits {
            split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI));
            split_table.register_as_downloaded(split_id.clone(), *num_bytes);
        }
        let new_split_id = new_test_split_id();
        split_table.report(new_split_id.clone(), Uri::for_test(TEST_STORAGE_URI));
        let DownloadOpportunity {
            splits_to_delete,
            split_to_download,
        } = split_table.find_download_opportunity().unwrap();
        assert_eq!(
            &splits_to_delete[..],
            &[splits[0].0.clone(), splits[1].0.clone()]
        );
        assert_eq!(split_to_download.split_id, new_split_id);
    }

    #[test]
    fn test_failed_download_can_be_re_reported() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(10),
                max_num_splits: NonZeroU32::new(5).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI));
        let candidate = split_table.start_download(&split_id).unwrap();
        // This report should be cancelled as we have a download currently running.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI));

        assert!(split_table.start_download(&split_id).is_none());
        std::mem::drop(candidate);

        // Still not possible to start a download.
        assert!(split_table.start_download(&split_id).is_none());

        // This report should be considered as our candidate (and its alive token has been dropped)
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI));

        let candidate2 = split_table.start_download(&split_id).unwrap();
        assert_eq!(candidate2.split_id, split_id);
    }

    #[test]
    fn test_split_table_truncate_candidates() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(10),
                max_num_splits: NonZeroU32::new(5).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        for i in 1..2_000 {
            let split_id = new_test_split_id();
            split_table.report(split_id, Uri::for_test(TEST_STORAGE_URI));
            assert_eq!(
                split_table.candidate_splits.len(),
                i.min(super::MAX_NUM_CANDIDATES)
            );
        }
    }

    // Unit test for #5334
    #[test]
    fn test_split_inserted_is_the_worst_candidate_5334() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(10),
                max_num_splits: NonZeroU32::new(2).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
            },
            Default::default(),
        );
        for i in (0u128..=super::MAX_NUM_CANDIDATES as u128).rev() {
            // ULID strings preserve the ordering of the underlying u128, so the lexicographic
            // ordering of these split ids matches the numeric ordering of `i`.
            let split_id = SplitId::from(Ulid(i).to_string());
            let candidate_split = CandidateSplit {
                storage_uri: Uri::for_test(TEST_STORAGE_URI),
                split_id: split_id.clone(),
                living_token: Arc::new(()),
            };
            let split_info = SplitInfo {
                split_key: SplitKey {
                    last_accessed: 0u64,
                    split_id,
                },
                status: Status::Candidate(candidate_split),
            };
            split_table.insert(split_info);
        }
        assert_eq!(
            split_table.candidate_splits.len(),
            super::MAX_NUM_CANDIDATES
        );
    }
}
