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

use crate::metrics::{SEARCHER_SPLIT_CACHE, SEARCHER_SPLIT_CACHE_DOWNLOADS_SKIPPED_TOO_LARGE};

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
    Downloading { alive_token: Weak<()>, num_bytes: u64 },
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
/// By default the split table size in bytes may exceed its limits by at most one
/// split (the incoming split's bytes are not reserved before its download
/// starts).
///
/// When `SplitCacheLimits::skip_oversized_splits` is enabled, splits larger than
/// `max_num_bytes` are never downloaded, and the incoming split plus all
/// in-flight downloads are reserved against the budget — so on-disk usage stays
/// within `max_num_bytes`, exceeded only transiently by downloads still in
/// flight (each already checked to fit).
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
        if let Status::Downloading { alive_token, .. } = &split_info.status
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
                && let Status::Downloading { alive_token, .. } = &split_info.status
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
                        // The size is not known on the search read path; a later
                        // `report` fills it in. Unknown (0) is treated as "fits".
                        num_bytes: 0,
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

    pub(crate) fn report(&mut self, split_id: SplitId, storage_uri: Uri, num_bytes: u64) {
        let origin_time = self.origin_time;
        self.mutate_split(split_id, move |split_info_opt, split_id| {
            if let Some(mut split_info) = split_info_opt {
                // The split is already known. Attach the size to a candidate that
                // was first discovered without one (e.g. via `touch`), but never
                // overwrite a known size, and never disturb downloading/on-disk
                // splits (they already carry an accurate size).
                if let Status::Candidate(candidate_split) = &mut split_info.status
                    && candidate_split.num_bytes == 0
                    && num_bytes > 0
                {
                    candidate_split.num_bytes = num_bytes;
                }
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
                    num_bytes,
                }),
            }
        });
    }

    /// Registers a split size discovered on the search path.
    ///
    /// This is a no-op unless the size guard is enabled and the size is known
    /// (`num_bytes > 0`), so the default download path is never affected by an
    /// extra candidate registration.
    pub(crate) fn report_split_size_from_search(
        &mut self,
        split_id: SplitId,
        storage_uri: Uri,
        num_bytes: u64,
    ) {
        if !self.limits.skip_oversized_splits || num_bytes == 0 {
            return;
        }
        // Only attach a size to a candidate (or a not-yet-known split). Splits
        // that are already downloading or on disk already carry an accurate size;
        // routing them through `report` (a remove + reinsert) would wrongly bump
        // the cache's eviction counters for what is actually a cache hit.
        let is_candidate_or_unknown = matches!(
            self.split_to_status.get(&split_id),
            None | Some(SplitInfo {
                status: Status::Candidate(_),
                ..
            })
        );
        if is_candidate_or_unknown {
            self.report(split_id, storage_uri, num_bytes);
        }
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
        // Carry the split size into the downloading state so its bytes are
        // reserved against the budget while the download is in flight.
        let num_bytes = candidate_split.num_bytes;
        self.insert(SplitInfo {
            split_key: split_info.split_key,
            status: Status::Downloading {
                alive_token,
                num_bytes,
            },
        });
        Some(candidate_split)
    }

    /// Returns the hottest candidate (highest last-accessed, ties broken by
    /// split id). Used by the default download path (size guard disabled).
    fn best_candidate(&self) -> Option<SplitKey> {
        self.candidate_splits.last().cloned()
    }

    /// Returns the known size in bytes of a candidate split, or 0 if the split
    /// is unknown, or not currently a candidate, or was reported without a size.
    ///
    /// A size of 0 is treated as "fits" by the download guard, so a candidate
    /// without a known size behaves exactly as it did before the guard existed.
    fn candidate_num_bytes(&self, split_id: &SplitId) -> u64 {
        match self.split_to_status.get(split_id) {
            Some(SplitInfo {
                status: Status::Candidate(candidate_split),
                ..
            }) => candidate_split.num_bytes,
            _ => 0,
        }
    }

    /// Sum of the reported sizes of the splits currently being downloaded.
    ///
    /// These bytes are not yet on disk but will be shortly, so they must be
    /// reserved against the budget to avoid overshooting when several downloads
    /// run concurrently (`num_concurrent_downloads > 1`). A downloading split
    /// with an unknown size (0) contributes nothing, matching legacy behavior.
    fn downloading_bytes(&self) -> u64 {
        self.downloading_splits
            .iter()
            .map(|key| match self.split_to_status.get(&key.split_id) {
                // Only count downloads that are still alive. A failed download
                // keeps a dead `Downloading` entry until it is garbage-collected;
                // reserving its bytes would needlessly block other downloads.
                Some(SplitInfo {
                    status: Status::Downloading {
                        alive_token,
                        num_bytes,
                    },
                    ..
                }) if alive_token.strong_count() > 0 => *num_bytes,
                _ => 0,
            })
            .sum()
    }

    /// Returns true if the table is (or, with the size guard, would be) over its
    /// byte or split-count budget.
    ///
    /// With the guard disabled (default), room is judged only on what is already
    /// on disk: neither the incoming split nor in-flight downloads are reserved,
    /// so the table may exceed its limits by at most one split (per the type
    /// invariant). Eviction can only reclaim on-disk splits, so an empty disk is
    /// never reported as over limit.
    ///
    /// With the guard enabled, the incoming split (`incoming_bytes`) and every
    /// in-flight download are reserved against the budget. Counting in-flight
    /// downloads matters when `num_concurrent_downloads > 1`, where two
    /// concurrent sub-budget splits could otherwise both pass and overshoot once
    /// they land.
    fn would_exceed_limits_with(&self, incoming_bytes: u64) -> bool {
        if !self.limits.skip_oversized_splits {
            if self.on_disk_splits.is_empty() {
                return false;
            }
            if self.on_disk_splits.len() + self.downloading_splits.len()
                >= self.limits.max_num_splits.get() as usize
            {
                return true;
            }
            return self.on_disk_bytes > self.limits.max_num_bytes.as_u64();
        }
        if self.on_disk_splits.len() + self.downloading_splits.len() + 1
            > self.limits.max_num_splits.get() as usize
        {
            return true;
        }
        self.on_disk_bytes + self.downloading_bytes() + incoming_bytes
            > self.limits.max_num_bytes.as_u64()
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
        incoming_bytes: u64,
    ) -> Result<Vec<SplitId>, NoRoomAvailable> {
        let mut split_infos = Vec::new();
        while self.would_exceed_limits_with(incoming_bytes) {
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
        if self.would_exceed_limits_with(incoming_bytes) {
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

    /// Selects the next split download opportunity.
    ///
    /// Default behavior (size guard disabled): take the single hottest candidate
    /// and download it; if room cannot be made for it, do nothing this round.
    ///
    /// With the size guard enabled, candidates are instead scanned hottest-first
    /// for the best *fitting* candidate that can also make room for itself. Two
    /// kinds of candidate are skipped in favour of the next one rather than
    /// aborting the scan — otherwise the downloader would stall on a single
    /// permanently-blocked (but permanently hot) split and starve the cache:
    /// - larger than the whole budget (`max_num_bytes`): can never coexist with
    ///   anything, so it is skipped (and counted) and left to the cold-storage
    ///   warmup path;
    /// - fits, but cannot make room (its eviction would remove a *fresher*
    ///   on-disk split): a colder candidate that fits without that eviction is
    ///   preferred.
    ///
    /// Candidates with an unknown size (0) are treated as fitting.
    pub(crate) fn find_download_opportunity(&mut self) -> Option<DownloadOpportunity> {
        if !self.limits.skip_oversized_splits {
            let best_candidate_split_key = self.best_candidate()?;
            let splits_to_delete: Vec<SplitId> = self
                .make_room_for_split_if_necessary(best_candidate_split_key.last_accessed, 0)
                .ok()?;
            let split_to_download: CandidateSplit =
                self.start_download(&best_candidate_split_key.split_id)?;
            return Some(DownloadOpportunity {
                splits_to_delete,
                split_to_download,
            });
        }
        let max_num_bytes = self.limits.max_num_bytes.as_u64();
        // Snapshot the candidate keys (hottest first) so we can mutate the table
        // (evict / start a download) while iterating. `make_room` only touches
        // on-disk splits and reinserts them on failure, so the snapshot stays
        // valid across a skipped candidate.
        let candidate_keys: Vec<SplitKey> =
            self.candidate_splits.iter().rev().cloned().collect();
        for candidate_key in candidate_keys {
            let incoming_bytes = self.candidate_num_bytes(&candidate_key.split_id);
            if incoming_bytes > max_num_bytes {
                SEARCHER_SPLIT_CACHE_DOWNLOADS_SKIPPED_TOO_LARGE.inc();
                continue;
            }
            let Ok(splits_to_delete) =
                self.make_room_for_split_if_necessary(candidate_key.last_accessed, incoming_bytes)
            else {
                // Could not make room without evicting a fresher split; try the
                // next-best fitting candidate.
                continue;
            };
            let split_to_download: CandidateSplit =
                self.start_download(&candidate_key.split_id)?;
            return Some(DownloadOpportunity {
                splits_to_delete,
                split_to_download,
            });
        }
        None
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
    /// Size of the split file in bytes, or 0 if unknown.
    ///
    /// An unknown size (0) is treated as "fits" by the download guard, so a
    /// candidate discovered without a size behaves exactly as it did before
    /// the guard existed.
    pub num_bytes: u64,
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
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        let split_id1 = split_ids[0].clone();
        let split_id2 = split_ids[1].clone();
        split_table.report(split_id1, Uri::for_test(TEST_STORAGE_URI), 0);
        split_table.report(split_id2.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
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
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        let split_id1 = split_ids[0].clone();
        let split_id2 = split_ids[1].clone();
        split_table.report(split_id1.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
        split_table.report(split_id2, Uri::for_test(TEST_STORAGE_URI), 0);
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
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        let split_id1 = new_test_split_id();
        split_table.report(split_id1.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
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
        split_table.report(split_id2.clone(), Uri::for_test("s3://test`/"), 0);
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
                skip_oversized_splits: false,
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
            split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
            split_table.register_as_downloaded(split_id.clone(), *num_bytes);
        }
        let new_split_id = new_test_split_id();
        split_table.report(new_split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
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
                skip_oversized_splits: false,
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
            split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
            split_table.register_as_downloaded(split_id.clone(), *num_bytes);
        }
        let new_split_id = new_test_split_id();
        split_table.report(new_split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
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
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
        let candidate = split_table.start_download(&split_id).unwrap();
        // This report should be cancelled as we have a download currently running.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);

        assert!(split_table.start_download(&split_id).is_none());
        std::mem::drop(candidate);

        // Still not possible to start a download.
        assert!(split_table.start_download(&split_id).is_none());

        // This report should be considered as our candidate (and its alive token has been dropped)
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);

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
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        for i in 1..2_000 {
            let split_id = new_test_split_id();
            split_table.report(split_id, Uri::for_test(TEST_STORAGE_URI), 0);
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
                skip_oversized_splits: false,
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
                num_bytes: 0,
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

    #[test]
    fn test_skip_download_of_oversized_split_and_advance() {
        // A split larger than the whole cache budget must never be downloaded, and
        // the downloader must advance to the next-best *fitting* candidate rather
        // than stalling on the (permanently hot) oversized split.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        let small_split_id = split_ids[0].clone();
        let oversized_split_id = split_ids[1].clone();
        // Report the fitting split first, then the oversized one. Reported later
        // and with the larger id, the oversized split is the hottest candidate.
        split_table.report(
            small_split_id.clone(),
            Uri::for_test(TEST_STORAGE_URI),
            100_000,
        );
        split_table.report(
            oversized_split_id.clone(),
            Uri::for_test(TEST_STORAGE_URI),
            2_000_000,
        );
        assert_eq!(
            split_table.best_candidate().unwrap().split_id,
            oversized_split_id
        );
        // The guard skips the oversized split and downloads the fitting one.
        let opportunity = split_table.find_download_opportunity().unwrap();
        assert_eq!(opportunity.split_to_download.split_id, small_split_id);
        // With only the oversized split left as a candidate there is nothing to
        // download, but crucially the oversized split is never selected.
        assert!(split_table.find_download_opportunity().is_none());
    }

    #[test]
    fn test_oversized_only_candidate_is_never_downloaded() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        split_table.report(split_id, Uri::for_test(TEST_STORAGE_URI), 5_000_000);
        assert!(split_table.find_download_opportunity().is_none());
    }

    #[test]
    fn test_eviction_accounts_for_incoming_split_size() {
        // Eviction must make room for the *incoming* split's bytes, not merely
        // trim already-on-disk bytes, so on-disk usage never transiently exceeds
        // the budget.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(3);
        // 3 x 300_000 = 900_000 on disk, under the 1_000_000 budget.
        for split_id in &split_ids {
            split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
            split_table.register_as_downloaded(split_id.clone(), 300_000);
        }
        assert_eq!(split_table.num_bytes(), 900_000);
        // A new 300_000-byte split: 900_000 + 300_000 > 1_000_000, so the oldest
        // on-disk split must be evicted first. A size-unaware check (as before)
        // would see 900_000 <= 1_000_000 and evict nothing.
        let new_split_id = new_test_split_id();
        split_table.report(
            new_split_id.clone(),
            Uri::for_test(TEST_STORAGE_URI),
            300_000,
        );
        let DownloadOpportunity {
            splits_to_delete,
            split_to_download,
        } = split_table.find_download_opportunity().unwrap();
        assert_eq!(split_to_download.split_id, new_split_id);
        assert_eq!(&splits_to_delete[..], &[split_ids[0].clone()]);
        // The evicted split's bytes are already reclaimed; completing the download
        // keeps on-disk usage within budget.
        assert_eq!(split_table.num_bytes(), 600_000);
        split_table.register_as_downloaded(new_split_id, 300_000);
        assert_eq!(split_table.num_bytes(), 900_000);
    }

    #[test]
    fn test_unknown_size_candidate_is_not_skipped() {
        // A candidate reported without a size (0) behaves exactly as before the
        // guard existed: it is still selected for download, even under a tiny
        // budget.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
        let opportunity = split_table.find_download_opportunity().unwrap();
        assert_eq!(opportunity.split_to_download.split_id, split_id);
    }

    #[test]
    fn test_report_attaches_size_to_sizeless_candidate() {
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        // Discovered on the search path first: the size is unknown.
        assert!(
            split_table
                .touch(split_id.clone(), &Uri::for_test(TEST_STORAGE_URI))
                .is_none()
        );
        assert_eq!(split_table.candidate_num_bytes(&split_id), 0);
        // A later report attaches the real size.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 4_096);
        assert_eq!(split_table.candidate_num_bytes(&split_id), 4_096);
        // A second report never overwrites an already-known size.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 9_999);
        assert_eq!(split_table.candidate_num_bytes(&split_id), 4_096);
    }

    #[test]
    fn test_size_attached_after_touch_enables_guard() {
        // Models the search path (Option A): a pre-existing oversized split is
        // first discovered via `touch` with no size — so it would be downloaded —
        // then its real size is attached (as `SearchSplitCache::report_split_size`
        // does at split-open time). The guard must then skip it.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        // Discovered via a search, size unknown: still a valid candidate.
        assert!(
            split_table
                .touch(split_id.clone(), &Uri::for_test(TEST_STORAGE_URI))
                .is_none()
        );
        assert_eq!(split_table.candidate_num_bytes(&split_id), 0);
        // The real (oversized) size is attached from the split's footer offsets.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 5_000_000);
        assert_eq!(split_table.candidate_num_bytes(&split_id), 5_000_000);
        // Now known to exceed the 1 MB budget, so it is never downloaded.
        assert!(split_table.find_download_opportunity().is_none());
    }

    #[test]
    fn test_concurrent_downloads_reserve_bytes() {
        // With more than one download slot, an in-flight download's bytes must be
        // reserved against the budget so two sub-budget splits can't both start
        // and overshoot once they land.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(2).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        // Each fits alone (600K <= 1M) but not together (1.2M > 1M).
        split_table.report(split_ids[0].clone(), Uri::for_test(TEST_STORAGE_URI), 600_000);
        split_table.report(split_ids[1].clone(), Uri::for_test(TEST_STORAGE_URI), 600_000);
        // First download starts; its bytes are now reserved as "in flight". The
        // returned opportunity keeps the split's living token alive.
        let first = split_table.find_download_opportunity();
        assert!(first.is_some());
        // The second download must not start: 600K in flight + 600K incoming > 1M.
        assert!(split_table.find_download_opportunity().is_none());
    }

    #[test]
    fn test_advances_to_next_candidate_when_room_cannot_be_made() {
        // If the hottest fitting candidate can only make room by evicting a
        // *fresher* on-disk split, the downloader must fall through to a colder
        // candidate that fits without eviction rather than stalling.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(10).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        // A small on-disk split, freshly accessed so eviction must not touch it
        // (a `touch` sets `last_accessed` to now, far fresher than the reported
        // candidates below, which are stamped ~10 minutes in the past).
        let on_disk_id = new_test_split_id();
        split_table.report(on_disk_id.clone(), Uri::for_test(TEST_STORAGE_URI), 0);
        split_table.register_as_downloaded(on_disk_id.clone(), 100_000);
        split_table.touch(on_disk_id.clone(), &Uri::for_test(TEST_STORAGE_URI));
        // Two candidates, hotter one first by id:
        //  - hot C1 (larger id): fits by size, but making room would evict the
        //    fresher on-disk split -> cannot make room;
        //  - cold C2 (smaller id): fits in the free space with no eviction.
        let split_ids = sorted_split_ids(2);
        let cold_c2 = split_ids[0].clone();
        let hot_c1 = split_ids[1].clone();
        split_table.report(cold_c2.clone(), Uri::for_test(TEST_STORAGE_URI), 50_000);
        split_table.report(hot_c1.clone(), Uri::for_test(TEST_STORAGE_URI), 950_000);
        let opportunity = split_table.find_download_opportunity().unwrap();
        // Advanced past C1 (could not make room) to C2.
        assert_eq!(opportunity.split_to_download.split_id, cold_c2);
        // The fresher on-disk split was preserved.
        assert!(opportunity.splits_to_delete.is_empty());
    }

    #[test]
    fn test_default_flow_ignores_split_size() {
        // With the guard disabled (the default), the reported split size is
        // ignored: an oversized split is still selected for download, exactly as
        // before this feature existed.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::kb(500),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: false,
            },
            Default::default(),
        );
        let split_id = new_test_split_id();
        // 5 MB split, far larger than the 500 KB budget.
        split_table.report(split_id.clone(), Uri::for_test(TEST_STORAGE_URI), 5_000_000);
        let opportunity = split_table.find_download_opportunity().unwrap();
        assert_eq!(opportunity.split_to_download.split_id, split_id);
    }

    #[test]
    fn test_failed_download_bytes_not_reserved() {
        // A failed download leaves a dead `Downloading` entry until it is
        // garbage-collected. Its bytes must not stay reserved, or an unrelated
        // candidate that would otherwise fit could be blocked indefinitely.
        let mut split_table = SplitTable::with_limits_and_existing_splits(
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(1),
                max_num_splits: NonZeroU32::new(30).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(2).unwrap(),
                max_file_descriptors: NonZeroU32::new(100).unwrap(),
                skip_oversized_splits: true,
            },
            Default::default(),
        );
        let split_ids = sorted_split_ids(2);
        split_table.report(split_ids[0].clone(), Uri::for_test(TEST_STORAGE_URI), 600_000);
        split_table.report(split_ids[1].clone(), Uri::for_test(TEST_STORAGE_URI), 600_000);
        // Start the first download, then simulate its failure by dropping the
        // returned guard (its living token is the only strong reference).
        let first = split_table.find_download_opportunity().unwrap();
        drop(first);
        // The dead download's 600K must no longer be reserved, so the second
        // 600K candidate can now start (600K <= 1M).
        assert!(split_table.find_download_opportunity().is_some());
    }
}
