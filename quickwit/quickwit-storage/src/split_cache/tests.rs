// Copyright (C) 2023 Quickwit, Inc.
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

use std::num::NonZeroU32;

use byte_unit::Byte;
use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use ulid::Ulid;

use crate::split_cache::split_table::{DownloadOpportunity, SplitTable};

const TEST_STORAGE_URI: &'static str = "s3://test";

#[test]
fn test_split_table() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(1000),
        max_num_splits: NonZeroU32::new(1).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    split_table.report(ulid1, Uri::for_test(TEST_STORAGE_URI));
    split_table.report(ulid2, Uri::for_test(TEST_STORAGE_URI));
    let candidate = split_table.best_candidate().unwrap();
    assert_eq!(candidate.split_ulid, ulid2);
}

#[test]
fn test_split_table_prefer_last_touched() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(1000),
        max_num_splits: NonZeroU32::new(1).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    split_table.report(ulid1, Uri::for_test(TEST_STORAGE_URI));
    split_table.report(ulid2, Uri::for_test(TEST_STORAGE_URI));
    let split_guard_opt = split_table.get_split_guard(ulid1, &Uri::for_test("s3://test1/"));
    assert!(split_guard_opt.is_none());
    let candidate = split_table.best_candidate().unwrap();
    assert_eq!(candidate.split_ulid, ulid1);
}

#[test]
fn test_split_table_prefer_start_download_prevent_new_report() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(1000),
        max_num_splits: NonZeroU32::new(1).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let ulid1 = Ulid::new();
    split_table.report(ulid1, Uri::for_test(TEST_STORAGE_URI));
    assert_eq!(split_table.num_bytes(), 0);
    let download = split_table.start_download(ulid1);
    assert!(download.is_some());
    assert!(split_table.start_download(ulid1).is_none());
    split_table.register_as_downloaded(ulid1, 10_000_000);
    assert_eq!(split_table.num_bytes(), 10_000_000);
    split_table.get_split_guard(ulid1, &Uri::for_test(TEST_STORAGE_URI));
    let ulid2 = Ulid::new();
    split_table.report(ulid2, Uri::for_test("s3://test`/"));
    let download = split_table.start_download(ulid2);
    assert!(download.is_some());
    assert!(split_table.start_download(ulid2).is_none());
    assert_eq!(split_table.num_bytes(), 10_000_000);
    split_table.register_as_downloaded(ulid2, 3_000_000);
    assert_eq!(split_table.num_bytes(), 13_000_000);
}

#[test]
fn test_eviction_due_to_size() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(1_000_000),
        max_num_splits: NonZeroU32::new(30).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let mut split_ulids: Vec<Ulid> = std::iter::repeat_with(Ulid::new).take(6).collect();
    split_ulids.sort();
    let splits = [
        (split_ulids[0], 10_000),
        (split_ulids[1], 20_000),
        (split_ulids[2], 300_000),
        (split_ulids[3], 400_000),
        (split_ulids[4], 100_000),
        (split_ulids[5], 300_000),
    ];
    for (split_ulid, num_bytes) in splits {
        split_table.report(split_ulid, Uri::for_test(TEST_STORAGE_URI));
        split_table.register_as_downloaded(split_ulid, num_bytes);
    }
    let new_ulid = Ulid::new();
    split_table.report(new_ulid, Uri::for_test(TEST_STORAGE_URI));
    let DownloadOpportunity {
        splits_to_delete,
        split_to_download,
    } = split_table.find_download_opportunity().unwrap();
    assert_eq!(
        &splits_to_delete[..],
        &[splits[0].0, splits[1].0, splits[2].0][..]
    );
    assert_eq!(split_to_download.split_ulid, new_ulid);
}

#[test]
fn test_eviction_due_to_num_splits() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(10_000_000),
        max_num_splits: NonZeroU32::new(5).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let mut split_ulids: Vec<Ulid> = std::iter::repeat_with(Ulid::new).take(6).collect();
    split_ulids.sort();
    let splits = [
        (split_ulids[0], 10_000),
        (split_ulids[1], 20_000),
        (split_ulids[2], 300_000),
        (split_ulids[3], 400_000),
        (split_ulids[4], 100_000),
        (split_ulids[5], 300_000),
    ];
    for (split_ulid, num_bytes) in splits {
        split_table.report(split_ulid, Uri::for_test(TEST_STORAGE_URI));
        split_table.register_as_downloaded(split_ulid, num_bytes);
    }
    let new_ulid = Ulid::new();
    split_table.report(new_ulid, Uri::for_test(TEST_STORAGE_URI));
    let DownloadOpportunity {
        splits_to_delete,
        split_to_download,
    } = split_table.find_download_opportunity().unwrap();
    assert_eq!(&splits_to_delete[..], &[splits[0].0][..]);
    assert_eq!(split_to_download.split_ulid, new_ulid);
}

#[test]
fn test_failed_download_can_be_re_reported() {
    let mut split_table = SplitTable::with_limits(SplitCacheLimits {
        max_num_bytes: Byte::from_bytes(10_000_000),
        max_num_splits: NonZeroU32::new(5).unwrap(),
        num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
    });
    let split_ulid = Ulid::new();
    split_table.report(split_ulid, Uri::for_test(TEST_STORAGE_URI));
    let candidate = split_table.start_download(split_ulid).unwrap();
    // This report should be cancelled as we have a download currently running.
    split_table.report(split_ulid, Uri::for_test(TEST_STORAGE_URI));

    assert!(split_table.start_download(split_ulid).is_none());
    std::mem::drop(candidate);

    // Still not possible to start a download.
    assert!(split_table.start_download(split_ulid).is_none());

    // This report should be considered as our candidate (and its alive token has been dropped)
    split_table.report(split_ulid, Uri::for_test(TEST_STORAGE_URI));

    let candidate2 = split_table.start_download(split_ulid).unwrap();
    assert_eq!(candidate2.split_ulid, split_ulid);
}
