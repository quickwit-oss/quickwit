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

use std::sync::Arc;

use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_storage::{
    CountingStorage, PutPayload, RamStorageBuilder, SplitPayloadBuilder, Storage, StorageError,
    StorageErrorKind,
};

use crate::SearcherContext;
use crate::leaf::open_split_bundle;

#[tokio::test]
async fn test_open_split_bundle_reads_footer_each_time_without_range_cache() {
    let hotcache_bytes = b"HOT";
    let mut builder = SplitPayloadBuilder::default();
    builder.add_payload("segment.fast".to_string(), Box::new(b"FASTDATA".to_vec()));
    let payload = builder.finalize(hotcache_bytes).unwrap();
    let footer_range = payload.footer_range.clone();
    let split_bytes = payload.read_all().await.unwrap();
    let storage = RamStorageBuilder::default()
        .put("uncached.split", split_bytes.as_slice())
        .build();
    let (storage, counters) = CountingStorage::instrument_storage(Arc::new(storage));
    let context = SearcherContext::for_test();
    let offsets = SplitIdAndFooterOffsets {
        split_id: "uncached".to_string(),
        split_footer_start: footer_range.start,
        split_footer_end: footer_range.end,
        ..Default::default()
    };

    for num_reads in 1..=2 {
        let (hotcache, _bundle) = open_split_bundle(&context, storage.clone(), &offsets)
            .await
            .unwrap();
        assert_eq!(hotcache.as_slice(), hotcache_bytes);
        assert_eq!(
            counters.snapshot(),
            (
                (footer_range.end - footer_range.start) * num_reads,
                num_reads
            ),
            "without a range cache each opening reads exactly the footer bytes"
        );
    }
}

#[tokio::test]
async fn test_open_split_bundle_preserves_footer_fetch_error() {
    let storage: Arc<dyn Storage> = Arc::new(RamStorageBuilder::default().build());
    let offsets = SplitIdAndFooterOffsets {
        split_id: "missing".to_string(),
        split_footer_start: 10,
        split_footer_end: 20,
        ..Default::default()
    };
    let error = open_split_bundle(&SearcherContext::for_test(), storage.clone(), &offsets)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "failed to fetch hotcache and footer from {} for split `missing`",
            storage.uri()
        )
    );
    assert_eq!(
        error.downcast_ref::<StorageError>().unwrap().kind(),
        StorageErrorKind::NotFound
    );
}
