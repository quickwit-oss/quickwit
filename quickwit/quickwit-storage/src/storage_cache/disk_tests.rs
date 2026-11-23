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

use std::path::Path;
use std::sync::Arc;

use super::{config_for_test, open_storage_cache};
use crate::{RamStorageBuilder, wrap_storage_with_storage_cache};

#[tokio::test]
async fn test_disk_enabled_cache_configuration_and_reads() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = config_for_test(temp_dir.path());
    // Decimal megabytes are not MiB-aligned; Feuer handles alignment internally.
    config.disk_capacity = bytesize::ByteSize::mb(512);
    let cache = open_storage_cache(&config, &crate::FeuerIoQueues::default()).unwrap();
    assert_eq!(cache.config().directory(), config.directory().unwrap());
    assert_eq!(cache.config().file_name(), "storage_cache");
    assert_eq!(
        cache.config().disk_capacity(),
        config.disk_capacity.as_u64()
    );
    assert_eq!(
        cache.config().memory_capacity(),
        config.memory_capacity.as_u64()
    );

    let lower = Arc::new(
        RamStorageBuilder::default()
            .put("a.split", b"abcde")
            .build(),
    );
    let storage = wrap_storage_with_storage_cache(cache, lower);
    for _ in 0..2 {
        assert_eq!(
            storage
                .get_slice(Path::new("a.split"), 1..4)
                .await
                .unwrap()
                .as_slice(),
            b"bcd"
        );
    }
}
