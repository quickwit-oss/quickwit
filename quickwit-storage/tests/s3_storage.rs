// Copyright (C) 2021 Quickwit, Inc.
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

// This file is an integration test that assumes that the environement
// makes it possible to connect to Amazon S3's quickwit-integration-test bucket.

use std::path::Path;

use quickwit_storage::{MultiPartPolicy, RegionProvider, S3CompatibleObjectStorage, Storage};

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_upload_single_part_file() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let localstack_region = RegionProvider::Localstack.get_region();
    let object_storage =
        S3CompatibleObjectStorage::new(localstack_region, "quickwit-integration-tests")?;
    object_storage
        .put(
            Path::new("test-s3-compatible-storage/hello_small.txt"),
            Box::new(b"hello, happy tax payer!".to_vec()),
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_upload_multiple_part_file() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let localstack_region = RegionProvider::Localstack.get_region();
    let mut object_storage =
        S3CompatibleObjectStorage::new(localstack_region, "quickwit-integration-tests")?;
    object_storage.set_policy(MultiPartPolicy {
        target_part_num_bytes: 5 * 1_024 * 1_024, //< the minimum on S3 is 5MB.
        max_num_parts: 10_000,
        multipart_threshold_num_bytes: 10_000_000,
        max_object_num_bytes: 5_000_000_000_000,
        max_concurrent_upload: 100,
    });
    let test_buffer = vec![0u8; 15_000_000];
    object_storage
        .put(
            Path::new("test-s3-compatible-storage/hello_large.txt"),
            Box::new(test_buffer),
        )
        .await?;
    Ok(())
}

#[cfg(feature = "testsuite")]
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
// Weirdly this does not work for localstack. The error messages seem off.
async fn test_suite_on_s3_storage() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let localstack_region = RegionProvider::Localstack.get_region();
    let mut object_storage =
        S3CompatibleObjectStorage::new(localstack_region, "quickwit-integration-tests")?;
    quickwit_storage::storage_test_suite(&mut object_storage).await?;
    Ok(())
}
