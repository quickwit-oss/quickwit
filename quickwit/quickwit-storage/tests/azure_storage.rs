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

// This file is an integration test that assumes that a connection
// to Azurite (the emulated azure blob storage environment)
// with default `loose` config is possible.

#[cfg(feature = "integration-testsuite")]
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn azure_storage_test_suite() -> anyhow::Result<()> {
    use std::path::PathBuf;

    use anyhow::Context;
    use azure_storage_blobs::prelude::ClientBuilder;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_storage::{AzureBlobStorage, MultiPartPolicy};
    let _ = tracing_subscriber::fmt::try_init();

    // Setup container.
    let container_name = append_random_suffix("quickwit").to_lowercase();
    let container_client = ClientBuilder::emulator().container_client(&container_name);
    container_client.create().into_future().await?;

    let mut object_storage = AzureBlobStorage::new_emulated(&container_name);
    quickwit_storage::storage_test_suite(&mut object_storage).await?;

    let mut object_storage = AzureBlobStorage::new_emulated(&container_name).with_prefix(
        PathBuf::from("/integration-tests/test-azure-compatible-storage"),
    );
    quickwit_storage::storage_test_single_part_upload(&mut object_storage)
        .await
        .context("test single-part upload failed")?;

    object_storage.set_policy(MultiPartPolicy {
        // On azure, block size is limited between 64KB and 100MB.
        target_part_num_bytes: 5 * 1_024 * 1_024, // 5MiB
        max_num_parts: 10_000,
        multipart_threshold_num_bytes: 10_000_000,
        max_object_num_bytes: 5_000_000_000_000,
        max_concurrent_uploads: 100,
    });
    quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
        .await
        .context("test multipart upload failed")?;

    // Teardown container.
    container_client.delete().into_future().await?;
    Ok(())
}
