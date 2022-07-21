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

// This file is an integration test that assumes that a connection
// to Azurite (the emulated azure blob storage environment)
// with default `loose` config is possible.

use std::path::Path;

use anyhow::Context;
use azure_storage::core::prelude::StorageClient;
use azure_storage_blobs::prelude::AsContainerClient;
#[cfg(feature = "azure")]
use quickwit_storage::AzureBlobStorage;
use quickwit_storage::MultiPartPolicy;

#[cfg(feature = "testsuite")]
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_suite_on_azure_storage() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Setup container.
    const CONTAINER: &str = "quickwit";
    let container_client = StorageClient::new_emulator_default().container_client(CONTAINER);
    container_client.create().into_future().await?;

    let mut object_storage = AzureBlobStorage::new_emulated(CONTAINER);
    quickwit_storage::storage_test_suite(&mut object_storage).await?;

    let mut object_storage = AzureBlobStorage::new_emulated(CONTAINER).with_prefix(Path::new(
        "/integration-tests/test-azure-compatible-storage",
    ));
    quickwit_storage::storage_test_single_part_upload(&mut object_storage)
        .await
        .context("test_single_part_upload")?;

    object_storage.set_policy(MultiPartPolicy {
        // On azure, block size is limited between 64KB and 100MB.
        target_part_num_bytes: 5 * 1_024 * 1_024, // 5MB
        max_num_parts: 10_000,
        multipart_threshold_num_bytes: 10_000_000,
        max_object_num_bytes: 5_000_000_000_000,
        max_concurrent_upload: 100,
    });
    quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
        .await
        .context("test_multi_part_upload")?;

    // Teardown container.
    container_client.delete().into_future().await?;

    Ok(())
}
