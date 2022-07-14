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

// This file is an integration test that assumes that the environement
// makes it possible to connect to Amazon S3's quickwit-integration-test bucket.

use std::path::Path;

use anyhow::Context;
use quickwit_storage::{AzureCompatibleBlobStorage, MultiPartPolicy};

#[cfg(feature = "testsuite")]
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_suite_on_azure_storage() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    // TODO: https://github.com/Azure/azure-sdk-for-rust/issues/936
    // Azurite,the Azure blob storage emulator does not support range download.
    // In the mean time, let's run tests against a real azure account.
    //
    // To achieve this we need these env variables.
    // export QW_AZURE_TEST_URI=azure://azure-storage-account/azure-container
    // export QW_AZURE_ACCESS_KEY=azure-access-key
    //
    let _ = std::env::var("QW_AZURE_ACCESS_KEY").expect("Set env variable QW_AZURE_ACCESS_KEY");
    let base_uri = std::env::var("QW_AZURE_TEST_URI").expect("Set env variable AZURE_TEST_URI");

    let test_id = uuid::Uuid::new_v4().to_string();

    let mut object_storage =
        AzureCompatibleBlobStorage::from_uri(&format!("{}/tests-{}", base_uri, test_id))?;
    quickwit_storage::storage_test_suite(&mut object_storage).await?;

    let mut object_storage = AzureCompatibleBlobStorage::from_uri(&format!(
        "{}/integration-tests-{}",
        base_uri, test_id
    ))?
    .with_prefix(Path::new("test-azure-compatible-storage"));
    quickwit_storage::storage_test_single_part_upload(&mut object_storage)
        .await
        .with_context(|| "test_single_part_upload")?;

    let mut object_storage = AzureCompatibleBlobStorage::from_uri(&format!(
        "{}/integration-tests-{}",
        base_uri, test_id
    ))?
    .with_prefix(Path::new("test-azure-compatible-storage"));
    object_storage.set_policy(MultiPartPolicy {
        target_part_num_bytes: 5 * 1_024 * 1_024, //< the minimum on S3 is 5MB.
        max_num_parts: 10_000,
        multipart_threshold_num_bytes: 10_000_000,
        max_object_num_bytes: 5_000_000_000_000,
        max_concurrent_upload: 100,
    });
    quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
        .await
        .with_context(|| "test_multi_part_upload")?;

    Ok(())
}
