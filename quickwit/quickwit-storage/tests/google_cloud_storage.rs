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
// to Fake GCS Server (the emulated google cloud storage environment)

#[cfg(all(feature = "integration-testsuite", feature = "gcs"))]
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn google_cloud_storage_test_suite() -> anyhow::Result<()> {
    use std::str::FromStr;

    use anyhow::Context;
    use quickwit_common::uri::Uri;
    use quickwit_storage::new_emulated_google_cloud_storage;
    let _ = tracing_subscriber::fmt::try_init();

    let mut object_storage =
        new_emulated_google_cloud_storage(&Uri::from_str("gs://sample-bucket")?)?;
    quickwit_storage::storage_test_suite(&mut object_storage).await?;

    let mut object_storage = new_emulated_google_cloud_storage(&Uri::from_str(
        "gs://sample-bucket/integration-tests/test-azure-compatible-storage",
    )?)?;
    quickwit_storage::storage_test_single_part_upload(&mut object_storage)
        .await
        .context("test single-part upload failed")?;

    quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
        .await
        .context("test multipart upload failed")?;
    Ok(())
}
