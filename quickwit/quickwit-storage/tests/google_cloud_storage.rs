// Copyright (C) 2024 Quickwit, Inc.
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
