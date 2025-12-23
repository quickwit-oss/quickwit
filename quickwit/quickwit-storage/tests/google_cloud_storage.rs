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
#[cfg_attr(not(feature = "ci-test"), ignore)]
mod gcp_storage_test_suite {
    use std::str::FromStr;

    use anyhow::Context;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::setup_logging_for_tests;
    use quickwit_common::uri::Uri;
    use quickwit_storage::test_config_helpers::{
        LOCAL_GCP_EMULATOR_ENDPOINT, new_emulated_google_cloud_storage,
    };

    pub fn sign_gcs_request(req: &mut reqwest::Request) {
        req.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str("Bearer dummy").unwrap(),
        );
    }

    async fn create_gcs_bucket(bucket_name: &str) -> anyhow::Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{LOCAL_GCP_EMULATOR_ENDPOINT}/storage/v1/b");
        let mut request = client
            .post(url)
            .body(serde_json::to_vec(&serde_json::json!({
                "name": bucket_name,
            }))?)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .build()?;

        sign_gcs_request(&mut request);

        let response = client.execute(request).await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            anyhow::bail!("Failed to create bucket: {}", error_text);
        };
        Ok(())
    }

    #[tokio::test]
    async fn google_cloud_storage_test_suite() -> anyhow::Result<()> {
        setup_logging_for_tests();

        let bucket_name = append_random_suffix("sample-bucket").to_lowercase();
        create_gcs_bucket(bucket_name.as_str())
            .await
            .context("Failed to create test GCS bucket")?;

        let mut object_storage =
            new_emulated_google_cloud_storage(&Uri::from_str(&format!("gs://{bucket_name}"))?)?;

        quickwit_storage::storage_test_suite(&mut object_storage).await?;

        let mut object_storage = new_emulated_google_cloud_storage(&Uri::from_str(&format!(
            "gs://{bucket_name}/integration-tests/test-gcs-storage"
        ))?)?;

        quickwit_storage::storage_test_single_part_upload(&mut object_storage)
            .await
            .context("test single-part upload failed")?;

        // TODO: Uncomment storage_test_multi_part_upload when the XML API is
        // supported in the emulated GCS server
        // (https://github.com/fsouza/fake-gcs-server/pull/1164)

        // object_storage.set_policy(MultiPartPolicy {
        //     target_part_num_bytes: 5 * 1_024 * 1_024,
        //     max_num_parts: 10_000,
        //     multipart_threshold_num_bytes: 10_000_000,
        //     max_object_num_bytes: 5_000_000_000_000,
        //     max_concurrent_uploads: 100,
        // });
        // quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
        //     .await
        //     .context("test multipart upload failed")?;
        Ok(())
    }
}
