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

use std::env::var;

use once_cell::sync::Lazy;
use quickwit_common::get_bool_from_env;

pub const CONFIGURATION_TEMPLATE: &str = r#"
version: 0.8
node_id: lambda-indexer
cluster_id: lambda-ephemeral
metastore_uri: s3://${QW_LAMBDA_METASTORE_BUCKET}/${QW_LAMBDA_METASTORE_PREFIX:-index}
default_index_root_uri: s3://${QW_LAMBDA_INDEX_BUCKET}/${QW_LAMBDA_INDEX_PREFIX:-index}
data_dir: /tmp
"#;

pub static INDEX_CONFIG_URI: Lazy<String> = Lazy::new(|| {
    var("QW_LAMBDA_INDEX_CONFIG_URI")
        .expect("environment variable `QW_LAMBDA_INDEX_CONFIG_URI` should be set")
});

pub static DISABLE_MERGE: Lazy<bool> =
    Lazy::new(|| get_bool_from_env("QW_LAMBDA_DISABLE_MERGE", false));

pub static DISABLE_JANITOR: Lazy<bool> =
    Lazy::new(|| get_bool_from_env("QW_LAMBDA_DISABLE_JANITOR", false));

pub static MAX_CHECKPOINTS: Lazy<usize> = Lazy::new(|| {
    var("QW_LAMBDA_MAX_CHECKPOINTS").map_or(100, |v| {
        v.parse()
            .expect("QW_LAMBDA_MAX_CHECKPOINTS must be a positive integer")
    })
});

#[cfg(test)]
mod tests {

    use quickwit_config::{ConfigFormat, NodeConfig};

    use super::*;

    #[tokio::test]
    #[serial_test::file_serial(with_env)]
    async fn test_load_config() {
        let bucket = "mock-test-bucket";
        std::env::set_var("QW_LAMBDA_METASTORE_BUCKET", bucket);
        std::env::set_var("QW_LAMBDA_INDEX_BUCKET", bucket);
        std::env::set_var(
            "QW_LAMBDA_INDEX_CONFIG_URI",
            "s3://mock-index-config-bucket",
        );
        std::env::set_var("QW_LAMBDA_INDEX_ID", "lambda-test");

        let node_config = NodeConfig::load(ConfigFormat::Yaml, CONFIGURATION_TEMPLATE.as_bytes())
            .await
            .unwrap();
        //
        assert_eq!(
            node_config.data_dir_path.to_string_lossy(),
            "/tmp",
            "only `/tmp` is writeable in AWS Lambda"
        );
        assert_eq!(
            node_config.default_index_root_uri,
            "s3://mock-test-bucket/index"
        );
        assert_eq!(
            node_config.metastore_uri.to_string(),
            "s3://mock-test-bucket/index"
        );

        std::env::remove_var("QW_LAMBDA_METASTORE_BUCKET");
        std::env::remove_var("QW_LAMBDA_INDEX_BUCKET");
        std::env::remove_var("QW_LAMBDA_INDEX_CONFIG_URI");
        std::env::remove_var("QW_LAMBDA_INDEX_ID");
    }
}
