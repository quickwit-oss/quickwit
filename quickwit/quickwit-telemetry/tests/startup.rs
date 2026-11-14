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

use std::path::PathBuf;
use std::process::Command;

use quickwit_telemetry::payload::QuickwitTelemetryInfo;

#[tokio::test]
async fn test_startup_disables_telemetry_without_install_id() {
    // Isolate the process-global sender and telemetry opt-out in child processes.
    if let Some(data_dir) = std::env::var_os("QW_TELEMETRY_TEST_DATA_DIR") {
        let handle = quickwit_telemetry::start_telemetry_loop(
            QuickwitTelemetryInfo::default(),
            &PathBuf::from(data_dir),
        );
        assert!(handle.is_none());
        assert!(!quickwit_telemetry::is_telemetry_enabled());
        return;
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let invalid_id_dir = temp_dir.path().join("invalid-id");
    std::fs::create_dir(&invalid_id_dir).unwrap();
    let install_id_path = invalid_id_dir.join("telemetry_install_id");
    std::fs::write(&install_id_path, "not-a-uuid").unwrap();
    let invalid_data_dir = temp_dir.path().join("not-a-directory");
    std::fs::write(&invalid_data_dir, "occupied").unwrap();

    for data_dir in [&invalid_id_dir, &invalid_data_dir] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "test_startup_disables_telemetry_without_install_id",
                "--nocapture",
            ])
            .env_remove("QW_DISABLE_TELEMETRY")
            .env("TELEMETRY_INGEST_API", "http://127.0.0.1:9/")
            .env("QW_TELEMETRY_TEST_DATA_DIR", data_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
