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

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use tempfile::NamedTempFile;
use tracing::warn;

const CSV_HEADER: &str = "metric_name,expiry_ts";

/// Loads known-metrics entries from a CSV file.
///
/// Returns a `HashMap<metric_name, expiry_timestamp>`. If the file does not
/// exist, returns an empty map (per D-04). Malformed rows are skipped with
/// a warning log; the header row is recognized and skipped.
pub fn load_from_csv(path: &Path) -> anyhow::Result<HashMap<String, u64>> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // D-04: missing file = empty known set
            return Ok(HashMap::new());
        }
        Err(err) => return Err(err.into()),
    };

    let reader = BufReader::new(file);
    let mut entries = HashMap::new();

    for (line_number, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(err) => {
                warn!(line_number, error = %err, "skipping unreadable CSV line");
                continue;
            }
        };

        // D-03: skip header row
        if line_number == 0 && line.trim() == CSV_HEADER {
            continue;
        }

        let Some((name, ts_str)) = line.split_once(',') else {
            warn!(line_number, line = %line, "skipping malformed CSV line: no comma");
            continue;
        };

        let expiry_ts: u64 = match ts_str.trim().parse() {
            Ok(ts) => ts,
            Err(err) => {
                warn!(
                    line_number,
                    value = %ts_str.trim(),
                    error = %err,
                    "skipping malformed CSV line: invalid timestamp"
                );
                continue;
            }
        };

        entries.insert(name.trim().to_string(), expiry_ts);
    }

    Ok(entries)
}

/// Atomically writes known-metrics entries to a CSV file.
///
/// Uses tempfile-then-rename pattern (D-05) to ensure readers never see
/// partial writes. The temp file is created in the same directory as `path`
/// to avoid cross-filesystem rename failures.
pub fn save_to_csv<'a>(
    path: &Path,
    entries: impl Iterator<Item = (&'a str, u64)>,
) -> anyhow::Result<()> {
    // D-05: create temp file in same directory for atomic rename
    let parent = path.parent().unwrap_or(Path::new("."));
    let mut temp_file = NamedTempFile::new_in(parent)?;

    // D-03: header row
    writeln!(temp_file, "{}", CSV_HEADER)?;

    // D-02: metric_name,expiry_ts
    for (name, expiry_ts) in entries {
        writeln!(temp_file, "{},{}", name, expiry_ts)?;
    }

    temp_file.flush()?;
    // D-05: atomic rename
    temp_file.persist(path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;

    use super::*;

    #[test]
    fn test_csv_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("known.csv");

        let mut original = HashMap::new();
        original.insert("cpu.user".to_string(), 1_700_000_000u64);
        original.insert("mem.free".to_string(), 1_700_100_000u64);

        save_to_csv(&path, original.iter().map(|(k, v)| (k.as_str(), *v))).unwrap();
        let loaded = load_from_csv(&path).unwrap();

        assert_eq!(original, loaded);
    }

    #[test]
    fn test_load_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.csv");

        let loaded = load_from_csv(&path).unwrap();
        assert!(loaded.is_empty(), "missing file should return empty map");
    }

    #[test]
    fn test_load_skips_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("with_header.csv");

        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "metric_name,expiry_ts").unwrap();
        writeln!(file, "cpu.user,1700000000").unwrap();
        drop(file);

        let loaded = load_from_csv(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.get("cpu.user"), Some(&1_700_000_000u64));
    }

    #[test]
    fn test_load_skips_malformed_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed.csv");

        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "metric_name,expiry_ts").unwrap();
        writeln!(file, "cpu.user,1700000000").unwrap(); // valid
        writeln!(file, "no_comma_here").unwrap(); // malformed: no comma
        writeln!(file, "bad.ts,not_a_number").unwrap(); // malformed: non-numeric timestamp
        writeln!(file, "mem.free,1700100000").unwrap(); // valid
        drop(file);

        let loaded = load_from_csv(&path).unwrap();
        assert_eq!(
            loaded.len(),
            2,
            "expected 2 valid entries, got: {loaded:?}"
        );
        assert_eq!(loaded.get("cpu.user"), Some(&1_700_000_000u64));
        assert_eq!(loaded.get("mem.free"), Some(&1_700_100_000u64));
    }

    #[test]
    fn test_save_empty_iterator() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.csv");

        save_to_csv(&path, std::iter::empty()).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "empty save should produce only header line"
        );
        assert_eq!(lines[0], "metric_name,expiry_ts");
    }

    #[test]
    fn test_save_creates_file_in_same_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.csv");

        let entries = vec![("test.metric", 1_700_000_000u64)];
        save_to_csv(&path, entries.into_iter()).unwrap();

        // Verify file exists by opening it (Path::exists is banned by clippy.toml)
        let file = std::fs::File::open(&path);
        assert!(
            file.is_ok(),
            "saved CSV file should be openable at the target path"
        );
    }
}
