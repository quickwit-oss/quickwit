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

//! In-memory map of metric name → [`Destination`], plus atomic CSV and
//! watermark persistence.
//!
//! Mirrors the responsibilities of `byoc-dualship-mgr/internal/filemanager`.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use tracing::warn;

use super::types::{ChangeSet, Destination, MetricRecord};

const CSV_HEADER: &str = "name,destination";
const WATERMARK_SUFFIX: &str = ".watermark";

/// Builds the path to the watermark sidecar for a given CSV path.
pub fn watermark_path(csv_path: &Path) -> PathBuf {
    let mut buf = csv_path.as_os_str().to_owned();
    buf.push(WATERMARK_SUFFIX);
    PathBuf::from(buf)
}

/// In-memory routing map plus the last-fetched watermark.
///
/// Only `saas` and `dual` entries are retained — `byoc` is the implicit
/// default and absent from the map. This matches the Go `filemanager`
/// behavior so the on-disk CSV stays compatible across runtimes.
#[derive(Debug, Default)]
pub struct DualShipStore {
    metrics: HashMap<String, Destination>,
    watermark: i64,
}

impl DualShipStore {
    /// Loads the CSV map and the watermark sidecar from disk. Missing files
    /// produce an empty map and a zero watermark (full-sync sentinel).
    pub fn load(csv_path: &Path) -> anyhow::Result<Self> {
        let metrics = load_csv(csv_path)?;
        let watermark = load_watermark(&watermark_path(csv_path))?;
        Ok(Self { metrics, watermark })
    }

    pub fn lookup(&self, name: &str) -> Option<Destination> {
        self.metrics.get(name).copied()
    }

    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    /// Returns a borrowed view of the in-memory metrics map. Callers in the
    /// poller acquire a read lock and pass the reference straight to
    /// [`write_csv_to_disk`] — no clone needed.
    pub fn metrics(&self) -> &HashMap<String, Destination> {
        &self.metrics
    }

    /// Test-only helper to seed a specific destination, including
    /// `Destination::Byoc` which is normally pruned from the map. Used by
    /// transform tests that need to exercise every match arm.
    #[cfg(test)]
    pub fn insert_for_test(&mut self, name: &str, destination: Destination) {
        self.metrics.insert(name.to_string(), destination);
    }

    /// Applies an incremental fetch.
    /// - `byoc` removes the entry if present.
    /// - `saas`/`dual` insert or replace (last-seen wins).
    pub fn merge(&mut self, records: &[MetricRecord]) -> ChangeSet {
        let mut cs = ChangeSet::default();
        for record in records {
            match record.destination {
                Destination::Byoc => {
                    if self.metrics.remove(&record.name).is_some() {
                        cs.removed += 1;
                    }
                }
                Destination::Saas | Destination::Dual => {
                    match self.metrics.get(&record.name) {
                        Some(existing) if *existing == record.destination => {
                            // same value, no-op
                        }
                        Some(_) => {
                            self.metrics.insert(record.name.clone(), record.destination);
                            cs.updated += 1;
                        }
                        None => {
                            self.metrics.insert(record.name.clone(), record.destination);
                            cs.added += 1;
                        }
                    }
                }
            }
        }
        cs
    }

    /// Replaces the in-memory map with the given records (full-sync path).
    /// `byoc` records are excluded from the resulting map. Returns a
    /// [`ChangeSet`] whose `removed` count is incremented by 1 to force the
    /// poller to rewrite the file even when the new map is empty — matches
    /// the Go `filemanager.Replace` behavior.
    pub fn replace(&mut self, records: &[MetricRecord]) -> ChangeSet {
        self.metrics.clear();
        let mut cs = ChangeSet::default();
        for record in records {
            match record.destination {
                Destination::Saas | Destination::Dual => {
                    self.metrics.insert(record.name.clone(), record.destination);
                    cs.added += 1;
                }
                Destination::Byoc => {}
            }
        }
        // Forces a write even when nothing was added so the previous CSV
        // (which may have been larger) is overwritten.
        cs.removed += 1;
        cs
    }

    /// Updates the in-memory watermark only. Disk persistence is performed
    /// separately by the poller via [`write_watermark_to_disk`] outside the
    /// store lock so the metric event hot path is never blocked on file IO.
    pub fn set_watermark(&mut self, ts: i64) {
        self.watermark = ts;
    }
}

/// Persists the metrics map to `csv_path` atomically. Free function so the
/// caller can pass an owned snapshot taken outside the store lock.
pub fn write_csv_to_disk(
    csv_path: &Path,
    entries: &HashMap<String, Destination>,
) -> anyhow::Result<()> {
    write_csv_atomic(csv_path, entries)
}

/// Persists `ts` to the watermark sidecar of `csv_path` atomically. Free
/// function so the caller can drop the store lock before performing IO.
pub fn write_watermark_to_disk(csv_path: &Path, ts: i64) -> anyhow::Result<()> {
    write_watermark_atomic(&watermark_path(csv_path), ts)
}

// ---------------------------------------------------------------------------
// File IO helpers
// ---------------------------------------------------------------------------

/// Returns the parent directory of `path`, falling back to the current
/// working directory when the path is a bare filename (parent is `None` or
/// the empty string). `NamedTempFile::new_in("")` fails on macOS, so we
/// must collapse the empty case to `"."`.
fn parent_or_cwd(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn load_csv(path: &Path) -> anyhow::Result<HashMap<String, Destination>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HashMap::new());
        }
        Err(err) => return Err(err.into()),
    };

    let reader = BufReader::new(file);
    let mut entries = HashMap::new();

    for (line_no, line_res) in reader.lines().enumerate() {
        let line = match line_res {
            Ok(line) => line,
            Err(err) => {
                warn!(line = line_no, error = %err, "skipping unreadable dual-ship CSV line");
                continue;
            }
        };

        if line_no == 0 && line.trim() == CSV_HEADER {
            continue;
        }

        let Some((name, dest_str)) = line.split_once(',') else {
            warn!(line = line_no, line_text = %line, "skipping malformed dual-ship CSV line");
            continue;
        };

        let destination = match dest_str.trim().parse::<Destination>() {
            Ok(destination) => destination,
            Err(err) => {
                warn!(
                    line = line_no,
                    value = %dest_str.trim(),
                    error = %err,
                    "skipping dual-ship CSV row with unknown destination"
                );
                continue;
            }
        };

        // `byoc` is the implicit default and never persisted; if a stale CSV
        // contains one, drop it so the runtime view matches Go.
        if matches!(destination, Destination::Byoc) {
            continue;
        }

        entries.insert(name.trim().to_string(), destination);
    }

    Ok(entries)
}

fn load_watermark(path: &Path) -> anyhow::Result<i64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };

    let text = std::str::from_utf8(&bytes)?.trim();
    let value: i64 = text.parse()?;
    Ok(value)
}

fn write_csv_atomic(path: &Path, entries: &HashMap<String, Destination>) -> anyhow::Result<()> {
    let parent = parent_or_cwd(path);
    let temp_file = NamedTempFile::new_in(parent)?;
    let mut writer = BufWriter::new(temp_file);

    writeln!(writer, "{CSV_HEADER}")?;

    // Sort for deterministic output (helps tests and human diffs).
    let mut names: Vec<&String> = entries.keys().collect();
    names.sort();
    for name in names {
        let destination = entries[name];
        writeln!(writer, "{name},{}", destination.as_str())?;
    }

    writer.flush()?;
    let temp_file = writer.into_inner().map_err(|err| err.into_error())?;
    temp_file.as_file().sync_all()?;
    temp_file.persist(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn write_watermark_atomic(path: &Path, ts: i64) -> anyhow::Result<()> {
    let parent = parent_or_cwd(path);
    let mut temp_file = NamedTempFile::new_in(parent)?;
    writeln!(temp_file, "{ts}")?;
    temp_file.as_file().sync_all()?;
    temp_file.persist(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(name: &str, dest: Destination, ts: i64) -> MetricRecord {
        MetricRecord {
            name: name.to_string(),
            destination: dest,
            last_updated_unix: ts,
        }
    }

    #[test]
    fn load_missing_files_yields_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");
        let store = DualShipStore::load(&csv).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.watermark(), 0);
    }

    #[test]
    fn merge_adds_saas_and_dual_records() {
        let mut store = DualShipStore::default();
        let cs = store.merge(&[
            rec("a.metric", Destination::Saas, 10),
            rec("b.metric", Destination::Dual, 12),
        ]);
        assert_eq!(cs.added, 2);
        assert_eq!(cs.updated, 0);
        assert_eq!(cs.removed, 0);
        assert_eq!(store.lookup("a.metric"), Some(Destination::Saas));
        assert_eq!(store.lookup("b.metric"), Some(Destination::Dual));
    }

    #[test]
    fn merge_updates_existing_entry_when_destination_changes() {
        let mut store = DualShipStore::default();
        store.merge(&[rec("a.metric", Destination::Saas, 10)]);
        let cs = store.merge(&[rec("a.metric", Destination::Dual, 11)]);
        assert_eq!(cs.added, 0);
        assert_eq!(cs.updated, 1);
        assert_eq!(cs.removed, 0);
        assert_eq!(store.lookup("a.metric"), Some(Destination::Dual));
    }

    #[test]
    fn merge_no_op_for_unchanged_destination() {
        let mut store = DualShipStore::default();
        store.merge(&[rec("a.metric", Destination::Saas, 10)]);
        let cs = store.merge(&[rec("a.metric", Destination::Saas, 11)]);
        assert_eq!(cs.total(), 0);
    }

    #[test]
    fn merge_removes_byoc_entries() {
        let mut store = DualShipStore::default();
        store.merge(&[rec("a.metric", Destination::Saas, 10)]);
        let cs = store.merge(&[rec("a.metric", Destination::Byoc, 11)]);
        assert_eq!(cs.removed, 1);
        assert!(store.lookup("a.metric").is_none());
    }

    #[test]
    fn merge_byoc_for_unknown_metric_is_noop() {
        let mut store = DualShipStore::default();
        let cs = store.merge(&[rec("missing", Destination::Byoc, 10)]);
        assert_eq!(cs.total(), 0);
    }

    #[test]
    fn replace_clears_stale_entries_and_excludes_byoc() {
        let mut store = DualShipStore::default();
        store.merge(&[rec("stale.metric", Destination::Saas, 10)]);
        let cs = store.replace(&[
            rec("a.metric", Destination::Saas, 11),
            rec("ignored", Destination::Byoc, 11),
        ]);
        assert_eq!(cs.added, 1);
        // removed is incremented by 1 to force a rewrite.
        assert_eq!(cs.removed, 1);
        assert!(store.lookup("stale.metric").is_none());
        assert_eq!(store.lookup("a.metric"), Some(Destination::Saas));
        assert!(store.lookup("ignored").is_none());
    }

    #[test]
    fn replace_with_empty_records_still_signals_change() {
        let mut store = DualShipStore::default();
        store.merge(&[rec("a.metric", Destination::Saas, 10)]);
        let cs = store.replace(&[]);
        assert_eq!(cs.added, 0);
        assert!(cs.total() > 0, "replace must always force a rewrite");
        assert!(store.is_empty());
    }

    #[test]
    fn csv_round_trip_preserves_entries() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        let mut store = DualShipStore::default();
        store.merge(&[
            rec("alpha", Destination::Saas, 1),
            rec("bravo", Destination::Dual, 2),
        ]);
        write_csv_to_disk(&csv, store.metrics()).unwrap();

        let reloaded = DualShipStore::load(&csv).unwrap();
        assert_eq!(reloaded.lookup("alpha"), Some(Destination::Saas));
        assert_eq!(reloaded.lookup("bravo"), Some(Destination::Dual));
        assert_eq!(reloaded.len(), 2);
    }

    #[test]
    fn csv_load_skips_byoc_entries_in_stale_files() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");
        std::fs::write(&csv, b"name,destination\nalpha,byoc\nbravo,saas\n").unwrap();

        let store = DualShipStore::load(&csv).unwrap();
        assert!(store.lookup("alpha").is_none());
        assert_eq!(store.lookup("bravo"), Some(Destination::Saas));
    }

    #[test]
    fn csv_load_skips_unknown_destination_strings() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");
        std::fs::write(&csv, b"name,destination\nalpha,bogus\nbravo,saas\n").unwrap();

        let store = DualShipStore::load(&csv).unwrap();
        assert!(store.lookup("alpha").is_none());
        assert_eq!(store.lookup("bravo"), Some(Destination::Saas));
    }

    #[test]
    fn watermark_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        let mut store = DualShipStore::default();
        write_watermark_to_disk(&csv, 1_700_000_000).unwrap();
        store.set_watermark(1_700_000_000);
        assert_eq!(store.watermark(), 1_700_000_000);

        let reloaded = DualShipStore::load(&csv).unwrap();
        assert_eq!(reloaded.watermark(), 1_700_000_000);
    }

    #[test]
    fn write_helpers_handle_bare_filename_paths() {
        let dir = tempfile::tempdir().unwrap();
        // Run the IO with the cwd inside the temp dir so a bare filename
        // resolves there. We restore the cwd at the end.
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let mut entries = HashMap::new();
        entries.insert("alpha".to_string(), Destination::Saas);
        write_csv_to_disk(Path::new("bare.csv"), &entries).unwrap();
        write_watermark_to_disk(Path::new("bare.csv"), 42).unwrap();

        let reloaded = DualShipStore::load(Path::new("bare.csv")).unwrap();
        assert_eq!(reloaded.lookup("alpha"), Some(Destination::Saas));
        assert_eq!(reloaded.watermark(), 42);

        std::env::set_current_dir(saved_cwd).unwrap();
    }

    #[test]
    fn watermark_path_appends_suffix() {
        let path = watermark_path(Path::new("/tmp/metrics_to_saas.csv"));
        assert_eq!(
            path,
            Path::new("/tmp/metrics_to_saas.csv.watermark").to_path_buf()
        );
    }
}
