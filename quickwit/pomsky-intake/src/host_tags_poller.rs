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

//! Background poller that fetches host tags from the DD metadata service
//! and merges them into the [`HostTagsStore`].

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use indexmap::IndexSet;
use rand::RngExt as _;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::host_tags::{HostTag, HostTagsEntry, HostTagsMap, HostTagsStore};
use crate::unix_timestamp::UnixTimestamp;

/// Maximum number of hostnames to resolve per request.
const MAX_HOSTS_PER_REQUEST: usize = 200;

static GLOBAL_COLLECTOR: LazyLock<UnknownHostsCollector> =
    LazyLock::new(UnknownHostsCollector::default);

/// FIFO collector of hostnames that the enrichment transform could not
/// find in the store, or found but with an expired TTL. Uses an
/// [`IndexSet`] to maintain insertion order while deduplicating, so the
/// oldest unknown hosts are resolved first.
#[derive(Clone, Default)]
pub struct UnknownHostsCollector {
    inner: Arc<Mutex<IndexSet<String>>>,
}

impl UnknownHostsCollector {
    /// Returns the global shared collector, creating one if needed.
    pub fn global() -> UnknownHostsCollector {
        GLOBAL_COLLECTOR.clone()
    }

    /// Records a hostname that was not found in the store, or was found
    /// with an expired TTL. Called from the synchronous transform hot
    /// path — the lock is held only for the duration of an
    /// `IndexSet::insert`.
    pub fn record(&self, hostname: String) {
        let mut guard = self
            .inner
            .lock()
            .expect("unknown host collector lock poisoned");
        guard.insert(hostname);
    }

    /// Drains up to `limit` unknown hostnames in FIFO order.
    pub fn drain(&self, limit: usize) -> Vec<String> {
        let mut guard = self
            .inner
            .lock()
            .expect("unknown host collector lock poisoned");
        let count = limit.min(guard.len());
        guard.drain(..count).collect()
    }
}

// -- Metadata service request/response types --

#[derive(Serialize)]
struct HostTagsRequest {
    #[serde(rename = "host_names")]
    hostnames: Vec<String>,
}

#[derive(Deserialize)]
struct HostTagsResponse {
    host_tags: Vec<HostTagEntry>,
}

#[derive(Deserialize)]
struct HostTagEntry {
    #[serde(rename = "host_name")]
    hostname: String,
    // `Option` because the Go server marshals a `nil` slice as JSON `null`
    // (not `[]`) when no tags are known; `Vec<String>` would reject null.
    #[serde(default)]
    tags: Option<Vec<String>>,
    // Numeric HMS host ID — absent on older API versions, hence `Option`.
    #[serde(default)]
    host_id: Option<i64>,
    // Go server marshals this with `omitempty` — field is absent on
    // success. `default` lets serde accept the missing field.
    #[serde(default)]
    #[allow(dead_code)]
    error: String,
}

// -- NDJSON cache types --

/// One line in the NDJSON cache file.
#[derive(Serialize, Deserialize)]
struct CacheEntry {
    #[serde(rename = "host_name")]
    hostname: String,
    /// Tags in `"key:value"` format (same as the metadata service response).
    tags: Vec<String>,
    /// Numeric HMS host ID; absent in cache files written before this field
    /// was added (older entries deserialise cleanly with `None`).
    #[serde(default)]
    host_id: Option<i64>,
    /// Unix timestamp (seconds) at which this entry expires.
    expires_at_unix: UnixTimestamp,
}

/// Parses a `"key:value"` tag string into a `(key, value)` pair.
/// Returns `None` for tags without a colon (matching the Go sidecar
/// behavior which silently drops malformed tags).
fn parse_tag(raw: &str) -> Option<HostTag> {
    let (key, value) = raw.split_once(':')?;
    Some((key.to_string(), value.to_string()))
}

/// Encodes a `(key, value)` tag pair back to `"key:value"` for persistence.
fn encode_tag(key: &str, value: &str) -> String {
    format!("{key}:{value}")
}

/// Returns a random TTL between `ttl_min` and `ttl_max`.
fn random_ttl(ttl_min: Duration, ttl_max: Duration) -> Duration {
    debug_assert!(ttl_min <= ttl_max);
    let ttl_range_secs = ttl_min.as_secs()..=ttl_max.as_secs();
    let ttl_secs = rand::rng().random_range(ttl_range_secs);
    Duration::from_secs(ttl_secs)
}

// -- Cache persistence --

/// Loads the NDJSON cache file. Expired entries are kept so their stale
/// tags can still be served — they will be re-fetched demand-side when
/// next seen in traffic.
pub fn load_cache(path: &Path) -> anyhow::Result<HostTagsMap> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);

    let mut map = HostTagsMap::new();

    for (line_no, line_res) in reader.lines().enumerate() {
        let line = match line_res {
            Ok(line) => line,
            Err(error) => {
                warn!(line = line_no + 1, %error, "skipping unreadable cache line");
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let cache_entry: CacheEntry = match serde_json::from_str(line) {
            Ok(cache_entry) => cache_entry,
            Err(error) => {
                warn!(line = line_no + 1, %error, "skipping malformed cache line");
                continue;
            }
        };

        let tags: Arc<[HostTag]> = cache_entry
            .tags
            .iter()
            .filter_map(|raw| parse_tag(raw))
            .collect();
        let host_tags_entry = HostTagsEntry {
            tags,
            host_id: cache_entry.host_id,
            expires_at: cache_entry.expires_at_unix,
        };
        map.insert(cache_entry.hostname, host_tags_entry);
    }

    Ok(map)
}

/// Atomically writes the full state to the NDJSON cache file via
/// temp+rename.
fn save_cache(path: &Path, store: &HostTagsStore) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));

    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;

    let snapshot = store.snapshot();
    for (host, entry) in snapshot.iter() {
        let encoded_tags: Vec<String> = entry.tags.iter().map(|(k, v)| encode_tag(k, v)).collect();

        let cache_entry = CacheEntry {
            hostname: host.clone(),
            tags: encoded_tags,
            host_id: entry.host_id,
            expires_at_unix: entry.expires_at,
        };
        serde_json::to_writer(&mut tmp, &cache_entry)?;
        tmp.write_all(b"\n")?;
    }
    // Durably persist the file's data before the rename, so a crash
    // between persist and the next fsync can't leave an empty file
    // under the target path.
    tmp.as_file().sync_data()?;
    tmp.persist(path)?;
    // Fsync the parent directory so the rename itself is durable.
    std::fs::File::open(parent)?.sync_all()?;

    Ok(())
}

/// Configuration for [`run_host_tags_poller`].
pub struct HostTagsPollerConfig {
    pub store: Arc<HostTagsStore>,
    pub collector: UnknownHostsCollector,
    pub metadata_service_url: String,
    pub dd_api_key: SecretString,
    pub poll_interval: Duration,
    /// HTTP request timeout. Must be strictly less than `poll_interval`.
    pub fetch_timeout: Duration,
    pub ttl_min: Duration,
    pub ttl_max: Duration,
    /// How long past expiry before an entry is evicted from the store.
    /// Must be strictly greater than `ttl_max`.
    pub stale_threshold: Duration,
    pub cache_path: Option<PathBuf>,
}

/// Runs the host-tags poller loop until the task is cancelled (e.g. by
/// the tokio runtime shutting down).
///
/// Each cycle:
/// 1. Evicts entries that have been expired longer than `stale_threshold`.
/// 2. Drains hostnames from the collector (hosts seen in traffic with no entry or an expired
///    entry).
/// 3. Fetches the drained set from the metadata service (batched).
/// 4. Merges the response into the store with fresh TTLs.
/// 5. Persists the full state to the cache file (if configured).
///
/// Hosts are only re-fetched when the pipeline sees them — not proactively
/// on TTL expiry. This prevents endless re-fetching of hosts that are still
/// tracked by the metadata service but never appear in traffic.
pub async fn run_host_tags_poller(config: HostTagsPollerConfig) {
    let HostTagsPollerConfig {
        store,
        collector,
        metadata_service_url,
        dd_api_key,
        poll_interval,
        fetch_timeout,
        ttl_min,
        ttl_max,
        stale_threshold,
        cache_path,
    } = config;
    debug_assert!(fetch_timeout < poll_interval);
    debug_assert!(stale_threshold > ttl_max);

    let client = reqwest::Client::builder()
        .timeout(fetch_timeout)
        .build()
        .expect("failed to build HTTP client");

    let endpoint = metadata_service_url;

    info!(
        %endpoint,
        poll_interval_secs = poll_interval.as_secs(),
        fetch_timeout_secs = fetch_timeout.as_secs(),
        ttl_min_secs = ttl_min.as_secs(),
        ttl_max_secs = ttl_max.as_secs(),
        stale_threshold_hours = stale_threshold.as_secs() / 3600,
        ?cache_path,
        "starting host-tags poller"
    );

    if let Some(ref path) = cache_path {
        match load_cache(path) {
            Ok(entries) => {
                info!(total = entries.len(), "loaded host tags from cache");
                store.store(entries);
            }
            Err(error) => {
                info!(%error, "no cache file loaded, starting fresh");
            }
        }
    }

    let mut interval = tokio::time::interval(poll_interval);

    loop {
        interval.tick().await;

        // 1. Evict entries that have been stale for longer than stale_threshold.
        // Entries expired for less than stale_threshold are kept so the pipeline
        // can continue serving their stale tags while a re-fetch is in flight.
        let stale_cutoff = UnixTimestamp::now() - stale_threshold;
        let stale_hosts: Vec<String> = store
            .snapshot()
            .iter()
            .filter(|(_, entry)| entry.expires_at < stale_cutoff)
            .map(|(host, _)| host.clone())
            .collect();
        if !stale_hosts.is_empty() {
            info!("evicting {} stale host tag entries", stale_hosts.len());
            store.evict(&stale_hosts);
            if let Some(ref path) = cache_path
                && let Err(error) = save_cache(path, &store)
            {
                warn!(%error, "failed to persist cache after eviction");
            }
        }

        // 2. Drain hostnames seen in traffic that need tag resolution.
        // This includes both genuinely unknown hosts and hosts whose cached
        // entry was expired when the transform last saw them.
        let to_fetch = collector.drain(MAX_HOSTS_PER_REQUEST);
        if to_fetch.is_empty() {
            continue;
        }
        debug!("fetching tags for {} hosts seen in traffic", to_fetch.len());

        // 3. Fetch from metadata service and merge with fresh TTLs.
        match fetch_host_tags(&client, &endpoint, &dd_api_key, &to_fetch).await {
            Ok(raw_tags) => {
                let now_after_fetch = UnixTimestamp::now();
                let fresh_entries: HostTagsMap = raw_tags
                    .into_iter()
                    .map(|(host, (tags, host_id))| {
                        let ttl = random_ttl(ttl_min, ttl_max);
                        let expires_at = now_after_fetch + ttl;
                        (
                            host,
                            HostTagsEntry {
                                tags,
                                host_id,
                                expires_at,
                            },
                        )
                    })
                    .collect();
                let fetched_count = fresh_entries.len();
                store.merge(fresh_entries);
                info!(
                    fetched_count,
                    total_hosts = store.len(),
                    memory_footprint = %store.memory_footprint(),
                    "merged fresh host tags into store"
                );
                if let Some(ref path) = cache_path
                    && let Err(error) = save_cache(path, &store)
                {
                    warn!(%error, "failed to save host-tags cache");
                }
            }
            Err(error) => {
                warn!(%error, "failed to fetch host tags from metadata service");
                // Re-queue for retry on the next cycle.
                for host in to_fetch {
                    collector.record(host);
                }
            }
        }
    }
}

async fn fetch_host_tags(
    client: &reqwest::Client,
    endpoint: &str,
    api_key: &SecretString,
    hosts: &[String],
) -> anyhow::Result<HashMap<String, (Arc<[HostTag]>, Option<i64>)>> {
    let response = client
        .post(endpoint)
        .header("DD-API-KEY", api_key.expose_secret())
        .json(&HostTagsRequest {
            hostnames: hosts.to_vec(),
        })
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("metadata service returned HTTP {status}: {body}");
    }
    let body: HostTagsResponse = response.json().await?;

    let mut result = HashMap::with_capacity(hosts.len());
    for entry in body.host_tags {
        if !entry.error.is_empty() {
            debug!(host = %entry.hostname, error = %entry.error, "metadata service reported error for host");
        }
        let tags: Arc<[HostTag]> = entry
            .tags
            .unwrap_or_default()
            .iter()
            .filter_map(|tag| parse_tag(tag))
            .collect();
        result.insert(entry.hostname, (tags, entry.host_id));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_parse_tag_with_colon() {
        assert_eq!(
            parse_tag("env:prod"),
            Some(("env".to_string(), "prod".to_string())),
        );
    }

    #[test]
    fn test_parse_tag_with_multiple_colons() {
        assert_eq!(
            parse_tag("url:http://example.com"),
            Some(("url".to_string(), "http://example.com".to_string())),
        );
    }

    #[test]
    fn test_parse_tag_without_colon_is_dropped() {
        assert_eq!(parse_tag("standalone"), None);
    }

    #[test]
    fn test_encode_tag() {
        assert_eq!(encode_tag("env", "prod"), "env:prod");
        assert_eq!(
            encode_tag("url", "http://example.com"),
            "url:http://example.com"
        );
    }

    #[test]
    fn test_random_ttl_within_bounds() {
        let min = Duration::from_secs(100);
        let max = Duration::from_secs(200);
        for _ in 0..100 {
            let ttl = random_ttl(min, max);
            assert!(ttl >= min);
            assert!(ttl <= max);
        }
    }

    #[test]
    fn test_random_ttl_equal_bounds() {
        let d = Duration::from_secs(42);
        assert_eq!(random_ttl(d, d), d);
    }

    #[test]
    fn test_unknown_host_collector_record_and_drain() {
        let collector = UnknownHostsCollector::default();
        collector.record("host-a".to_string());
        collector.record("host-b".to_string());
        collector.record("host-a".to_string()); // duplicate

        let drained = collector.drain(10);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained, vec!["host-a", "host-b"]); // FIFO order

        assert!(collector.drain(10).is_empty());
    }

    #[test]
    fn test_unknown_host_collector_drain_respects_limit() {
        let collector = UnknownHostsCollector::default();
        for idx in 0..10 {
            collector.record(format!("host-{idx}"));
        }
        let drained = collector.drain(3);
        assert_eq!(drained.len(), 3);
        assert_eq!(drained, vec!["host-0", "host-1", "host-2"]);

        let rest = collector.drain(100);
        assert_eq!(rest.len(), 7);
    }

    #[test]
    fn test_host_tags_response_accepts_null_tags() {
        let body = r#"{"host_tags":[{"host_name":"marmoset-m3-max","tags":null}]}"#;
        let parsed: HostTagsResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.host_tags.len(), 1);
        assert_eq!(parsed.host_tags[0].hostname, "marmoset-m3-max");
        assert!(parsed.host_tags[0].tags.is_none());
    }

    #[test]
    fn test_host_tags_response_accepts_missing_tags() {
        let body = r#"{"host_tags":[{"host_name":"h1"}]}"#;
        let parsed: HostTagsResponse = serde_json::from_str(body).unwrap();
        assert!(parsed.host_tags[0].tags.is_none());
    }

    #[test]
    fn test_host_tags_response_accepts_populated_tags() {
        let body = r#"{"host_tags":[{"host_name":"h1","tags":["env:prod","region:us-east-1"]}]}"#;
        let parsed: HostTagsResponse = serde_json::from_str(body).unwrap();
        assert_eq!(
            parsed.host_tags[0].tags.as_deref(),
            Some(&["env:prod".to_string(), "region:us-east-1".to_string()][..]),
        );
    }

    #[test]
    fn test_load_cache_expired_entries_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_tags.ndjson");

        let now = UnixTimestamp::now();
        let future_unix = (now + Duration::from_secs(3600)).0;
        let past_unix = (now - Duration::from_secs(100)).0;

        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            r#"{{"host_name":"web-01","tags":["env:prod"],"expires_at_unix":{future_unix}}}"#,
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"host_name":"db-01","tags":["env:staging"],"expires_at_unix":{past_unix}}}"#,
        )
        .unwrap();
        writeln!(file, "not-json").unwrap();
        drop(file);

        let entries = load_cache(&path).unwrap();

        // Both entries loaded — expired ones are kept for stale-serving.
        assert_eq!(entries.len(), 2);
        assert_eq!(entries["web-01"].expires_at, UnixTimestamp(future_unix));
        assert_eq!(entries["db-01"].expires_at, UnixTimestamp(past_unix));
    }

    #[test]
    fn test_load_and_save_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_tags.ndjson");

        let now = UnixTimestamp::now();
        let future_unix = (now + Duration::from_secs(3600)).0;
        let past_unix = (now - Duration::from_secs(100)).0;

        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            r#"{{"host_name":"web-01","tags":["env:prod","region:us-east-1"],"expires_at_unix":{future_unix}}}"#,
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"host_name":"db-01","tags":["env:staging"],"expires_at_unix":{past_unix}}}"#,
        )
        .unwrap();
        writeln!(file, "not-json").unwrap();
        drop(file);

        let entries = load_cache(&path).unwrap();
        assert_eq!(entries.len(), 2);

        let web_tags = &entries["web-01"].tags;
        assert_eq!(web_tags.len(), 2);
        assert!(web_tags.contains(&("env".to_string(), "prod".to_string())));

        let db_tags: &[_] = &entries["db-01"].tags;
        assert_eq!(db_tags, [("env".to_string(), "staging".to_string())]);

        let store = HostTagsStore::default();
        store.store(entries);
        save_cache(&path, &store).unwrap();

        let reloaded = load_cache(&path).unwrap();
        assert_eq!(reloaded.len(), 2);
        assert!(reloaded.contains_key("web-01"));
        assert!(reloaded.contains_key("db-01"));
    }

    #[test]
    fn test_load_cache_missing_file() {
        let result = load_cache(Path::new("/nonexistent/path.ndjson"));
        assert!(result.is_err());
    }

    // -- Mock server tests for `fetch_host_tags` --

    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("failed to build test HTTP client")
    }

    #[tokio::test]
    async fn test_fetch_host_tags_success() {
        let server = MockServer::start().await;
        let endpoint = format!("{}/host-tags", server.uri());

        Mock::given(method("POST"))
            .and(path("/host-tags"))
            .and(header("DD-API-KEY", "test-api-key"))
            .and(body_json(json!({ "host_names": ["web-01", "db-01"] })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "host_tags": [
                    {
                        "host_name": "web-01",
                        "tags": ["env:prod", "region:us-east-1"],
                        "error": ""
                    },
                    {
                        "host_name": "db-01",
                        "tags": ["env:staging"],
                        "error": ""
                    }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let hosts = vec!["web-01".to_string(), "db-01".to_string()];
        let api_key = SecretString::from("test-api-key".to_string());
        let result = fetch_host_tags(&test_client(), &endpoint, &api_key, &hosts)
            .await
            .expect("fetch should succeed");

        assert_eq!(result.len(), 2);
        let web = &result["web-01"].0;
        assert!(web.contains(&("env".to_string(), "prod".to_string())));
        assert!(web.contains(&("region".to_string(), "us-east-1".to_string())));
        assert_eq!(
            &*result["db-01"].0,
            [("env".to_string(), "staging".to_string())]
        );
    }

    #[tokio::test]
    async fn test_fetch_host_tags_http_error_returns_err() {
        let server = MockServer::start().await;
        let endpoint = format!("{}/host-tags", server.uri());

        Mock::given(method("POST"))
            .and(path("/host-tags"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream unavailable"))
            .expect(1)
            .mount(&server)
            .await;

        let hosts = vec!["web-01".to_string()];
        let api_key = SecretString::from("test-api-key".to_string());
        let error = fetch_host_tags(&test_client(), &endpoint, &api_key, &hosts)
            .await
            .expect_err("fetch should fail on 503");
        let message = error.to_string();
        assert!(
            message.contains("503"),
            "expected status in error, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_fetch_host_tags_malformed_tag_is_dropped() {
        let server = MockServer::start().await;
        let endpoint = format!("{}/host-tags", server.uri());

        Mock::given(method("POST"))
            .and(path("/host-tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "host_tags": [{
                    "host_name": "web-01",
                    "tags": ["env:prod", "no-colon-here", "region:us-east-1"],
                    "error": ""
                }]
            })))
            .mount(&server)
            .await;

        let hosts = vec!["web-01".to_string()];
        let api_key = SecretString::from("test-api-key".to_string());
        let result = fetch_host_tags(&test_client(), &endpoint, &api_key, &hosts)
            .await
            .expect("fetch should succeed");

        let web = &result["web-01"].0;
        assert_eq!(web.len(), 2);
        assert!(web.contains(&("env".to_string(), "prod".to_string())));
        assert!(web.contains(&("region".to_string(), "us-east-1".to_string())));
    }
}
