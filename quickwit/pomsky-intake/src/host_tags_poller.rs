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
use std::time::{Duration, SystemTime};

use indexmap::IndexSet;
use rand::RngExt as _;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::host_tags::{HostTag, HostTagsMap, HostTagsStore};

/// Maximum number of hostnames to resolve per request.
const MAX_HOSTS_PER_REQUEST: usize = 200;

static GLOBAL_COLLECTOR: LazyLock<UnknownHostsCollector> =
    LazyLock::new(UnknownHostsCollector::default);

/// FIFO collector of hostnames that the enrichment transform could not
/// find in the store. Uses an [`IndexSet`] to maintain insertion order
/// while deduplicating, so the oldest unknown hosts are resolved first.
#[derive(Clone, Default)]
pub struct UnknownHostsCollector {
    inner: Arc<Mutex<IndexSet<String>>>,
}

impl UnknownHostsCollector {
    /// Returns the global shared collector, creating one if needed.
    pub fn global() -> UnknownHostsCollector {
        GLOBAL_COLLECTOR.clone()
    }

    /// Records a hostname that was not found in the store.
    /// Called from the synchronous transform hot path — the lock is held
    /// only for the duration of an `IndexSet::insert`.
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
    /// Unix timestamp (seconds) at which this entry expires.
    expires_at_unix: u64,
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

/// Returns the current unix timestamp in seconds.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
}

// -- Cache persistence --

/// Result of loading the cache file.
pub struct CacheSnapshot {
    pub tag_map: HostTagsMap,
    /// Maps each tracked hostname to its expiry unix timestamp (seconds).
    pub host_expiry: HashMap<String, u64>,
    pub expired_hosts: Vec<String>,
}

/// Loads the NDJSON cache file. Returns the tag map (for non-expired
/// entries), the expiry map, and a list of expired hosts that need
/// immediate re-fetching.
pub fn load_cache(path: &Path) -> anyhow::Result<CacheSnapshot> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let now = now_unix();

    let mut tag_map = HostTagsMap::new();
    let mut host_expiry = HashMap::new();
    let mut expired_hosts = Vec::new();

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
        let entry: CacheEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(error) => {
                warn!(line = line_no + 1, %error, "skipping malformed cache line");
                continue;
            }
        };

        let tags: Arc<[HostTag]> = entry.tags.iter().filter_map(|raw| parse_tag(raw)).collect();
        let expiry_unix = entry.expires_at_unix;

        if expiry_unix <= now {
            // Expired: load the stale tags (better than nothing) and mark
            // for immediate re-fetch.
            expired_hosts.push(entry.hostname.clone());
        }

        tag_map.insert(entry.hostname.clone(), tags);
        host_expiry.insert(entry.hostname, expiry_unix);
    }

    Ok(CacheSnapshot {
        tag_map,
        host_expiry,
        expired_hosts,
    })
}

/// Atomically writes the full state to the NDJSON cache file via
/// temp+rename.
fn save_cache(
    path: &Path,
    store: &HostTagsStore,
    host_expiry: &HashMap<String, u64>,
) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));

    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;

    let snapshot = store.snapshot();
    for (host, tags) in snapshot.iter() {
        let expires_at_unix = host_expiry.get(host).copied().unwrap_or(0);

        let encoded_tags: Vec<String> = tags.iter().map(|(k, v)| encode_tag(k, v)).collect();

        let entry = CacheEntry {
            hostname: host.clone(),
            tags: encoded_tags,
            expires_at_unix,
        };
        serde_json::to_writer(&mut tmp, &entry)?;
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
    pub dd_api_key: String,
    pub poll_interval: Duration,
    /// HTTP request timeout. Must be strictly less than `poll_interval`.
    pub fetch_timeout: Duration,
    pub ttl_min: Duration,
    pub ttl_max: Duration,
    pub cache_path: Option<PathBuf>,
}

/// Runs the host-tags poller loop until the task is cancelled (e.g. by
/// the tokio runtime shutting down).
///
/// Each cycle:
/// 1. Drains unknown hostnames from the collector (new hosts).
/// 2. Finds tracked hosts whose TTL has expired (stale hosts).
/// 3. Sends the combined set to the metadata service (batched).
/// 4. Merges the response into the store and resets their TTLs.
/// 5. Persists the full state to the cache file (if configured).
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
        cache_path,
    } = config;
    debug_assert!(fetch_timeout < poll_interval);
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
        ?cache_path,
        "starting host-tags poller"
    );

    // Maps each tracked hostname to its expiry unix timestamp (seconds).
    let mut host_expiry: HashMap<String, u64> = HashMap::new();

    // Load persisted state if available.
    if let Some(ref path) = cache_path {
        match load_cache(path) {
            Ok(snapshot) => {
                let total = snapshot.tag_map.len();
                let expired = snapshot.expired_hosts.len();
                info!(total, expired, "loaded host tags from cache");

                host_expiry = snapshot.host_expiry;
                store.store(snapshot.tag_map);

                // Queue expired hosts for immediate re-fetch.
                for host in snapshot.expired_hosts {
                    collector.record(host);
                }
            }
            Err(error) => {
                info!(%error, "no cache file loaded, starting fresh");
            }
        }
    }
    let mut interval = tokio::time::interval(poll_interval);

    loop {
        interval.tick().await;
        let now = now_unix();

        // 1. Drain newly-seen unknown hosts — they need immediate resolution.
        let new_hosts = collector.drain(MAX_HOSTS_PER_REQUEST);
        if !new_hosts.is_empty() {
            debug!(count = new_hosts.len(), "discovered new unknown hosts");
        }

        // 2. Collect expired hosts that need refreshing.
        let expired_hosts: Vec<String> = host_expiry
            .iter()
            .filter(|(_, expiry)| **expiry <= now)
            .map(|(host, _)| host.clone())
            .collect();
        if !expired_hosts.is_empty() {
            debug!(count = expired_hosts.len(), "hosts with expired TTL");
        }

        // 3. Combine new + expired into the set to query.
        let mut to_query: Vec<String> = Vec::with_capacity(new_hosts.len() + expired_hosts.len());
        to_query.extend(new_hosts);
        to_query.extend(expired_hosts);
        to_query.sort_unstable();
        to_query.dedup();

        if !to_query.is_empty() {
            match fetch_host_tags(&client, &endpoint, &dd_api_key, &to_query).await {
                Ok(fresh_tags) => {
                    let fetched_count = fresh_tags.len();

                    // Assign a random TTL to each fetched host.
                    let fetch_completed_at = now_unix();
                    for host in fresh_tags.keys() {
                        let ttl = random_ttl(ttl_min, ttl_max);
                        host_expiry.insert(host.clone(), fetch_completed_at + ttl.as_secs());
                    }

                    // Merge into the store (only updates the fetched subset).
                    store.merge(fresh_tags);
                    info!(
                        fetched_count,
                        total_hosts = store.len(),
                        memory_footprint_bytes = store.memory_footprint_bytes(),
                        "merged fresh host tags into store"
                    );

                    // Persist to disk.
                    if let Some(ref path) = cache_path
                        && let Err(error) = save_cache(path, &store, &host_expiry)
                    {
                        warn!(%error, "failed to save host-tags cache");
                    }
                }
                Err(error) => {
                    warn!(%error, "failed to fetch host tags from metadata service");
                    // On failure, re-insert new hosts with an immediate expiry
                    // so they're retried next cycle.
                    for host in to_query {
                        host_expiry.entry(host).or_insert(now);
                    }
                }
            }
        }
    }
}

/// Fetches host tags from the metadata service, batching requests if
/// the host list exceeds [`MAX_HOSTS_PER_REQUEST`].
async fn fetch_host_tags(
    client: &reqwest::Client,
    endpoint: &str,
    api_key: &str,
    hosts: &[String],
) -> anyhow::Result<HostTagsMap> {
    let mut result = HostTagsMap::with_capacity(hosts.len());

    for chunk in hosts.chunks(MAX_HOSTS_PER_REQUEST) {
        let request_body = HostTagsRequest {
            hostnames: chunk.to_vec(),
        };

        let response = client
            .post(endpoint)
            .header("DD-API-KEY", api_key)
            .json(&request_body)
            .send()
            .await?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("metadata service returned HTTP {status}: {body}");
        }
        let body: HostTagsResponse = response.json().await?;

        for entry in body.host_tags {
            if !entry.error.is_empty() {
                debug!(
                    host = %entry.hostname,
                    error = %entry.error,
                    "metadata service reported error for host"
                );
            }
            let tags: Arc<[HostTag]> = entry
                .tags
                .unwrap_or_default()
                .iter()
                .filter_map(|tag| parse_tag(tag))
                .collect();
            result.insert(entry.hostname, tags);
        }
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

        // Drain again — should be empty.
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
        // First 3 in insertion order.
        assert_eq!(drained, vec!["host-0", "host-1", "host-2"]);

        // Remaining 7 still in the set.
        let rest = collector.drain(100);
        assert_eq!(rest.len(), 7);
    }

    #[test]
    fn test_host_tags_response_accepts_null_tags() {
        // The Go metadata service returns `"tags":null` (not `[]`) when a
        // host has no known tags. Deserialization must accept null so the
        // poller doesn't fail the whole batch.
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
    fn test_load_and_save_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_tags.ndjson");

        // Write a cache file manually.
        let now_unix = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let future_unix = now_unix + 3600;
        let past_unix = now_unix.saturating_sub(100);

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
        // Malformed line should be skipped.
        writeln!(file, "not-json").unwrap();
        drop(file);

        // Load.
        let snapshot = load_cache(&path).unwrap();
        assert_eq!(snapshot.tag_map.len(), 2);
        assert_eq!(snapshot.expired_hosts, vec!["db-01".to_string()]);

        let web_tags = &snapshot.tag_map["web-01"];
        assert_eq!(web_tags.len(), 2);
        assert!(web_tags.contains(&("env".to_string(), "prod".to_string())));

        let db_tags: &[_] = &snapshot.tag_map["db-01"];
        assert_eq!(db_tags, [("env".to_string(), "staging".to_string())]);

        // Now save via the store + host_expiry and re-load.
        let store = HostTagsStore::default();
        store.store(snapshot.tag_map);

        save_cache(&path, &store, &snapshot.host_expiry).unwrap();

        let reloaded = load_cache(&path).unwrap();
        assert_eq!(reloaded.tag_map.len(), 2);
        assert!(reloaded.tag_map.contains_key("web-01"));
        assert!(reloaded.tag_map.contains_key("db-01"));
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
            .and(header("DD-API-KEY", "test-key"))
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
        let result = fetch_host_tags(&test_client(), &endpoint, "test-key", &hosts)
            .await
            .expect("fetch should succeed");

        assert_eq!(result.len(), 2);
        let web = &result["web-01"];
        assert!(web.contains(&("env".to_string(), "prod".to_string())));
        assert!(web.contains(&("region".to_string(), "us-east-1".to_string())));
        assert_eq!(
            &*result["db-01"],
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
        let error = fetch_host_tags(&test_client(), &endpoint, "test-key", &hosts)
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
        let result = fetch_host_tags(&test_client(), &endpoint, "test-key", &hosts)
            .await
            .expect("fetch should succeed");

        let web = &result["web-01"];
        assert_eq!(web.len(), 2);
        assert!(web.contains(&("env".to_string(), "prod".to_string())));
        assert!(web.contains(&("region".to_string(), "us-east-1".to_string())));
    }

    #[tokio::test]
    async fn test_fetch_host_tags_batches_over_max_per_request() {
        let server = MockServer::start().await;
        let endpoint = format!("{}/host-tags", server.uri());

        // Any POST returns an empty host_tags list. We only need to assert
        // on request count, which wiremock tracks.
        Mock::given(method("POST"))
            .and(path("/host-tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "host_tags": []
            })))
            .expect(2)
            .mount(&server)
            .await;

        let hosts: Vec<String> = (0..MAX_HOSTS_PER_REQUEST + 1)
            .map(|idx| format!("host-{idx}"))
            .collect();

        let _result = fetch_host_tags(&test_client(), &endpoint, "test-key", &hosts)
            .await
            .expect("fetch should succeed");
        // `expect(2)` on the mock is verified on drop.
    }
}
