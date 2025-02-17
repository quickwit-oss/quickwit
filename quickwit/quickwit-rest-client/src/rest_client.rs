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
use std::time::Duration;

use bytes::Bytes;
use quickwit_cluster::ClusterSnapshot;
use quickwit_config::{ConfigFormat, SourceConfig};
use quickwit_indexing::actors::IndexingServiceCounters;
pub use quickwit_ingest::CommitType;
use quickwit_metastore::{IndexMetadata, Split, SplitInfo};
use quickwit_proto::ingest::Shard;
use quickwit_serve::{ListSplitsQueryParams, ListSplitsResponse, SearchRequestQueryString};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::{Client, ClientBuilder, Method, StatusCode, Url};
use serde::Serialize;
use serde_json::json;

use crate::error::Error;
use crate::models::{
    ApiResponse, CumulatedIngestResponse, IngestSource, SearchResponseRestClient, Timeout,
};
use crate::BatchLineReader;

pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:7280";
pub const DEFAULT_CONTENT_TYPE: &str = "application/json";
pub const INGEST_CONTENT_LENGTH_LIMIT: usize = 10 * 1024 * 1024; // 10MiB
pub const DEFAULT_CLIENT_CONNECT_TIMEOUT: Timeout = Timeout::from_secs(5);
pub const DEFAULT_CLIENT_TIMEOUT: Timeout = Timeout::from_secs(10);
pub const DEFAULT_CLIENT_SEARCH_TIMEOUT: Timeout = Timeout::from_mins(1);
pub const DEFAULT_CLIENT_INGEST_TIMEOUT: Timeout = Timeout::from_mins(1);
pub const DEFAULT_CLIENT_COMMIT_TIMEOUT: Timeout = Timeout::from_mins(30);

struct Transport {
    base_url: Url,
    api_url: Url,
    client: Client,
}

impl Transport {
    fn new(endpoint: Url, connect_timeout: Timeout) -> Self {
        let base_url = endpoint;
        let api_url = base_url
            .join("api/v1/")
            .expect("Endpoint should not be malformed.");
        let mut client_builder = ClientBuilder::new();
        if let Some(duration) = connect_timeout.as_duration_opt() {
            client_builder = client_builder.connect_timeout(duration);
        }
        Self {
            base_url,
            api_url,
            client: client_builder.build().expect("Client should be built."),
        }
    }

    /// Creates an asynchronous request that can be awaited
    async fn send<Q: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        header_map: Option<HeaderMap>,
        query_string: Option<&Q>,
        body: Option<Bytes>,
        timeout: Timeout,
    ) -> Result<ApiResponse, Error> {
        let url = if path.starts_with('/') {
            self.base_url.join(path)
        } else {
            self.api_url.join(path)
        }
        .map_err(|error| Error::UrlParse(error.to_string()))?;
        let mut request_builder = self.client.request(method, url);
        if let Some(duration) = timeout.as_duration_opt() {
            request_builder = request_builder.timeout(duration);
        }
        let mut request_headers = HeaderMap::new();
        request_headers.insert(CONTENT_TYPE, HeaderValue::from_static(DEFAULT_CONTENT_TYPE));
        if let Some(header_map_val) = header_map {
            request_headers.extend(header_map_val.into_iter());
        }
        request_builder = request_builder.headers(request_headers);
        if let Some(bytes) = body {
            request_builder = request_builder.body(bytes);
        };
        if let Some(qs) = query_string {
            request_builder = request_builder.query(qs);
        }
        let response = request_builder.send().await?;

        Ok(ApiResponse::new(response))
    }
}

pub struct QuickwitClientBuilder {
    /// Base url for the client
    base_url: Url,
    /// Connection timeout.
    connect_timeout: Timeout,
    /// Timeout for most operations except search and ingest.
    timeout: Timeout,
    /// Timeout for search operations.
    search_timeout: Timeout,
    /// Timeout for the ingest operations with auto commit.
    ingest_timeout: Timeout,
    /// Timeout for the ingest operations that require waiting for commit.
    commit_timeout: Timeout,
    /// Forces use of ingest v1.
    use_legacy_ingest: bool,
    /// Request detailed parse failures report from the ingest api.
    detailed_response: bool,
}

impl QuickwitClientBuilder {
    pub fn new(endpoint: Url) -> Self {
        QuickwitClientBuilder {
            base_url: endpoint,
            connect_timeout: DEFAULT_CLIENT_CONNECT_TIMEOUT,
            timeout: DEFAULT_CLIENT_TIMEOUT,
            search_timeout: DEFAULT_CLIENT_SEARCH_TIMEOUT,
            ingest_timeout: DEFAULT_CLIENT_INGEST_TIMEOUT,
            commit_timeout: DEFAULT_CLIENT_COMMIT_TIMEOUT,
            use_legacy_ingest: false,
            detailed_response: false,
        }
    }

    pub fn connect_timeout(mut self, timeout: Timeout) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn timeout(mut self, timeout: Timeout) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn search_timeout(mut self, timeout: Timeout) -> Self {
        self.search_timeout = timeout;
        self
    }

    pub fn ingest_timeout(mut self, timeout: Timeout) -> Self {
        self.ingest_timeout = timeout;
        self
    }

    // TODO(#5604)
    pub fn use_legacy_ingest(mut self, use_legacy_ingest: bool) -> Self {
        self.use_legacy_ingest = use_legacy_ingest;
        self
    }

    pub fn detailed_response(mut self, is_detailed: bool) -> Self {
        self.detailed_response = is_detailed;
        self
    }

    pub fn commit_timeout(mut self, timeout: Timeout) -> Self {
        self.commit_timeout = timeout;
        self
    }

    pub fn build(self) -> QuickwitClient {
        let transport = Transport::new(self.base_url, self.connect_timeout);
        QuickwitClient {
            transport,
            timeout: self.timeout,
            search_timeout: self.search_timeout,
            ingest_timeout: self.ingest_timeout,
            commit_timeout: self.commit_timeout,
            use_legacy_ingest: self.use_legacy_ingest,
            detailed_response: self.detailed_response,
        }
    }
}

/// Root client for top level APIs.
pub struct QuickwitClient {
    transport: Transport,
    /// Timeout for all operations except search and ingest.
    timeout: Timeout,
    /// Timeout for search operations.
    search_timeout: Timeout,
    /// Timeout for the ingest operations.
    ingest_timeout: Timeout,
    /// Timeout for the ingest operations that require waiting for commit.
    commit_timeout: Timeout,
    /// Forces use of ingest v1.
    use_legacy_ingest: bool,
    /// Request detailed parse failures report from the ingest api.
    detailed_response: bool,
}

impl QuickwitClient {
    pub async fn search(
        &self,
        index_id: &str,
        search_query: SearchRequestQueryString,
    ) -> Result<SearchResponseRestClient, Error> {
        let path = format!("{index_id}/search");
        let bytes = serde_json::to_string(&search_query)
            .unwrap()
            .as_bytes()
            .to_vec();
        let body = Bytes::from(bytes);
        let response = self
            .transport
            .send::<()>(
                Method::POST,
                &path,
                None,
                None,
                Some(body),
                self.search_timeout,
            )
            .await?;
        let search_response = response.deserialize().await?;
        Ok(search_response)
    }

    pub fn indexes(&self) -> IndexClient {
        IndexClient::new(&self.transport, self.timeout)
    }

    pub fn splits<'a>(&'a self, index_id: &'a str) -> SplitClient {
        SplitClient::new(&self.transport, self.timeout, index_id)
    }

    pub fn sources<'a>(&'a self, index_id: &'a str) -> SourceClient {
        SourceClient::new(&self.transport, self.timeout, index_id)
    }

    pub fn cluster(&self) -> ClusterClient {
        ClusterClient::new(&self.transport, self.timeout)
    }

    pub fn node_stats(&self) -> NodeStatsClient {
        NodeStatsClient::new(&self.transport, self.timeout)
    }

    pub fn node_health(&self) -> NodeHealthClient {
        NodeHealthClient::new(&self.transport, self.timeout)
    }

    pub async fn ingest(
        &self,
        index_id: &str,
        ingest_source: IngestSource,
        batch_size_limit_opt: Option<usize>,
        mut on_ingest_event: Option<&mut (dyn FnMut(IngestEvent) + Sync)>,
        last_block_commit: CommitType,
    ) -> Result<CumulatedIngestResponse, Error> {
        let ingest_path = format!("{index_id}/ingest");
        let mut query_params = HashMap::new();
        // TODO(#5604)
        if self.use_legacy_ingest {
            query_params.insert("use_legacy_ingest", "true");
        }
        if self.detailed_response {
            query_params.insert("detailed_response", "true");
        }
        let batch_size_limit = batch_size_limit_opt.unwrap_or(INGEST_CONTENT_LENGTH_LIMIT);
        let mut batch_reader = match ingest_source {
            IngestSource::File(filepath) => {
                BatchLineReader::from_file(&filepath, batch_size_limit).await?
            }
            IngestSource::Stdin => BatchLineReader::from_stdin(batch_size_limit),
            IngestSource::Str(ingest_payload) => {
                BatchLineReader::from_string(ingest_payload, batch_size_limit)
            }
        };
        let mut cumulated_resp = CumulatedIngestResponse::default();
        while let Some(batch) = batch_reader.next_batch().await? {
            loop {
                let timeout = if !batch_reader.has_next() && last_block_commit != CommitType::Auto {
                    self.commit_timeout
                } else {
                    self.ingest_timeout
                };
                match last_block_commit {
                    CommitType::Auto => {}
                    CommitType::WaitFor => {
                        query_params.insert("commit", "wait_for");
                    }
                    CommitType::Force => {
                        query_params.insert("commit", "force");
                    }
                }
                let response = self
                    .transport
                    .send(
                        Method::POST,
                        &ingest_path,
                        None,
                        Some(&query_params),
                        Some(batch.clone()),
                        timeout,
                    )
                    .await?;
                if response.status_code() == StatusCode::TOO_MANY_REQUESTS {
                    cumulated_resp.num_too_many_requests += 1;
                } else {
                    let current_parsed_resp = response.deserialize().await?;
                    cumulated_resp = cumulated_resp.merge(current_parsed_resp);
                    break;
                }
                if let Some(event_fn) = &mut on_ingest_event {
                    event_fn(IngestEvent::Sleep)
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if let Some(event_fn) = &mut on_ingest_event {
                event_fn(IngestEvent::IngestedDocBatch(batch.len()))
            }
        }

        Ok(cumulated_resp)
    }
}

pub enum IngestEvent {
    IngestedDocBatch(usize),
    Sleep,
}

/// Client for indexes APIs.
pub struct IndexClient<'a> {
    transport: &'a Transport,
    timeout: Timeout,
}

impl<'a> IndexClient<'a> {
    fn new(transport: &'a Transport, timeout: Timeout) -> Self {
        Self { transport, timeout }
    }

    pub async fn create(
        &self,
        index_config: impl AsRef<[u8]>,
        config_format: ConfigFormat,
        overwrite: bool,
    ) -> Result<IndexMetadata, Error> {
        let header_map = header_from_config_format(config_format);
        let body = Bytes::copy_from_slice(index_config.as_ref());
        let response = self
            .transport
            .send(
                Method::POST,
                "indexes",
                Some(header_map),
                Some(&[("overwrite", overwrite)]),
                Some(body),
                self.timeout,
            )
            .await?;
        let index_metadata = response.deserialize().await?;
        Ok(index_metadata)
    }

    pub async fn update(
        &self,
        index_id: &str,
        index_config: impl AsRef<[u8]>,
        config_format: ConfigFormat,
    ) -> Result<IndexMetadata, Error> {
        let header_map = header_from_config_format(config_format);
        let body = Bytes::copy_from_slice(index_config.as_ref());
        let path = format!("indexes/{index_id}");
        let response = self
            .transport
            .send::<()>(
                Method::PUT,
                &path,
                Some(header_map),
                None,
                Some(body),
                self.timeout,
            )
            .await?;
        let index_metadata = response.deserialize().await?;
        Ok(index_metadata)
    }

    pub async fn list(&self) -> Result<Vec<IndexMetadata>, Error> {
        let response = self
            .transport
            .send::<()>(Method::GET, "indexes", None, None, None, self.timeout)
            .await?;
        let indexes_metadatas = response.deserialize().await?;
        Ok(indexes_metadatas)
    }

    pub async fn get(&self, index_id: &str) -> Result<IndexMetadata, Error> {
        let path = format!("indexes/{index_id}");
        let response = self
            .transport
            .send::<()>(Method::GET, &path, None, None, None, self.timeout)
            .await?;
        let index_metadata = response.deserialize().await?;
        Ok(index_metadata)
    }

    pub async fn clear(&self, index_id: &str) -> Result<(), Error> {
        let path = format!("indexes/{index_id}/clear");
        let response = self
            .transport
            .send::<()>(Method::PUT, &path, None, None, None, self.timeout)
            .await?;
        response.check().await?;
        Ok(())
    }

    pub async fn delete(&self, index_id: &str, dry_run: bool) -> Result<Vec<SplitInfo>, Error> {
        let path = format!("indexes/{index_id}");
        let response = self
            .transport
            .send(
                Method::DELETE,
                &path,
                None,
                Some(&[("dry_run", dry_run)]),
                None,
                self.timeout,
            )
            .await?;
        let file_entries = response.deserialize().await?;
        Ok(file_entries)
    }
}

/// Client for splits APIs.
pub struct SplitClient<'a, 'b> {
    transport: &'a Transport,
    timeout: Timeout,
    index_id: &'b str,
}

impl<'a, 'b> SplitClient<'a, 'b> {
    fn new(transport: &'a Transport, timeout: Timeout, index_id: &'b str) -> Self {
        Self {
            transport,
            timeout,
            index_id,
        }
    }

    fn splits_root_url(&self) -> String {
        format!("indexes/{}/splits", self.index_id)
    }

    pub async fn list(
        &self,
        list_splits_query_params: ListSplitsQueryParams,
    ) -> Result<Vec<Split>, Error> {
        let path = self.splits_root_url();
        let response = self
            .transport
            .send(
                Method::GET,
                &path,
                None,
                Some(&list_splits_query_params),
                None,
                self.timeout,
            )
            .await?;
        let list_splits_response: ListSplitsResponse = response.deserialize().await?;
        Ok(list_splits_response.splits)
    }

    pub async fn mark_for_deletion(&self, split_ids: Vec<String>) -> Result<(), Error> {
        let path = format!("{}/mark-for-deletion", self.splits_root_url());
        let body = Bytes::from(serde_json::to_vec(&json!({ "split_ids": split_ids }))?);
        let response = self
            .transport
            .send::<()>(Method::PUT, &path, None, None, Some(body), self.timeout)
            .await?;
        response.check().await?;
        Ok(())
    }
}

/// Client for source APIs.
pub struct SourceClient<'a> {
    transport: &'a Transport,
    timeout: Timeout,
    index_id: &'a str,
}

impl<'a> SourceClient<'a> {
    fn new(transport: &'a Transport, timeout: Timeout, index_id: &'a str) -> Self {
        Self {
            transport,
            timeout,
            index_id,
        }
    }

    fn sources_root_url(&self) -> String {
        format!("indexes/{}/sources", self.index_id)
    }

    pub async fn create(
        &self,
        source_config_input: impl AsRef<[u8]>,
        config_format: ConfigFormat,
    ) -> Result<SourceConfig, Error> {
        let header_map = header_from_config_format(config_format);
        let source_config_bytes = Bytes::copy_from_slice(source_config_input.as_ref());
        let response = self
            .transport
            .send::<()>(
                Method::POST,
                &self.sources_root_url(),
                Some(header_map),
                None,
                Some(source_config_bytes),
                self.timeout,
            )
            .await?;
        let source_config = response.deserialize().await?;
        Ok(source_config)
    }

    pub async fn update(
        &self,
        source_id: &str,
        source_config_input: impl AsRef<[u8]>,
        config_format: ConfigFormat,
    ) -> Result<SourceConfig, Error> {
        let header_map = header_from_config_format(config_format);
        let source_config_bytes = Bytes::copy_from_slice(source_config_input.as_ref());
        let path = format!("{}/{source_id}", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(
                Method::PUT,
                &path,
                Some(header_map),
                None,
                Some(source_config_bytes),
                self.timeout,
            )
            .await?;
        let source_config = response.deserialize().await?;
        Ok(source_config)
    }

    pub async fn get(&self, source_id: &str) -> Result<SourceConfig, Error> {
        let path = format!("{}/{source_id}", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(Method::GET, &path, None, None, None, self.timeout)
            .await?;
        let source_config = response.deserialize().await?;
        Ok(source_config)
    }

    pub async fn toggle(&self, source_id: &str, enable: bool) -> Result<(), Error> {
        let json_value = json!({ "enable": enable });
        let json_bytes = serde_json::to_vec(&json_value).expect("Serialization should never fail.");
        let path = format!("{}/{source_id}/toggle", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(
                Method::PUT,
                &path,
                None,
                None,
                Some(Bytes::from(json_bytes)),
                self.timeout,
            )
            .await?;
        response.check().await?;
        Ok(())
    }

    pub async fn reset_checkpoint(&self, source_id: &str) -> Result<(), Error> {
        let path = format!("{}/{source_id}/reset-checkpoint", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(Method::PUT, &path, None, None, None, self.timeout)
            .await?;
        response.check().await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<SourceConfig>, Error> {
        let response = self
            .transport
            .send::<()>(
                Method::GET,
                &self.sources_root_url(),
                None,
                None,
                None,
                self.timeout,
            )
            .await?;
        let source_configs = response.deserialize().await?;
        Ok(source_configs)
    }

    pub async fn delete(&self, source_id: &str) -> Result<(), Error> {
        let path = format!("{}/{source_id}", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(Method::DELETE, &path, None, None, None, self.timeout)
            .await?;
        response.check().await?;
        Ok(())
    }

    pub async fn get_shards(&self, source_id: &str) -> Result<Vec<Shard>, Error> {
        let path = format!("{}/{source_id}/shards", self.sources_root_url());
        let response = self
            .transport
            .send::<()>(Method::GET, &path, None, None, None, self.timeout)
            .await?;
        let source_config = response.deserialize().await?;
        Ok(source_config)
    }
}

/// Client for Cluster APIs.
pub struct ClusterClient<'a> {
    transport: &'a Transport,
    timeout: Timeout,
}

impl<'a> ClusterClient<'a> {
    fn new(transport: &'a Transport, timeout: Timeout) -> Self {
        Self { transport, timeout }
    }

    pub async fn snapshot(&self) -> Result<ClusterSnapshot, Error> {
        let response = self
            .transport
            .send::<()>(Method::GET, "cluster", None, None, None, self.timeout)
            .await?;
        let cluster_snapshot = response.deserialize().await?;
        Ok(cluster_snapshot)
    }
}

/// Client for Node-level Stats APIs.
pub struct NodeStatsClient<'a> {
    transport: &'a Transport,
    timeout: Timeout,
}

impl<'a> NodeStatsClient<'a> {
    fn new(transport: &'a Transport, timeout: Timeout) -> Self {
        Self { transport, timeout }
    }

    pub async fn indexing(&self) -> Result<IndexingServiceCounters, Error> {
        let response = self
            .transport
            .send::<()>(Method::GET, "indexing", None, None, None, self.timeout)
            .await?;
        let indexing_stats = response.deserialize().await?;
        Ok(indexing_stats)
    }
}

/// Client for Node-level Health APIs.
pub struct NodeHealthClient<'a> {
    transport: &'a Transport,
    timeout: Timeout,
}

impl<'a> NodeHealthClient<'a> {
    fn new(transport: &'a Transport, timeout: Timeout) -> Self {
        Self { transport, timeout }
    }

    /// Returns true if the node is healthy, returns false or an error otherwise.
    pub async fn is_live(&self) -> Result<bool, Error> {
        let response = self
            .transport
            .send::<()>(Method::GET, "/health/livez", None, None, None, self.timeout)
            .await?;
        let result: bool = response.deserialize().await?;
        Ok(result)
    }

    /// Returns true if the node is ready, returns false or an error otherwise.
    pub async fn is_ready(&self) -> Result<bool, Error> {
        let response = self
            .transport
            .send::<()>(
                Method::GET,
                "/health/readyz",
                None,
                None,
                None,
                self.timeout,
            )
            .await?;
        let result: bool = response.deserialize().await?;
        Ok(result)
    }
}

fn header_from_config_format(config_format: ConfigFormat) -> HeaderMap {
    let mut header_map = HeaderMap::new();
    let content_type_value = format!("application/{}", config_format.as_str());
    header_map.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&content_type_value).expect("Content type should always be valid."),
    );
    header_map
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::str::FromStr;

    use quickwit_config::{ConfigFormat, SourceConfig};
    use quickwit_indexing::mock_split;
    use quickwit_ingest::CommitType;
    use quickwit_metastore::IndexMetadata;
    use quickwit_serve::{
        ListSplitsQueryParams, ListSplitsResponse, RestIngestResponse, SearchRequestQueryString,
    };
    use reqwest::header::CONTENT_TYPE;
    use reqwest::{StatusCode, Url};
    use serde_json::json;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use wiremock::matchers::{
        body_bytes, body_json, header, method, path, query_param, query_param_is_missing,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::error::Error;
    use crate::models::{CumulatedIngestResponse, IngestSource, SearchResponseRestClient};
    use crate::rest_client::QuickwitClientBuilder;

    #[tokio::test]
    async fn test_client_no_server() {
        let port = quickwit_common::net::find_available_tcp_port().unwrap();
        let server_url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let error = qw_client.indexes().list().await.unwrap_err();

        assert!(matches!(error, Error::Client(_)));
        assert!(error.to_string().contains("tcp connect error"));
    }

    #[tokio::test]
    async fn test_search_endpoint() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        // Search
        let search_query_params = SearchRequestQueryString {
            ..Default::default()
        };
        let expected_search_response = SearchResponseRestClient {
            num_hits: 0,
            hits: Vec::new(),
            snippets: None,
            aggregations: None,
            elapsed_time_micros: 100,
            errors: Vec::new(),
        };
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/search"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(
                json!({"num_hits": 0, "hits": [], "elapsed_time_micros": 100, "errors": []}),
            ))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client
                .search("my-index", search_query_params)
                .await
                .unwrap(),
            expected_search_response
        );
    }

    fn get_ndjson_filepath(ndjson_dataset_filename: &str) -> String {
        format!(
            "{}/resources/tests/{}",
            env!("CARGO_MANIFEST_DIR"),
            ndjson_dataset_filename
        )
    }

    #[tokio::test]
    async fn test_ingest_endpoint() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let ndjson_filepath = get_ndjson_filepath("documents_to_ingest.json");
        let mut buffer = Vec::new();
        File::open(&ndjson_filepath)
            .await
            .unwrap()
            .read_to_end(&mut buffer)
            .await
            .unwrap();
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/ingest"))
            .and(query_param_is_missing("commit"))
            .and(body_bytes(buffer.clone()))
            .respond_with(ResponseTemplate::new(StatusCode::TOO_MANY_REQUESTS))
            .up_to_n_times(2)
            .expect(2)
            .mount(&mock_server)
            .await;
        let mock_response = RestIngestResponse {
            num_docs_for_processing: 2,
            num_ingested_docs: Some(2),
            num_rejected_docs: Some(0),
            parse_failures: Some(Vec::new()),
        };
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/ingest"))
            .and(query_param_is_missing("commit"))
            .and(body_bytes(buffer))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(&mock_response))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        let ingest_source = IngestSource::File(PathBuf::from_str(&ndjson_filepath).unwrap());
        let actual_response = qw_client
            .ingest("my-index", ingest_source, None, None, CommitType::Auto)
            .await
            .unwrap();
        assert_eq!(
            actual_response,
            CumulatedIngestResponse {
                num_docs_for_processing: 2,
                num_ingested_docs: Some(2),
                num_rejected_docs: Some(0),
                parse_failures: Some(Vec::new()),
                num_too_many_requests: 2,
            }
        );
    }

    #[tokio::test]
    async fn test_ingest_endpoint_with_force_commit() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let ndjson_filepath = get_ndjson_filepath("documents_to_ingest.json");
        let mut buffer = Vec::new();
        File::open(&ndjson_filepath)
            .await
            .unwrap()
            .read_to_end(&mut buffer)
            .await
            .unwrap();
        let mock_response = RestIngestResponse {
            num_docs_for_processing: 2,
            num_ingested_docs: Some(2),
            num_rejected_docs: Some(0),
            parse_failures: Some(Vec::new()),
        };
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/ingest"))
            .and(query_param("commit", "force"))
            .and(body_bytes(buffer))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(&mock_response))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        let ingest_source = IngestSource::File(PathBuf::from_str(&ndjson_filepath).unwrap());
        let actual_response = qw_client
            .ingest("my-index", ingest_source, None, None, CommitType::Force)
            .await
            .unwrap();
        assert_eq!(
            actual_response,
            CumulatedIngestResponse {
                num_docs_for_processing: 2,
                num_ingested_docs: Some(2),
                num_rejected_docs: Some(0),
                parse_failures: Some(Vec::new()),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_ingest_endpoint_with_wait_for_commit() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let ndjson_filepath = get_ndjson_filepath("documents_to_ingest.json");
        let mut buffer = Vec::new();
        File::open(&ndjson_filepath)
            .await
            .unwrap()
            .read_to_end(&mut buffer)
            .await
            .unwrap();
        let mock_response = RestIngestResponse {
            num_docs_for_processing: 2,
            num_ingested_docs: Some(2),
            num_rejected_docs: Some(0),
            parse_failures: Some(Vec::new()),
        };
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/ingest"))
            .and(query_param("commit", "wait_for"))
            .and(body_bytes(buffer))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(&mock_response))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        let ingest_source = IngestSource::File(PathBuf::from_str(&ndjson_filepath).unwrap());
        let actual_response = qw_client
            .ingest("my-index", ingest_source, None, None, CommitType::WaitFor)
            .await
            .unwrap();
        assert_eq!(
            actual_response,
            CumulatedIngestResponse {
                num_docs_for_processing: 2,
                num_ingested_docs: Some(2),
                num_rejected_docs: Some(0),
                parse_failures: Some(Vec::new()),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_ingest_endpoint_should_return_api_error() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let ndjson_filepath = get_ndjson_filepath("documents_to_ingest.json");
        let mut buffer = Vec::new();
        File::open(&ndjson_filepath)
            .await
            .unwrap()
            .read_to_end(&mut buffer)
            .await
            .unwrap();
        Mock::given(method("POST"))
            .and(path("/api/v1/my-index/ingest"))
            .and(body_bytes(buffer.clone()))
            .respond_with(
                ResponseTemplate::new(405).set_body_json(json!({"message": "internal error"})),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        let ingest_source = IngestSource::File(PathBuf::from_str(&ndjson_filepath).unwrap());
        let error = qw_client
            .ingest(
                "my-index",
                ingest_source,
                Some(4096),
                None,
                CommitType::Auto,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Api(_)));
        assert!(error.to_string().contains("internal error"));
    }

    #[tokio::test]
    async fn test_indexes_endpoints() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
        // GET indexes
        Mock::given(method("GET"))
            .and(path("/api/v1/indexes"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(vec![index_metadata.clone()]),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client.indexes().list().await.unwrap(),
            vec![index_metadata.clone()]
        );

        // POST create index
        let index_config_to_create = index_metadata.index_config.clone();
        Mock::given(method("POST"))
            .and(path("/api/v1/indexes"))
            .and(body_json(index_config_to_create.clone()))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(index_metadata.clone()),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        let post_body = serde_json::to_string(&index_config_to_create).unwrap();
        assert_eq!(
            qw_client
                .indexes()
                .create(post_body, ConfigFormat::Json, false)
                .await
                .unwrap(),
            index_metadata
        );

        // POST create index with yaml
        Mock::given(method("POST"))
            .and(path("/api/v1/indexes"))
            .and(header(CONTENT_TYPE.as_str(), "application/yaml"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(index_metadata.clone()),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client
                .indexes()
                .create("", ConfigFormat::Yaml, false)
                .await
                .unwrap(),
            index_metadata
        );

        // PUT clear index
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/clear"))
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client.indexes().clear("my-index").await.unwrap();

        // PUT clear index returns an error
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/clear"))
            .respond_with(ResponseTemplate::new(StatusCode::BAD_REQUEST))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client.indexes().clear("my-index").await.unwrap_err();

        // DELETE index
        Mock::given(method("DELETE"))
            .and(path("/api/v1/indexes/my-index"))
            .and(query_param("dry_run", "true"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(json!([{
                "split_id": "my-split",
                "num_docs": 1,
                "uncompressed_docs_size_bytes": 1024,
                "file_name": "my-split.split",
                "file_size_bytes": 128,
            }])))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client.indexes().delete("my-index", true).await.unwrap();

        // DELETE index returns an error
        Mock::given(method("DELETE"))
            .and(path("/api/v1/indexes/my-index"))
            .respond_with(ResponseTemplate::new(StatusCode::UNSUPPORTED_MEDIA_TYPE))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .indexes()
            .delete("my-index", true)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_splits_endpoints() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let split = mock_split("split-1");
        // GET splits
        let list_splits_params = ListSplitsQueryParams {
            start_timestamp: Some(1),
            ..Default::default()
        };
        let response = ListSplitsResponse {
            offset: 0,
            size: 1,
            splits: vec![split.clone()],
        };
        Mock::given(method("GET"))
            .and(path("/api/v1/indexes/my-index/splits"))
            .and(query_param("start_timestamp", "1"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(response))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client
                .splits("my-index")
                .list(list_splits_params)
                .await
                .unwrap(),
            vec![split.clone()]
        );

        // Mark for deletion
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/splits/mark-for-deletion"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_json(json!({"split_ids": ["split-1"]})),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .splits("my-index")
            .mark_for_deletion(vec!["split-1".to_string()])
            .await
            .unwrap();

        // Mark for deletion returns an error
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/splits/mark-for-deletion"))
            .respond_with(ResponseTemplate::new(StatusCode::METHOD_NOT_ALLOWED))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .splits("my-index")
            .mark_for_deletion(vec!["split-1".to_string()])
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_sources_endpoints() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();
        let source_config = SourceConfig::ingest_api_default();
        // POST create source with toml
        Mock::given(method("POST"))
            .and(path("/api/v1/indexes/my-index/sources"))
            .and(header(CONTENT_TYPE.as_str(), "application/toml"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(source_config.clone()),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client
                .sources("my-index")
                .create("", ConfigFormat::Toml)
                .await
                .unwrap(),
            source_config
        );

        // PUT update source with yaml
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/sources/my-source-1"))
            .and(header(CONTENT_TYPE.as_str(), "application/yaml"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(source_config.clone()),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client
                .sources("my-index")
                .update("my-source-1", "", ConfigFormat::Yaml)
                .await
                .unwrap(),
            source_config
        );

        // GET sources
        Mock::given(method("GET"))
            .and(path("/api/v1/indexes/my-index/sources"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(vec![source_config.clone()]),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        assert_eq!(
            qw_client.sources("my-index").list().await.unwrap(),
            vec![source_config.clone()]
        );

        // Toggle source
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/sources/my-source-1/toggle"))
            .respond_with(
                ResponseTemplate::new(StatusCode::OK).set_body_json(json!({"enable": true})),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .toggle("my-source-1", true)
            .await
            .unwrap();

        // Toggle source returns an error
        Mock::given(method("PUT"))
            .and(path("/api/v1/indexes/my-index/sources/my-source-2/toggle"))
            .respond_with(ResponseTemplate::new(StatusCode::BAD_REQUEST))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .toggle("my-source-2", true)
            .await
            .unwrap_err();

        // PUT reset checkpoint
        Mock::given(method("PUT"))
            .and(path(
                "/api/v1/indexes/my-index/sources/my-source/reset-checkpoint",
            ))
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .reset_checkpoint("my-source")
            .await
            .unwrap();

        // PUT reset checkpoint returns an error
        Mock::given(method("PUT"))
            .and(path(
                "/api/v1/indexes/my-index/sources/my-source/reset-checkpoint",
            ))
            .respond_with(ResponseTemplate::new(StatusCode::BAD_GATEWAY))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .reset_checkpoint("my-source")
            .await
            .unwrap_err();

        // DELETE source
        Mock::given(method("DELETE"))
            .and(path("/api/v1/indexes/my-index/sources/my-source"))
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .delete("my-source")
            .await
            .unwrap();

        // DELETE source returns an error
        Mock::given(method("DELETE"))
            .and(path("/api/v1/indexes/my-index/sources/my-source"))
            .respond_with(ResponseTemplate::new(StatusCode::BAD_GATEWAY))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        qw_client
            .sources("my-index")
            .delete("my-source")
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_health_endpoints() {
        let mock_server = MockServer::start().await;
        let server_url = Url::parse(&mock_server.uri()).unwrap();
        let qw_client = QuickwitClientBuilder::new(server_url).build();

        assert!(qw_client.node_health().is_live().await.is_err());
        assert!(qw_client.node_health().is_ready().await.is_err());

        // GET /health/livez
        Mock::given(method("GET"))
            .and(path("/health/livez"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(true))
            .expect(1)
            .mount(&mock_server)
            .await;
        assert!(qw_client.node_health().is_live().await.unwrap());

        // GET /health/readyz
        Mock::given(method("GET"))
            .and(path("/health/readyz"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(true))
            .expect(1)
            .mount(&mock_server)
            .await;
        assert!(qw_client.node_health().is_ready().await.unwrap());
    }
}
