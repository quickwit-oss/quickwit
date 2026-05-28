use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytesize::ByteSize;
use quickwit_proto::cloudprem::{
    CloudPremService, CloudPremServiceClient, CloudPremServiceGrpcClientAdapter, PingRequest,
    cloud_prem_service_grpc_client,
};
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, GetKvRequest, LeafListFieldsRequest, LeafListTermsRequest,
    LeafListTermsResponse, LeafSearchRequest, LeafSearchResponse, ListFieldsRequest,
    ListFieldsResponse, ListTermsRequest, ListTermsResponse, PutKvRequest, ReportSplitsRequest,
    ReportSplitsResponse, ScrollRequest, SearchPlanResponse, SearchRequest, SearchResponse,
};
use quickwit_search::SearchService;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{ClientTlsConfig, Endpoint, Uri};

const MAX_MESSAGE_SIZE: ByteSize = ByteSize::mib(5);

pub struct CloudPremRootSearchService(CloudPremServiceClient);

impl CloudPremRootSearchService {
    pub async fn new(
        target: &str,
        proxy_addr: Option<SocketAddr>,
        tls_config: Option<ClientTlsConfig>,
        mtls_header: Option<String>,
    ) -> anyhow::Result<Self> {
        let mtls_header: Option<MetadataValue<Ascii>> = mtls_header
            .map(|mtls_header| mtls_header.parse())
            .transpose()?;
        let scheme = if tls_config.is_some() {
            "https"
        } else {
            "http"
        };

        let target_addr = if let Some(proxy_addr) = proxy_addr {
            proxy_addr
        } else {
            target
                .to_socket_addrs()?
                .next()
                .context("failed to resolve target")?
        };

        let channel = {
            let connect_uri = Uri::builder()
                .scheme(scheme)
                .authority(target_addr.to_string())
                .path_and_query("/")
                .build()
                .expect("provided arguments should be valid");

            let origin_uri = Uri::builder()
                .scheme(scheme)
                .authority(target.to_string())
                .path_and_query("/")
                .build()
                .expect("provided arguments should be valid");

            let mut endpoint = Endpoint::from(connect_uri)
                .connect_timeout(Duration::from_secs(5))
                .origin(origin_uri);

            if let Some(tls_config) = tls_config {
                endpoint = endpoint
                    .tls_config(tls_config)
                    .context("failed to load tls configuration")?;
            }
            endpoint.connect().await?
        };

        let cloudprem_client = {
            let (_, connection_keys_watcher) =
                tokio::sync::watch::channel(std::collections::HashSet::from_iter([target_addr]));
            let client =
                cloud_prem_service_grpc_client::CloudPremServiceGrpcClient::with_interceptor(
                    channel,
                    move |mut req: Request<()>| {
                        if let Some(mtls_header) = &mtls_header {
                            req.metadata_mut()
                                .insert("x-amzn-mtls-clientcert", mtls_header.clone());
                        }
                        Ok(req)
                    },
                )
                .max_decoding_message_size(MAX_MESSAGE_SIZE.0 as usize)
                .max_encoding_message_size(MAX_MESSAGE_SIZE.0 as usize);
            let adapter = CloudPremServiceGrpcClientAdapter::new(client, connection_keys_watcher);
            CloudPremServiceClient::new(adapter)
        };

        cloudprem_client
            .ping(PingRequest {
                org_id: 0,
                scope: Default::default(),
            })
            .await?;

        Ok(CloudPremRootSearchService(cloudprem_client))
    }
}

fn unimplemented<T, R>(_request: T) -> quickwit_search::Result<R> {
    // returning the name of the function would be better, but type of the request is good enought
    Err(quickwit_search::SearchError::Internal(format!(
        "unimplemented request kind: {}",
        std::any::type_name::<T>()
    )))
}

#[async_trait]
impl SearchService for CloudPremRootSearchService {
    async fn root_search(&self, request: SearchRequest) -> quickwit_search::Result<SearchResponse> {
        self.0.root_search(request).await.map_err(Into::into)
    }

    async fn root_list_terms(
        &self,
        request: ListTermsRequest,
    ) -> quickwit_search::Result<ListTermsResponse> {
        self.0.root_list_terms(request).await.map_err(Into::into)
    }

    async fn leaf_search(
        &self,
        request: LeafSearchRequest,
    ) -> quickwit_search::Result<LeafSearchResponse> {
        unimplemented(request)
    }
    async fn fetch_docs(
        &self,
        request: FetchDocsRequest,
    ) -> quickwit_search::Result<FetchDocsResponse> {
        unimplemented(request)
    }

    async fn leaf_list_terms(
        &self,
        request: LeafListTermsRequest,
    ) -> quickwit_search::Result<LeafListTermsResponse> {
        unimplemented(request)
    }
    async fn scroll(&self, request: ScrollRequest) -> quickwit_search::Result<SearchResponse> {
        unimplemented(request)
    }
    async fn put_kv(&self, _request: PutKvRequest) {}
    async fn get_kv(&self, _request: GetKvRequest) -> Option<Vec<u8>> {
        None
    }
    async fn report_splits(&self, _request: ReportSplitsRequest) -> ReportSplitsResponse {
        ReportSplitsResponse {}
    }
    async fn root_list_fields(
        &self,
        request: ListFieldsRequest,
    ) -> quickwit_search::Result<ListFieldsResponse> {
        unimplemented(request)
    }
    async fn leaf_list_fields(
        &self,
        request: LeafListFieldsRequest,
    ) -> quickwit_search::Result<ListFieldsResponse> {
        unimplemented(request)
    }
    async fn search_plan(
        &self,
        request: SearchRequest,
    ) -> quickwit_search::Result<SearchPlanResponse> {
        unimplemented(request)
    }
    async fn get_load(&self) -> usize {
        0
    }
}
