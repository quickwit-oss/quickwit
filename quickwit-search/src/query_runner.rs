use std::{collections::{HashMap, HashSet}, net::SocketAddr, sync::Arc};
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};

use itertools::{Itertools};
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{FetchDocsRequest, FetchDocsResult, LeafSearchRequest, LeafSearchResult, PartialHit, SearchRequest};

use crate::{ClientPool, SearchClientPool, SearchError, SearchServiceClient, client_pool::Job, list_relevant_splits, root::job_for_splits};


#[derive(Debug)]
pub struct NodeAddrSearchError {
    pub search_error: SearchError,
    pub split_ids: Vec<String>,
    pub node_addr: SocketAddr,
}

// ExecutionPlan gives the list of requests to send to 
// cluster nodes. The struct does nothing currently,
// could possible be removed. I keep it only to
// have the concept of plan.
struct ExecutionPlan<R> {
    requests: Vec<(SearchServiceClient, R)>
}


// DistributedQueryRunner takes a root request, 
// compute the query plan for it and then execute
// the plan with a retry policy.
#[async_trait]
trait DistributedQueryRunner {
    type RootRequest: Send + Sync;
    type LeafRequest: Send + Sync;
    type LeafResult: Send + Sync;

    /// Generate query plan of the query. The plan defines which requests needs to be sent to each node.
    async fn query_plan(&self, request: Self::RootRequest) -> anyhow::Result<ExecutionPlan<Self::LeafRequest>>;
    
    /// Execute a leaf request.
    async fn execute_leaf(&self, mut client: SearchServiceClient, leaf_request: Self::LeafRequest) -> Result<Self::LeafResult, NodeAddrSearchError>;

    /// Execute and distribute the request to cluster nodes and gather results.
    async fn execute(&self, request: Self::RootRequest) -> anyhow::Result<Vec<Result<Self::LeafResult, NodeAddrSearchError>>> {
        let execution_plan = self.query_plan(request).await?;
        let results = self.execute_plan(execution_plan).await;
        let (mut successes, errors): (Vec<Result<Self::LeafResult, NodeAddrSearchError>>, Vec<Result<Self::LeafResult, NodeAddrSearchError>>) = results
            .into_iter()
            .partition(|result| result.is_ok());
        if errors.is_empty() {
            return Ok(successes)
        }
        if let Some(retry_plan) = self.retry_plan(&successes, &errors).await {
            // TODO: this is not clean, someone can implement a retry plan that will return requests even on successful results.
            let retry_results = self.execute_plan(retry_plan).await;
            successes.extend(retry_results);
        } else {
            // too bad, return success and errors together
            successes.extend(errors);
        }

        Ok(vec![])
    }

    /// Retry failed requests. No retry by default.
    async fn retry_plan(&self, _successes: &[Result<Self::LeafResult, NodeAddrSearchError>], _errors: &[Result<Self::LeafResult, NodeAddrSearchError>]) -> Option<ExecutionPlan<Self::LeafRequest>> {
        None
    }

    /// Execute the query plan.
    async fn execute_plan(&self, execution_plan: ExecutionPlan<Self::LeafRequest>) -> Vec<Result<Self::LeafResult, NodeAddrSearchError>> {
        execution_plan.requests
            .into_iter()
            .map(|(client, request)| async {
                self.execute_leaf(client, request).await
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<Result<Self::LeafResult, NodeAddrSearchError>>>().await
    }

}

struct SearchQueryExecutor {
    metastore: Arc<dyn Metastore>,
    client_pool: Arc<SearchClientPool>
}

#[async_trait]
impl DistributedQueryRunner for SearchQueryExecutor {
    type RootRequest = SearchRequest;
    type LeafRequest = LeafSearchRequest;
    type LeafResult = LeafSearchResult;

    async fn execute_leaf(&self, mut client: SearchServiceClient, leaf_request: Self::LeafRequest) -> Result<Self::LeafResult, NodeAddrSearchError> {
        client.leaf_search(leaf_request)
            .await
            .map_err(|search_error| NodeAddrSearchError {
                search_error,
                split_ids: vec![],
                node_addr: client.grpc_addr(),
            })
    }

    async fn query_plan(&self, request: Self::RootRequest) -> anyhow::Result<ExecutionPlan<Self::LeafRequest>> {
        let split_metadata_list = list_relevant_splits(&request, self.metastore.as_ref()).await?;
        let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
            .into_iter()
            .map(|split_metadata| (split_metadata.split_id.clone(), split_metadata))
            .collect();
        let leaf_search_jobs: Vec<Job> =
            job_for_splits(&split_metadata_map.keys().collect(), &split_metadata_map);
        let assigned_leaf_search_jobs = self.client_pool
            .assign_jobs(leaf_search_jobs, &HashSet::default())
            .await?;
        let requests = assigned_leaf_search_jobs.iter()
            .map(|(client, jobs)| {
                let mut request_with_offset_0 = request.clone();
                request_with_offset_0.start_offset = 0;
                request_with_offset_0.max_hits += request.start_offset;
                let leaf_request = LeafSearchRequest {
                    search_request: Some(request_with_offset_0),
                    split_ids: jobs.iter().map(|job| job.split.clone()).collect(),
                };
                (client.clone(), leaf_request)
            })
            .collect_vec();
        Ok(ExecutionPlan::<Self::LeafRequest>{ requests })
    }
}

struct FetchDocsExecutor {
    client_pool: Arc<SearchClientPool>
}

#[async_trait]
impl DistributedQueryRunner for FetchDocsExecutor {
    type RootRequest = FetchDocsRequest;
    type LeafRequest = FetchDocsRequest;
    type LeafResult = FetchDocsResult;

    async fn execute_leaf(&self, mut client: SearchServiceClient, leaf_request: Self::LeafRequest) -> Result<Self::LeafResult, NodeAddrSearchError> {
        client.fetch_docs(leaf_request)
            .await
            .map_err(|search_error| NodeAddrSearchError {
                search_error,
                split_ids: vec![],
                node_addr: client.grpc_addr(),
            })
    }

    async fn query_plan(&self, request: Self::RootRequest) -> anyhow::Result<ExecutionPlan<Self::LeafRequest>> {
        let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
        for partial_hit in request.partial_hits.iter() {
            partial_hits_map
                .entry(partial_hit.split_id.clone())
                .or_insert_with(Vec::new)
                .push(partial_hit.clone());
        }
        let fetch_docs_req_jobs = request.partial_hits
            .iter()
            .map(|hit| Job {
                split: hit.split_id.to_string(),
                cost: 1,
            })
            .collect_vec();
        let doc_fetch_jobs = self.client_pool
            .assign_jobs(fetch_docs_req_jobs, &HashSet::default())
            .await?;
        let requests = doc_fetch_jobs.iter()
            .map(|(client, jobs)| {
                let partial_hits = jobs.iter()
                    .map(|job| partial_hits_map.get(&job.split).unwrap())
                    .flat_map(|l| l.clone())
                    .collect_vec();
                let fetch_docs_request = FetchDocsRequest {
                    partial_hits: partial_hits,
                    index_id: request.index_id.clone(),
                };
                (client.clone(), fetch_docs_request)
            })
            .collect_vec();
        Ok(ExecutionPlan::<Self::LeafRequest>{ requests })
    }
}
