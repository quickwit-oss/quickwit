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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use anyhow::bail;
use async_trait::async_trait;
use quickwit_common::SocketAddrLegacyHash;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::rendezvous_hasher::{node_affinity, sort_by_rendez_vous_hash};
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use tracing::{info, warn};

use crate::{SEARCH_METRICS, SearchJob, SearchServiceClient, SearcherPool};

/// Job.
/// The unit in which distributed search is performed.
///
/// The `split_id` is used to define an affinity between a leaf nodes and a job.
/// The `cost` is used to spread the work evenly amongst nodes.
pub trait Job {
    /// Split ID of the targeted split.
    fn split_id(&self) -> &str;

    /// Estimation of the load associated with running a given job.
    ///
    /// A list of jobs will be assigned to leaf nodes in a way that spread
    /// the sum of cost evenly.
    fn cost(&self) -> usize;

    /// Compares the cost of two jobs in reverse order, breaking ties by split ID.
    fn compare_cost(&self, other: &Self) -> Ordering {
        self.cost()
            .cmp(&other.cost())
            .reverse()
            .then_with(|| self.split_id().cmp(other.split_id()))
    }
}

/// Search job placer.
/// It assigns jobs to search clients.
#[derive(Clone, Default)]
pub struct SearchJobPlacer {
    /// Search clients pool.
    searcher_pool: SearcherPool,
}

#[async_trait]
impl EventSubscriber<ReportSplitsRequest> for SearchJobPlacer {
    async fn handle_event(&mut self, evt: ReportSplitsRequest) {
        let mut nodes: HashMap<SocketAddr, SearchServiceClient> =
            self.searcher_pool.pairs().into_iter().collect();
        if nodes.is_empty() {
            return;
        }
        let mut splits_per_node: HashMap<SocketAddr, Vec<ReportSplit>> =
            HashMap::with_capacity(nodes.len().min(evt.report_splits.len()));
        for report_split in evt.report_splits {
            let node_addr = nodes
                .keys()
                .max_by_key(|node_addr| {
                    node_affinity(SocketAddrLegacyHash(node_addr), &report_split.split_id)
                })
                // This actually never happens thanks to the if-condition at the
                // top of this function.
                .expect("`nodes` should not be empty");
            splits_per_node
                .entry(*node_addr)
                .or_default()
                .push(report_split);
        }
        for (node_addr, report_splits) in splits_per_node {
            if let Some(search_client) = nodes.get_mut(&node_addr) {
                let report_splits_req = ReportSplitsRequest { report_splits };
                let _ = search_client.report_splits(report_splits_req).await;
            }
        }
    }
}

impl fmt::Debug for SearchJobPlacer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SearchJobPlacer").finish()
    }
}

impl SearchJobPlacer {
    /// Returns an [`SearchJobPlacer`] from a search service client pool.
    pub fn new(searcher_pool: SearcherPool) -> Self {
        Self { searcher_pool }
    }
}

struct SocketAddrAndClient {
    socket_addr: SocketAddr,
    client: SearchServiceClient,
}

impl Hash for SocketAddrAndClient {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        SocketAddrLegacyHash(&self.socket_addr).hash(hasher);
    }
}

impl SearchJobPlacer {
    /// Returns an iterator over the search nodes, ordered by their affinity
    /// with the `affinity_key`, as defined by rendez-vous hashing.
    pub async fn best_nodes_per_affinity(
        &self,
        affinity_key: &[u8],
    ) -> impl Iterator<Item = SearchServiceClient> {
        let mut nodes: Vec<SocketAddrAndClient> = self
            .searcher_pool
            .pairs()
            .into_iter()
            .map(|(socket_addr, client)| SocketAddrAndClient {
                socket_addr,
                client,
            })
            .collect();
        sort_by_rendez_vous_hash(&mut nodes[..], affinity_key);
        nodes
            .into_iter()
            .map(|socket_addr_and_client| socket_addr_and_client.client)
    }

    /// Assign the given job to the clients
    /// Returns a list of pair (SocketAddr, `Vec<Job>`)
    ///
    /// When exclude_addresses filters all clients it is ignored.
    pub async fn assign_jobs<J: Job>(
        &self,
        mut jobs: Vec<J>,
        excluded_addrs: &HashSet<SocketAddr>,
    ) -> anyhow::Result<impl Iterator<Item = (SearchServiceClient, Vec<J>)> + use<J>> {
        let mut all_nodes = self.searcher_pool.pairs();

        if all_nodes.is_empty() {
            bail!(
                "failed to assign search jobs: there are no available searcher nodes in the \
                 cluster"
            );
        }
        if !excluded_addrs.is_empty() && excluded_addrs.len() < all_nodes.len() {
            all_nodes.retain(|(grpc_addr, _)| !excluded_addrs.contains(grpc_addr));

            // This should never happen, but... belt and suspenders policy.
            if all_nodes.is_empty() {
                bail!(
                    "failed to assign search jobs: there are no searcher nodes candidates for \
                     these jobs"
                );
            }
            info!(
                "excluded {} nodes from search job placement, {} remaining",
                excluded_addrs.len(),
                all_nodes.len()
            );
        }
        let mut candidate_nodes: Vec<CandidateNode> = all_nodes
            .into_iter()
            .map(|(grpc_addr, client)| CandidateNode {
                grpc_addr,
                client,
                load: 0,
            })
            .collect();

        jobs.sort_unstable_by(Job::compare_cost);

        let num_nodes = candidate_nodes.len();

        let mut job_assignments: HashMap<SocketAddr, (SearchServiceClient, Vec<J>)> =
            HashMap::with_capacity(num_nodes);

        let total_load: usize = jobs.iter().map(|job| job.cost()).sum();

        // allow around 5% disparity. Round up so we never end up in a case where
        // target_load * num_nodes < total_load
        // some of our tests needs 2 splits to be put on 2 different searchers. It makes sense for
        // these tests to keep doing so (testing root merge). Either we can make the allowed
        // difference stricter, find the right split names ("split6" instead of "split2" works).
        // or modify mock_split_meta() so that not all splits have the same job cost
        // for now i went with the mock_split_meta() changes.
        const ALLOWED_DIFFERENCE: usize = 105;
        let target_load = (total_load * ALLOWED_DIFFERENCE).div_ceil(num_nodes * 100);
        for job in jobs {
            sort_by_rendez_vous_hash(&mut candidate_nodes, job.split_id());

            let (chosen_node_idx, chosen_node) = if let Some((idx, node)) = candidate_nodes
                .iter_mut()
                .enumerate()
                .find(|(_pos, node)| node.load < target_load)
            {
                (idx, node)
            } else {
                warn!("found no lightly loaded searcher for split, this should never happen");
                (0, &mut candidate_nodes[0])
            };
            let metric_node_idx = match chosen_node_idx {
                0 => "0",
                1 => "1",
                _ => "> 1",
            };
            SEARCH_METRICS
                .job_assigned_total
                .with_label_values([metric_node_idx])
                .inc();
            chosen_node.load += job.cost();

            job_assignments
                .entry(chosen_node.grpc_addr)
                .or_insert_with(|| (chosen_node.client.clone(), Vec::new()))
                .1
                .push(job);
        }
        Ok(job_assignments.into_values())
    }

    /// Assigns a single job to a client.
    pub async fn assign_job<J: Job>(
        &self,
        job: J,
        excluded_addrs: &HashSet<SocketAddr>,
    ) -> anyhow::Result<SearchServiceClient> {
        let client = self
            .assign_jobs(vec![job], excluded_addrs)
            .await?
            .next()
            .map(|(client, _jobs)| client)
            .expect("`assign_jobs` should return at least one client or fail.");
        Ok(client)
    }
}

#[derive(Debug, Clone)]
struct CandidateNode {
    pub grpc_addr: SocketAddr,
    pub client: SearchServiceClient,
    pub load: usize,
}

impl Hash for CandidateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        SocketAddrLegacyHash(&self.grpc_addr).hash(state);
    }
}

impl PartialEq for CandidateNode {
    fn eq(&self, other: &Self) -> bool {
        self.grpc_addr == other.grpc_addr
    }
}

impl Eq for CandidateNode {}

/// Groups jobs by index id and returns a list of `SearchJob` per index
pub fn group_jobs_by_index_id(
    jobs: Vec<SearchJob>,
    cb: impl FnMut(Vec<SearchJob>) -> crate::Result<()>,
) -> crate::Result<()> {
    // Group jobs by index uid.
    group_by(jobs, |job| &job.index_uid, cb)?;
    Ok(())
}

/// Note: The data will be sorted.
///
/// Returns slices of the input data grouped by passed closure.
pub fn group_by<T, K: Ord, F>(
    mut data: Vec<T>,
    compare_by: impl Fn(&T) -> &K,
    mut callback: F,
) -> crate::Result<()>
where
    F: FnMut(Vec<T>) -> crate::Result<()>,
{
    data.sort_by(|job1, job2| compare_by(job2).cmp(compare_by(job1)));
    while !data.is_empty() {
        let last_element = data.last().unwrap();
        let count = data
            .iter()
            .rev()
            .take_while(|&x| compare_by(x) == compare_by(last_element))
            .count();

        let group = data.split_off(data.len() - count);
        callback(group)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockSearchService, SearchJob, searcher_pool_for_test};

    #[test]
    fn test_group_by_1() {
        let data = vec![1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 5];
        let mut outputs: Vec<Vec<i32>> = Vec::new();
        group_by(
            data,
            |el| el,
            |group| {
                outputs.push(group);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outputs.len(), 5);
        assert_eq!(outputs[0], vec![1, 1]);
        assert_eq!(outputs[1], vec![2, 2, 2]);
        assert_eq!(outputs[2], vec![3]);
        assert_eq!(outputs[3], vec![4, 4]);
        assert_eq!(outputs[4], vec![5, 5, 5]);
    }
    #[test]
    fn test_group_by_all_same() {
        let data = vec![1, 1];
        let mut outputs: Vec<Vec<i32>> = Vec::new();
        group_by(
            data,
            |el| el,
            |group| {
                outputs.push(group);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0], vec![1, 1]);
    }
    #[test]
    fn test_group_by_empty() {
        let data = vec![];
        let mut outputs: Vec<Vec<i32>> = Vec::new();
        group_by(
            data,
            |el| el,
            |group| {
                outputs.push(group);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outputs.len(), 0);
    }

    #[tokio::test]
    async fn test_search_job_placer() {
        {
            let searcher_pool = SearcherPool::default();
            let search_job_placer = SearchJobPlacer::new(searcher_pool);
            assert!(
                search_job_placer
                    .assign_jobs::<SearchJob>(Vec::new(), &HashSet::new())
                    .await
                    .is_err()
            );
        }
        {
            let searcher_pool =
                searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
            let search_job_placer = SearchJobPlacer::new(searcher_pool);
            let jobs = vec![
                SearchJob::for_test("split1", 1),
                SearchJob::for_test("split2", 2),
                SearchJob::for_test("split3", 3),
                SearchJob::for_test("split4", 4),
            ];
            let assigned_jobs: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
                .assign_jobs(jobs, &HashSet::default())
                .await
                .unwrap()
                .map(|(client, jobs)| (client.grpc_addr(), jobs))
                .collect();
            let expected_searcher_addr: SocketAddr = ([127, 0, 0, 1], 1001).into();
            let expected_assigned_jobs = vec![(
                expected_searcher_addr,
                vec![
                    SearchJob::for_test("split4", 4),
                    SearchJob::for_test("split3", 3),
                    SearchJob::for_test("split2", 2),
                    SearchJob::for_test("split1", 1),
                ],
            )];
            assert_eq!(assigned_jobs, expected_assigned_jobs);
        }
        {
            let searcher_pool = searcher_pool_for_test([
                ("127.0.0.1:1001", MockSearchService::new()),
                ("127.0.0.1:1002", MockSearchService::new()),
            ]);
            let search_job_placer = SearchJobPlacer::new(searcher_pool);
            let jobs = vec![
                SearchJob::for_test("split1", 1),
                SearchJob::for_test("split2", 2),
                SearchJob::for_test("split3", 3),
                SearchJob::for_test("split4", 4),
                SearchJob::for_test("split5", 5),
                SearchJob::for_test("split6", 6),
            ];
            let mut assigned_jobs: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
                .assign_jobs(jobs, &HashSet::default())
                .await
                .unwrap()
                .map(|(client, jobs)| (client.grpc_addr(), jobs))
                .collect();
            assigned_jobs.sort_unstable_by_key(|(node_uid, _)| *node_uid);

            let expected_searcher_addr_1: SocketAddr = ([127, 0, 0, 1], 1001).into();
            let expected_searcher_addr_2: SocketAddr = ([127, 0, 0, 1], 1002).into();
            // on a small number of splits, we may be unbalanced
            let expected_assigned_jobs = vec![
                (
                    expected_searcher_addr_1,
                    vec![
                        SearchJob::for_test("split5", 5),
                        SearchJob::for_test("split4", 4),
                        SearchJob::for_test("split3", 3),
                    ],
                ),
                (
                    expected_searcher_addr_2,
                    vec![
                        SearchJob::for_test("split6", 6),
                        SearchJob::for_test("split2", 2),
                        SearchJob::for_test("split1", 1),
                    ],
                ),
            ];
            assert_eq!(assigned_jobs, expected_assigned_jobs);
        }
        {
            let searcher_pool = searcher_pool_for_test([
                ("127.0.0.1:1001", MockSearchService::new()),
                ("127.0.0.1:1002", MockSearchService::new()),
            ]);
            let search_job_placer = SearchJobPlacer::new(searcher_pool);
            let jobs = vec![
                SearchJob::for_test("split1", 1000),
                SearchJob::for_test("split2", 1),
            ];
            let mut assigned_jobs: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
                .assign_jobs(jobs, &HashSet::default())
                .await
                .unwrap()
                .map(|(client, jobs)| (client.grpc_addr(), jobs))
                .collect();
            assigned_jobs.sort_unstable_by_key(|(node_uid, _)| *node_uid);

            let expected_searcher_addr_1: SocketAddr = ([127, 0, 0, 1], 1001).into();
            let expected_searcher_addr_2: SocketAddr = ([127, 0, 0, 1], 1002).into();
            let expected_assigned_jobs = vec![
                (
                    expected_searcher_addr_1,
                    vec![SearchJob::for_test("split1", 1000)],
                ),
                (
                    expected_searcher_addr_2,
                    vec![SearchJob::for_test("split2", 1)],
                ),
            ];
            assert_eq!(assigned_jobs, expected_assigned_jobs);
        }
    }

    #[tokio::test]
    async fn test_search_job_placer_many_splits() {
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", MockSearchService::new()),
            ("127.0.0.1:1002", MockSearchService::new()),
            ("127.0.0.1:1003", MockSearchService::new()),
            ("127.0.0.1:1004", MockSearchService::new()),
            ("127.0.0.1:1005", MockSearchService::new()),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let jobs = (0..1000)
            .map(|id| SearchJob::for_test(&format!("split{id}"), 1))
            .collect();
        let jobs_len: Vec<usize> = search_job_placer
            .assign_jobs(jobs, &HashSet::default())
            .await
            .unwrap()
            .map(|(_, jobs)| jobs.len())
            .collect();
        for job_len in jobs_len {
            assert!(job_len <= 1050 / 5);
        }
    }
}
