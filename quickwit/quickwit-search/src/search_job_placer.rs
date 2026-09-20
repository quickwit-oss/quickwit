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
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::rendezvous_hasher::{node_affinity, sort_by_rendez_vous_hash};
use quickwit_metrics::counter;
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use quickwit_proto::types::NodeId;
use tracing::{info, warn};

use crate::metrics::JOB_ASSIGNED_TOTAL;
use crate::{SearchJob, SearchServiceClient, SearcherNode, SearcherPool};

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
        let mut nodes: HashMap<SocketAddr, SearcherNode> =
            self.searcher_pool.pairs().into_iter().collect();
        if nodes.is_empty() {
            return;
        }
        let mut splits_per_node: HashMap<SocketAddr, Vec<ReportSplit>> =
            HashMap::with_capacity(nodes.len().min(evt.report_splits.len()));
        for report_split in evt.report_splits {
            let node_addr = nodes
                .iter()
                .max_by_key(|(_node_addr, node)| {
                    node_affinity(&node.node_id, &report_split.split_id)
                })
                // This actually never happens thanks to the if-condition at the
                // top of this function.
                .map(|(node_addr, _node)| *node_addr)
                .expect("`nodes` should not be empty");
            splits_per_node
                .entry(node_addr)
                .or_default()
                .push(report_split);
        }
        for (node_addr, report_splits) in splits_per_node {
            if let Some(searcher_node) = nodes.get_mut(&node_addr) {
                let report_splits_req = ReportSplitsRequest { report_splits };
                let _ = searcher_node.client.report_splits(report_splits_req).await;
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

struct NodeIdAndClient {
    node_id: NodeId,
    client: SearchServiceClient,
}

impl Hash for NodeIdAndClient {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.node_id.hash(hasher);
    }
}

impl SearchJobPlacer {
    /// Returns an iterator over the search nodes, ordered by their affinity
    /// with the `affinity_key`, as defined by rendez-vous hashing.
    pub async fn best_nodes_per_affinity(
        &self,
        affinity_key: &[u8],
    ) -> impl Iterator<Item = SearchServiceClient> {
        let mut nodes: Vec<NodeIdAndClient> = self
            .searcher_pool
            .pairs()
            .into_iter()
            .map(|(_socket_addr, searcher_node)| NodeIdAndClient {
                node_id: searcher_node.node_id,
                client: searcher_node.client,
            })
            .collect();
        sort_by_rendez_vous_hash(&mut nodes[..], affinity_key);
        nodes
            .into_iter()
            .map(|node_id_and_client| node_id_and_client.client)
    }

    /// Returns searcher node IDs ordered by decreasing affinity with `affinity_key`.
    #[cfg(test)]
    async fn best_node_ids_per_affinity(&self, affinity_key: impl Hash) -> Vec<NodeId> {
        let mut nodes: Vec<SearcherNode> = self
            .searcher_pool
            .pairs()
            .into_iter()
            .map(|(_grpc_addr, searcher_node)| searcher_node)
            .collect();
        sort_by_rendez_vous_hash(&mut nodes[..], affinity_key);
        nodes
            .into_iter()
            .map(|searcher_node| searcher_node.node_id)
            .collect()
    }

    /// Assign the given job to the clients
    /// Returns a list of pair (SocketAddr, `Vec<Job>`)
    ///
    /// Balances only this request's cost, preferring stable node-ID affinity.
    /// Ignoring other requests' queued/active work keeps repeated placements cache-local.
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
            .map(|(grpc_addr, searcher_node)| CandidateNode {
                affinity_id: searcher_node.node_id,
                grpc_addr,
                client: searcher_node.client,
                load: 0,
            })
            .collect();

        jobs.sort_unstable_by(Job::compare_cost);

        let num_nodes = candidate_nodes.len();

        let mut job_assignments: HashMap<SocketAddr, (SearchServiceClient, Vec<J>)> =
            HashMap::with_capacity(num_nodes);

        let total_load: usize = jobs.iter().map(|job| job.cost()).sum();

        // Allow around 5% disparity, rounding up. Only cost assigned within this
        // request counts toward the target; existing work on nodes is ignored.
        const ALLOWED_DIFFERENCE: usize = 105;
        let target_load = (total_load * ALLOWED_DIFFERENCE).div_ceil(num_nodes * 100);
        for job in jobs {
            sort_by_rendez_vous_hash(&mut candidate_nodes, job.split_id());

            let (chosen_node_idx, chosen_node) = if let Some((idx, node)) = candidate_nodes
                .iter_mut()
                .enumerate()
                // Check before adding this job: a single job may exceed the target.
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
            counter!(parent: JOB_ASSIGNED_TOTAL, "affinity" => metric_node_idx).inc();
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
    affinity_id: NodeId,
    pub grpc_addr: SocketAddr,
    pub client: SearchServiceClient,
    /// Cost assigned to this node within the current placement call.
    pub load: usize,
}

impl Hash for CandidateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.affinity_id.hash(state);
    }
}

impl PartialEq for CandidateNode {
    fn eq(&self, other: &Self) -> bool {
        self.affinity_id == other.affinity_id
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
    use std::sync::Arc;

    use super::*;
    use crate::{
        MockSearchService, SearchJob, SearchServiceClient, SearcherNode, searcher_pool_for_test,
    };

    fn searcher_pool_with_loads_for_test(
        iter: impl IntoIterator<Item = (&'static str, usize)>,
    ) -> SearcherPool {
        SearcherPool::from_iter(iter.into_iter().map(|(grpc_addr_str, load)| {
            let grpc_addr: SocketAddr = grpc_addr_str
                .parse()
                .expect("the gRPC address should be a valid socket address");
            let client =
                SearchServiceClient::from_service(Arc::new(MockSearchService::new()), grpc_addr)
                    .with_test_load(load);
            (grpc_addr, SearcherNode::for_test(client))
        }))
    }

    fn searcher_pool_for_named_nodes(
        iter: impl IntoIterator<Item = (&'static str, &'static str, usize)>,
    ) -> SearcherPool {
        SearcherPool::from_iter(iter.into_iter().map(|(node_id, grpc_addr_str, load)| {
            let grpc_addr: SocketAddr = grpc_addr_str
                .parse()
                .expect("the gRPC address should be a valid socket address");
            let client =
                SearchServiceClient::from_service(Arc::new(MockSearchService::new()), grpc_addr)
                    .with_test_load(load);
            (
                grpc_addr,
                SearcherNode {
                    node_id: NodeId::from_str(node_id),
                    client,
                },
            )
        }))
    }

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
                        SearchJob::for_test("split4", 4),
                        SearchJob::for_test("split3", 3),
                        SearchJob::for_test("split2", 2),
                        SearchJob::for_test("split1", 1),
                    ],
                ),
                (
                    expected_searcher_addr_2,
                    vec![
                        SearchJob::for_test("split6", 6),
                        SearchJob::for_test("split5", 5),
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

    // With both nodes at equal load, each split should go to its highest-affinity
    // node as determined by rendezvous hashing.
    //
    // Affinities for the (node-1001, node-1002) pool:
    //   1001 ← split3
    //   1002 ← split1
    #[tokio::test]
    async fn test_equal_load_affinity_respected() {
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", MockSearchService::new()),
            ("127.0.0.1:1002", MockSearchService::new()),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        // split1 → 1002, split3 → 1001 at equal load.
        let jobs = vec![
            SearchJob::for_test("split1", 1),
            SearchJob::for_test("split3", 3),
        ];
        let mut assigned: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
            .assign_jobs(jobs, &HashSet::default())
            .await
            .unwrap()
            .map(|(client, jobs)| (client.grpc_addr(), jobs))
            .collect();
        assigned.sort_unstable_by_key(|(addr, _)| *addr);
        let addr_1001: SocketAddr = ([127, 0, 0, 1], 1001).into();
        let addr_1002: SocketAddr = ([127, 0, 0, 1], 1002).into();
        assert_eq!(assigned.len(), 2);
        assert_eq!(
            assigned[0],
            (addr_1001, vec![SearchJob::for_test("split3", 3)])
        );
        assert_eq!(
            assigned[1],
            (addr_1002, vec![SearchJob::for_test("split1", 1)])
        );
    }

    #[tokio::test]
    async fn test_placement_does_not_query_load() {
        use futures::FutureExt;

        // Local test clients bypass GetLoad, so use an uncached gRPC client.
        // Without a server responding, querying its load would have to yield.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr = listener.local_addr().unwrap();
        let client = crate::client::create_search_client_from_grpc_addr(
            grpc_addr,
            bytesize::ByteSize::mb(1),
        );
        let pool = SearcherPool::from_iter([(grpc_addr, SearcherNode::for_test(client))]);
        let placer = SearchJobPlacer::new(pool);
        let assigned: Vec<_> = placer
            .assign_jobs(vec![SearchJob::for_test("split1", 1)], &HashSet::new())
            .now_or_never()
            .expect("placement must not wait for GetLoad")
            .unwrap()
            .collect();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0.grpc_addr(), grpc_addr);
        assert_eq!(assigned[0].1, vec![SearchJob::for_test("split1", 1)]);
    }

    #[tokio::test]
    async fn test_placement_is_repeatable_and_balances_request_cost() {
        let primary_addr: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let secondary_addr: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        let placer = SearchJobPlacer::new(searcher_pool_for_named_nodes([
            ("searcher-0", "127.0.0.1:1001", 0),
            ("searcher-1", "127.0.0.1:1002", 0),
        ]));
        let mut split_ids = Vec::new();
        for split_ord in 0..200 {
            let split_id = format!("split-{split_ord:03}");
            let ordered = placer.best_node_ids_per_affinity(split_id.as_str()).await;
            if ordered[0].as_str() == "searcher-0" {
                split_ids.push(split_id);
                if split_ids.len() == 5 {
                    break;
                }
            }
        }
        assert_eq!(split_ids.len(), 5);
        let jobs: Vec<SearchJob> = split_ids
            .iter()
            .zip([4, 4, 3, 3, 3])
            .map(|(split_id, cost)| SearchJob::for_test(split_id, cost))
            .collect();
        // ceil(17 * 105 / 200) = 9. The primary takes costs 4 + 4 + 3:
        // eligibility checks the assigned cost BEFORE adding the next job.
        let expected = HashMap::from([
            (primary_addr, jobs[..3].to_vec()),
            (secondary_addr, jobs[3..].to_vec()),
        ]);
        for (primary_load, secondary_load) in [(0, 0), (1_000_000, 0), (0, 1_000_000)] {
            let placer = SearchJobPlacer::new(searcher_pool_for_named_nodes([
                ("searcher-0", "127.0.0.1:1001", primary_load),
                ("searcher-1", "127.0.0.1:1002", secondary_load),
            ]));
            let mut input = jobs.clone();
            for _ in 0..2 {
                input.reverse();
                let assigned: HashMap<SocketAddr, Vec<SearchJob>> = placer
                    .assign_jobs(input.clone(), &HashSet::new())
                    .await
                    .unwrap()
                    .map(|(client, jobs)| (client.grpc_addr(), jobs))
                    .collect();
                // Also verifies descending cost and ascending split-ID tie ordering.
                assert_eq!(assigned, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_placement_respects_exclusions() {
        let placer = SearchJobPlacer::new(searcher_pool_with_loads_for_test([
            ("127.0.0.1:1001", 0),
            ("127.0.0.1:1002", 1_000_000),
        ]));
        let primary_addr: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let secondary_addr: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        // split3 prefers node-1001; exclusion wins over affinity, not existing load.
        let selected = placer
            .assign_job(
                SearchJob::for_test("split3", 1),
                &HashSet::from([primary_addr]),
            )
            .await
            .unwrap();
        assert_eq!(selected.grpc_addr(), secondary_addr);
        // Preserve the existing contract: excluding every node ignores exclusions.
        let selected = placer
            .assign_job(
                SearchJob::for_test("split3", 1),
                &HashSet::from([primary_addr, secondary_addr]),
            )
            .await
            .unwrap();
        assert_eq!(selected.grpc_addr(), primary_addr);
    }

    #[tokio::test]
    async fn test_rendezvous_order_survives_address_change_for_same_node_ids() {
        let first_pool = searcher_pool_for_named_nodes([
            ("searcher-0", "127.0.0.1:1001", 0),
            ("searcher-1", "127.0.0.1:1002", 0),
        ]);
        let restarted_pool = searcher_pool_for_named_nodes([
            ("searcher-0", "127.0.0.1:2001", 0),
            ("searcher-1", "127.0.0.1:2002", 0),
        ]);
        let before = SearchJobPlacer::new(first_pool)
            .best_node_ids_per_affinity(b"split-a")
            .await;
        let after = SearchJobPlacer::new(restarted_pool)
            .best_node_ids_per_affinity(b"split-a")
            .await;
        assert_eq!(before, after);
        assert_eq!(before.len(), 2);
    }
}
