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
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use futures::future::join_all;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::rendezvous_hasher::{node_affinity, sort_by_rendez_vous_hash};
use quickwit_common::{SocketAddrLegacyHash, get_bool_from_env};
use quickwit_metrics::counter;
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use tracing::{info, warn};

use crate::metrics::JOB_ASSIGNED_TOTAL;
use crate::{SearchJob, SearchServiceClient, SearcherPool};

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

static LOAD_ESTIMATION_DISABLED: LazyLock<bool> =
    LazyLock::new(|| get_bool_from_env("QW_DISABLE_LOAD_ESTIMATION", false));

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
        jobs: Vec<J>,
        excluded_addrs: &HashSet<SocketAddr>,
    ) -> anyhow::Result<impl Iterator<Item = (SearchServiceClient, Vec<J>)> + use<J>> {
        self.assign_jobs_inner(jobs, excluded_addrs, true).await
    }

    /// Same as [`Self::assign_jobs`] but does not query nodes for their current
    /// load. This saves a round-trip for jobs that wouldn't go into a queue.
    ///
    /// Placement still spreads cost evenly across nodes via rendezvous hashing,
    /// but starts from a uniform zero existing load.
    pub async fn assign_jobs_ignoring_load<J: Job>(
        &self,
        jobs: Vec<J>,
        excluded_addrs: &HashSet<SocketAddr>,
    ) -> anyhow::Result<impl Iterator<Item = (SearchServiceClient, Vec<J>)> + use<J>> {
        self.assign_jobs_inner(jobs, excluded_addrs, false).await
    }

    async fn assign_jobs_inner<J: Job>(
        &self,
        mut jobs: Vec<J>,
        excluded_addrs: &HashSet<SocketAddr>,
        load_aware: bool,
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
                load: None,
            })
            .collect();

        if load_aware && !*LOAD_ESTIMATION_DISABLED {
            // Seed each candidate node with its current load so the placer avoids
            // routing work to already-loaded nodes. If a node fails to report its
            // load (error or timeout), `load` stays `None`: we still route work
            // there if all other nodes are overloaded, but we prefer reachable
            // nodes first.
            //
            // The timeout is intentionally short: a slow response is treated the
            // same as no response so that one unresponsive node cannot delay the
            // entire query.
            const GET_LOAD_TIMEOUT: Duration = Duration::from_millis(200);
            let load_futures = candidate_nodes.iter_mut().map(|node| {
                let mut client = node.client.clone();
                async move { tokio::time::timeout(GET_LOAD_TIMEOUT, client.get_load()).await }
            });
            let loads = join_all(load_futures).await;
            for (node, load_result) in candidate_nodes.iter_mut().zip(loads) {
                match load_result {
                    Ok(Ok(load)) => node.load = Some(load),
                    Ok(Err(err)) => {
                        warn!(
                            grpc_addr=%node.grpc_addr,
                            err=%err,
                            "failed to get load from searcher node; node will only be used as last resort"
                        );
                    }
                    Err(_timeout) => {
                        warn!(
                            grpc_addr=%node.grpc_addr,
                            "timed out getting load from searcher node; node will only be used as last resort"
                        );
                    }
                }
            }
        } else {
            for node in candidate_nodes.iter_mut() {
                node.load = Some(0);
            }
        }

        jobs.sort_unstable_by(Job::compare_cost);

        let num_nodes = candidate_nodes.len();

        let mut job_assignments: HashMap<SocketAddr, (SearchServiceClient, Vec<J>)> =
            HashMap::with_capacity(num_nodes);

        let total_load: usize = jobs.iter().map(|job| job.cost()).sum();

        // Compute `target_load` using only reachable nodes (those with a known
        // load), iteratively excluding nodes whose existing load already exceeds
        // the computed target. This converges because:
        //   (a) each round can only shrink the schedulable set, and
        //   (b) with total_load > 0 a single-node set is always stable: its
        //       target = (load + total_load) * 1.05 > load, so it never
        //       excludes itself.
        //
        // After convergence every schedulable node has load < target, which
        // guarantees total remaining capacity across those nodes is at least
        // total_load * 1.05 > total_load. The "found no lightly loaded
        // searcher" warn path is therefore unreachable in practice.
        //
        // allow around 5% disparity. Round up so we never end up in a case where
        // target_load * schedulable_nodes < total_load.
        // some of our tests needs 2 splits to be put on 2 different searchers. It makes sense for
        // these tests to keep doing so (testing root merge). Either we can make the allowed
        // difference stricter, find the right split names ("split6" instead of "split2" works).
        // or modify mock_split_meta() so that not all splits have the same job cost
        // for now i went with the mock_split_meta() changes.
        const ALLOWED_DIFFERENCE: usize = 105;
        let target_load = {
            let mut schedulable: Vec<usize> = candidate_nodes
                .iter()
                .filter_map(|node| node.load)
                .collect();
            loop {
                if schedulable.is_empty() {
                    // All nodes are unreachable; the job loop falls through to
                    // the warn path and uses candidate_nodes[0] as a fallback.
                    break 0;
                }
                let existing_load: usize = schedulable.iter().sum();
                let target = ((existing_load + total_load) * ALLOWED_DIFFERENCE)
                    .div_ceil(schedulable.len() * 100);
                let prev_len = schedulable.len();
                schedulable.retain(|&load| load < target);
                if schedulable.len() == prev_len {
                    break target;
                }
            }
        };
        for job in jobs {
            sort_by_rendez_vous_hash(&mut candidate_nodes, job.split_id());

            let (chosen_node_idx, chosen_node) = if let Some((idx, node)) = candidate_nodes
                .iter_mut()
                .enumerate()
                .find(|(_pos, node)| node.load.map(|load| load < target_load).unwrap_or(false))
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
            if let Some(load) = &mut chosen_node.load {
                *load += job.cost();
            }

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
    /// Current load of this node in job-cost units. `None` means the node
    /// could not be reached and should only be used as a last resort.
    pub load: Option<usize>,
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
    use std::sync::Arc;

    use super::*;
    use crate::{MockSearchService, SearchJob, SearchServiceClient, searcher_pool_for_test};

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
            (grpc_addr, client)
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

    // With both nodes at equal load, each split should go to its highest-affinity
    // node as determined by rendezvous hashing.
    //
    // Affinities for the (1001, 1002) pool (from test_search_job_placer):
    //   1001 ← split3, split4, split5
    //   1002 ← split1, split2, split6
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

    // A node with extreme existing load should receive no new jobs, and the
    // remaining idle nodes should receive a balanced share.
    //
    // This specifically exercises the two-pass target computation. With a
    // single-pass mean, the overloaded node inflates target_load to ~350_035,
    // which means both idle nodes are far below target and all 100 jobs pile
    // onto whichever one rendezvous hash prefers most (100:0 split). The
    // second pass recomputes the target over only the two idle nodes (~53),
    // forcing balanced distribution (~50:50).
    #[tokio::test]
    async fn test_extreme_load_excluded_remaining_balanced() {
        let searcher_pool = searcher_pool_with_loads_for_test([
            ("127.0.0.1:1001", 1_000_000),
            ("127.0.0.1:1002", 0),
            ("127.0.0.1:1003", 0),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let jobs = (0..100)
            .map(|id| SearchJob::for_test(&format!("split{id}"), 1))
            .collect();
        let mut assigned: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
            .assign_jobs(jobs, &HashSet::default())
            .await
            .unwrap()
            .map(|(client, jobs)| (client.grpc_addr(), jobs))
            .collect();
        assigned.sort_unstable_by_key(|(addr, _)| *addr);

        let overloaded_addr: SocketAddr = ([127, 0, 0, 1], 1001).into();
        for (addr, _) in &assigned {
            assert_ne!(
                *addr, overloaded_addr,
                "overloaded node must not receive new jobs"
            );
        }
        assert_eq!(
            assigned.len(),
            2,
            "only the two idle nodes should receive jobs"
        );
        for (addr, jobs) in &assigned {
            assert!(
                jobs.len() >= 35 && jobs.len() <= 65,
                "node {} received {} jobs, expected roughly 50",
                addr,
                jobs.len()
            );
        }
    }

    // Verifies that pre-existing load on a node shifts new jobs away from it,
    // even for splits whose affinity points to the loaded node.
    //
    // Node 1001 has load 1000; node 1002 is idle.
    // split3 prefers 1001 by affinity; split1 prefers 1002.
    // Both should land on 1002 because 1001 is excluded by the target computation.
    #[tokio::test]
    async fn test_search_job_placer_existing_load() {
        let searcher_pool =
            searcher_pool_with_loads_for_test([("127.0.0.1:1001", 1000), ("127.0.0.1:1002", 0)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let jobs = vec![
            SearchJob::for_test("split1", 1),
            SearchJob::for_test("split3", 3),
        ];
        let mut assigned_jobs: Vec<(SocketAddr, Vec<SearchJob>)> = search_job_placer
            .assign_jobs(jobs, &HashSet::default())
            .await
            .unwrap()
            .map(|(client, jobs)| (client.grpc_addr(), jobs))
            .collect();
        assigned_jobs.sort_unstable_by_key(|(addr, _)| *addr);

        assert_eq!(assigned_jobs.len(), 1);
        let (addr, jobs) = &assigned_jobs[0];
        let expected_addr: SocketAddr = ([127, 0, 0, 1], 1002).into();
        assert_eq!(*addr, expected_addr);
        let mut split_ids: Vec<&str> = jobs.iter().map(|job| job.split_id()).collect();
        split_ids.sort_unstable();
        assert_eq!(split_ids, vec!["split1", "split3"]);
    }
}
