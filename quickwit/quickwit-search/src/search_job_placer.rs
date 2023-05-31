// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use anyhow::bail;
use quickwit_common::rendezvous_hasher::sort_by_rendez_vous_hash;

use crate::{SearchServiceClient, SearcherPool};

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

impl SearchJobPlacer {
    /// Returns an [`SearchJobPlacer`] from a search service client pool.
    pub fn new(searcher_pool: SearcherPool) -> Self {
        Self { searcher_pool }
    }
}

impl SearchJobPlacer {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SocketAddr, `Vec<Job>`)
    ///
    /// When exclude_addresses filters all clients it is ignored.
    pub async fn assign_jobs<J: Job>(
        &self,
        mut jobs: Vec<J>,
        excluded_addrs: &HashSet<SocketAddr>,
    ) -> anyhow::Result<impl Iterator<Item = (SearchServiceClient, Vec<J>)>> {
        let num_nodes = self.searcher_pool.len().await;

        let mut candidate_nodes: Vec<CandidateNodes> = self
            .searcher_pool
            .all()
            .await
            .into_iter()
            .filter(|(grpc_addr, _)| {
                excluded_addrs.is_empty()
                    || excluded_addrs.len() == num_nodes
                    || !excluded_addrs.contains(grpc_addr)
            })
            .map(|(grpc_addr, client)| CandidateNodes {
                grpc_addr,
                client,
                load: 0,
            })
            .collect();

        if candidate_nodes.is_empty() {
            bail!(
                "Failed to assign search jobs. There are no available searcher nodes in the pool."
            );
        }
        jobs.sort_unstable_by(Job::compare_cost);

        let mut job_assignments: HashMap<SocketAddr, (SearchServiceClient, Vec<J>)> =
            HashMap::with_capacity(num_nodes);

        for job in jobs {
            sort_by_rendez_vous_hash(&mut candidate_nodes, job.split_id());
            // Select the least loaded node.
            let chosen_node_idx = if candidate_nodes.len() >= 2 {
                usize::from(candidate_nodes[0].load > candidate_nodes[1].load)
            } else {
                0
            };
            let chosen_node = &mut candidate_nodes[chosen_node_idx];
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
struct CandidateNodes {
    pub grpc_addr: SocketAddr,
    pub client: SearchServiceClient,
    pub load: usize,
}

impl Hash for CandidateNodes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.grpc_addr.hash(state);
    }
}

impl PartialEq for CandidateNodes {
    fn eq(&self, other: &Self) -> bool {
        self.grpc_addr == other.grpc_addr
    }
}

impl Eq for CandidateNodes {}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use crate::root::SearchJob;
    use crate::{searcher_pool_for_test, MockSearchService, SearchJobPlacer, SearcherPool};

    #[tokio::test]
    async fn test_search_job_placer() {
        {
            let searcher_pool = SearcherPool::default();
            let search_job_placer = SearchJobPlacer::new(searcher_pool);
            assert!(search_job_placer
                .assign_jobs::<SearchJob>(Vec::new(), &HashSet::new())
                .await
                .is_err());
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
            let expected_assigned_jobs = vec![
                (
                    expected_searcher_addr_1,
                    vec![
                        SearchJob::for_test("split6", 6),
                        SearchJob::for_test("split3", 3),
                        SearchJob::for_test("split1", 1),
                    ],
                ),
                (
                    expected_searcher_addr_2,
                    vec![
                        SearchJob::for_test("split5", 5),
                        SearchJob::for_test("split4", 4),
                        SearchJob::for_test("split2", 2),
                    ],
                ),
            ];
            assert_eq!(assigned_jobs, expected_assigned_jobs);
        }
    }
}
