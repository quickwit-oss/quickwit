// Copyright (C) 2021 Quickwit, Inc.
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

pub mod search_client_pool;

use std::collections::HashSet;
use std::net::SocketAddr;

use async_trait::async_trait;

use crate::SearchServiceClient;

/// Job.
/// The unit in which distributed search is performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// Split ID.
    pub split_id: String,

    /// The cost of the job. This is used to sort jobs.
    pub cost: u32,
}

/// ClientPool meant to manage Quickwit's clients.
/// The client pool provides the available clients.
#[async_trait]
pub trait ClientPool: Send + Sync + 'static {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SearchServiceClient, Vec<Job>)
    ///
    /// Excluded addresses are ignored when they include all the clients.
    async fn assign_jobs(
        &self,
        jobs: Vec<Job>,
        excluded_addresses: &HashSet<SocketAddr>,
    ) -> anyhow::Result<Vec<(SearchServiceClient, Vec<Job>)>>;

    /// Assigns one job to a client.
    async fn assign_job(
        &self,
        job: Job,
        excluded_addresses: &HashSet<SocketAddr>,
    ) -> anyhow::Result<SearchServiceClient> {
        self.assign_jobs(vec![job], excluded_addresses)
            .await?
            .into_iter()
            .next()
            .map(|(client, _jobs)| client)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "`assign_jobs` with {} excluded addresses failed to return at least one \
                     client.",
                    excluded_addresses.len()
                )
            })
    }
}
