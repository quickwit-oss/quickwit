/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

pub mod search_client_pool;

use async_trait::async_trait;
use mockall::mock;

use crate::client::WrappedSearchServiceClient;

/// Job.
/// The unit in which distributed search is performed.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Job {
    /// Split ID.
    pub split: String,

    /// The cost of the job. This is used to sort jobs.
    pub cost: u32,
}

/// ClientPool meant to manage Quickwit's clients.
/// The client pool provides the available clients.
#[async_trait]
pub trait ClientPool: Send + Sync + 'static {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SocketAddr, Vec<Job>)
    async fn assign_jobs(
        &self,
        jobs: Vec<Job>,
    ) -> anyhow::Result<Vec<(WrappedSearchServiceClient, Vec<Job>)>>;
}

mock! {
    pub ClientPool {
        pub async fn assign_jobs(
            &self,
            jobs: Vec<Job>,
        ) -> anyhow::Result<Vec<(WrappedSearchServiceClient, Vec<Job>)>>;
    }
}

#[async_trait]
impl ClientPool for MockClientPool {
    async fn assign_jobs(
        &self,
        jobs: Vec<Job>,
    ) -> anyhow::Result<Vec<(WrappedSearchServiceClient, Vec<Job>)>> {
        Ok(self.assign_jobs(jobs).await?)
    }
}
