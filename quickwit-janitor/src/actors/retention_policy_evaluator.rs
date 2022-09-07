// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{IndexMetadata, Metastore, Split};
use tracing::{error, info};

const RUN_INTERVAL: Duration = Duration::from_secs(60); // 1 minutes

#[derive(Clone, Debug, Default)]
pub struct RetentionPolicyEvaluatorCounters {
    /// The number of refresh the config passes.
    pub num_refresh_passes: usize,

    /// The number of evaluation passes.
    pub num_evaluation_passes: usize,

    /// The number of deleted splits.
    pub num_discarded_splits: usize,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct Evaluate {
    index_id: String,
}

/// An actor for collecting garbage periodically from an index.
pub struct RetentionPolicyEvaluator {
    metastore: Arc<dyn Metastore>,
    index_metadatas: HashMap<String, IndexMetadata>,
    counters: RetentionPolicyEvaluatorCounters,
}

impl RetentionPolicyEvaluator {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            metastore,
            index_metadatas: HashMap::new(),
            counters: RetentionPolicyEvaluatorCounters::default(),
        }
    }
}

#[async_trait]
impl Actor for RetentionPolicyEvaluator {
    type ObservableState = RetentionPolicyEvaluatorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "RetentionPolicyEvaluator".to_string()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle(Loop, ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Loop> for RetentionPolicyEvaluator {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        info!("retention-policy-executor-refresh-indexes-operation");
        self.counters.num_refresh_passes += 1;

        let index_metadatas = self
            .metastore
            .list_indexes_metadatas()
            .await
            .context("Failed to list indexes.")?;
        info!(index_ids=%index_metadatas.iter().map(|im| &im.index_id).join(", "), "Garbage collecting indexes.");

        for index_metadata in index_metadatas.into_iter() {
            let retention_policy = match &index_metadata.retention_policy {
                Some(policy) => policy,
                None => continue,
            };

            match self.index_metadatas.entry(index_metadata.index_id.clone()) {
                Entry::Occupied(entry) => {
                    // This update serves as a way to refresh retention config
                    *entry.into_mut() = index_metadata;
                }
                Entry::Vacant(v) => {
                    let evaluate_msg = Evaluate {
                        index_id: index_metadata.index_id.clone(),
                    };
                    let next_interval = retention_policy
                        .duration_till_next_evaluation()
                        .expect("Should always extract next execution interval.");
                    v.insert(index_metadata);
                    ctx.schedule_self_msg(next_interval, evaluate_msg).await;
                }
            }
        }

        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Evaluate> for RetentionPolicyEvaluator {
    type Reply = ();

    async fn handle(
        &mut self,
        message: Evaluate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        let index_id = message.index_id;
        info!(index_id=%index_id, "retention-policy-evaluation-operation");
        self.counters.num_evaluation_passes += 1;

        let index_metadata = self
            .index_metadatas
            .get(&index_id)
            .expect("Failed to retrieve index metadata");
        let retention_policy = index_metadata.retention_policy.as_ref().unwrap();

        let evaluation_result = evaluate_retention(
            &index_id,
            self.metastore.clone(),
            retention_policy,
            Some(ctx),
        )
        .await;
        match evaluation_result {
            Ok(splits) => self.counters.num_discarded_splits += splits.len(),
            Err(error) => {
                error!(index_id=%index_id, error=?error, "Failed to evaluate retention on index.")
            }
        }

        let next_interval = retention_policy
            .duration_till_next_evaluation()
            .expect("Should always extract next execution interval.");
        ctx.schedule_self_msg(next_interval, Evaluate { index_id })
            .await;
        Ok(())
    }
}

async fn evaluate_retention(
    index_id: &str,
    metastore: Arc<dyn Metastore>,
    retention_policy: &RetentionPolicy,
    ctx_opt: Option<&ActorContext<RetentionPolicyEvaluator>>,
) -> anyhow::Result<Vec<Split>> {
    // TODO: make query to mark splits MarkForDeletion
    // retention will no delete, Gc will do

    Ok(vec![])
}
