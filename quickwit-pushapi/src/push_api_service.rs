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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, ActorRunner, Handler, QueueCapacity};
use quickwit_metastore::Metastore;
use quickwit_proto::push_api::{
    CreateQueueRequest, DropQueueRequest, FetchRequest, FetchResponse, IngestRequest,
    SuggestTruncateRequest, TailRequest,
};

use crate::{iter_doc_payloads, Position, PushApiError, Queues};

pub struct PushApiService {
    queues: Queues,
    metastore: Arc<dyn Metastore>,
}

impl PushApiService {
    pub fn with_queue_path(
        queue_path: &Path,
        metastore: Arc<dyn Metastore>,
    ) -> crate::Result<Self> {
        let queues = Queues::open(queue_path)?;
        Ok(PushApiService { queues, metastore })
    }

    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<()> {
        // Check all indexes exist.
        let tasks = request.doc_batches.iter().map(|batch| {
            let moved_metastore = self.metastore.clone();
            let index_id = batch.index_id.clone();
            tokio::spawn(async move {
                let result = moved_metastore.check_index_available(&index_id).await;
                (index_id, result)
            })
        });

        let queue_ids = futures::future::try_join_all(tasks)
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, result)| result.is_err())
            .map(|(index_id, _)| index_id)
            .collect::<Vec<_>>();
        if !queue_ids.is_empty() {
            return Err(PushApiError::IndexDoesNotExist { queue_ids });
        }

        for doc_batch in &request.doc_batches {
            // Attempt to create the queue if it does not exist.
            if let Err(error) = self.queues.create_queue(&doc_batch.index_id) {
                match error {
                    PushApiError::QueueAlreadyExists { .. } => (),
                    _ => return Err(error),
                }
            }

            // TODO better error handling.
            // If there is an error, we probably want a transactional behavior.
            let records_it = iter_doc_payloads(doc_batch);
            self.queues.append_batch(&doc_batch.index_id, records_it)?;
        }
        Ok(())
    }

    fn fetch(&mut self, fetch_req: FetchRequest) -> crate::Result<FetchResponse> {
        let start_from_opt: Option<Position> = fetch_req.start_after.map(Position::from);
        self.queues.fetch(&fetch_req.index_id, start_from_opt)
    }

    fn suggest_truncate(&mut self, request: SuggestTruncateRequest) -> crate::Result<()> {
        self.queues.suggest_truncate(
            &request.index_id,
            Position::from(request.up_to_position_included),
        )?;
        Ok(())
    }
}

#[async_trait]
impl Actor for PushApiService {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn runner(&self) -> ActorRunner {
        ActorRunner::DedicatedThread
    }

    /// The Actor's incoming mailbox queue capacity. It is set when the actor is spawned.
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(3)
    }
}

#[async_trait]
impl Handler<CreateQueueRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        create_queue_req: CreateQueueRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<()>, ActorExitStatus> {
        if let Some(queue_id) = create_queue_req.queue_id.as_ref() {
            Ok(self.queues.create_queue(queue_id))
        } else {
            Ok(Ok(()))
        }
    }
}

#[async_trait]
impl Handler<DropQueueRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        drop_queue_req: DropQueueRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<()>, ActorExitStatus> {
        if let Some(queue_id) = drop_queue_req.queue_id.as_ref() {
            Ok(self.queues.drop_queue(queue_id))
        } else {
            Ok(Ok(()))
        }
    }
}

#[async_trait]
impl Handler<IngestRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        ingest_req: IngestRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<()>, ActorExitStatus> {
        Ok(self.ingest(ingest_req).await)
    }
}

#[async_trait]
impl Handler<FetchRequest> for PushApiService {
    type Reply = crate::Result<FetchResponse>;
    async fn handle(
        &mut self,
        request: FetchRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<FetchResponse>, ActorExitStatus> {
        Ok(self.fetch(request))
    }
}

#[async_trait]
impl Handler<TailRequest> for PushApiService {
    type Reply = crate::Result<FetchResponse>;
    async fn handle(
        &mut self,
        request: TailRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<FetchResponse>, ActorExitStatus> {
        Ok(self.queues.tail(&request.index_id))
    }
}

#[async_trait]
impl Handler<SuggestTruncateRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        request: SuggestTruncateRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<crate::Result<()>, ActorExitStatus> {
        Ok(self.suggest_truncate(request))
    }
}
