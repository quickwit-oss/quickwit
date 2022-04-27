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

use std::collections::HashSet;
use std::path::Path;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, ActorRunner, Handler, QueueCapacity};
use quickwit_proto::push_api::{
    CreateQueueIfNonExistentRequest, CreateQueueRequest, DropQueueRequest, FetchRequest,
    FetchResponse, IngestRequest, IngestResponse, QueueExistsRequest, SuggestTruncateRequest,
    TailRequest,
};

use crate::{iter_doc_payloads, Position, PushApiError, Queues};

pub struct PushApiService {
    queues: Queues,
}

impl PushApiService {
    pub fn with_queue_path(queue_path: &Path) -> crate::Result<Self> {
        let queues = Queues::open(queue_path)?;
        Ok(PushApiService { queues })
    }

    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        // Check all indexes exist assuming existing queues always have a corresponding index.
        let first_non_existing_queue_opt = request
            .doc_batches
            .iter()
            .map(|batch| batch.index_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .find(|index_id| !self.queues.queue_exists(index_id));

        if let Some(index_id) = first_non_existing_queue_opt {
            return Err(PushApiError::IndexDoesNotExist { index_id });
        }

        let mut num_docs = 0usize;
        for doc_batch in &request.doc_batches {
            // TODO better error handling.
            // If there is an error, we probably want a transactional behavior.
            let records_it = iter_doc_payloads(doc_batch);
            self.queues.append_batch(&doc_batch.index_id, records_it)?;
            num_docs += doc_batch.doc_lens.len();
        }
        Ok(IngestResponse {
            num_ingested_docs: num_docs as u64,
        })
    }

    fn fetch(&mut self, fetch_req: FetchRequest) -> crate::Result<FetchResponse> {
        let start_from_opt: Option<Position> = fetch_req.start_after.map(Position::from);
        let num_bytes_limit_opt: Option<usize> = fetch_req
            .num_bytes_limit
            .map(|num_bytes_limit| num_bytes_limit as usize);
        self.queues
            .fetch(&fetch_req.index_id, start_from_opt, num_bytes_limit_opt)
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
impl Handler<QueueExistsRequest> for PushApiService {
    type Reply = crate::Result<bool>;
    async fn handle(
        &mut self,
        queue_exists_req: QueueExistsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(Ok(self.queues.queue_exists(&queue_exists_req.queue_id)))
    }
}

#[async_trait]
impl Handler<CreateQueueRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        create_queue_req: CreateQueueRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.create_queue(&create_queue_req.queue_id))
    }
}

#[async_trait]
impl Handler<CreateQueueIfNonExistentRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        create_queue_inf_req: CreateQueueIfNonExistentRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if self.queues.queue_exists(&create_queue_inf_req.queue_id) {
            return Ok(Ok(()));
        }
        Ok(self.queues.create_queue(&create_queue_inf_req.queue_id))
    }
}

#[async_trait]
impl Handler<DropQueueRequest> for PushApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        drop_queue_req: DropQueueRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.drop_queue(&drop_queue_req.queue_id))
    }
}

#[async_trait]
impl Handler<IngestRequest> for PushApiService {
    type Reply = crate::Result<IngestResponse>;
    async fn handle(
        &mut self,
        ingest_req: IngestRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
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
    ) -> Result<Self::Reply, ActorExitStatus> {
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
    ) -> Result<Self::Reply, ActorExitStatus> {
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
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.suggest_truncate(request))
    }
}
