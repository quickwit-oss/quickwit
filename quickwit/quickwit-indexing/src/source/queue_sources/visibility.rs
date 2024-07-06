// Copyright (C) 2024 Quickwit, Inc.
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

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::error;

use super::Queue;
use crate::models::PublishLock;

const REQUESTED_VISIBILITY_EXTENSION: Duration = Duration::from_secs(60);
const VISIBILITY_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const LOCK_KILL_TIMEOUT: Duration = Duration::from_secs(1);

pub struct VisibilityTaskHandle {
    _task_handle: JoinHandle<()>,
    pub ack_id: String,
}

pub fn spawn_visibility_task(
    queue: Arc<dyn Queue>,
    ack_id: String,
    initial_deadline: Instant,
    publish_lock: PublishLock,
) -> VisibilityTaskHandle {
    let task_handle = tokio::spawn(handle_visibility(
        queue,
        ack_id.clone(),
        initial_deadline,
        publish_lock,
    ));
    VisibilityTaskHandle {
        _task_handle: task_handle,
        ack_id,
    }
}

/// This is still work in progress. Using the `PublishLock` isn't enough to
/// communicate visibility extension failures:
/// - we don't want to fail the pipeline if we fail to extend the visibility of a message that is
///   still waiting for processing
/// - the Processor must also be notified that it shouldn't process this message anymore
async fn handle_visibility(
    queue: Arc<dyn Queue>,
    ack_id: String,
    initial_deadline: Instant,
    publish_lock: PublishLock,
) {
    let mut next_deadline: tokio::time::Instant = initial_deadline.into();
    loop {
        tokio::time::sleep_until(next_deadline - VISIBILITY_REQUEST_TIMEOUT - LOCK_KILL_TIMEOUT)
            .await;

        if publish_lock.is_dead() {
            break;
        }

        let res = tokio::time::timeout(
            VISIBILITY_REQUEST_TIMEOUT,
            queue.modify_deadlines(&ack_id, REQUESTED_VISIBILITY_EXTENSION),
        )
        .await;
        match res {
            Ok(Ok(new_deadline)) => {
                next_deadline = new_deadline.into();
            }
            Ok(Err(err)) => {
                error!(err=%err, "failed to modify message deadline");
                publish_lock.kill().await;
                break;
            }
            Err(_) => {
                error!("failed to modify message deadline on time");
                publish_lock.kill().await;
                break;
            }
        }
    }
}

/// Acknowledges the messages of a list of visibility handles and stops the
/// associated tasks.
pub async fn acknowledge_and_abort(
    queue: &dyn Queue,
    handles: Vec<VisibilityTaskHandle>,
) -> anyhow::Result<()> {
    let ack_ids = handles
        .iter()
        .map(|handle| handle.ack_id.as_str())
        .collect::<Vec<_>>();
    queue.acknowledge(&ack_ids).await?;
    for handle in handles {
        handle._task_handle.abort();
    }
    Ok(())
}
