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

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tracing::error;

use super::Queue;

const REQUESTED_VISIBILITY_EXTENSION: Duration = Duration::from_secs(60);
const VISIBILITY_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const LOCK_KILL_TIMEOUT: Duration = Duration::from_secs(1);

struct Inner {
    queue: Arc<dyn Queue>,
    ack_id: String,
    extension_failed: AtomicBool,
}

/// A handle to a visibility extension task
///
/// To safely manage their lifecycle, tasks are stopped when their handle is
/// dropped. This ensures for instance that all running visibility tasks are
/// aborted when the indexing pipeline is restarted.
///
/// This struct is not `Clone` on purpose. A single owned reference to the task
/// should be enough.
pub struct VisibilityTaskHandle {
    inner: Arc<Inner>,
}

impl VisibilityTaskHandle {
    pub fn ack_id(&self) -> &str {
        &self.inner.ack_id
    }

    pub fn extension_failed(&self) -> bool {
        self.inner
            .extension_failed
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub fn spawn_visibility_task(
    queue: Arc<dyn Queue>,
    ack_id: String,
    initial_deadline: Instant,
) -> VisibilityTaskHandle {
    let inner = Arc::new(Inner {
        queue,
        ack_id: ack_id.clone(),
        extension_failed: AtomicBool::new(false),
    });
    tokio::spawn(extend_visibility_loop(
        initial_deadline,
        Arc::downgrade(&inner),
    ));
    VisibilityTaskHandle { inner }
}

async fn extend_visibility_loop(initial_deadline: Instant, inner_weak: Weak<Inner>) {
    let mut next_deadline: tokio::time::Instant = initial_deadline.into();
    loop {
        tokio::time::sleep_until(next_deadline - VISIBILITY_REQUEST_TIMEOUT - LOCK_KILL_TIMEOUT)
            .await;

        let inner = match inner_weak.upgrade() {
            Some(inner) => inner,
            None => {
                break;
            }
        };

        let res = tokio::time::timeout(
            VISIBILITY_REQUEST_TIMEOUT,
            inner
                .queue
                .modify_deadlines(&inner.ack_id, REQUESTED_VISIBILITY_EXTENSION),
        )
        .await;
        match res {
            Ok(Ok(new_deadline)) => {
                next_deadline = new_deadline.into();
            }
            Ok(Err(err)) => {
                error!(err=%err, "failed to modify message deadline");
                inner
                    .extension_failed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                break;
            }
            Err(_) => {
                error!("failed to modify message deadline on time");
                inner
                    .extension_failed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                break;
            }
        }
    }
}
