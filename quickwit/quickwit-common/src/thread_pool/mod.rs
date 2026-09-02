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

use std::fmt;

use quickwit_metrics::{LazyGauge, lazy_gauge};

mod simple;
pub mod with_priority;

pub use simple::{get_underlying_rayon_thread_pool, run_cpu_intensive};

pub(super) static THREAD_POOL_ONGOING_TASKS: LazyGauge = lazy_gauge!(
    name: "ongoing_tasks",
    description: "number of tasks being currently processed by threads in the thread pool",
    subsystem: "thread_pool",
);

pub(super) static THREAD_POOL_PENDING_TASKS: LazyGauge = lazy_gauge!(
    name: "pending_tasks",
    description: "number of tasks waiting in the queue before being processed by the thread pool",
    subsystem: "thread_pool",
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Panicked;

impl fmt::Display for Panicked {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduled task panicked")
    }
}

impl std::error::Error for Panicked {}
