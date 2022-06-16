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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::bail;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

static RUNTIMES: OnceCell<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceCell::new();

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum RuntimeType {
    Blocking,
    NonBlocking,
    IngestApi,
}

#[derive(Debug)]
pub struct RuntimesConfiguration {
    num_threads_non_blocking: usize,
    num_threads_blocking: usize,
}

impl RuntimesConfiguration {
    pub fn with_num_cpus(num_cpus: usize) -> Self {
        let num_threads_non_blocking = if num_cpus > 6 { 2 } else { 1 };
        let num_threads_blocking = (num_cpus - num_threads_non_blocking).max(1);
        RuntimesConfiguration {
            num_threads_non_blocking,
            num_threads_blocking,
        }
    }
}

impl Default for RuntimesConfiguration {
    fn default() -> RuntimesConfiguration {
        let num_cpus = num_cpus::get();
        RuntimesConfiguration::with_num_cpus(num_cpus)
    }
}

fn start_runtimes(config: RuntimesConfiguration) -> HashMap<RuntimeType, Runtime> {
    let mut runtimes = HashMap::default();
    let blocking_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.num_threads_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("blocking-{}", id)
        })
        .enable_all()
        .build()
        .unwrap();
    runtimes.insert(RuntimeType::Blocking, blocking_runtime);
    let non_blocking_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.num_threads_non_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("non-blocking-{}", id)
        })
        .enable_all()
        .build()
        .unwrap();
    runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);
    let ingest_api_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    runtimes.insert(RuntimeType::IngestApi, ingest_api_runtime);
    runtimes
}

impl RuntimeType {
    // TODO call this in quickwit's main
    pub fn initialize(runtime_config: RuntimesConfiguration) -> anyhow::Result<()> {
        let runtimes = start_runtimes(runtime_config);
        if RUNTIMES.set(runtimes).is_err() {
            bail!("Runtimes have already been initialized.");
        }
        Ok(())
    }

    pub fn get_runtime_handle(self) -> tokio::runtime::Handle {
        RUNTIMES
            .get_or_init(|| start_runtimes(RuntimesConfiguration::default()))
            .get(&self)
            .unwrap()
            .handle()
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtimes_config_default() {
        let runtime_default = RuntimesConfiguration::default();
        assert!(runtime_default.num_threads_non_blocking <= runtime_default.num_threads_blocking);
        assert!(runtime_default.num_threads_non_blocking <= 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_10() {
        let runtime = RuntimesConfiguration::with_num_cpus(10);
        assert_eq!(runtime.num_threads_blocking, 8);
        assert_eq!(runtime.num_threads_non_blocking, 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_3() {
        let runtime = RuntimesConfiguration::with_num_cpus(3);
        assert_eq!(runtime.num_threads_blocking, 2);
        assert_eq!(runtime.num_threads_non_blocking, 1);
    }
}
