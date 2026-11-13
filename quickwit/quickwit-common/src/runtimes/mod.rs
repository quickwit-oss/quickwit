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

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, OnceLock};

use tokio::runtime::Runtime;
use tracing::warn;

pub mod metrics;

static RUNTIMES: OnceLock<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceLock::new();

/// Describes which runtime an actor should run on.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum RuntimeType {
    /// The blocking runtime runs blocking actors.
    /// This runtime is only used as a nice thread pool with
    /// the interface as tokio stasks.
    ///
    /// This runtime should not be used to run tokio
    /// io operations.
    ///
    /// Tasks are allowed to block for an arbitrary amount of time.
    ///
    /// This runtime must stay multi-threaded: some actors call
    /// `tokio::task::block_in_place`, which panics on a current-thread runtime.
    Blocking,

    /// The non-blocking runtime is closer to what one would expect from
    /// a regular tokio runtime.
    ///
    /// Task are expect to yield within 500 micros.
    NonBlocking,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimesConfig {
    /// Number of worker threads allocated to the non-blocking runtime.
    pub num_threads_non_blocking: usize,
    /// Number of worker threads allocated to the blocking runtime.
    pub num_threads_blocking: usize,
}

impl RuntimesConfig {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn light_for_tests() -> RuntimesConfig {
        RuntimesConfig {
            num_threads_blocking: 1,
            num_threads_non_blocking: 1,
        }
    }

    pub fn with_num_cpus(num_cpus: usize) -> Self {
        // Non blocking task are supposed to be io intensive, and not require many threads.
        // On the other hand the blocking actors are cpu intensive. We allocate
        // almost all of the threads to them.
        match num_cpus {
            0..=3 => {
                // We do not have enough vCPUs to allocate a full thread to
                // non-blocking.
                RuntimesConfig {
                    num_threads_non_blocking: 1,
                    num_threads_blocking: num_cpus,
                }
            }
            4..=6 => RuntimesConfig {
                num_threads_non_blocking: 1,
                num_threads_blocking: num_cpus - 1,
            },
            7.. => RuntimesConfig {
                num_threads_non_blocking: 2,
                num_threads_blocking: num_cpus - 2,
            },
        }
    }
}

impl RuntimesConfig {
    /// Returns the runtimes configuration of this process.
    ///
    /// Each runtime size can be overridden independently with the
    /// `QW_TOKIO_BLOCKING_RUNTIME_NUM_THREADS` and `QW_TOKIO_NON_BLOCKING_RUNTIME_NUM_THREADS`
    /// environment variables. Values must be strictly positive.
    pub fn get() -> RuntimesConfig {
        static RUNTIMES_CONFIG: LazyLock<RuntimesConfig> = LazyLock::new(RuntimesConfig::from_env);
        *RUNTIMES_CONFIG
    }

    fn from_env() -> RuntimesConfig {
        let num_cpus = crate::num_cpus();
        let default_config = Self::with_num_cpus(num_cpus);
        let num_threads_blocking = get_num_threads_from_env(
            "QW_TOKIO_BLOCKING_RUNTIME_NUM_THREADS",
            default_config.num_threads_blocking,
        );
        let num_threads_non_blocking = get_num_threads_from_env(
            "QW_TOKIO_NON_BLOCKING_RUNTIME_NUM_THREADS",
            default_config.num_threads_non_blocking,
        );
        RuntimesConfig {
            num_threads_blocking,
            num_threads_non_blocking,
        }
    }
}

/// Tokio panics when a multi-threaded runtime is built with zero worker threads, so we parse the
/// override as a `NonZeroUsize`.
fn get_num_threads_from_env(key: &str, default_num_threads: usize) -> usize {
    crate::get_from_env_opt::<NonZeroUsize>(key, false)
        .map(NonZeroUsize::get)
        .unwrap_or(default_num_threads)
}

const QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY: &str = "QW_TOKIO_DISABLE_LIFO_SLOT";
/// Deprecated alias of [`QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY`], kept for backward compatibility.
const QW_DISABLE_TOKIO_LIFO_SLOT_ENV_KEY: &str = "QW_DISABLE_TOKIO_LIFO_SLOT";

/// Reads `QW_TOKIO_DISABLE_LIFO_SLOT`, falling back to the deprecated
/// `QW_DISABLE_TOKIO_LIFO_SLOT`. The new key takes precedence when both are set. Defaults to
/// `true`.
fn get_disable_lifo_slot_from_env() -> bool {
    let new_value_opt = crate::get_bool_from_env_opt(QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY);
    let deprecated_value_opt = crate::get_bool_from_env_opt(QW_DISABLE_TOKIO_LIFO_SLOT_ENV_KEY);

    if deprecated_value_opt.is_some() {
        warn!(
            "environment variable `{QW_DISABLE_TOKIO_LIFO_SLOT_ENV_KEY}` is deprecated, use \
             `{QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY}` instead"
        );
    }
    if let (Some(new_value), Some(deprecated_value)) = (new_value_opt, deprecated_value_opt)
        && new_value != deprecated_value
    {
        warn!(
            "environment variables `{QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY}` and \
             `{QW_DISABLE_TOKIO_LIFO_SLOT_ENV_KEY}` conflict, using \
             `{QW_TOKIO_DISABLE_LIFO_SLOT_ENV_KEY}={new_value}`"
        );
    }
    new_value_opt.or(deprecated_value_opt).unwrap_or(true)
}

fn start_runtimes(config: RuntimesConfig) -> HashMap<RuntimeType, Runtime> {
    let mut runtimes = HashMap::with_capacity(2);

    let disable_lifo_slot = get_disable_lifo_slot_from_env();

    let mut blocking_runtime_builder = tokio::runtime::Builder::new_multi_thread();
    if disable_lifo_slot {
        blocking_runtime_builder.disable_lifo_slot();
    }
    let blocking_runtime = blocking_runtime_builder
        .worker_threads(config.num_threads_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    metrics::register_runtime(blocking_runtime.handle(), "blocking");
    runtimes.insert(RuntimeType::Blocking, blocking_runtime);

    let non_blocking_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.num_threads_non_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("non-blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    metrics::register_runtime(non_blocking_runtime.handle(), "non_blocking");
    runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);

    runtimes
}

pub fn initialize_runtimes(runtimes_config: RuntimesConfig) -> anyhow::Result<()> {
    RUNTIMES.get_or_init(|| start_runtimes(runtimes_config));
    Ok(())
}

impl RuntimeType {
    pub fn get_runtime_handle(self) -> tokio::runtime::Handle {
        RUNTIMES
            .get_or_init(|| {
                #[cfg(any(test, feature = "testsuite"))]
                {
                    warn!("starting Tokio actor runtimes for tests");
                    start_runtimes(RuntimesConfig::light_for_tests())
                }
                #[cfg(not(any(test, feature = "testsuite")))]
                {
                    panic!("Tokio runtimes not initialized. Please, report this issue on GitHub: https://github.com/quickwit-oss/quickwit/issues.");
                }
            })
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
    fn test_runtimes_config_get() {
        let runtime_default = RuntimesConfig::get();
        assert!(runtime_default.num_threads_non_blocking <= runtime_default.num_threads_blocking);
        assert!(runtime_default.num_threads_non_blocking <= 2);
    }

    #[test]
    fn test_blocking_runtime_is_multi_threaded() {
        let runtime_flavor = RuntimeType::Blocking.get_runtime_handle().runtime_flavor();
        assert_eq!(runtime_flavor, tokio::runtime::RuntimeFlavor::MultiThread);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_10() {
        let runtime = RuntimesConfig::with_num_cpus(10);
        assert_eq!(runtime.num_threads_blocking, 8);
        assert_eq!(runtime.num_threads_non_blocking, 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_3() {
        let runtime = RuntimesConfig::with_num_cpus(3);
        assert_eq!(runtime.num_threads_blocking, 3);
        assert_eq!(runtime.num_threads_non_blocking, 1);
    }
}
