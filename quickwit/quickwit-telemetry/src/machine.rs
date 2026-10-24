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

use sysinfo::{MemoryRefreshKind, RefreshKind, System};

const GIB: u64 = 1 << 30;

pub(crate) fn cpu_count_bucket(num_cpus: usize) -> &'static str {
    match num_cpus {
        0 | 1 => "1",
        2 => "2",
        3..=4 => "3-4",
        5..=8 => "5-8",
        9..=16 => "9-16",
        17..=32 => "17-32",
        33..=64 => "33-64",
        _ => "65+",
    }
}

pub(crate) fn memory_bucket(num_bytes: u64) -> &'static str {
    match num_bytes {
        num_bytes if num_bytes < 2 * GIB => "<2GiB",
        num_bytes if num_bytes < 4 * GIB => "2-4GiB",
        num_bytes if num_bytes < 8 * GIB => "4-8GiB",
        num_bytes if num_bytes < 16 * GIB => "8-16GiB",
        num_bytes if num_bytes < 32 * GIB => "16-32GiB",
        num_bytes if num_bytes < 64 * GIB => "32-64GiB",
        num_bytes if num_bytes < 128 * GIB => "64-128GiB",
        _ => "128GiB+",
    }
}

pub(crate) fn available_memory_num_bytes() -> u64 {
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    match system.cgroup_limits() {
        Some(cgroup_limits) => cgroup_limits.total_memory,
        None => system.total_memory(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buckets() {
        assert_eq!(cpu_count_bucket(4), "3-4");
        assert_eq!(cpu_count_bucket(96), "65+");
        assert_eq!(memory_bucket(GIB), "<2GiB");
        assert_eq!(memory_bucket(31 * GIB), "16-32GiB");
        assert_eq!(memory_bucket(512 * GIB), "128GiB+");
    }
}
