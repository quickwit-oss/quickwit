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

#[cfg(target_os = "linux")]
pub(crate) fn available_memory_num_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let physical_memory_num_bytes = parse_mem_total_num_bytes(&meminfo)?;
    let Some(cgroup_limit_num_bytes) = cgroup_memory_limit_num_bytes() else {
        return Some(physical_memory_num_bytes);
    };
    Some(physical_memory_num_bytes.min(cgroup_limit_num_bytes))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn available_memory_num_bytes() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", test))]
fn parse_mem_total_num_bytes(meminfo: &str) -> Option<u64> {
    let mem_total_kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()?;
    Some(mem_total_kib * 1024)
}

/// Without a limit, cgroup v2 holds `max`, which doesn't parse, and cgroup v1 a huge number, which
/// the caller caps with the physical memory.
#[cfg(target_os = "linux")]
fn cgroup_memory_limit_num_bytes() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(limit) = std::fs::read_to_string(path) {
            return limit.trim().parse().ok();
        }
    }
    None
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

    #[test]
    fn test_parse_mem_total_num_bytes() {
        let meminfo = "MemTotal:       16303516 kB\nMemFree:         1102768 kB\n";
        assert_eq!(parse_mem_total_num_bytes(meminfo), Some(16_303_516 * 1024));
        assert_eq!(parse_mem_total_num_bytes("MemFree: 1 kB\n"), None);
    }
}
