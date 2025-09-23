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

use std::num::NonZero;
use std::sync::OnceLock;

use tracing::{error, info, warn};

const QW_NUM_CPUS_ENV_KEY: &str = "QW_NUM_CPUS";
const KUBERNETES_LIMITS_CPU: &str = "KUBERNETES_LIMITS_CPU";

/// Return the number of vCPU/hyperthreads available.
/// The following methods are used in order:
/// - from the `QW_NUM_CPUS` environment variable
/// - from the `KUBERNETES_LIMITS_CPU` environment variable
/// - from the operating system
/// - default to 2.
pub fn num_cpus() -> usize {
    static NUM_CPUS: OnceLock<usize> = OnceLock::new();
    *NUM_CPUS.get_or_init(num_cpus_aux)
}

fn num_cpus_aux() -> usize {
    let num_cpus_from_os_opt = std::thread::available_parallelism()
        .map(NonZero::get)
        .inspect_err(|err| {
            error!(error=?err, "failed to detect the number of threads available: arbitrarily returning 2");
        })
        .ok();
    let num_cpus_from_env_opt = get_num_cpus_from_env(QW_NUM_CPUS_ENV_KEY);
    let num_cpus_from_k8s_limit = get_num_cpus_from_env(KUBERNETES_LIMITS_CPU);

    if let Some(num_cpus) = num_cpus_from_env_opt {
        return num_cpus;
    }

    if let Some(num_cpus_from_k8s_limit) = num_cpus_from_k8s_limit {
        info!(
            "num cpus from k8s limit: {},  possibly overriding os value {:?}",
            num_cpus_from_k8s_limit, num_cpus_from_env_opt
        );
        return num_cpus_from_k8s_limit;
    }

    if let Some(num_cpus_from_os_opt) = num_cpus_from_os_opt {
        info!("num cpus from os: {}", num_cpus_from_os_opt);
        return num_cpus_from_os_opt;
    }

    warn!("failed to detect number of cpus. defaulting to 2");
    2
}

fn parse_cpu_to_mcpu(cpu_string: &str) -> Result<usize, &'static str> {
    let trimmed_str = cpu_string.trim();

    if trimmed_str.is_empty() {
        return Err("input cpu_string cannot be empty");
    }

    if let Some(val_str) = trimmed_str.strip_suffix('m') {
        // The value is already in millicores.
        val_str
            .parse::<usize>()
            .map_err(|_| "invalid millicore value")
    } else {
        // The value is in CPU cores.
        let value = trimmed_str
            .parse::<f64>()
            .map_err(|_| "invalid float value")?;
        Ok((value * 1000.0f64) as usize)
    }
}

// Get the number of CPUs from an environment variable.
// The value is expected to be in k8s format (200m means 200 millicores, 2 means 2 cores)
//
// We then get the number of vCPUs by ceiling any non integer value.
fn get_num_cpus_from_env(env_key: &str) -> Option<usize> {
    let k8s_cpu_limit_str: String = crate::get_from_env_opt(env_key, false)?;
    let mcpus = parse_cpu_to_mcpu(&k8s_cpu_limit_str)
        .inspect_err(|err_msg| {
            warn!(
                "failed to parse k8s cpu limit (`{}`): {}",
                k8s_cpu_limit_str, err_msg
            );
        })
        .ok()?;
    let num_vcpus = mcpus.div_ceil(1000);
    Some(num_vcpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_millicores() {
        assert_eq!(parse_cpu_to_mcpu("500m").unwrap(), 500);
        assert_eq!(parse_cpu_to_mcpu("100m").unwrap(), 100);
        assert_eq!(parse_cpu_to_mcpu("2500m").unwrap(), 2500);
    }

    #[test]
    fn test_cores() {
        assert_eq!(parse_cpu_to_mcpu("1").unwrap(), 1000);
        assert_eq!(parse_cpu_to_mcpu("2").unwrap(), 2000);
    }

    #[test]
    fn test_fractional_cores() {
        assert_eq!(parse_cpu_to_mcpu("0.5").unwrap(), 500);
        assert_eq!(parse_cpu_to_mcpu("1.5").unwrap(), 1500);
        assert_eq!(parse_cpu_to_mcpu("0.25").unwrap(), 250);
    }

    #[test]
    fn test_with_whitespace() {
        assert_eq!(parse_cpu_to_mcpu(" 750m ").unwrap(), 750);
        assert_eq!(parse_cpu_to_mcpu(" 0.75 ").unwrap(), 750);
    }

    #[test]
    fn test_invalid_input() {
        assert!(parse_cpu_to_mcpu("").is_err());
        assert!(parse_cpu_to_mcpu("   ").is_err());
        assert!(parse_cpu_to_mcpu("abc").is_err());
        assert!(parse_cpu_to_mcpu("1a").is_err());
        assert!(parse_cpu_to_mcpu("m500").is_err());
        assert!(parse_cpu_to_mcpu("500m1").is_err());
    }
}
