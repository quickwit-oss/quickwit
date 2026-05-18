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

use std::sync::LazyLock;

use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DeploymentInfo {
    pub cpu_limits: Option<String>,
    pub cpu_requests: Option<String>,
    pub mem_limits: Option<String>,
    pub mem_requests: Option<String>,
    pub storage_class: Option<String>,
    pub storage_size: Option<String>,
}

impl DeploymentInfo {
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<DeploymentInfo> = LazyLock::new(|| {
            let cpu_limits = std::env::var("KUBERNETES_LIMITS_CPU").ok();
            let cpu_requests = std::env::var("KUBERNETES_REQUESTS_CPU").ok();
            let mem_limits = std::env::var("KUBERNETES_LIMITS_MEMORY").ok();
            let mem_requests = std::env::var("KUBERNETES_REQUESTS_MEMORY").ok();
            let storage_class = std::env::var("KUBERNETES_STORAGE_CLASS").ok();
            let storage_size = std::env::var("KUBERNETES_STORAGE_SIZE").ok();

            DeploymentInfo {
                cpu_limits,
                cpu_requests,
                mem_limits,
                mem_requests,
                storage_class,
                storage_size,
            }
        });

        &INSTANCE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_info() {
        DeploymentInfo::get();
    }

    #[test]
    fn test_deployment_info_json_serialization() {
        serde_json::to_string(DeploymentInfo::get()).unwrap();
    }
}
