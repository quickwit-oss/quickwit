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

use quickwit_common::net::Host;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntakeConfig {
    pub logs_endpoint: String,
    pub metrics_endpoint: String,
    pub traces_endpoint: String,
}

impl Default for IntakeConfig {
    fn default() -> Self {
        Self {
            logs_endpoint: format!(
                "http://{}:{}/api/datadog/v1/byoc/logs",
                Host::default(),
                7280
            ),
            metrics_endpoint: format!(
                "http://{}:{}/api/datadog/v1/byoc/metrics",
                Host::default(),
                7280
            ),
            traces_endpoint: format!(
                "http://{}:{}/api/datadog/v1/byoc/traces",
                Host::default(),
                7280
            ),
        }
    }
}
