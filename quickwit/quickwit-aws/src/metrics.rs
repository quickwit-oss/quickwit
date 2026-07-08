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

use quickwit_metrics::{LazyCounter, lazy_counter};

pub static NEW_HTTP_CONNECTIONS_TOTAL: LazyCounter = lazy_counter!(
        name: "new_http_connections_total",
        description: "Number of new outbound connections established by the AWS SDK HTTP client, \
                      labeled by host. Counted via the DNS resolver, which is invoked once per new \
                      connection but skipped on pooled reuse; a proxy for connection churn.",
        subsystem: "storage",
);
