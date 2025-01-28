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

use std::env::var;

use once_cell::sync::Lazy;
use quickwit_common::get_bool_from_env;

pub static INDEX_ID: Lazy<String> = Lazy::new(|| {
    var("QW_LAMBDA_INDEX_ID").expect("environment variable `QW_LAMBDA_INDEX_ID` should be set")
});

/// Configures the fmt tracing subscriber to log as json and include span
/// boundaries. This is very verbose and is only used to generate advanced KPIs
/// from Lambda runs (e.g. for blog post benchmarks)
pub static ENABLE_VERBOSE_JSON_LOGS: Lazy<bool> =
    Lazy::new(|| get_bool_from_env("QW_LAMBDA_ENABLE_VERBOSE_JSON_LOGS", false));

pub static OPENTELEMETRY_URL: Lazy<Option<String>> =
    Lazy::new(|| var("QW_LAMBDA_OPENTELEMETRY_URL").ok());

pub static OPENTELEMETRY_AUTHORIZATION: Lazy<Option<String>> =
    Lazy::new(|| var("QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION").ok());
