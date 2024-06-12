// Copyright (C) 2024 Quickwit, Inc.
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
