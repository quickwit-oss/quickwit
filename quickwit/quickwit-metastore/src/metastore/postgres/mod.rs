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

mod error;
mod factory;
mod metastore;
mod metrics;
mod migrator;
mod model;
mod pool;
mod split_stream;
mod tags;
mod utils;

pub use factory::PostgresqlMetastoreFactory;
pub use metastore::PostgresqlMetastore;

const QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY: &str = "QW_POSTGRES_SKIP_MIGRATIONS";
const QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY: &str = "QW_POSTGRES_SKIP_MIGRATION_LOCKING";
const QW_POSTGRES_READ_ONLY_ENV_KEY: &str = "QW_POSTGRES_READ_ONLY";
