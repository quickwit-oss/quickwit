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

use once_cell::sync::Lazy;

/// Expands the list of QW environment variables into constants of the form `const <ENV_VAR_KEY>:
/// usize = <env var index>;` and builds the map `QW_EN_VARS` of environment variable index to
/// environment variable key.
macro_rules! qw_env_vars {
    (@step $idx:expr,) => {};

    (@step $idx:expr, $head:ident, $($tail:ident,)*) => {
        pub(crate) const $head: usize = $idx;

        qw_env_vars!(@step $idx + 1usize, $($tail,)*);
    };

    ($($ident:ident),*) => {
        qw_env_vars!(@step 0usize, $($ident,)*);

        pub(crate) static QW_ENV_VARS: Lazy<HashMap<usize, &'static str>> = Lazy::new(|| {
            let mut env_vars = HashMap::new();
            $(env_vars.insert($ident, stringify!($ident));)*
            env_vars
        });
    }
}

// These environment variable keys can be declared in any order with the exception of `QW_NONE`,
// which must be declared first.
qw_env_vars!(
    QW_NONE,
    QW_CLUSTER_ID,
    QW_NODE_ID,
    QW_ENABLED_SERVICES,
    QW_LISTEN_ADDRESS,
    QW_ADVERTISE_ADDRESS,
    QW_REST_LISTEN_PORT,
    QW_GOSSIP_LISTEN_PORT,
    QW_GRPC_LISTEN_PORT,
    QW_GOSSIP_INTERVAL_MS,
    QW_PEER_SEEDS,
    QW_DATA_DIR,
    QW_METASTORE_URI,
    QW_DEFAULT_INDEX_ROOT_URI
);

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_qw_env_vars_expansion() {
        assert_eq!(QW_NONE, 0);

        assert_eq!(QW_CLUSTER_ID, 1);
        assert_eq!(QW_ENV_VARS.get(&QW_CLUSTER_ID).unwrap(), &"QW_CLUSTER_ID");

        assert_eq!(QW_ENV_VARS.get(&QW_NODE_ID).unwrap(), &"QW_NODE_ID");
        assert_eq!(QW_NODE_ID, 2);
    }
}
