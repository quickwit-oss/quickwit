// Copyright (C) 2022 Quickwit, Inc.
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
        qw_env_vars!(@step 1usize, $($ident,)*);

        pub(crate) const QW_ENV_VARS: Lazy<HashMap<usize, &'static str>> = Lazy::new(|| {
            let mut env_vars = HashMap::new();
            $(env_vars.insert($ident, stringify!($ident));)*
            env_vars
        });
    }
}

pub(crate) const QW_NONE: usize = 0;

qw_env_vars!(
    QW_CLUSTER_ID,
    QW_NODE_ID,
    QW_LISTEN_ADDRESS,
    QW_ADVERTISE_ADDRESS,
    QW_REST_LISTEN_PORT,
    QW_GOSSIP_LISTEN_PORT,
    QW_GRPC_LISTEN_PORT,
    QW_DATA_DIR,
    QW_METASTORE_URI,
    QW_DEFAULT_ROOT_INDEX_URI
);

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_qw_env_vars_expansion() {
        assert_eq!(QW_CLUSTER_ID, 1);
        assert_eq!(QW_ENV_VARS.get(&QW_CLUSTER_ID).unwrap(), &"QW_CLUSTER_ID");

        assert_eq!(QW_ENV_VARS.get(&QW_NODE_ID).unwrap(), &"QW_NODE_ID");
        assert_eq!(QW_NODE_ID, 2);
    }
}
