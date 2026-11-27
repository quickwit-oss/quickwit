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

mod index_resource;
mod rest_handler;
mod source_resource;
mod split_resource;

use warp::{Filter, Rejection};

pub use self::index_resource::get_index_metadata_handler;
pub use self::rest_handler::{IndexApi, index_management_handlers};
pub use self::split_resource::{ListSplitsQueryParams, ListSplitsResponse};

fn indexes_path_segment() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path("indexes").or(warp::path("indices")).unify()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_indexes_path_segment() {
        let index_api_root = indexes_path_segment().and(warp::path::end());

        assert!(
            warp::test::request()
                .path("/indexes")
                .filter(&index_api_root)
                .await
                .is_ok()
        );
        assert!(
            warp::test::request()
                .path("/indices")
                .filter(&index_api_root)
                .await
                .is_ok()
        );
        assert!(
            warp::test::request()
                .path("/index")
                .filter(&index_api_root)
                .await
                .is_err()
        );
    }
}
