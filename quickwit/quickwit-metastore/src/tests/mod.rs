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

use std::collections::BTreeSet;

use async_trait::async_trait;
use bytesize::ByteSize;
use itertools::Itertools;
use quickwit_proto::metastore::metastore_service_grpc_client::MetastoreServiceGrpcClient;
use quickwit_proto::metastore::{
    DeleteIndexRequest, DeleteSplitsRequest, ListSplitsRequest, MarkSplitsForDeletionRequest,
    MetastoreServiceClient, MetastoreServiceGrpcClientAdapter,
};
use quickwit_proto::tonic::transport::Channel;
use quickwit_proto::types::IndexUid;

pub(crate) mod delete_task;
pub(crate) mod get_identity;
pub(crate) mod index;
pub(crate) mod list_splits;
pub(crate) mod shard;
pub(crate) mod source;
pub(crate) mod split;
pub(crate) mod template;

use crate::metastore::MetastoreServiceStreamSplitsExt;
use crate::{ListSplitsRequestExt, MetastoreServiceExt, Split};

const MAX_GRPC_MESSAGE_SIZE: ByteSize = ByteSize::mib(1);

#[async_trait]
pub trait DefaultForTest {
    async fn default_for_test() -> Self;
}

// We implement the trait to test the gRPC adapter backed by a file backed metastore.
#[async_trait]
impl DefaultForTest for MetastoreServiceGrpcClientAdapter<MetastoreServiceGrpcClient<Channel>> {
    async fn default_for_test() -> Self {
        use quickwit_proto::tonic::transport::Server;
        use quickwit_storage::RamStorage;

        use crate::FileBackedMetastore;
        let metastore =
            FileBackedMetastore::try_new(std::sync::Arc::new(RamStorage::default()), None)
                .await
                .unwrap();
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    MetastoreServiceClient::new(metastore).as_grpc_service(MAX_GRPC_MESSAGE_SIZE),
                )
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        let channel = create_channel(client).await.unwrap();
        let (_, connection_keys_watcher) =
            tokio::sync::watch::channel(std::collections::HashSet::new());

        MetastoreServiceGrpcClientAdapter::new(
            MetastoreServiceGrpcClient::new(channel),
            connection_keys_watcher,
        )
    }
}

impl MetastoreServiceExt
    for MetastoreServiceGrpcClientAdapter<MetastoreServiceGrpcClient<Channel>>
{
}

async fn create_channel(client: tokio::io::DuplexStream) -> anyhow::Result<Channel> {
    use http::Uri;
    use quickwit_proto::tonic::transport::Endpoint;

    let mut outer_client_opt = Some(client);
    let channel = Endpoint::try_from("http://test.server")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let inner_client_opt = outer_client_opt.take();
            async move {
                let client = inner_client_opt
                    .ok_or_else(|| std::io::Error::other("client already taken"))?;
                std::io::Result::Ok(hyper_util::rt::TokioIo::new(client))
            }
        }))
        .await?;
    Ok(channel)
}

// crate::metastore_test_suite!(
//     quickwit_proto::metastore::MetastoreServiceGrpcClientAdapter<
//         quickwit_proto::metastore::metastore_service_grpc_client::MetastoreServiceGrpcClient<
//             quickwit_proto::tonic::transport::Channel,
//         >,
//     >
// );

fn collect_split_ids(splits: &[Split]) -> Vec<&str> {
    splits
        .iter()
        .map(|split| split.split_id())
        .sorted()
        .collect()
}

fn to_btree_set(tags: &[&str]) -> BTreeSet<String> {
    tags.iter().map(|tag| tag.to_string()).collect()
}

async fn cleanup_index(metastore: &mut dyn MetastoreServiceExt, index_uid: IndexUid) {
    // List all splits.
    let all_splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();

    if !all_splits.is_empty() {
        let all_split_ids: Vec<String> = all_splits
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();

        // Mark splits for deletion.
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), all_split_ids.clone());
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        // Delete splits.
        let delete_splits_request = DeleteSplitsRequest {
            index_uid: index_uid.clone().into(),
            split_ids: all_split_ids,
        };
        metastore
            .delete_splits(delete_splits_request)
            .await
            .unwrap();
    }
    // Delete index.
    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();
}

/// macro used to generate a testsuite for an implementation of Metastore
#[macro_export]
macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {

            // Index API tests
            //
            //  - create_index
            //  - update_index
            //  - index_exists
            //  - index_metadata
            //  - indexes_metadata
            //  - list_indexes
            //  - delete_index

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_create_index() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_create_index_with_sources() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_create_index_with_sources::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_retention_policy() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_update_retention_policy::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_search_settings() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_update_search_settings::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_doc_mapping() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_update_doc_mapping::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_indexing_settings() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_update_indexing_settings::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_ingest_settings() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_update_ingest_settings::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_create_index_enforces_index_id_maximum_length() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_create_index_enforces_index_id_maximum_length::<
                    $metastore_type,
                >()
                .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_index_exists() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_index_exists::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_index_metadata() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_indexes_metadata() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_indexes_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_list_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_all_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_list_all_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_index() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::index::test_metastore_delete_index::<$metastore_type>().await;
            }

            // Split API tests
            //
            //  - stage_splits
            //  - publish_splits
            //  - stream_splits
            //  - mark_splits_for_deletion
            //  - delete_splits

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_publish_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_publish_splits_concurrency() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_publish_splits_concurrency::<$metastore_type>(
                )
                .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_publish_splits_empty_splits_array_is_allowed() {
                $crate::tests::split::test_metastore_publish_splits_empty_splits_array_is_allowed::<
                            $metastore_type,
                        >()
                        .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_replace_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_mark_splits_for_deletion() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_mark_splits_for_deletion::<$metastore_type>()
                    .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_stream_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_stream_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_all_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_splits::<$metastore_type>().await;
            }


            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_splits_by_node() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_splits_by_node_id::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_split_update_timestamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_split_update_timestamp::<$metastore_type>()
                    .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_add_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::source::test_metastore_add_source::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::source::test_metastore_update_source::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_toggle_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::source::test_metastore_toggle_source::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::source::test_metastore_delete_source::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_reset_checkpoint() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::source::test_metastore_reset_checkpoint::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_create_delete_task() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::delete_task::test_metastore_create_delete_task::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_last_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::delete_task::test_metastore_last_delete_opstamp::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_index_with_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::delete_task::test_metastore_delete_index_with_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_delete_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::delete_task::test_metastore_list_delete_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_stale_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_stale_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_sorted_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_sorted_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_after_split() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_after_split::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_splits_from_all_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::list_splits::test_metastore_list_splits_from_all_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_update_splits_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_update_splits_delete_opstamp::<$metastore_type>()
                    .await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_stage_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::split::test_metastore_stage_splits::<$metastore_type>().await;
            }

            /// Shard API tests

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_open_shards() {
                $crate::tests::shard::test_metastore_open_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_acquire_shards() {
                $crate::tests::shard::test_metastore_acquire_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_shards() {
                $crate::tests::shard::test_metastore_list_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_shards() {
                $crate::tests::shard::test_metastore_delete_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_prune_shards() {
                $crate::tests::shard::test_metastore_prune_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::serial]
            async fn test_metastore_apply_checkpoint_delta_v2_single_shard() {
                $crate::tests::shard::test_metastore_apply_checkpoint_delta_v2_single_shard::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_apply_checkpoint_delta_v2_multi_shards() {
                $crate::tests::shard::test_metastore_apply_checkpoint_delta_v2_multi_shards::<$metastore_type>().await;
            }

            /// Index Template API tests

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_create_index_template() {
                $crate::tests::template::test_metastore_create_index_template::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_get_index_template() {
                $crate::tests::template::test_metastore_get_index_template::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_find_index_template_matches() {
                $crate::tests::template::test_metastore_find_index_template_matches::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_list_index_templates() {
                $crate::tests::template::test_metastore_list_index_templates::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_delete_index_templates() {
                $crate::tests::template::test_metastore_delete_index_templates::<$metastore_type>().await;
            }

            #[tokio::test]
            #[serial_test::file_serial]
            async fn test_metastore_get_identity() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::get_identity::test_metastore_get_identity::<$metastore_type>().await;
            }
        }
    };
}
