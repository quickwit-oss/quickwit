/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

 #[cfg(test)]
mod mock {
    use std::sync::Arc;

    use mockall::automock;

    #[automock]
    pub trait SearchService {
        fn root_search(
            &self,
            request: tonic::Request<quickwit_proto::SearchRequest>,
        ) -> Result<tonic::Response<quickwit_proto::SearchResult>, tonic::Status>;
        fn leaf_search(
            &self,
            request: tonic::Request<quickwit_proto::LeafSearchRequest>,
        ) -> Result<tonic::Response<quickwit_proto::LeafSearchResult>, tonic::Status>;
        fn get_docs(
            &self,
            request: tonic::Request<quickwit_proto::FetchDocsRequest>,
        ) -> Result<tonic::Response<quickwit_proto::FetchDocsResult>, tonic::Status>;
    }

    #[derive(Clone)]
    pub struct MockSearchServiceAdapter(Arc<dyn SearchService + Send + Sync>);

    impl<T: SearchService + Send + Sync + 'static> From<T> for MockSearchServiceAdapter {
        fn from(search_service: T) -> Self {
            MockSearchServiceAdapter(Arc::new(search_service))
        }
    }

    #[tonic::async_trait]
    impl quickwit_proto::search_service_server::SearchService for MockSearchServiceAdapter {
        async fn root_search(
            &self,
            request: tonic::Request<quickwit_proto::SearchRequest>,
        ) -> Result<tonic::Response<quickwit_proto::SearchResult>, tonic::Status> {
            self.0.root_search(request)
        }

        async fn leaf_search(
            &self,
            request: tonic::Request<quickwit_proto::LeafSearchRequest>,
        ) -> Result<tonic::Response<quickwit_proto::LeafSearchResult>, tonic::Status> {
            self.0.leaf_search(request)
        }

        async fn fetch_docs(
            &self,
            request: tonic::Request<quickwit_proto::FetchDocsRequest>,
        ) -> Result<tonic::Response<quickwit_proto::FetchDocsResult>, tonic::Status> {
            self.0.get_docs(request)
        }
    }

    impl Into<super::SearchServiceForTest> for MockSearchService {
        fn into(self) -> super::SearchServiceForTest {
            let adapter = MockSearchServiceAdapter::from(self);
            let server = quickwit_proto::search_service_server::SearchServiceServer::new(adapter);
            let client = quickwit_proto::search_service_client::SearchServiceClient::new(server);
            super::SearchServiceForTest::Mock(client)
        }
    }
}

use tonic::transport::Channel;

use quickwit_proto::search_service_client as grpc;

#[cfg(test)]
pub use self::mock::*;

#[derive(Clone)]
pub enum SearchServiceForTest {
    Grpc(grpc::SearchServiceClient<Channel>),
    #[cfg(test)]
    Mock(
        grpc::SearchServiceClient<
            quickwit_proto::search_service_server::SearchServiceServer<MockSearchServiceAdapter>,
        >,
    ),
}

impl SearchServiceForTest {
    pub fn new(channel: Channel) -> Self {
        SearchServiceForTest::Grpc(
            quickwit_proto::search_service_client::SearchServiceClient::new(channel),
        )
    }

    pub async fn root_search(
        &mut self,
        request: impl tonic::IntoRequest<quickwit_proto::SearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::SearchResult>, tonic::Status> {
        match self {
            SearchServiceForTest::Grpc(grpc_client) => grpc_client.root_search(request).await,
            #[cfg(test)]
            SearchServiceForTest::Mock(mock) => mock.root_search(request).await,
        }
    }

    pub async fn leaf_search(
        &mut self,
        request: impl tonic::IntoRequest<quickwit_proto::LeafSearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::LeafSearchResult>, tonic::Status> {
        match self {
            SearchServiceForTest::Grpc(grpc_client) => grpc_client.leaf_search(request).await,
            #[cfg(test)]
            SearchServiceForTest::Mock(mock) => mock.leaf_search(request).await,
        }
    }

    pub async fn fetch_docs(
        &mut self,
        request: impl tonic::IntoRequest<quickwit_proto::FetchDocsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::FetchDocsResult>, tonic::Status> {
        match self {
            SearchServiceForTest::Grpc(grpc_client) => grpc_client.fetch_docs(request).await,
            #[cfg(test)]
            SearchServiceForTest::Mock(mock) => mock.fetch_docs(request).await,
        }
    }
}
