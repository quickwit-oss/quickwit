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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::BoxStream;
use ouroboros::self_referencing;
use sqlx::Postgres;
use tokio_stream::Stream;

use super::pool::TrackedPool;

#[self_referencing(pub_extras)]
pub struct SplitStream<T> {
    connection_pool: TrackedPool<Postgres>,
    sql: String,
    #[borrows(connection_pool, sql)]
    #[covariant]
    inner: BoxStream<'this, Result<T, sqlx::Error>>,
}

impl<T> Stream for SplitStream<T> {
    type Item = Result<T, sqlx::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        SplitStream::with_inner_mut(&mut self, |this| Pin::new(&mut this.as_mut()).poll_next(cx))
    }
}
