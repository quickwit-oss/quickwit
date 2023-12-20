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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::BoxStream;
use ouroboros::self_referencing;
use sqlx::{Pool, Postgres};
use tokio_stream::Stream;

#[self_referencing(pub_extras)]
pub struct SplitStream<T> {
    connection_pool: Pool<Postgres>,
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
