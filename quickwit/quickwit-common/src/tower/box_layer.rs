// Copyright (C) 2023 Quickwit, Inc.
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

use std::fmt;
use std::sync::Arc;

use tower::layer::layer_fn;
use tower::{Layer, Service};

use crate::tower::BoxService;

pub struct BoxLayer<S, R, T, E> {
    inner: Arc<dyn Layer<S, Service = BoxService<R, T, E>> + Send + Sync + 'static>,
}

impl<S, R, T, E> BoxLayer<S, R, T, E> {
    pub fn new<L>(inner_layer: L) -> Self
    where
        L: Layer<S> + Send + Sync + 'static,
        L::Service: Service<R, Response = T, Error = E> + Clone + Send + Sync + 'static,
        <L::Service as Service<R>>::Future: Send + 'static,
    {
        let layer = layer_fn(move |inner_svc: S| {
            let outer_layer = inner_layer.layer(inner_svc);
            BoxService::new(outer_layer)
        });

        Self {
            inner: Arc::new(layer),
        }
    }
}

impl<S, R, T, E> Layer<S> for BoxLayer<S, R, T, E> {
    type Service = BoxService<R, T, E>;

    fn layer(&self, inner: S) -> Self::Service {
        self.inner.layer(inner)
    }
}

impl<S, R, T, E> Clone for BoxLayer<S, R, T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, R, T, E> fmt::Debug for BoxLayer<S, R, T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BoxLayer").finish()
    }
}
