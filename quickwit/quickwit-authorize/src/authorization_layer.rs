use std::fmt;
use std::task::{Context, Poll};

use futures::future::Either;
use quickwit_common::tower::RpcName;
use tower::{Layer, Service};

use crate::AuthorizationError;

pub struct AuthorizationLayer;

impl<S: Clone> Layer<S> for AuthorizationLayer {
    type Service = AuthorizationService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthorizationService { service }
    }
}

#[derive(Clone)]
pub struct AuthorizationService<S> {
    service: S,
}

impl<S, Request> Service<Request> for AuthorizationService<S>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: From<AuthorizationError> + Send + 'static,
    Request: fmt::Debug + Send + RpcName + crate::Authorization + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        futures::future::Either<futures::future::Ready<Result<S::Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if let Err(authorization_err) = crate::authorize_request(&request) {
            let err = S::Error::from(authorization_err);
            let result: Result<S::Response, S::Error> = Err(err);
            return Either::Left(futures::future::ready(result));
        }
        let service_fut = self.service.call(request);
        Either::Right(service_fut)
    }
}
