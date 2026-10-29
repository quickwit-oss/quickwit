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

use std::task::{Context, Poll};

use futures::future::{Either, Ready, ready};
use tower::{Layer, Service};

use crate::{Authorization, AuthorizationError, authorize_request};

/// Rejects requests that the token of the current task is not allowed to perform.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthorizationLayer;

impl<S> Layer<S> for AuthorizationLayer {
    type Service = AuthorizationService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthorizationService { service }
    }
}

#[derive(Clone, Debug)]
pub struct AuthorizationService<S> {
    service: S,
}

impl<S, Request> Service<Request> for AuthorizationService<S>
where
    S: Service<Request>,
    S::Error: From<AuthorizationError>,
    Request: Authorization,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<Ready<Result<S::Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if let Err(authorization_error) = authorize_request(&request) {
            return Either::Left(ready(Err(S::Error::from(authorization_error))));
        }
        Either::Right(self.service.call(request))
    }
}

#[cfg(test)]
mod tests {
    use tower::ServiceExt;

    use super::*;
    use crate::RequestFamily;

    struct AdminRequest;

    impl Authorization for AdminRequest {
        fn request_family(&self) -> RequestFamily {
            RequestFamily::IndexAdmin
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl From<AuthorizationError> for TestError {
        fn from(_: AuthorizationError) -> Self {
            TestError
        }
    }

    #[tokio::test]
    async fn test_authorization_layer_disabled() {
        let service = tower::service_fn(|_request: AdminRequest| async { Ok::<_, TestError>(()) });
        let mut service = AuthorizationLayer.layer(service);
        service
            .ready()
            .await
            .unwrap()
            .call(AdminRequest)
            .await
            .unwrap();
    }
}
