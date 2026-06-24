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

use std::sync::Arc;

use tonic::metadata::KeyAndValueRef;
use tonic::service::Interceptor;

pub type GrpcInterceptor =
    Arc<dyn Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Send + Sync>;

#[derive(Clone, Default)]
pub struct GrpcInterceptors {
    interceptors: Vec<GrpcInterceptor>,
}

impl GrpcInterceptors {
    pub fn new(interceptors: impl IntoIterator<Item = GrpcInterceptor>) -> Self {
        Self {
            interceptors: interceptors.into_iter().collect(),
        }
    }
}

impl Interceptor for GrpcInterceptors {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        self.interceptors
            .iter()
            .try_fold(request, |request, interceptor| interceptor(request))
    }
}

pub fn fixed_headers_interceptor(headers: tonic::metadata::MetadataMap) -> GrpcInterceptor {
    Arc::new(move |mut request: tonic::Request<()>| {
        for key_and_value in headers.iter() {
            match key_and_value {
                KeyAndValueRef::Ascii(key, value) => {
                    request.metadata_mut().insert(key, value.to_owned());
                }
                KeyAndValueRef::Binary(key, value) => {
                    request.metadata_mut().insert_bin(key, value.to_owned());
                }
            }
        }
        Ok(request)
    })
}

#[cfg(test)]
mod tests {
    use tonic::metadata::{BinaryMetadataValue, MetadataMap, MetadataValue};

    use super::*;

    #[test]
    fn fixed_headers_interceptor_copies_ascii_and_binary_metadata() {
        let mut headers = MetadataMap::new();
        headers.insert("x-ascii", MetadataValue::from_static("ascii-value"));
        headers.insert_bin("x-bin", BinaryMetadataValue::from_bytes(b"binary-value"));

        let mut interceptors = GrpcInterceptors::new([fixed_headers_interceptor(headers)]);
        let request = interceptors.call(tonic::Request::new(())).unwrap();

        assert_eq!(
            request.metadata().get("x-ascii").unwrap(),
            MetadataValue::from_static("ascii-value")
        );
        assert_eq!(
            request
                .metadata()
                .get_bin("x-bin")
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            b"binary-value"
        );
    }

    #[test]
    fn grpc_interceptors_apply_multiple_interceptors_in_order() {
        let first: GrpcInterceptor = Arc::new(|mut request| {
            request
                .metadata_mut()
                .insert("x-order", MetadataValue::from_static("first"));
            Ok(request)
        });
        let second: GrpcInterceptor = Arc::new(|mut request| {
            let value = request.metadata().get("x-order").unwrap().to_str().unwrap();
            assert_eq!(value, "first");
            request
                .metadata_mut()
                .insert("x-order", MetadataValue::from_static("second"));
            Ok(request)
        });

        let mut interceptors = GrpcInterceptors::new([first, second]);
        let request = interceptors.call(tonic::Request::new(())).unwrap();

        assert_eq!(
            request.metadata().get("x-order").unwrap(),
            MetadataValue::from_static("second")
        );
    }
}
