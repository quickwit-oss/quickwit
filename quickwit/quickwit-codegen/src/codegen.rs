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

use proc_macro2::TokenStream;
use prost_build::{Method, Service, ServiceGenerator};
use quote::{quote, ToTokens};
use syn::Ident;

pub struct Codegen;

impl Codegen {
    pub fn run(
        proto: &str,
        out_dir: &str,
        result_path: &str,
        error_path: &str,
    ) -> anyhow::Result<()> {
        println!("cargo:rerun-if-changed={proto}");

        let mut prost_config = prost_build::Config::default();
        prost_config
            .protoc_arg("--experimental_allow_proto3_optional")
            .type_attribute(
                ".",
                "#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]",
            )
            .out_dir(out_dir);

        let service_generator = Box::new(QuickwitServiceGenerator::new(result_path, error_path));

        prost_config
            .service_generator(service_generator)
            .compile_protos(&[proto], &["protos"])?;
        Ok(())
    }
}

struct QuickwitServiceGenerator {
    result_path: String,
    error_path: String,
    inner: Box<dyn ServiceGenerator>,
}

impl QuickwitServiceGenerator {
    fn new(result_path: &str, error_path: &str) -> Self {
        let inner = Box::new(WithSuffixServiceGenerator::new(
            "Grpc",
            tonic_build::configure().service_generator(),
        ));
        Self {
            result_path: result_path.to_string(),
            error_path: error_path.to_string(),
            inner,
        }
    }
}

impl ServiceGenerator for QuickwitServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        let tokens = generate_all(&service, &self.result_path, &self.error_path);
        let ast: syn::File = syn::parse2(tokens).expect("Tokenstream should be a valid Syn AST.");
        let pretty_code = prettyplease::unparse(&ast);
        buf.push_str(&pretty_code);

        self.inner.generate(service, buf)
    }

    fn finalize(&mut self, buf: &mut String) {
        self.inner.finalize(buf);
    }
}

fn generate_all(service: &Service, result_path: &str, error_path: &str) -> TokenStream {
    let package_name = quote::format_ident!("{}", service.package);
    let trait_name = quote::format_ident!("{}", service.name);
    let client_name = quote::format_ident!("{}Client", service.name);
    let result_path = syn::parse_str::<syn::Path>(result_path)
        .expect("Result path should be a valid result path such as `crate::HelloResult`.");
    let error_path = syn::parse_str::<syn::Path>(error_path)
        .expect("Result path should be a valid result path such as `crate::error::HelloError`.");

    let service_trait = generate_trait(&trait_name, &service.methods, &result_path);
    let client = generate_client(&trait_name, &client_name, &service.methods, &result_path);
    let tower_service = generate_tower_service(&client_name, &service.methods, &error_path);
    let grpc_client_adapter =
        generate_grpc_client_adapter(&package_name, &trait_name, &service.methods, &result_path);
    let grpc_server_adapter =
        generate_grpc_server_adapter(&package_name, &trait_name, &service.methods);

    quote! {
        // The line below is necessary to opt out of the license header check.
        /// BEGIN quickwit-codegen
        #service_trait

        #client

        pub type BoxFuture<T, E> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>>;

        #tower_service

        #grpc_client_adapter

        #grpc_server_adapter
    }
}

fn generate_trait(trait_name: &Ident, methods: &[Method], result_path: &syn::Path) -> TokenStream {
    let trait_methods = generate_trait_methods(methods, result_path);
    let mock_name = quote::format_ident!("Mock{}", trait_name);

    quote! {
        #[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
        #[async_trait::async_trait]
        pub trait #trait_name: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
            #trait_methods
        }

        dyn_clone::clone_trait_object!(#trait_name);

        #[cfg(any(test, feature = "testsuite"))]
        impl Clone for #mock_name {
            fn clone(&self) -> Self {
                #mock_name::new()
            }
        }
    }
}

fn generate_trait_methods(methods: &[Method], result_path: &syn::Path) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in methods {
        let method_name = quote::format_ident!("{}", method.name);
        let request_type = syn::parse_str::<syn::Path>(&method.input_type)
            .unwrap()
            .to_token_stream();
        let response_type = syn::parse_str::<syn::Path>(&method.output_type)
            .unwrap()
            .to_token_stream();

        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_path<#response_type>;
        };
        stream.extend(method);
    }
    stream
}

fn generate_client(
    trait_name: &Ident,
    client_name: &Ident,
    methods: &[Method],
    result_path: &syn::Path,
) -> TokenStream {
    let client_methods = generate_client_methods(methods, result_path);

    quote! {
        #[derive(Debug, Clone)]
        pub struct #client_name {
            inner: Box<dyn #trait_name>
        }

        impl #client_name {
            pub fn new<T>(instance: T) -> Self
            where T: #trait_name {
                Self {
                    inner: Box::new(instance),
                }
            }
        }

        #[async_trait::async_trait]
        impl #trait_name for #client_name {
            #client_methods
        }
    }
}

fn generate_client_methods(methods: &[Method], result_path: &syn::Path) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in methods {
        let method_name = quote::format_ident!("{}", method.name);
        let request_type = syn::parse_str::<syn::Path>(&method.input_type)
            .unwrap()
            .to_token_stream();
        let response_type = syn::parse_str::<syn::Path>(&method.output_type)
            .unwrap()
            .to_token_stream();

        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_path<#response_type> {
                self.inner.#method_name(request).await
            }
        };
        stream.extend(method);
    }
    stream
}

fn generate_tower_service(
    client_name: &Ident,
    methods: &[Method],
    error_path: &syn::Path,
) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in methods {
        let method_name = quote::format_ident!("{}", method.name);
        let request_type = syn::parse_str::<syn::Path>(&method.input_type)
            .unwrap()
            .to_token_stream();
        let response_type = syn::parse_str::<syn::Path>(&method.output_type)
            .unwrap()
            .to_token_stream();

        let service = quote! {
            impl tower::Service<#request_type> for #client_name {
                type Response = #response_type;
                type Error = #error_path;
                type Future = BoxFuture<Self::Response, Self::Error>;

                fn poll_ready(
                    &mut self,
                    _cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Result<(), Self::Error>> {
                    std::task::Poll::Ready(Ok(()))
                }

                fn call(&mut self, request: #request_type) -> Self::Future {
                    let mut svc = self.clone();
                    let fut = async move { svc.#method_name(request).await };
                    Box::pin(fut)
                }
            }
        };
        stream.extend(service);
    }
    stream
}

fn generate_grpc_client_adapter(
    package_name: &Ident,
    trait_name: &Ident,
    methods: &[Method],
    result_path: &syn::Path,
) -> TokenStream {
    let grpc_client_package_name = quote::format_ident!("{}_grpc_client", package_name);
    let grpc_client_name = quote::format_ident!("{}GrpcClient", trait_name);
    let grpc_client_adapter_name = quote::format_ident!("{}GrpcClientAdapter", trait_name);
    let grpc_server_adapter_methods = generate_grpc_client_adapter_methods(methods, result_path);

    quote! {
        #[derive(Debug, Clone)]
        pub struct #grpc_client_adapter_name<T> {
            inner: T
        }

        impl<T> #grpc_client_adapter_name<T> {
            pub fn new(instance: T) -> Self {
                Self {
                    inner: instance
                }
            }
        }

        #[async_trait::async_trait]
        impl<T> #trait_name for #grpc_client_adapter_name<#grpc_client_package_name::#grpc_client_name<T>>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>
                + std::fmt::Debug
                + Clone
                + Send
                + Sync
                + 'static,
            T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
            T::Future: Send
        {
            #grpc_server_adapter_methods
        }
    }
}

fn generate_grpc_client_adapter_methods(
    methods: &[Method],
    result_path: &syn::Path,
) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in methods {
        let method_name = quote::format_ident!("{}", method.name);
        let request_type = syn::parse_str::<syn::Path>(&method.input_type)
            .unwrap()
            .to_token_stream();
        let response_type = syn::parse_str::<syn::Path>(&method.output_type)
            .unwrap()
            .to_token_stream();

        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_path<#response_type> {
                self.inner
                    .#method_name(request)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|error| error.into())
            }
        };
        stream.extend(method);
    }
    stream
}

fn generate_grpc_server_adapter(
    package_name: &Ident,
    trait_name: &Ident,
    methods: &[Method],
) -> TokenStream {
    let grpc_server_package_name = quote::format_ident!("{}_grpc_server", package_name);
    let grpc_service_name = quote::format_ident!("{}Grpc", trait_name);
    let grpc_server_adapter_name = quote::format_ident!("{}GrpcServerAdapter", trait_name);
    let grpc_server_adapter_methods = generate_grpc_server_adapter_methods(methods);

    quote! {
        #[derive(Debug)]
        pub struct #grpc_server_adapter_name {
            inner: Box<dyn #trait_name>,
        }

        impl #grpc_server_adapter_name {
            pub fn new<T>(instance: T) -> Self
            where T: #trait_name {
                Self {
                    inner: Box::new(instance),
                }
            }
        }

        #[async_trait::async_trait]
        impl #grpc_server_package_name::#grpc_service_name for #grpc_server_adapter_name {
            #grpc_server_adapter_methods
        }
    }
}

fn generate_grpc_server_adapter_methods(methods: &[Method]) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in methods {
        let method_name = quote::format_ident!("{}", method.name);
        let request_type = syn::parse_str::<syn::Path>(&method.input_type)
            .unwrap()
            .to_token_stream();
        let response_type = syn::parse_str::<syn::Path>(&method.output_type)
            .unwrap()
            .to_token_stream();

        let method = quote! {
            async fn #method_name(&self, request: tonic::Request<#request_type>) -> Result<tonic::Response<#response_type>, tonic::Status> {
                self.inner
                    .clone()
                    .#method_name(request.into_inner())
                    .await
                    .map(tonic::Response::new)
                    .map_err(Into::into)
            }
        };
        stream.extend(method);
    }
    stream
}

/// A [`ServiceGenerator`] wrapper that appends a suffix to the name of the wrapped service. It is
/// used to add a `Grpc` suffix to the service, client, and server generated by tonic.
struct WithSuffixServiceGenerator {
    suffix: String,
    inner: Box<dyn ServiceGenerator>,
}

impl WithSuffixServiceGenerator {
    fn new(suffix: &str, service_generator: Box<dyn ServiceGenerator>) -> Self {
        Self {
            suffix: suffix.to_string(),
            inner: service_generator,
        }
    }
}

impl ServiceGenerator for WithSuffixServiceGenerator {
    fn generate(&mut self, mut service: Service, buf: &mut String) {
        service.name = format!("{}{}", service.name, self.suffix);
        self.inner.generate(service, buf);
    }

    fn finalize(&mut self, buf: &mut String) {
        self.inner.finalize(buf);
    }

    fn finalize_package(&mut self, package: &str, buf: &mut String) {
        self.inner.finalize_package(package, buf);
    }
}
