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

use std::collections::HashSet;

use heck::ToSnakeCase;
use proc_macro2::TokenStream;
use prost_build::{Comments, Method, Service, ServiceGenerator};
use quote::{quote, ToTokens};
use syn::{parse_quote, Ident};

use crate::ProstConfig;

pub struct Codegen;

impl Codegen {
    #[allow(clippy::too_many_arguments)]
    pub fn run_with_config(
        protos: &[&str],
        out_dir: &str,
        result_type_path: &str,
        error_type_path: &str,
        generate_extra_service_methods: bool,
        generate_prom_label_for_requests: bool,
        includes: &[&str],
        mut prost_config: ProstConfig,
    ) -> anyhow::Result<()> {
        let service_generator = Box::new(QuickwitServiceGenerator::new(
            result_type_path,
            error_type_path,
            generate_extra_service_methods,
            generate_prom_label_for_requests,
        ));

        prost_config
            .protoc_arg("--experimental_allow_proto3_optional")
            .type_attribute(
                ".",
                "#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]",
            )
            .field_attribute(
                "DocBatch.doc_buffer",
                "#[schema(value_type = String, format = Binary)]",
            )
            .enum_attribute(".", "#[serde(rename_all=\"snake_case\")]")
            .service_generator(service_generator)
            .out_dir(out_dir);

        for proto in protos {
            println!("cargo:rerun-if-changed={proto}");
            prost_config.compile_protos(&[proto], includes)?;
        }
        Ok(())
    }

    pub fn builder() -> CodegenBuilder {
        CodegenBuilder::default()
    }
}

#[derive(Default)]
pub struct CodegenBuilder {
    protos: Vec<String>,
    includes: Vec<String>,
    out_dir: String,
    prost_config: ProstConfig,
    result_type_path: String,
    error_type_path: String,
    generate_extra_service_methods: bool,
    generate_prom_label_for_requests: bool,
}

impl CodegenBuilder {
    pub fn with_protos(mut self, protos: &[&str]) -> Self {
        self.protos = protos.iter().map(|protos| protos.to_string()).collect();
        self
    }

    pub fn with_includes(mut self, includes: &[&str]) -> Self {
        self.includes = includes
            .iter()
            .map(|includes| includes.to_string())
            .collect();
        self
    }

    pub fn with_output_dir(mut self, path: &str) -> Self {
        self.out_dir = path.into();
        self
    }

    pub fn with_result_type_path(mut self, path: &str) -> Self {
        self.result_type_path = path.into();
        self
    }

    pub fn with_error_type_path(mut self, path: &str) -> Self {
        self.error_type_path = path.into();
        self
    }

    pub fn with_prost_config(mut self, prost_config: ProstConfig) -> Self {
        self.prost_config = prost_config;
        self
    }

    pub fn enable_extra_service_methods(mut self) -> Self {
        self.generate_extra_service_methods = true;
        self
    }

    pub fn enable_prom_label_for_requests(mut self) -> Self {
        self.generate_prom_label_for_requests = true;
        self
    }

    pub fn run(self) -> anyhow::Result<()> {
        Codegen::run_with_config(
            self.protos
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
            &self.out_dir,
            &self.result_type_path,
            &self.error_type_path,
            self.generate_extra_service_methods,
            self.generate_prom_label_for_requests,
            self.includes
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
            self.prost_config,
        )
    }
}

struct QuickwitServiceGenerator {
    result_type_path: String,
    error_type_path: String,
    generate_extra_service_methods: bool,
    generate_prom_labels_for_requests: bool,
    inner: Box<dyn ServiceGenerator>,
}

impl QuickwitServiceGenerator {
    fn new(
        result_type_path: &str,
        error_type_path: &str,
        generate_extra_service_methods: bool,
        generate_prom_labels_for_requests: bool,
    ) -> Self {
        let inner = Box::new(WithSuffixServiceGenerator::new(
            "Grpc",
            tonic_build::configure().service_generator(),
        ));
        Self {
            result_type_path: result_type_path.to_string(),
            error_type_path: error_type_path.to_string(),
            generate_extra_service_methods,
            generate_prom_labels_for_requests,
            inner,
        }
    }
}

impl ServiceGenerator for QuickwitServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        let tokens = generate_all(
            &service,
            &self.result_type_path,
            &self.error_type_path,
            self.generate_extra_service_methods,
            self.generate_prom_labels_for_requests,
        );
        let ast: syn::File = syn::parse2(tokens).expect("Tokenstream should be a valid Syn AST.");
        let pretty_code = prettyplease::unparse(&ast);
        buf.push_str(&pretty_code);

        self.inner.generate(service, buf)
    }

    fn finalize(&mut self, buf: &mut String) {
        self.inner.finalize(buf);
    }
}

struct CodegenContext {
    package_name: String,
    service_name: Ident,
    result_type: syn::Path,
    error_type: syn::Path,
    stream_type: Ident,
    stream_type_alias: TokenStream,
    methods: Vec<SynMethod>,
    client_name: Ident,
    tower_block_name: Ident,
    tower_block_builder_name: Ident,
    mailbox_name: Ident,
    mock_mod_name: Ident,
    mock_name: Ident,
    grpc_client_name: Ident,
    grpc_client_adapter_name: Ident,
    grpc_client_package_name: Ident,
    grpc_server_name: Ident,
    grpc_server_adapter_name: Ident,
    grpc_server_package_name: Ident,
    grpc_service_name: Ident,
    generate_extra_service_methods: bool,
}

impl CodegenContext {
    fn from_service(
        service: &Service,
        result_type_path: &str,
        error_type_path: &str,
        generate_extra_service_methods: bool,
    ) -> Self {
        let service_name = quote::format_ident!("{}", service.name);
        let mock_mod_name = quote::format_ident!("{}_mock", service.name.to_snake_case());
        let mock_name = quote::format_ident!("Mock{}", service.name);

        let result_type = syn::parse_str::<syn::Path>(result_type_path)
            .expect("Result path should be a valid result path such as `crate::HelloResult`.");
        let error_type = syn::parse_str::<syn::Path>(error_type_path)
            .expect("Error path should be a valid result path such as `crate::error::HelloError`.");
        let stream_type = quote::format_ident!("{}Stream", service.name);
        let stream_type_alias = if service.methods.iter().any(|method| method.server_streaming) {
            quote! {
                pub type #stream_type<T> = quickwit_common::ServiceStream<#result_type<T>>;
            }
        } else {
            TokenStream::new()
        };

        let methods = SynMethod::parse_prost_methods(&service.methods);

        let client_name = quote::format_ident!("{}Client", service.name);
        let tower_block_name = quote::format_ident!("{}TowerBlock", service.name);
        let tower_block_builder_name = quote::format_ident!("{}TowerBlockBuilder", service.name);
        let mailbox_name = quote::format_ident!("{}Mailbox", service.name);

        let grpc_client_name = quote::format_ident!("{}GrpcClient", service.name);
        let grpc_client_adapter_name = quote::format_ident!("{}GrpcClientAdapter", service.name);
        let grpc_client_package_name =
            quote::format_ident!("{}_grpc_client", service.name.to_snake_case());
        let package_name = service.package.clone();

        let grpc_server_name = quote::format_ident!("{}GrpcServer", service.name);
        let grpc_server_adapter_name = quote::format_ident!("{}GrpcServerAdapter", service.name);
        let grpc_server_package_name =
            quote::format_ident!("{}_grpc_server", service.name.to_snake_case());

        let grpc_service_name = quote::format_ident!("{}Grpc", service.name);

        Self {
            package_name,
            service_name,
            result_type,
            error_type,
            stream_type,
            stream_type_alias,
            methods,
            client_name,
            tower_block_name,
            tower_block_builder_name,
            mailbox_name,
            mock_mod_name,
            mock_name,
            grpc_client_name,
            grpc_client_adapter_name,
            grpc_client_package_name,
            grpc_server_name,
            grpc_server_adapter_name,
            grpc_server_package_name,
            grpc_service_name,
            generate_extra_service_methods,
        }
    }
}

fn generate_all(
    service: &Service,
    result_type_path: &str,
    error_type_path: &str,
    generate_extra_service_methods: bool,
    implement_prom_labels_for_requests: bool,
) -> TokenStream {
    let context = CodegenContext::from_service(
        service,
        result_type_path,
        error_type_path,
        generate_extra_service_methods,
    );
    let stream_type_alias = &context.stream_type_alias;
    let service_trait = generate_service_trait(&context);
    let client = generate_client(&context);
    let tower_services = generate_tower_services(&context);
    let tower_block = generate_tower_block(&context);
    let tower_block_builder = generate_tower_block_builder(&context);
    let tower_mailbox = generate_tower_mailbox(&context);
    let grpc_client_adapter = generate_grpc_client_adapter(&context);
    let grpc_server_adapter = generate_grpc_server_adapter(&context);
    let prom_labels_impl = if implement_prom_labels_for_requests {
        generate_prom_labels_impl_for_requests(&context)
    } else {
        TokenStream::new()
    };

    quote! {
        // The line below is necessary to opt out of the license header check.
        /// BEGIN quickwit-codegen
        use tower::{Layer, Service, ServiceExt};
        #prom_labels_impl

        #stream_type_alias

        #service_trait

        #client

        pub type BoxFuture<T, E> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>>;

        #tower_services

        #tower_block

        #tower_block_builder

        #tower_mailbox

        #grpc_client_adapter

        #grpc_server_adapter
    }
}

struct SynMethod {
    name: Ident,
    proto_name: Ident,
    comments: Vec<syn::Attribute>,
    request_type: syn::Path,
    response_type: syn::Path,
    client_streaming: bool,
    server_streaming: bool,
}

impl SynMethod {
    fn request_prom_label(&self) -> String {
        self.request_type
            .segments
            .last()
            .unwrap()
            .ident
            .to_string()
            .trim_end_matches("Request")
            .to_snake_case()
    }

    fn request_type(&self, mock: bool) -> TokenStream {
        let request_type = if mock {
            let request_type = &self.request_type;
            quote! { super::#request_type }
        } else {
            self.request_type.to_token_stream()
        };
        if self.client_streaming {
            quote! { quickwit_common::ServiceStream<#request_type> }
        } else {
            request_type
        }
    }

    fn response_type(&self, context: &CodegenContext, mock: bool) -> TokenStream {
        let response_type = if mock {
            let response_type = &self.response_type;
            quote! { super::#response_type }
        } else {
            self.response_type.to_token_stream()
        };
        if self.server_streaming {
            let stream_type = &context.stream_type;
            quote! { #stream_type<#response_type> }
        } else {
            response_type
        }
    }

    fn parse_prost_methods(methods: &[Method]) -> Vec<Self> {
        let mut syn_methods = Vec::with_capacity(methods.len());

        for method in methods {
            let name = quote::format_ident!("{}", method.name);
            let proto_name = quote::format_ident!("{}", method.proto_name);
            let comments = generate_comment_attributes(&method.comments);
            let request_type = syn::parse_str::<syn::Path>(&method.input_type).unwrap();
            let response_type = syn::parse_str::<syn::Path>(&method.output_type).unwrap();

            let syn_method = SynMethod {
                name,
                proto_name,
                comments,
                request_type,
                response_type,
                client_streaming: method.client_streaming,
                server_streaming: method.server_streaming,
            };
            syn_methods.push(syn_method);
        }
        syn_methods
    }
}

fn generate_prom_labels_impl_for_requests(context: &CodegenContext) -> TokenStream {
    let mut stream = TokenStream::new();
    stream.extend(quote! {
        use quickwit_common::metrics::{PrometheusLabels, OwnedPrometheusLabels};
    });
    let mut implemented_request_types: HashSet<String> = HashSet::new();
    for syn_method in &context.methods {
        if syn_method.client_streaming {
            continue;
        }
        let request_type = syn_method.request_type(false);
        let request_type_snake_case = syn_method.request_prom_label();
        if implemented_request_types.contains(&request_type_snake_case) {
            continue;
        } else {
            implemented_request_types.insert(request_type_snake_case.clone());
            let method = quote! {
                impl PrometheusLabels<1> for #request_type {
                    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
                        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed(#request_type_snake_case),])
                    }
                }
            };
            stream.extend(method);
        }
    }
    stream
}

fn generate_comment_attributes(comments: &Comments) -> Vec<syn::Attribute> {
    let mut attributes = Vec::with_capacity(comments.leading.len());

    for comment in &comments.leading {
        let comment = syn::LitStr::new(comment, proc_macro2::Span::call_site());
        let attribute: syn::Attribute = parse_quote! {
            #[doc = #comment]
        };
        attributes.push(attribute);
    }
    attributes
}

fn generate_service_trait(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let trait_methods = generate_service_trait_methods(context);
    let mock_name = &context.mock_name;
    let extra_trait_methods = if context.generate_extra_service_methods {
        quote! {
            async fn check_connectivity(&mut self) -> anyhow::Result<()>;
            fn endpoints(&self) -> Vec<quickwit_common::uri::Uri>;
        }
    } else {
        TokenStream::new()
    };

    quote! {
        #[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
        #[async_trait::async_trait]
        pub trait #service_name: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
            #trait_methods
            #extra_trait_methods
        }

        dyn_clone::clone_trait_object!(#service_name);

        #[cfg(any(test, feature = "testsuite"))]
        impl Clone for #mock_name {
            fn clone(&self) -> Self {
                #mock_name::new()
            }
        }
    }
}

fn generate_service_trait_methods(context: &CodegenContext) -> TokenStream {
    let result_type = &context.result_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let comments = &syn_method.comments;
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);
        let method = quote! {
            #(#comments)*
            async fn #method_name(&mut self, request: #request_type) -> #result_type<#response_type>;
        };
        stream.extend(method);
    }
    stream
}

fn generate_additional_methods_calling_inner() -> TokenStream {
    quote! {
        async fn check_connectivity(&mut self) -> anyhow::Result<()> {
            self.inner.check_connectivity().await
        }

        fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
            self.inner.endpoints()
        }
    }
}

fn generate_client(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let client_name = &context.client_name;

    let grpc_client_name = &context.grpc_client_name;
    let grpc_client_adapter_name = &context.grpc_client_adapter_name;
    let grpc_client_package_name = &context.grpc_client_package_name;

    let grpc_server_name = &context.grpc_server_name;
    let grpc_server_adapter_name = &context.grpc_server_adapter_name;
    let grpc_server_package_name = &context.grpc_server_package_name;

    let client_methods = generate_client_methods(context, false);
    let mock_mod_name = &context.mock_mod_name;
    let mock_methods = generate_client_methods(context, true);
    let mailbox_name = &context.mailbox_name;
    let tower_block_builder_name = &context.tower_block_builder_name;
    let mock_name = &context.mock_name;
    let mock_wrapper_name = quote::format_ident!("{}Wrapper", mock_name);
    let error_mesage = format!(
        "`{}` must be wrapped in a `{}`. Use `{}::from(mock)` to instantiate the client.",
        mock_name, mock_wrapper_name, mock_name
    );
    let additional_client_methods = if context.generate_extra_service_methods {
        generate_additional_methods_calling_inner()
    } else {
        TokenStream::new()
    };
    let additional_mock_methods = if context.generate_extra_service_methods {
        quote! {
            async fn check_connectivity(&mut self) -> anyhow::Result<()> {
                self.inner.lock().await.check_connectivity().await
            }

            fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
                futures::executor::block_on(self.inner.lock()).endpoints()
            }
        }
    } else {
        TokenStream::new()
    };

    quote! {
        #[derive(Debug, Clone)]
        pub struct #client_name {
            inner: Box<dyn #service_name>
        }

        impl #client_name {
            pub fn new<T>(instance: T) -> Self
            where
                T: #service_name,
            {
                #[cfg(any(test, feature = "testsuite"))]
                assert!(std::any::TypeId::of::<T>() != std::any::TypeId::of::<#mock_name>(), #error_mesage);
                Self {
                    inner: Box::new(instance),
                }
            }

            pub fn as_grpc_service(&self) -> #grpc_server_package_name::#grpc_server_name<#grpc_server_adapter_name> {
                let adapter = #grpc_server_adapter_name::new(self.clone());
                #grpc_server_package_name::#grpc_server_name::new(adapter)
            }

            pub fn from_channel(addr: std::net::SocketAddr, channel: tonic::transport::Channel) -> Self
            {
                let (_, connection_keys_watcher) = tokio::sync::watch::channel(std::collections::HashSet::from_iter([addr]));
                let adapter = #grpc_client_adapter_name::new(#grpc_client_package_name::#grpc_client_name::new(channel), connection_keys_watcher);
                Self::new(adapter)
            }

            pub fn from_balance_channel(balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>) -> #client_name
            {
                let connection_keys_watcher = balance_channel.connection_keys_watcher();
                let adapter = #grpc_client_adapter_name::new(#grpc_client_package_name::#grpc_client_name::new(balance_channel), connection_keys_watcher);
                Self::new(adapter)
            }

            pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
            where
                A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
                #mailbox_name<A>: #service_name,
            {
                #client_name::new(#mailbox_name::new(mailbox))
            }

            pub fn tower() -> #tower_block_builder_name {
                #tower_block_builder_name::default()
            }

            #[cfg(any(test, feature = "testsuite"))]
            pub fn mock() -> #mock_name {
                #mock_name::new()
            }
        }

        #[async_trait::async_trait]
        impl #service_name for #client_name {
            #client_methods
            #additional_client_methods
        }

        #[cfg(any(test, feature = "testsuite"))]
        pub mod #mock_mod_name {
            use super::*;

            #[derive(Debug, Clone)]
            struct #mock_wrapper_name {
                inner: std::sync::Arc<tokio::sync::Mutex<#mock_name>>
            }

            #[async_trait::async_trait]
            impl #service_name for #mock_wrapper_name {
                #mock_methods
                #additional_mock_methods
            }

            impl From<#mock_name> for #client_name {
                fn from(mock: #mock_name) -> Self {
                    let mock_wrapper = #mock_wrapper_name {
                        inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock))
                    };
                    #client_name::new(mock_wrapper)
                }
            }
        }
    }
}

fn generate_client_methods(context: &CodegenContext, mock: bool) -> TokenStream {
    let result_type = &context.result_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(mock);
        let response_type = syn_method.response_type(context, mock);

        let body = if !mock {
            quote! {
                self.inner.#method_name(request).await
            }
        } else {
            quote! {
                self.inner.lock().await.#method_name(request).await
            }
        };
        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_type<#response_type> {
                #body
            }
        };
        stream.extend(method);
    }
    stream
}

fn generate_tower_services(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let error_type = &context.error_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let service = quote! {
            impl tower::Service<#request_type> for Box<dyn #service_name> {
                type Response = #response_type;
                type Error = #error_type;
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

fn generate_tower_block(context: &CodegenContext) -> TokenStream {
    let tower_block_name = &context.tower_block_name;
    let service_name = &context.service_name;
    let tower_block_attributes = generate_tower_block_attributes(context);
    let tower_block_clone_impl = generate_tower_block_clone_impl(context);
    let tower_block_service_impl = generate_tower_block_service_impl(context);

    quote! {
        /// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
        #[derive(Debug)]
        struct #tower_block_name {
            inner: Box<dyn #service_name>,

            #tower_block_attributes
        }

        #tower_block_clone_impl

        #tower_block_service_impl
    }
}

fn generate_tower_block_attributes(context: &CodegenContext) -> TokenStream {
    let error_type = &context.error_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let attribute_name = quote::format_ident!("{}_svc", syn_method.name);
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let attribute = quote! {
            #attribute_name: quickwit_common::tower::BoxService<#request_type, #response_type, #error_type>,
        };
        stream.extend(attribute);
    }
    stream
}

fn generate_tower_block_clone_impl(context: &CodegenContext) -> TokenStream {
    let tower_block_name = &context.tower_block_name;

    let mut cloned_attributes = TokenStream::new();

    for syn_method in &context.methods {
        let attribute_name = quote::format_ident!("{}_svc", syn_method.name);
        let attribute = quote! {
            #attribute_name: self.#attribute_name.clone(),
        };
        cloned_attributes.extend(attribute);
    }

    quote! {
        impl Clone for #tower_block_name {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                    #cloned_attributes
                }
            }
        }
    }
}

fn generate_tower_block_service_impl(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let tower_block_name = &context.tower_block_name;
    let result_type = &context.result_type;
    let additional_client_methods = if context.generate_extra_service_methods {
        generate_additional_methods_calling_inner()
    } else {
        TokenStream::new()
    };
    let mut methods = TokenStream::new();

    for syn_method in &context.methods {
        let attribute_name = quote::format_ident!("{}_svc", syn_method.name);
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let attribute = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_type<#response_type> {
                self.#attribute_name.ready().await?.call(request).await
            }
        };
        methods.extend(attribute);
    }

    quote! {
        #[async_trait::async_trait]
        impl #service_name for #tower_block_name {
            #methods
            #additional_client_methods
        }
    }
}

fn generate_tower_block_builder(context: &CodegenContext) -> TokenStream {
    let tower_block_builder_name = &context.tower_block_builder_name;
    let tower_block_builder_attributes = generate_tower_block_builder_attributes(context);
    let tower_block_builder_impl = generate_tower_block_builder_impl(context);

    quote! {
        #[derive(Debug, Default)]
        pub struct #tower_block_builder_name {
            #tower_block_builder_attributes
        }

        #tower_block_builder_impl
    }
}

fn generate_tower_block_builder_attributes(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let error_type = &context.error_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let attribute_name = quote::format_ident!("{}_layer", syn_method.name);
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let attribute = quote! {
            #[allow(clippy::type_complexity)]
            #attribute_name: Option<quickwit_common::tower::BoxLayer<Box<dyn #service_name>, #request_type, #response_type, #error_type>>,
        };
        stream.extend(attribute);
    }
    stream
}

fn generate_tower_block_builder_impl(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let client_name = &context.client_name;
    let mailbox_name = &context.mailbox_name;
    let tower_block_name = &context.tower_block_name;
    let tower_block_builder_name = &context.tower_block_builder_name;
    let error_type = &context.error_type;

    let mut layer_method_bounds = TokenStream::new();
    let mut layer_method_statements = TokenStream::new();
    let mut layer_methods = TokenStream::new();
    let mut svc_statements = TokenStream::new();
    let mut svc_attribute_idents = Vec::with_capacity(context.methods.len());

    for (i, syn_method) in context.methods.iter().enumerate() {
        let layer_attribute_name = quote::format_ident!("{}_layer", syn_method.name);
        let svc_attribute_name = quote::format_ident!("{}_svc", syn_method.name);
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let layer_method_bound = quote! {
            L::Service: tower::Service<#request_type, Response = #response_type, Error = #error_type> + Clone + Send + Sync + 'static,
            <L::Service as tower::Service<#request_type>>::Future: Send + 'static,
        };

        let layer_method_statement = if i == context.methods.len() - 1 {
            quote! {
                self.#layer_attribute_name = Some(quickwit_common::tower::BoxLayer::new(layer));
            }
        } else {
            quote! {
                self.#layer_attribute_name = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
            }
        };

        let layer_method = quote! {
            pub fn #layer_attribute_name<L>(
                mut self,
                layer: L
            ) -> Self
            where
                L: tower::Layer<Box<dyn #service_name>> + Send + Sync + 'static,
                #layer_method_bound
            {
                self.#layer_attribute_name = Some(quickwit_common::tower::BoxLayer::new(layer));
                self
            }
        };
        layer_method_bounds.extend(layer_method_bound);
        layer_method_statements.extend(layer_method_statement);
        layer_methods.extend(layer_method);

        let svc_statement = quote! {
            let #svc_attribute_name = if let Some(layer) = self.#layer_attribute_name {
                layer.layer(boxed_instance.clone())
            } else {
                quickwit_common::tower::BoxService::new(boxed_instance.clone())
            };
        };
        svc_statements.extend(svc_statement);

        svc_attribute_idents.push(svc_attribute_name);
    }

    quote! {
        impl #tower_block_builder_name {
            pub fn shared_layer<L>(mut self, layer: L) -> Self
            where
                L: tower::Layer<Box<dyn #service_name>> + Clone + Send + Sync + 'static,
                #layer_method_bounds
            {
                #layer_method_statements
                self
            }

            #layer_methods

            pub fn build<T>(self, instance: T) -> #client_name
            where
                T: #service_name
            {
                self.build_from_boxed(Box::new(instance))
            }

            pub fn build_from_channel(self, addr: std::net::SocketAddr, channel: tonic::transport::Channel) -> #client_name
            {
                self.build_from_boxed(Box::new(#client_name::from_channel(addr, channel)))
            }

            pub fn build_from_balance_channel(self, balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>) -> #client_name
            {
                self.build_from_boxed(Box::new(#client_name::from_balance_channel(balance_channel)))
            }

            pub fn build_from_mailbox<A>(self, mailbox: quickwit_actors::Mailbox<A>) -> #client_name
            where
                A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
                #mailbox_name<A>: #service_name,
            {
                self.build_from_boxed(Box::new(#mailbox_name::new(mailbox)))
            }

            fn build_from_boxed(self, boxed_instance: Box<dyn #service_name>) -> #client_name
            {
                #svc_statements

                let tower_block = #tower_block_name {
                    inner: boxed_instance.clone(),
                    #(#svc_attribute_idents),*
                };
                #client_name::new(tower_block)
            }
        }
    }
}

fn generate_tower_mailbox(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let mailbox_name = &context.mailbox_name;
    let error_type = &context.error_type;
    let additional_mailbox_methods = if context.generate_extra_service_methods {
        quote! {
            async fn check_connectivity(&mut self) -> anyhow::Result<()> {
                if self.inner.is_disconnected() {
                    anyhow::bail!("actor `{}` is disconnected", self.inner.actor_instance_id())
                }
                Ok(())
            }

            fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
                vec![quickwit_common::uri::Uri::from_well_formed(format!("actor://localhost/{}", self.inner.actor_instance_id()))]
            }
        }
    } else {
        TokenStream::new()
    };

    let (mailbox_bounds, mailbox_methods) = generate_mailbox_bounds_and_methods(context);

    quote! {
        #[derive(Debug, Clone)]
        struct MailboxAdapter<A: quickwit_actors::Actor, E> {
            inner: quickwit_actors::Mailbox<A>,
            phantom: std::marker::PhantomData<E>,
        }

        impl<A, E> std::ops::Deref for MailboxAdapter<A, E> where A: quickwit_actors::Actor {
            type Target = quickwit_actors::Mailbox<A>;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        #[derive(Debug)]
        pub struct #mailbox_name<A: quickwit_actors::Actor> {
            inner: MailboxAdapter<A, #error_type>
        }

        impl <A: quickwit_actors::Actor> #mailbox_name<A> {
            pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
                let inner = MailboxAdapter {
                    inner: instance,
                    phantom: std::marker::PhantomData,
                };
                Self {
                    inner
                }
            }
        }

        impl <A: quickwit_actors::Actor> Clone for #mailbox_name<A> {
            fn clone(&self) -> Self {
                let inner = MailboxAdapter {
                    inner: self.inner.clone(),
                    phantom: std::marker::PhantomData,
                };
                Self { inner }
            }
        }

        impl<A, M, T, E> tower::Service<M> for #mailbox_name<A>
        where
            A: quickwit_actors::Actor + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send + 'static,
            M: std::fmt::Debug + Send + 'static,
            T: Send + 'static,
            E: std::fmt::Debug + Send + 'static,
            #error_type: From<quickwit_actors::AskError<E>>,
        {
            type Response = T;
            type Error = #error_type;
            type Future = BoxFuture<Self::Response, Self::Error>;

            fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
                //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
                //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
                //! mailbox in `poll_ready` and consume it in `call`.
                std::task::Poll::Ready(Ok(()))
            }

            fn call(&mut self, message: M) -> Self::Future {
                let mailbox = self.inner.clone();
                let fut = async move {
                    mailbox
                        .ask_for_res(message)
                        .await
                        .map_err(|error| error.into())
                };
                Box::pin(fut)
            }
        }

        #[async_trait::async_trait]
        impl<A> #service_name for #mailbox_name<A>
        where
            A: quickwit_actors::Actor + std::fmt::Debug,
            #mailbox_name<A>: #(#mailbox_bounds)+*,
        {
            #mailbox_methods
            #additional_mailbox_methods
        }
    }
}

fn generate_mailbox_bounds_and_methods(
    context: &CodegenContext,
) -> (Vec<TokenStream>, TokenStream) {
    let result_type = &context.result_type;
    let error_type = &context.error_type;

    let mut bounds = Vec::with_capacity(context.methods.len());
    let mut methods = TokenStream::new();

    for syn_method in &context.methods {
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let bound = quote! {
            tower::Service<#request_type, Response = #response_type, Error = #error_type, Future = BoxFuture<#response_type, #error_type>>
        };
        bounds.push(bound);

        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_type<#response_type> {
                self.call(request).await
            }
        };
        methods.extend(method);
    }
    (bounds, methods)
}

fn generate_grpc_client_adapter(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let service_name_string = service_name.to_string();
    let grpc_client_package_name = &context.grpc_client_package_name;
    let grpc_client_package_name_string = &context.package_name.to_string();
    let grpc_client_name = &context.grpc_client_name;
    let grpc_client_adapter_name = &context.grpc_client_adapter_name;
    let grpc_server_adapter_methods = generate_grpc_client_adapter_methods(context);
    let additional_grpc_server_adapter_methods = if context.generate_extra_service_methods {
        quote! {
            async fn check_connectivity(&mut self) -> anyhow::Result<()> {
                if self.connection_addrs_rx.borrow().len() == 0 {
                    anyhow::bail!("no server currently available")
                }
                Ok(())
            }

            fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
                self.connection_addrs_rx
                    .borrow()
                    .iter()
                    .map(|addr| quickwit_common::uri::Uri::from_well_formed(format!(r"grpc://{}/{}.{}", addr, #grpc_client_package_name_string, #service_name_string)))
                    .collect()
            }
        }
    } else {
        TokenStream::new()
    };

    quote! {
        #[derive(Debug, Clone)]
        pub struct #grpc_client_adapter_name<T> {
            inner: T,
            // TODO: remove this field once `check_connectivity` is used for all services.
            #[allow(dead_code)]
            connection_addrs_rx: tokio::sync::watch::Receiver<std::collections::HashSet<std::net::SocketAddr>>,
        }

        impl<T> #grpc_client_adapter_name<T> {
            pub fn new(instance: T, connection_addrs_rx: tokio::sync::watch::Receiver<std::collections::HashSet<std::net::SocketAddr>>) -> Self {
                Self {
                    inner: instance,
                    connection_addrs_rx
                }
            }
        }

        #[async_trait::async_trait]
        impl<T> #service_name for #grpc_client_adapter_name<#grpc_client_package_name::#grpc_client_name<T>>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send + Sync + 'static,
            T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
            T::Future: Send
        {
            #grpc_server_adapter_methods
            #additional_grpc_server_adapter_methods
        }
    }
}

fn generate_grpc_client_adapter_methods(context: &CodegenContext) -> TokenStream {
    let result_type = &context.result_type;

    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let method_name = syn_method.name.to_token_stream();
        let request_type = syn_method.request_type(false);
        let response_type = syn_method.response_type(context, false);

        let into_response_type = if syn_method.server_streaming {
            quote! { |response|
                {
                    let streaming: tonic::Streaming<_> = response.into_inner();
                    let stream = quickwit_common::ServiceStream::from(streaming);
                    stream.map_err(|error| error.into())
                }
            }
        } else {
            quote! { |response| response.into_inner() }
        };
        let method = quote! {
            async fn #method_name(&mut self, request: #request_type) -> #result_type<#response_type> {
                self.inner
                    .#method_name(request)
                    .await
                    .map(#into_response_type)
                    .map_err(|error| error.into())
            }
        };
        stream.extend(method);
    }
    stream
}

fn generate_grpc_server_adapter(context: &CodegenContext) -> TokenStream {
    let service_name = &context.service_name;
    let grpc_server_package_name = &context.grpc_server_package_name;
    let grpc_service_name = &context.grpc_service_name;
    let grpc_server_adapter_name = &context.grpc_server_adapter_name;
    let grpc_server_adapter_methods = generate_grpc_server_adapter_methods(context);

    quote! {
        #[derive(Debug)]
        pub struct #grpc_server_adapter_name {
            inner: Box<dyn #service_name>,
        }

        impl #grpc_server_adapter_name {
            pub fn new<T>(instance: T) -> Self
            where T: #service_name {
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

fn generate_grpc_server_adapter_methods(context: &CodegenContext) -> TokenStream {
    let mut stream = TokenStream::new();

    for syn_method in &context.methods {
        let method_name = syn_method.name.to_token_stream();
        let request_type = if syn_method.client_streaming {
            let request_type = &syn_method.request_type;
            quote! { tonic::Streaming<#request_type> }
        } else {
            syn_method.request_type.to_token_stream()
        };
        let method_arg = if syn_method.client_streaming {
            quote! {
                {
                    let streaming: tonic::Streaming<_> = request.into_inner();
                    quickwit_common::ServiceStream::from(streaming)
                }
            }
        } else {
            quote! { request.into_inner() }
        };
        let response_type = if syn_method.server_streaming {
            let associated_type_name = quote::format_ident!("{}Stream", syn_method.proto_name);
            quote! { Self::#associated_type_name }
        } else {
            syn_method.response_type.to_token_stream()
        };
        let associated_type = if syn_method.server_streaming {
            let associated_type_name = quote::format_ident!("{}Stream", syn_method.proto_name);
            let response_type = &syn_method.response_type;
            quote! { type #associated_type_name = quickwit_common::ServiceStream<tonic::Result<#response_type>>; }
        } else {
            TokenStream::new()
        };
        let into_response_type = if syn_method.server_streaming {
            quote! {
                |stream| tonic::Response::new(stream.map_err(|error| error.into()))
            }
        } else {
            quote! { tonic::Response::new }
        };
        let method = quote! {
            #associated_type

            async fn #method_name(&self, request: tonic::Request<#request_type>) -> Result<tonic::Response<#response_type>, tonic::Status> {
                self.inner
                    .clone()
                    .#method_name(#method_arg)
                    .await
                    .map(#into_response_type)
                    .map_err(|error| error.into())
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
