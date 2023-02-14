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

use std::path::Path;

use proc_macro2::TokenStream;
use prost_build::{Method, Service, ServiceGenerator};
use quote::{quote, ToTokens};
use syn::Ident;

pub struct Codegen;

impl Codegen {
    pub fn run(proto: &Path, out_dir: &Path, result_path: &str) -> anyhow::Result<()> {
        let mut prost_config = prost_build::Config::default();
        prost_config
            .protoc_arg("--experimental_allow_proto3_optional")
            .type_attribute(".", "#[derive(Serialize, Deserialize, utoipa::ToSchema)]")
            .out_dir(out_dir);

        let service_generator = Box::new(QuickwitServiceGenerator::new(result_path));

        prost_config
            .service_generator(service_generator)
            .compile_protos(&[proto], &["protos"])?;
        Ok(())
    }
}

struct QuickwitServiceGenerator {
    result_path: String,
    tonic_svc_generator: Box<dyn ServiceGenerator>,
}

impl QuickwitServiceGenerator {
    fn new(result_path: &str) -> Self {
        Self {
            result_path: result_path.to_string(),
            tonic_svc_generator: tonic_build::configure().service_generator(),
        }
    }
}

impl ServiceGenerator for QuickwitServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        let tokens = generate_all(&service, &self.result_path);
        let ast: syn::File = syn::parse2(tokens).expect("Tokenstream should be a valid Syn AST.");
        let pretty_code = prettyplease::unparse(&ast);
        buf.push_str(&pretty_code);

        self.tonic_svc_generator.generate(service, buf)
    }

    fn finalize(&mut self, buf: &mut String) {
        self.tonic_svc_generator.finalize(buf);
    }
}

fn generate_all(service: &Service, result_path: &str) -> TokenStream {
    let service_trait_name = quote::format_ident!("{}", service.name);
    let result_path = syn::parse_str::<syn::Path>(result_path)
        .expect("Result path should be a valid result path such as `crate::Result`.");

    let service_trait = generate_trait(&service_trait_name, &service.methods, &result_path);
    let service_client = generate_client(&service_trait_name, &service.methods, &result_path);

    quote! {
        #service_trait

        #service_client
    }
}

fn generate_trait(trait_name: &Ident, methods: &[Method], result_path: &syn::Path) -> TokenStream {
    let trait_methods = generate_trait_methods(methods, result_path);
    let mock_name = quote::format_ident!("Mock{}", trait_name);

    quote! {
        #[mockall::automock]
        #[async_trait]
        pub trait #trait_name: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
            #trait_methods
        }

        dyn_clone::clone_trait_object!(#trait_name);

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

fn generate_client(trait_name: &Ident, methods: &[Method], result_path: &syn::Path) -> TokenStream {
    let client_name = quote::format_ident!("{}Client", trait_name);
    let client_methods = generate_client_methods(methods, result_path);

    quote! {
        #[derive(Debug, Clone)]
        pub struct #client_name {
            inner: Box<dyn #trait_name>,
        }

        impl #client_name {
            pub fn new<T>(instance: T) -> Self
            where
                T: #trait_name,
            {
                Self {
                    inner: Box::new(instance),
                }
            }
        }

        #[async_trait]
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
