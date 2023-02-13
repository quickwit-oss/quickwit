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

    let service_trait = generate_service_trait(&service_trait_name, &service.methods, &result_path);

    quote! {
        #service_trait
    }
}

fn generate_service_trait(
    trait_name: &Ident,
    methods: &[Method],
    result_path: &syn::Path,
) -> TokenStream {
    let trait_methods = generate_service_trait_methods(methods, result_path);

    quote! {
        #[async_trait]
        pub trait #trait_name: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {

            #trait_methods
        }

        dyn_clone::clone_trait_object!(#trait_name);
    }
}

fn generate_service_trait_methods(methods: &[Method], result_path: &syn::Path) -> TokenStream {
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
