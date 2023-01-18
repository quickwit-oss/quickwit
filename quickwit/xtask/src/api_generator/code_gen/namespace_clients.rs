// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::path::PathBuf;

use inflector::Inflector;
use quote::Tokens;

use crate::api_generator::code_gen::request::request_builder::RequestBuilder;
use crate::api_generator::code_gen::*;
use crate::api_generator::*;

/// Generates the source code for a namespaced client
pub fn generate(api: &Api, docs_dir: &PathBuf) -> anyhow::Result<Vec<(String, String)>> {
    let mut output = Vec::new();

    for (namespace_name, namespace) in &api.namespaces {
        let mut tokens = Tokens::new();
        if let Some(attr) = namespace.stability.inner_cfg_attr() {
            tokens.append(attr);
        }
        if let Some(mut attr) = stability_doc(namespace.stability) {
            attr.style = syn::AttrStyle::Inner;
            tokens.append(quote! { #attr });
        }

        tokens.append(use_declarations());

        let namespace_pascal_case = namespace_name.to_pascal_case();
        let namespace_replaced_pascal_case = namespace_name.replace("_", " ").to_pascal_case();
        let namespace_client_name = ident(&namespace_pascal_case);
        let name_for_docs = match namespace_replaced_pascal_case.as_ref() {
            "Ccr" => "Cross Cluster Replication",
            "Ilm" => "Index Lifecycle Management",
            "Slm" => "Snapshot Lifecycle Management",
            "Ml" => "Machine Learning",
            "Xpack" => "X-Pack",
            name => name,
        };

        let namespace_doc = doc(format!("Namespace client for {} APIs", &name_for_docs));
        let namespace_fn_doc = doc(format!(
            "Creates a namespace client for {} APIs",
            &name_for_docs
        ));
        let new_namespace_client_doc = doc(format!(
            "Creates a new instance of [{}]",
            &namespace_pascal_case
        ));
        let namespace_name = ident(namespace_name.to_string());

        let (builders, methods): (Vec<Tokens>, Vec<Tokens>) = namespace
            .endpoints()
            .iter()
            .map(|(name, endpoint)| {
                let builder_name = format!("{}{}", &namespace_pascal_case, name.to_pascal_case());
                RequestBuilder::new(
                    docs_dir,
                    &namespace_pascal_case,
                    name,
                    &builder_name,
                    &api.common_params,
                    &endpoint,
                    false,
                )
                .build()
            })
            .unzip();

        let cfg_attr = namespace.stability.outer_cfg_attr();
        let cfg_doc = stability_doc(namespace.stability);
        tokens.append(quote!(
            #(#builders)*

            #namespace_doc
            #cfg_doc
            #cfg_attr
            pub struct #namespace_client_name<'a> {
                transport: &'a Transport
            }

            #cfg_attr
            impl<'a> #namespace_client_name<'a> {
                #new_namespace_client_doc
                pub fn new(transport: &'a Transport) -> Self {
                    Self {
                        transport
                    }
                }

                pub fn transport(&self) -> &Transport {
                    self.transport
                }

                #(#methods)*
            }

            #cfg_attr
            impl Elasticsearch {
                #namespace_fn_doc
                pub fn #namespace_name(&self) -> #namespace_client_name {
                    #namespace_client_name::new(self.transport())
                }
            }
        ));

        let generated = tokens.to_string();
        output.push((namespace_name.to_string(), generated));
    }

    Ok(output)
}
