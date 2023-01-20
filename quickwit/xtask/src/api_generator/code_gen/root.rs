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

use inflector::Inflector;
use quote::Tokens;

use super::url::enum_builder::EnumBuilder;
use crate::api_generator::code_gen::*;
use crate::api_generator::*;

/// Generates the source code for endpoints params
pub fn generate(api: &Api) -> anyhow::Result<String> {
    let mut tokens = quote!(
        use serde::{Serialize, Deserialize};
        use super::{from_simple_list, to_simple_list, SimpleList, TrackTotalHits};
        use warp::{Filter, Rejection};
    );

    // AST for builder structs and methods
    let structs: Vec<Tokens> = api
        .root
        .endpoints()
        .iter()
        .map(|(endpoint_name, endpoint)| {
            generate_endpoint(endpoint_name, &api.common_params, endpoint)
        })
        .collect();

    tokens.append(quote!(
        #(#structs)*
    ));

    let generated = tokens.to_string();
    Ok(generated)
}

fn generate_endpoint(
    endpoint_name: &str,
    common_params: &BTreeMap<String, Type>,
    endpoint: &ApiEndpoint,
) -> Tokens {
    let mut enum_builder = EnumBuilder::new(endpoint_name.to_pascal_case().as_ref());
    for path in &endpoint.url.paths {
        enum_builder = enum_builder.with_path(path);
    }

    // TODO: generate body struct if needed.
    // let accepts_nd_body = endpoint.supports_nd_body();

    let query_string_params = {
        let mut p = endpoint.params.clone();
        p.append(&mut common_params.clone());
        p
    };

    create_query_string_struct_type(
        &format!("{}QueryParams", endpoint_name.to_pascal_case()),
        &query_string_params,
    )
}

/// Create the AST for an expression that builds a struct of query string parameters
fn create_query_string_struct_type(name: &str, endpoint_params: &BTreeMap<String, Type>) -> Tokens {
    if endpoint_params.is_empty() {
        quote!(None::<()>)
    } else {
        let query_struct_ty = ident(name);
        let struct_fields = endpoint_params.iter().map(|(param_name, param_type)| {
            let field = create_struct_field((param_name, param_type));

            let renamed = field.ident.as_ref().unwrap() != param_name;
            let serde_rename = if renamed {
                let field_rename = lit(param_name);
                quote! {
                    #[serde(rename = #field_rename)]
                }
            } else {
                quote!()
            };

            // TODO: we special case expand_wildcards here to be a list, but this should be
            // fixed upstream
            let expand = param_type.ty == TypeKind::List || param_name == "expand_wildcards";
            let serialize_with = if expand {
                quote! {
                    #[serde(serialize_with = "to_simple_list")]
                    #[serde(deserialize_with = "from_simple_list")]
                }
            } else {
                quote!()
            };

            quote! {
                #serde_rename
                #serialize_with
                #[serde(default)]
                #field
            }
        });

        quote! {
            #[serde_with::skip_serializing_none]
            #[derive(Default, Debug, Serialize, Deserialize)]
            pub struct #query_struct_ty {
                #(#struct_fields,)*
            }
        }
    }
}

/// Creates the AST for a field for a struct
fn create_struct_field(f: (&String, &Type)) -> syn::Field {
    syn::Field {
        ident: Some(ident(valid_name(&f.0).to_lowercase())),
        vis: syn::Visibility::Inherited,
        attrs: vec![],
        ty: typekind_to_ty(&f.0, &f.1.ty, false, false),
    }
}
