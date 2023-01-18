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
use regex::Regex;

use crate::api_generator::code_gen::stability_doc;
use crate::api_generator::*;

pub fn generate(api: &Api) -> anyhow::Result<String> {
    let mut tokens = Tokens::new();

    for e in &api.enums {
        generate_param(&mut tokens, &e);
    }

    let generated = tokens.to_string();
    Ok(generated)
}

fn generate_param(tokens: &mut Tokens, e: &ApiEnum) {
    let name = syn::Ident::from(e.name.to_pascal_case());
    let (renames, variants): (Vec<String>, Vec<syn::Ident>) = e
        .values
        .iter()
        .map(|v| {
            if v.is_empty() {
                (v.to_owned(), syn::Ident::from("Unspecified"))
            } else if !v.contains('(') {
                (v.to_owned(), syn::Ident::from(v.to_pascal_case()))
            } else {
                lazy_static! {
                    static ref PARENS_REGEX: Regex = Regex::new(r"^(.*?)\s*\(.*?\)\s*$").unwrap();
                }
                if let Some(c) = PARENS_REGEX.captures(v) {
                    (
                        c.get(1).unwrap().as_str().to_owned(),
                        syn::Ident::from(c.get(1).unwrap().as_str().to_pascal_case()),
                    )
                } else {
                    (v.to_owned(), syn::Ident::from(v.to_pascal_case()))
                }
            }
        })
        .unzip();

    let doc = match &e.description {
        Some(description) => Some(code_gen::doc(description)),
        None => None,
    };

    let cfg_attr = e.stability.outer_cfg_attr();
    let cfg_doc = stability_doc(e.stability);

    let generated_enum_tokens = quote!(
        #doc
        #cfg_doc
        #cfg_attr
        #[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
        pub enum #name {
            #(#[serde(rename = #renames)] #variants),*
        }
    );

    tokens.append(generated_enum_tokens);
}
