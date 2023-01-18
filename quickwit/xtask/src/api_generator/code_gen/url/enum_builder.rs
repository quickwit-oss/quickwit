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
// Some types from or based on types from elastic: https://github.com/elastic-rs/elastic
//
// Licensed under Apache 2.0: https://github.com/elastic-rs/elastic/blob/51298dd64278f34d2db911bd1a35eb757c336198/LICENSE-APACHE
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
use inflector::Inflector;

use crate::api_generator::code_gen::url::url_builder::{IntoExpr, UrlBuilder};
use crate::api_generator::code_gen::*;
use crate::api_generator::{ApiEndpoint, Path};

/// Builder for request url parts enum
///
/// The output of this structure is an enum that only accepts valid parameter combinations,
/// based on what's given in the paths for an endpoint.
#[derive(Debug, Clone)]
pub struct EnumBuilder<'a> {
    ident: syn::Ident,
    api_name: String,
    variants: Vec<syn::Variant>,
    paths: Vec<&'a Path>,
    has_lifetime: bool,
}

impl<'a> EnumBuilder<'a> {
    pub fn new(prefix: &str) -> Self {
        let name = Self::name(prefix);
        let api_name = split_on_pascal_case(prefix);
        EnumBuilder {
            ident: ident(name),
            api_name,
            variants: vec![],
            paths: vec![],
            has_lifetime: false,
        }
    }

    fn name(prefix: &str) -> String {
        format!("{}Parts", prefix.to_pascal_case())
    }

    /// Whether this instance already contains a path with parts matching the given path
    fn contains_path_with_parts(&self, path: &'a Path) -> bool {
        let params = path.path.params();
        self.paths.iter().any(|&p| p.path.params() == params)
    }

    /// Whether this instance contains only a single path with no parts
    pub fn contains_single_parameterless_part(&self) -> bool {
        match self.paths.len() {
            1 => self.paths[0].parts.is_empty(),
            _ => false,
        }
    }

    pub fn with_path(mut self, path: &'a Path) -> Self {
        if !self.contains_path_with_parts(path) {
            let variant = match &path.parts.len() {
                0 => Self::parts_none(),
                _ => {
                    self.has_lifetime = true;
                    Self::parts(&path)
                }
            };

            self.variants.push(variant);
            self.paths.push(path);
        }

        self
    }

    /// AST for a parts variant.
    fn parts(path: &Path) -> syn::Variant {
        let params = &path.path.params();

        let name = params
            .iter()
            .map(|k| k.to_pascal_case())
            .collect::<Vec<_>>()
            .join("");

        let doc = match params.len() {
            1 => doc(params[0].replace("_", " ").to_pascal_case()),
            n => {
                let mut d: String = params
                    .iter()
                    .enumerate()
                    .filter(|&(i, _)| i != n - 1)
                    .map(|(_, e)| e.replace("_", " ").to_pascal_case())
                    .collect::<Vec<_>>()
                    .join(", ");

                d.push_str(
                    format!(" and {}", params[n - 1].replace("_", " ").to_pascal_case()).as_str(),
                );
                doc(d)
            }
        };

        syn::Variant {
            ident: ident(name),
            attrs: vec![doc],
            discriminant: None,
            data: syn::VariantData::Tuple(
                path.path
                    .params()
                    .iter()
                    .map(|&p| {
                        let ty = &path.parts[p].ty;
                        syn::Field {
                            ident: None,
                            vis: syn::Visibility::Inherited,
                            attrs: vec![],
                            ty: typekind_to_ty(p, ty, true, false),
                        }
                    })
                    .collect(),
            ),
        }
    }

    /// AST for a `None` parts variant.
    fn parts_none() -> syn::Variant {
        syn::Variant {
            ident: ident("None"),
            attrs: vec![doc("No parts")],
            data: syn::VariantData::Unit,
            discriminant: None,
        }
    }

    fn match_path(ty: &syn::Ty, variant: &syn::Variant) -> syn::Path {
        let mut path = ty.get_path().to_owned();
        // Remove lifetimes from the enum type.
        for segment in &mut path.segments {
            segment.parameters = syn::PathParameters::none();
        }

        path.segments
            .push(syn::PathSegment::from(variant.ident.to_string()));
        path
    }

    /// Get the field names for the enum tuple variant to match.
    fn match_fields(path: &Path) -> Vec<syn::Pat> {
        path.path
            .params()
            .iter()
            .map(|&p| {
                syn::Pat::Ident(
                    syn::BindingMode::ByRef(syn::Mutability::Immutable),
                    ident(valid_name(p)),
                    None,
                )
            })
            .collect()
    }

    /// Build this enum and return ASTs for its type, struct declaration and impl
    pub fn build(self) -> (syn::Ty, syn::Item, syn::Item) {
        let variants = match self.variants.len() {
            0 => vec![Self::parts_none()],
            _ => self.variants,
        };

        let (enum_ty, generics) = {
            if self.has_lifetime {
                (ty_b(self.ident.as_ref()), generics_b())
            } else {
                (ty(self.ident.as_ref()), generics_none())
            }
        };

        let enum_impl = {
            let mut arms = Vec::new();
            for (variant, &path) in variants.iter().zip(self.paths.iter()) {
                let match_path = Self::match_path(&enum_ty, variant);
                let fields = Self::match_fields(path);

                let arm = match fields.len() {
                    0 => syn::Pat::Path(None, match_path),
                    _ => syn::Pat::TupleStruct(match_path, fields, None),
                };

                let body = UrlBuilder::new(path).build();
                arms.push(syn::Arm {
                    attrs: vec![],
                    pats: vec![arm],
                    guard: None,
                    body: Box::new(body),
                });
            }
            let match_expr: syn::Expr =
                syn::ExprKind::Match(Box::new(path_none("self").into_expr()), arms).into();

            let fn_decl = syn::FnDecl {
                inputs: vec![syn::FnArg::SelfValue(syn::Mutability::Immutable)],
                output: syn::FunctionRetTy::Ty(ty("Cow<'static, str>")),
                variadic: false,
            };

            let item = syn::ImplItem {
                ident: ident("url"),
                vis: syn::Visibility::Public,
                defaultness: syn::Defaultness::Final,
                attrs: vec![doc(format!(
                    "Builds a relative URL path to the {} API",
                    self.api_name
                ))],
                node: syn::ImplItemKind::Method(
                    syn::MethodSig {
                        unsafety: syn::Unsafety::Normal,
                        constness: syn::Constness::NotConst,
                        abi: None,
                        decl: fn_decl,
                        generics: generics_none(),
                    },
                    syn::Block {
                        stmts: vec![match_expr.into_stmt()],
                    },
                ),
            };

            syn::Item {
                ident: ident(""),
                vis: syn::Visibility::Public,
                attrs: vec![],
                node: syn::ItemKind::Impl(
                    syn::Unsafety::Normal,
                    syn::ImplPolarity::Positive,
                    generics.clone(),
                    None,
                    Box::new(enum_ty.clone()),
                    vec![item],
                ),
            }
        };

        let enum_decl = syn::Item {
            ident: self.ident,
            vis: syn::Visibility::Public,
            attrs: vec![
                syn::Attribute {
                    is_sugared_doc: false,
                    style: syn::AttrStyle::Outer,
                    value: syn::MetaItem::List(
                        ident("derive"),
                        vec![
                            syn::NestedMetaItem::MetaItem(syn::MetaItem::Word(ident("Debug"))),
                            syn::NestedMetaItem::MetaItem(syn::MetaItem::Word(ident("Clone"))),
                            syn::NestedMetaItem::MetaItem(syn::MetaItem::Word(ident("PartialEq"))),
                        ],
                    ),
                },
                doc(format!("API parts for the {} API", self.api_name)),
            ],
            node: syn::ItemKind::Enum(variants, generics),
        };

        (enum_ty, enum_decl, enum_impl)
    }
}

impl<'a> From<&'a (String, ApiEndpoint)> for EnumBuilder<'a> {
    fn from(value: &'a (String, ApiEndpoint)) -> Self {
        let endpoint = &value.1;
        let mut builder = EnumBuilder::new(value.0.to_pascal_case().as_ref());
        for path in &endpoint.url.paths {
            builder = builder.with_path(&path);
        }

        builder
    }
}

#[cfg(test)]
mod tests {
    #![cfg_attr(rustfmt, rustfmt_skip)]

    use super::*;
    use crate::api_generator::{Url, Path, HttpMethod, Body, Deprecated, Type, TypeKind, Documentation, ast_eq, Stability};
    use std::collections::BTreeMap;
    use crate::api_generator::code_gen::url::url_builder::PathString;

    #[test]
    #[ignore] // TODO: now that rust_fmt is not used, ast_eq function emits _slightly_ different Tokens which fail comparison...
    fn generate_parts_enum_from_endpoint() {
        let endpoint = (
            "search".to_string(),
            ApiEndpoint {
                full_name: Some("search".to_string()),
                documentation: Documentation {
                    description: None,
                    url: None,
                },
                stability: Stability::Stable,
                deprecated: None,
                url: Url {
                    paths: vec![
                        Path {
                            path: PathString("/_search".to_string()),
                            methods: vec![HttpMethod::Get, HttpMethod::Post],
                            parts: BTreeMap::new(),
                            deprecated: None,
                        },
                        Path {
                            path: PathString("/{index}/_search".to_string()),
                            methods: vec![HttpMethod::Get, HttpMethod::Post],
                            parts: {
                                let mut map = BTreeMap::new();
                                map.insert("index".to_string(), Type {
                                    ty: TypeKind::List,
                                    description: Some("A comma-separated list of document types to search".to_string()),
                                    options: vec![],
                                    default: None,
                                });
                                map
                            },
                            deprecated: None,
                        },
                        Path {
                            path: PathString("/{index}/{type}/_search".to_string()),
                            methods: vec![HttpMethod::Get, HttpMethod::Post],
                            parts: {
                                let mut map = BTreeMap::new();
                                map.insert("index".to_string(), Type {
                                    ty: TypeKind::List,
                                    description: Some("A comma-separated list of index names to search".to_string()),
                                    options: vec![],
                                    default: None,
                                });
                                map.insert("type".to_string(), Type {
                                    ty: TypeKind::List,
                                    description: Some("A comma-separated list of document types to search".to_string()),
                                    options: vec![],
                                    default: None,
                                });
                                map
                            },
                            deprecated: Some(Deprecated {
                                version: "7.0.0".to_string(),
                                description: "types are going away".to_string()
                            }),
                        },
                    ],
                },
                params: BTreeMap::new(),
                body: Some(Body {
                    description: Some("The search request".to_string()),
                    required: Some(false),
                    serialize: None
                }),
            },
        );

        let (enum_ty, enum_decl, enum_impl) = EnumBuilder::from(&endpoint).build();

        assert_eq!(ty_b("SearchParts"), enum_ty);

        let expected_decl = quote!(
            #[derive(Debug, Clone, PartialEq)]
            #[doc = "API parts for the Search API"]
            pub enum SearchParts<'b> {
                #[doc = "No parts"]
                None,
                #[doc = "Index"]
                Index(&'b [&'b str]),
                #[doc = "Index and Type"]
                IndexType(&'b [&'b str], &'b [&'b str]),
            }
        );

        ast_eq(expected_decl, enum_decl);

        let expected_impl = quote!(
            impl<'b> SearchParts<'b> {
                #[doc = "Builds a relative URL path to the Search API"]
                pub fn url(self) -> Cow<'static, str> {
                    match self {
                        SearchParts::None => "/_search".into(),
                        SearchParts::Index(ref index) => {
                            let index_str = index.join(",");
                            let encoded_index: Cow<str> = percent_encode(index_str.as_bytes(), PARTS_ENCODED).into();
                            let mut p = String::with_capacity(9usize + encoded_index.len());
                            p.push_str("/");
                            p.push_str(encoded_index.as_ref());
                            p.push_str("/_search");
                            p.into()
                        }
                        SearchParts::IndexType(ref index, ref ty) => {
                            let index_str = index.join(",");
                            let ty_str = ty.join(",");
                            let encoded_index: Cow<str> = percent_encode(index_str.as_bytes(), PARTS_ENCODED).into();
                            let encoded_ty: Cow<str> = percent_encode(ty_str.as_bytes(), PARTS_ENCODED).into();
                            let mut p = String::with_capacity(10usize + encoded_index.len() + encoded_ty.len());
                            p.push_str("/");
                            p.push_str(encoded_index.as_ref());
                            p.push_str("/");
                            p.push_str(encoded_ty.as_ref());
                            p.push_str("/_search");
                            p.into()
                        }
                    }
                }
            }
        );

        ast_eq(expected_impl, enum_impl);
    }
}
