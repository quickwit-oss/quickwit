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
use std::collections::BTreeMap;
use std::iter::Iterator;
use std::{fmt, str};

use quote::ToTokens;
use serde::{Deserialize, Deserializer};

use crate::api_generator::code_gen::*;
use crate::api_generator::{Path, Type, TypeKind};

/// A URL path
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct PathString(#[serde(deserialize_with = "rooted_path_string")] pub String);

/// Ensure all deserialized paths have a leading `/`
fn rooted_path_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;

    if !s.starts_with('/') {
        Ok(format!("/{}", s))
    } else {
        Ok(s)
    }
}

impl fmt::Display for PathString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PathString {
    /// Splits a path into a vector of parameter and literal parts
    pub fn split(&self) -> Vec<PathPart> {
        PathString::parse(self.0.as_bytes(), PathParseState::Literal, Vec::new())
    }

    /// Gets the parameters from the path
    pub fn params(&self) -> Vec<&str> {
        self.split()
            .iter()
            .filter_map(|p| match *p {
                PathPart::Param(p) => Some(p),
                _ => None,
            })
            .collect()
    }

    fn parse<'a>(i: &'a [u8], state: PathParseState, r: Vec<PathPart<'a>>) -> Vec<PathPart<'a>> {
        if i.is_empty() {
            return r;
        }

        let mut r = r;
        match state {
            PathParseState::Literal => {
                let (rest, part) = PathString::parse_literal(i);
                if !part.is_empty() {
                    r.push(PathPart::Literal(part));
                }

                PathString::parse(rest, PathParseState::Param, r)
            }
            PathParseState::Param => {
                let (rest, part) = PathString::parse_param(i);
                if !part.is_empty() {
                    r.push(PathPart::Param(part));
                }

                PathString::parse(rest, PathParseState::Literal, r)
            }
        }
    }

    fn parse_literal(i: &[u8]) -> (&[u8], &str) {
        if i[0] == b'}' {
            let i = shift(i, 1);
            take_while(i, |c| c != b'{')
        } else {
            take_while(i, |c| c != b'{')
        }
    }

    fn parse_param(i: &[u8]) -> (&[u8], &str) {
        if i[0] == b'{' {
            let i = shift(i, 1);
            take_while(i, |c| c != b'}')
        } else {
            take_while(i, |c| c != b'}')
        }
    }
}

enum PathParseState {
    Literal,
    Param,
}

/// A part of a Path
#[derive(Debug, PartialEq)]
pub enum PathPart<'a> {
    Literal(&'a str),
    Param(&'a str),
}

pub trait PathParams<'a> {
    fn params(&'a self) -> Vec<&'a str>;
}

impl<'a> PathParams<'a> for Vec<PathPart<'a>> {
    fn params(&'a self) -> Vec<&'a str> {
        self.iter()
            .filter_map(|p| match *p {
                PathPart::Param(p) => Some(p),
                _ => None,
            })
            .collect()
    }
}

/// Builder for an efficient url value replacer.
pub struct UrlBuilder<'a> {
    path: Vec<PathPart<'a>>,
    parts: &'a BTreeMap<String, Type>,
}

impl<'a> UrlBuilder<'a> {
    pub fn new(path: &'a Path) -> Self {
        let path_parts = path.path.split();
        let parts = &path.parts;

        UrlBuilder {
            path: path_parts,
            parts,
        }
    }

    /// Build the AST for an allocated url from the path literals and params.
    fn build_owned(self) -> syn::Block {
        // collection of let {name}_str = [self.]{name}.[join(",")|to_string()];
        let let_params_exprs = Self::let_parameters_exprs(&self.path, &self.parts);

        let mut let_encoded_params_exprs = Self::let_encoded_exprs(&self.path, &self.parts);

        let url_ident = ident("p");
        let len_expr = {
            let lit_len_expr = Self::literal_length_expr(&self.path);
            let mut params_len_exprs = Self::parameter_length_exprs(&self.path);
            let mut len_exprs = vec![lit_len_expr];
            len_exprs.append(&mut params_len_exprs);
            Self::summed_length_expr(len_exprs)
        };
        let let_stmt = Self::let_p_stmt(url_ident.clone(), len_expr);

        let mut push_stmts = Self::push_str_stmts(url_ident.clone(), &self.path);
        let return_expr = syn::Stmt::Expr(Box::new(parse_expr(quote!(#url_ident.into()))));

        let mut stmts = let_params_exprs;
        stmts.append(&mut let_encoded_params_exprs);
        stmts.push(let_stmt);
        stmts.append(&mut push_stmts);
        stmts.push(return_expr);
        syn::Block { stmts }
    }

    /// Build the AST for a literal path
    fn build_borrowed_literal(self) -> syn::Expr {
        let path: Vec<&'a str> = self
            .path
            .iter()
            .map(|p| match *p {
                PathPart::Literal(p) => p,
                _ => panic!("Only PathPart::Literal is supported by a borrowed url."),
            })
            .collect();

        let path = path.join("");
        let lit = syn::Lit::Str(path, syn::StrStyle::Cooked);
        parse_expr(quote!(#lit.into()))
    }

    /// Get the number of chars in all literal parts for the url.
    fn literal_length_expr(url: &[PathPart<'a>]) -> syn::Expr {
        let len = url
            .iter()
            .filter_map(|p| match *p {
                PathPart::Literal(p) => Some(p),
                _ => None,
            })
            .fold(0, |acc, p| acc + p.len());

        syn::ExprKind::Lit(syn::Lit::Int(len as u64, syn::IntTy::Usize)).into()
    }

    /// Creates the AST for a let expression to percent encode path parts
    fn let_encoded_exprs(url: &[PathPart<'a>], parts: &BTreeMap<String, Type>) -> Vec<syn::Stmt> {
        url.iter()
            .filter_map(|p| match *p {
                PathPart::Param(p) => {
                    let name = valid_name(p);
                    let path_expr = match &parts[p].ty {
                        TypeKind::String => path_none(name).into_expr(),
                        _ => path_none(format!("{}_str", name).as_str()).into_expr(),
                    };

                    let encoded_ident = ident(format!("encoded_{}", name));
                    let percent_encode_call: syn::Expr = syn::ExprKind::Call(
                        Box::new(path_none("percent_encode").into_expr()),
                        vec![
                            syn::ExprKind::MethodCall(ident("as_bytes"), vec![], vec![path_expr])
                                .into(),
                            path_none("PARTS_ENCODED").into_expr(),
                        ],
                    )
                    .into();

                    let into_call: syn::Expr =
                        syn::ExprKind::MethodCall(ident("into"), vec![], vec![percent_encode_call])
                            .into();

                    Some(syn::Stmt::Local(Box::new(syn::Local {
                        pat: Box::new(syn::Pat::Ident(
                            syn::BindingMode::ByValue(syn::Mutability::Immutable),
                            encoded_ident,
                            None,
                        )),
                        ty: Some(Box::new(ty_path("Cow", vec![], vec![ty("str")]))),
                        init: Some(Box::new(into_call)),
                        attrs: vec![],
                    })))
                }
                _ => None,
            })
            .collect()
    }

    /// Creates the AST for a let expression for path parts
    fn let_parameters_exprs(
        url: &[PathPart<'a>],
        parts: &BTreeMap<String, Type>,
    ) -> Vec<syn::Stmt> {
        url.iter()
            .filter_map(|p| match *p {
                PathPart::Param(p) => {
                    let name = valid_name(p);
                    let name_ident = ident(&name);
                    let ty = &parts[p].ty;

                    // don't generate an assignment expression for strings
                    if ty == &TypeKind::String {
                        return None;
                    }

                    let tokens = quote!(#name_ident);
                    // build a different expression, depending on the type of parameter
                    let (ident, init) = match ty {
                        TypeKind::List => {
                            // Join list values together
                            let name_str = format!("{}_str", &name);
                            let name_str_ident = ident(&name_str);
                            let join_call = syn::ExprKind::MethodCall(
                                ident("join"),
                                vec![],
                                vec![
                                    parse_expr(tokens),
                                    syn::ExprKind::Lit(syn::Lit::Str(
                                        ",".into(),
                                        syn::StrStyle::Cooked,
                                    ))
                                    .into(),
                                ],
                            )
                            .into();

                            (name_str_ident, join_call)
                        }
                        _ => {
                            // Handle enums, long, int, etc. by calling to_string()
                            let to_string_call = syn::ExprKind::MethodCall(
                                ident("to_string"),
                                vec![],
                                vec![parse_expr(tokens)],
                            )
                            .into();

                            (ident(format!("{}_str", name)), to_string_call)
                        }
                    };

                    Some(syn::Stmt::Local(Box::new(syn::Local {
                        pat: Box::new(syn::Pat::Ident(
                            syn::BindingMode::ByValue(syn::Mutability::Immutable),
                            ident,
                            None,
                        )),
                        ty: None,
                        init: Some(Box::new(init)),
                        attrs: vec![],
                    })))
                }
                _ => None,
            })
            .collect()
    }

    /// Get an expression to find the number of chars in each parameter part for the url.
    fn parameter_length_exprs(url: &[PathPart<'a>]) -> Vec<syn::Expr> {
        url.iter()
            .filter_map(|p| match *p {
                PathPart::Param(p) => {
                    let name = format!("encoded_{}", valid_name(p));
                    Some(
                        syn::ExprKind::MethodCall(
                            ident("len"),
                            vec![],
                            vec![path_none(name.as_ref()).into_expr()],
                        )
                        .into(),
                    )
                }
                _ => None,
            })
            .collect()
    }

    /// Get an expression that is the binary addition of each of the given expressions.
    fn summed_length_expr(len_exprs: Vec<syn::Expr>) -> syn::Expr {
        match len_exprs.len() {
            1 => len_exprs.into_iter().next().unwrap(),
            _ => {
                let mut len_iter = len_exprs.into_iter();

                let first_expr = Box::new(len_iter.next().unwrap());

                *(len_iter.map(Box::new).fold(first_expr, |acc, p| {
                    Box::new(syn::ExprKind::Binary(syn::BinOp::Add, acc, p).into())
                }))
            }
        }
    }

    /// Get a statement to build a `String` with a capacity of the given expression.
    fn let_p_stmt(url_ident: syn::Ident, len_expr: syn::Expr) -> syn::Stmt {
        let string_with_capacity = syn::ExprKind::Call(
            Box::new(
                syn::ExprKind::Path(None, {
                    let mut method = path_none("String");
                    method
                        .segments
                        .push(syn::PathSegment::from("with_capacity"));
                    method
                })
                .into(),
            ),
            vec![len_expr],
        )
        .into();

        syn::Stmt::Local(Box::new(syn::Local {
            pat: Box::new(syn::Pat::Ident(
                syn::BindingMode::ByValue(syn::Mutability::Mutable),
                url_ident,
                None,
            )),
            ty: None,
            init: Some(Box::new(string_with_capacity)),
            attrs: vec![],
        }))
    }

    /// Get a list of statements that append each part to a `String` in order.
    fn push_str_stmts(url_ident: syn::Ident, url: &[PathPart<'a>]) -> Vec<syn::Stmt> {
        url.iter()
            .map(|p| match *p {
                PathPart::Literal(p) => {
                    let lit = syn::Lit::Str(p.to_string(), syn::StrStyle::Cooked);
                    syn::Stmt::Semi(Box::new(parse_expr(quote!(#url_ident.push_str(#lit)))))
                }
                PathPart::Param(p) => {
                    let name = format!("encoded_{}", valid_name(p));
                    let ident = ident(name);
                    syn::Stmt::Semi(Box::new(parse_expr(
                        quote!(#url_ident.push_str(#ident.as_ref())),
                    )))
                }
            })
            .collect()
    }

    pub fn build(self) -> syn::Expr {
        let has_params = self.path.iter().any(|p| match *p {
            PathPart::Param(_) => true,
            _ => false,
        });

        if has_params {
            self.build_owned().into_expr()
        } else {
            self.build_borrowed_literal()
        }
    }
}

/// Helper for wrapping a value as a quotable expression.
pub trait IntoExpr {
    fn into_expr(self) -> syn::Expr;
}

impl IntoExpr for syn::Path {
    fn into_expr(self) -> syn::Expr {
        syn::ExprKind::Path(None, self).into()
    }
}

impl IntoExpr for syn::Block {
    fn into_expr(self) -> syn::Expr {
        syn::ExprKind::Block(syn::Unsafety::Normal, self).into()
    }
}

impl IntoExpr for syn::Ty {
    fn into_expr(self) -> syn::Expr {
        // TODO: Must be a nicer conversion than this
        let mut tokens = quote::Tokens::new();
        self.to_tokens(&mut tokens);
        parse_expr(tokens)
    }
}

impl IntoExpr for syn::Pat {
    fn into_expr(self) -> syn::Expr {
        // TODO: Must be a nicer conversion than this
        let mut tokens = quote::Tokens::new();
        self.to_tokens(&mut tokens);
        parse_expr(tokens)
    }
}
