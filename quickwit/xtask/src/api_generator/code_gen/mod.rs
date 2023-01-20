pub mod namespace_clients;
pub mod params;
pub mod request;
pub mod root;
pub mod url;
pub mod warp;

use std::str;

use inflector::Inflector;
use quote::Tokens;

use crate::api_generator::{Stability, TypeKind};

/// use declarations common across builders
pub fn use_declarations() -> Tokens {
    quote!(
        #![allow(unused_imports)]

        use crate::{
            client::Elasticsearch,
            params::*,
            error::Error,
            http::{
                headers::{HeaderName, HeaderMap, HeaderValue, CONTENT_TYPE, ACCEPT},
                Method,
                request::{Body, NdBody, JsonBody, PARTS_ENCODED},
                response::Response,
                transport::Transport,
            },
        };
        use std::{
            borrow::Cow,
            time::Duration
        };
        use percent_encoding::percent_encode;
        use serde::{Serialize, Deserialize };
    )
}

/// AST for a string literal
fn lit<I: Into<String>>(lit: I) -> syn::Lit {
    syn::Lit::Str(lit.into(), syn::StrStyle::Cooked)
}

/// AST for an identifier
fn ident<I: AsRef<str>>(name: I) -> syn::Ident {
    syn::Ident::from(name.as_ref())
}

/// AST for document attribute
fn doc<I: Into<String>>(comment: I) -> syn::Attribute {
    syn::Attribute {
        style: syn::AttrStyle::Outer,
        value: syn::MetaItem::NameValue(ident("doc".to_string()), lit(comment)),
        is_sugared_doc: true,
    }
}

fn stability_doc(stability: Stability) -> Option<syn::Attribute> {
    match stability {
        Stability::Experimental => Some(doc(r#"&nbsp;
# Optional, experimental
This requires the `experimental-apis` feature. Can have breaking changes in future
versions or might even be removed entirely.
        "#)),
        Stability::Beta => Some(doc(r#"&nbsp;
# Optional, beta
This requires the `beta-apis` feature. On track to become stable but breaking changes can
happen in minor versions.
        "#)),
        Stability::Stable => None,
    }
}

/// AST for an expression parsed from quoted tokens
pub fn parse_expr(input: quote::Tokens) -> syn::Expr {
    syn::parse_expr(input.to_string().as_ref()).unwrap()
}

/// Ensures that the name generated is one that is valid for Rust
pub fn valid_name(s: &str) -> &str {
    match s {
        "type" => "ty",
        s => s,
    }
}

/// AST for a path variable.
fn path(path: &str, lifetimes: Vec<syn::Lifetime>, types: Vec<syn::Ty>) -> syn::Path {
    path_segments(vec![(path, lifetimes, types)])
}

/// AST for a simple path variable.
fn path_none(path_ident: &str) -> syn::Path {
    path(path_ident, vec![], vec![])
}

/// AST for a path variable.
fn path_segments(paths: Vec<(&str, Vec<syn::Lifetime>, Vec<syn::Ty>)>) -> syn::Path {
    syn::Path {
        global: false,
        segments: paths
            .into_iter()
            .map(|(path, lifetimes, types)| syn::PathSegment {
                ident: syn::Ident::new(valid_name(path)),
                parameters: syn::PathParameters::AngleBracketed(syn::AngleBracketedParameterData {
                    lifetimes,
                    types,
                    bindings: vec![],
                }),
            })
            .collect(),
    }
}

pub trait GetPath {
    fn get_path(&self) -> &syn::Path;
}

impl GetPath for syn::Ty {
    fn get_path(&self) -> &syn::Path {
        match *self {
            syn::Ty::Path(_, ref p) => &p,
            ref p => panic!("Expected syn::Ty::Path, but found {:?}", p),
        }
    }
}

impl GetPath for syn::Path {
    fn get_path(&self) -> &syn::Path {
        &self
    }
}

pub trait GetIdent {
    fn get_ident(&self) -> &syn::Ident;
}

impl<T: GetPath> GetIdent for T {
    fn get_ident(&self) -> &syn::Ident {
        &self.get_path().segments[0].ident
    }
}

/// Gets the Ty syntax token for a TypeKind
/// TODO: This function is serving too many purposes. Refactor it
fn typekind_to_ty(name: &str, kind: &TypeKind, required: bool, fn_arg: bool) -> syn::Ty {
    let mut v = String::new();
    if !required {
        v.push_str("Option<");
    }

    let str_type = "String";
    match kind {
        TypeKind::Unknown(_) => v.push_str(str_type),
        TypeKind::List => {
            v.push_str("Vec<");
            v.push_str(str_type);
            v.push_str(">");
        }
        TypeKind::Enum => match name {
            // opened https://github.com/elastic/elasticsearch/issues/53212
            // to discuss whether this really should be a collection
            "expand_wildcards" => {
                // Expand wildcards should
                v.push_str("Vec<");
                v.push_str(name.to_pascal_case().as_str());
                v.push_str(">");
            }
            _ => v.push_str(name.to_pascal_case().as_str()),
        },
        TypeKind::String => v.push_str(str_type),
        TypeKind::Text => v.push_str(str_type),
        TypeKind::Boolean => match name {
            // keep until https://github.com/elastic/elasticsearch/pull/57329 is merged
            "track_total_hits" => {
                if fn_arg {
                    v.push_str(format!("Into<{}>", name.to_pascal_case()).as_str())
                } else {
                    v.push_str(name.to_pascal_case().as_str())
                }
            }
            _ => v.push_str("bool"),
        },
        TypeKind::Number => v.push_str("i64"),
        TypeKind::Float => v.push_str("f32"),
        TypeKind::Double => v.push_str("f64"),
        TypeKind::Integer => v.push_str("i32"),
        TypeKind::Long => v.push_str("i64"),
        TypeKind::Date => v.push_str(str_type),
        TypeKind::Time => v.push_str(str_type),
        TypeKind::Union(u) => match name {
            "slices" => v.push_str("Slices"),
            "track_total_hits" => {
                if fn_arg {
                    v.push_str(format!("Into<{}>", name.to_pascal_case()).as_str())
                } else {
                    v.push_str(name.to_pascal_case().as_str())
                }
            }
            _ => panic!("unsupported union type: {:?} for {}", u, name),
        },
    };

    if !required {
        v.push_str(">");
    }

    syn::parse_type(v.as_str()).unwrap()
}

/// A standard `'b` lifetime
pub fn lifetime_b() -> syn::Lifetime {
    syn::Lifetime {
        ident: syn::Ident::new("'b"),
    }
}

pub trait HasLifetime {
    fn has_lifetime(&self) -> bool;
}

impl<T: GetPath> HasLifetime for T {
    fn has_lifetime(&self) -> bool {
        match self.get_path().segments[0].parameters {
            syn::PathParameters::AngleBracketed(ref params) => !params.lifetimes.is_empty(),
            _ => false,
        }
    }
}

/// Generics with a standard `'b` lifetime
pub fn generics_b() -> syn::Generics {
    generics(vec![lifetime_b()], vec![])
}

/// Generics with no parameters.
pub fn generics_none() -> syn::Generics {
    generics(vec![], vec![])
}

/// Generics with the given lifetimes and type bounds.
pub fn generics(lifetimes: Vec<syn::Lifetime>, types: Vec<syn::TyParam>) -> syn::Generics {
    syn::Generics {
        lifetimes: lifetimes
            .into_iter()
            .map(|l| syn::LifetimeDef {
                attrs: vec![],
                lifetime: l,
                bounds: vec![],
            })
            .collect(),
        ty_params: types,
        where_clause: syn::WhereClause::none(),
    }
}

/// AST for a path type with lifetimes and type parameters.
pub fn ty_path(ty: &str, lifetimes: Vec<syn::Lifetime>, types: Vec<syn::Ty>) -> syn::Ty {
    syn::Ty::Path(None, path(ty, lifetimes, types))
}

/// AST for a path type with a `'b` lifetime.
pub fn ty_b(ty: &str) -> syn::Ty {
    ty_path(ty, vec![lifetime_b()], vec![])
}

/// AST for a simple path type.
pub fn ty(ty: &str) -> syn::Ty {
    ty_path(ty, vec![], vec![])
}

/// Helper for wrapping a value as a quotable statement.
pub trait IntoStmt {
    fn into_stmt(self) -> syn::Stmt;
}

impl IntoStmt for syn::Item {
    fn into_stmt(self) -> syn::Stmt {
        syn::Stmt::Item(Box::new(self))
    }
}

impl IntoStmt for syn::Expr {
    fn into_stmt(self) -> syn::Stmt {
        syn::Stmt::Expr(Box::new(self))
    }
}

pub fn take_while<F>(i: &[u8], f: F) -> (&[u8], &str)
where F: Fn(u8) -> bool {
    let mut ctr = 0;

    for c in i {
        if f(*c) {
            ctr += 1;
        } else {
            break;
        }
    }

    (&i[ctr..], str::from_utf8(&i[0..ctr]).unwrap())
}

pub fn shift(i: &[u8], c: usize) -> &[u8] {
    match c {
        c if c >= i.len() => &[],
        _ => &i[c..],
    }
}

pub fn split_on_pascal_case(s: &str) -> String {
    s.chars()
        .enumerate()
        .flat_map(|(i, c)| {
            if i != 0 && c.is_uppercase() {
                Some(' ')
            } else {
                None
            }
            .into_iter()
            .chain(std::iter::once(c))
        })
        .collect()
}
