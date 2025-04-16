// Copyright 2021-Present Datadog, Inc.
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

use std::mem;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Error, Field, Fields, FieldsNamed, Ident, ItemStruct, Meta, Path, Token, Visibility,
    parenthesized,
};

#[proc_macro_attribute]
pub fn serde_multikey(attr: TokenStream, item: TokenStream) -> TokenStream {
    match serde_multikey_inner(attr, item) {
        Ok(ts) => ts,
        Err(e) => e.to_compile_error().into(),
    }
}

fn serde_multikey_inner(_attr: TokenStream, item: TokenStream) -> Result<TokenStream, Error> {
    let Ok(input) = syn::parse::<ItemStruct>(item) else {
        return Err(Error::new(
            Span::call_site(),
            "the attribute can only be applied to struct",
        ));
    };

    let main_struct = generate_main_struct(input.clone())?;

    let proxy_struct = generate_proxy_struct(input)?;

    Ok(quote!(
    #main_struct
    #proxy_struct
    )
    .into())
}

/// Generate the main struct. It's a copy of the original struct, but with most
/// ser/de attributes removed, and serde try_from/into `__MultiKey{}` added.
fn generate_main_struct(mut input: ItemStruct) -> Result<TokenStream2, Error> {
    let (serialize, deserialize) = get_ser_de(&input.attrs)?;
    let has_utoipa_schema = get_and_remove_utoipa_schema(&mut input.attrs)?;

    if !deserialize && !serialize {
        return Err(Error::new(
            Span::call_site(),
            "`serde_multikey` was applied to a non Serialize/Deserialize struct",
        ));
    }

    // remove serde and utoipa attributes from fields
    for field in input.fields.iter_mut() {
        let attrs = mem::take(&mut field.attrs);
        field.attrs = attrs
            .into_iter()
            .filter(|attr| {
                !(attr.path().is_ident("serde_multikey")
                    || attr.path().is_ident("serde")
                    || attr.path().is_ident("serde_as")
                    || attr.path().is_ident("schema"))
            })
            .collect();
    }

    // remove serde attributes from struct
    let attrs = mem::take(&mut input.attrs);
    input.attrs = attrs
        .into_iter()
        .filter(|attr| !(attr.path().is_ident("serde") || attr.path().is_ident("serde_as")))
        .collect();

    if deserialize {
        let mut attr = Attribute::parse_outer
            .parse_str(&format!(
                r#"#[serde(try_from = "__MultiKey{}")]"#,
                input.ident
            ))
            .unwrap();
        input.attrs.append(&mut attr);
    }

    if serialize {
        let mut attr = Attribute::parse_outer
            .parse_str(&format!(r#"#[serde(into = "__MultiKey{}")]"#, input.ident))
            .unwrap();
        input.attrs.append(&mut attr);
    }

    let utoipa = if has_utoipa_schema {
        let main_ident = input.ident.clone();
        let main_ident_str = main_ident.to_string();
        let proxy_ident = Ident::new(&format!("__MultiKey{}", input.ident), input.ident.span());

        Some(quote!(
            impl<'__s> utoipa::ToSchema<'__s> for #main_ident {
                fn schema() -> (
                    &'__s str,
                    utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
                ) {
                    (
                        #main_ident_str,
                        <#proxy_ident as utoipa::ToSchema>::schema().1,
                    )
                }
            }
        ))
    } else {
        None
    };

    Ok(quote!(
        #input

        #utoipa
    ))
}

/// Generate the proxy struct. It is a copy of the original struct, but fields marked
/// with `serde_multikey` have been replaced with the fields the correspond to.
/// Also generate TryFrom/Into as required.
fn generate_proxy_struct(mut input: ItemStruct) -> Result<TokenStream2, Error> {
    let main_ident = input.ident.clone();
    let proxy_ident = Ident::new(&format!("__MultiKey{}", input.ident), input.ident.span());

    input.ident = proxy_ident.clone();
    input.vis = Visibility::Inherited;
    // TODO wait for https://github.com/juhaku/utoipa/issues/704 to re-enable
    // input.attrs.append(&mut Attribute::parse_outer
    // .parse_str(&"#[doc(hidden)]")
    // .unwrap());

    let (ser, de) = get_ser_de(&input.attrs)?;

    let mut pass_through = Vec::<Ident>::new();
    let mut final_fields = Punctuated::<Field, Token![,]>::new();
    let mut try_from_conv = Vec::<TokenStream2>::new();
    let mut into_pre_conv = Vec::<TokenStream2>::new();
    let mut into_in_conv = Vec::<TokenStream2>::new();

    let Fields::Named(FieldsNamed { brace_token, named }) = input.fields else {
        return Err(Error::new(
            Span::call_site(),
            "`serde_multikey` was applied to a tuple-struct or an empty struct",
        ));
    };
    for pair in named.into_pairs() {
        let (mut field, ponct) = pair.into_tuple();
        // we are in a "normal" struct, not a tuple-struct, unwrap is fine.
        let field_name = field.ident.clone().unwrap();

        let (field_config, attrs) = parse_attributes(field.attrs, &field_name)?;
        field.attrs = attrs;

        if let Some(field_config) = field_config {
            let value = Ident::new("value", Span::call_site());
            for field in &field_config.proxy_fields {
                final_fields.push(field.clone());
            }
            match (ser, field_config.get_into(&value)) {
                (true, Some((pre_conv, in_conv))) => {
                    into_pre_conv.push(pre_conv);
                    into_in_conv.push(in_conv);
                }
                (false, None) => (),
                (true, None) => {
                    return Err(Error::new(
                        field_name.span(),
                        "structure implement serialize but no serializer defined",
                    ));
                }
                (false, Some(_)) => {
                    return Err(Error::new(
                        field_name.span(),
                        "structure doesn't implement serialize but a serializer is defined",
                    ));
                }
            }
            match (de, field_config.get_try_from(&value)) {
                (true, Some(conv)) => {
                    try_from_conv.push(conv);
                }
                (false, None) => (),
                (true, None) => {
                    return Err(Error::new(
                        field_name.span(),
                        "structure implement deserialize but no deserializer defined",
                    ));
                }
                (false, Some(_)) => {
                    return Err(Error::new(
                        field_name.span(),
                        "structure doesn't implement deserialize but a deserializer is defined",
                    ));
                }
            }
        } else {
            pass_through.push(field_name);
            final_fields.push(field);
            if let Some(ponct) = ponct {
                final_fields.push_punct(ponct);
            }
        }
    }
    input.fields = Fields::Named(FieldsNamed {
        brace_token,
        named: final_fields,
    });

    let into = if ser {
        Some(quote!(
            impl From<#main_ident> for #proxy_ident {
                fn from(value: #main_ident) -> #proxy_ident {
                    #(#into_pre_conv)*
                    #proxy_ident {
                        #(#pass_through: value.#pass_through,)*
                        #(#into_in_conv)*
                    }
                }
            }
        ))
    } else {
        None
    };
    let try_from = if de {
        Some(quote!(
            impl TryFrom<#proxy_ident> for #main_ident {
                type Error = String;

                fn try_from(value: #proxy_ident) -> Result<Self, Self::Error> {
                    Ok(#main_ident {
                        #(#pass_through: value.#pass_through,)*
                        #(#try_from_conv)*
                    })
                }
            }
        ))
    } else {
        None
    };
    Ok(quote!(
        #input

        #into
        #try_from
    ))
}

fn get_ser_de(attributes: &[Attribute]) -> Result<(bool, bool), Error> {
    let mut ser = false;
    let mut de = false;

    for attr in attributes {
        if !attr.path().is_ident("derive") {
            continue;
        }
        let Meta::List(ref derives) = attr.meta else {
            continue;
        };
        let derives =
            Punctuated::<Path, Token![,]>::parse_terminated.parse2(derives.tokens.clone())?;

        for path in derives.iter() {
            ser |= path_equiv(path, &["serde", "Serialize"]);
            de |= path_equiv(path, &["serde", "Deserialize"]);
        }
    }
    Ok((ser, de))
}

fn get_and_remove_utoipa_schema(attributes: &mut [Attribute]) -> Result<bool, Error> {
    let mut has_schema = false;
    for attr in attributes {
        if !attr.path().is_ident("derive") {
            continue;
        }
        let Meta::List(ref mut derives) = attr.meta else {
            continue;
        };

        let derive_list =
            Punctuated::<Path, Token![,]>::parse_terminated.parse2(derives.tokens.clone())?;
        let mut new_derives = Punctuated::<Path, Token![,]>::new();
        for path in derive_list {
            if path_equiv(&path, &["utoipa", "ToSchema"]) {
                has_schema = true;
            } else {
                new_derives.push(path);
            }
        }
        derives.tokens = quote!(#new_derives);
    }

    Ok(has_schema)
}

fn path_equiv(path: &Path, reference: &[&str]) -> bool {
    if path.segments.is_empty() || reference.is_empty() {
        return false;
    }

    path.segments
        .iter()
        .rev()
        .zip(reference.iter().rev())
        .fold(true, |equal, (path_part, ref_part)| {
            equal && path_part.ident == ref_part
        })
}

#[derive(Debug)]
struct MultiKeyOptions {
    main_field_name: Ident,
    deserializer: Option<Path>,
    serializer: Option<Path>,
    proxy_fields: Vec<Field>,
}

impl MultiKeyOptions {
    fn get_into(&self, this: &Ident) -> Option<(TokenStream2, TokenStream2)> {
        if let Some(ref serializer) = self.serializer {
            let field_names: Vec<_> = self
                .proxy_fields
                .iter()
                .map(|field| field.ident.clone().unwrap())
                .collect();
            let main_field_name = &self.main_field_name;

            let pre = quote!(
                let (#(#field_names,)*) = #serializer(#this.#main_field_name);
            );
            let in_struct = quote!(
                #(
                    #field_names,
                )*
            );
            Some((pre, in_struct))
        } else {
            None
        }
    }

    fn get_try_from(&self, this: &Ident) -> Option<TokenStream2> {
        if let Some(ref deserializer) = self.deserializer {
            let field_names: Vec<_> = self
                .proxy_fields
                .iter()
                .map(|field| field.ident.clone().unwrap())
                .collect();
            let main_field_name = &self.main_field_name;

            Some(quote!(
                #main_field_name: match #deserializer( #(#this.#field_names,)* ) {
                    Ok(val) => val,
                    Err(e) => return Err(e.to_string()),
                },
            ))
        } else {
            None
        }
    }
}

enum MultiKeyOption {
    Deserializer(Path),
    Serializer(Path),
    Fields(Vec<Field>),
}

impl Parse for MultiKeyOption {
    fn parse(input: ParseStream) -> Result<Self, Error> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "serializer" => {
                input.parse::<Token![=]>()?;
                Ok(MultiKeyOption::Serializer(input.parse::<Path>()?))
            }
            "deserializer" => {
                input.parse::<Token![=]>()?;
                Ok(MultiKeyOption::Deserializer(input.parse::<Path>()?))
            }
            "fields" => {
                input.parse::<Token![=]>()?;
                let content;
                parenthesized!(content in input);
                let fields = content.parse_terminated(Field::parse_named, Token![,])?;
                Ok(MultiKeyOption::Fields(fields.into_iter().collect()))
            }
            _ => Err(Error::new(ident.span(), "unknown field")),
        }
    }
}

impl Parse for MultiKeyOptions {
    fn parse(input: ParseStream) -> Result<Self, Error> {
        let mut res = MultiKeyOptions {
            main_field_name: Ident::new("tmp_name", Span::call_site()),
            deserializer: None,
            serializer: None,
            proxy_fields: Vec::new(),
        };

        let options = Punctuated::<MultiKeyOption, Token![,]>::parse_terminated(input)?;
        for option in options {
            match option {
                MultiKeyOption::Deserializer(path) => {
                    if res.deserializer.is_none() {
                        res.deserializer = Some(path);
                    } else {
                        todo!("throw error");
                    }
                }
                MultiKeyOption::Serializer(path) => {
                    if res.serializer.is_none() {
                        res.serializer = Some(path);
                    } else {
                        todo!("throw error");
                    }
                }
                MultiKeyOption::Fields(fields) => {
                    if res.proxy_fields.is_empty() {
                        res.proxy_fields = fields;
                    } else {
                        todo!("throw error");
                    }
                }
            }
        }

        if res.proxy_fields.is_empty() {
            todo!("throw error")
        }

        Ok(res)
    }
}

fn parse_attributes(
    attributes: Vec<Attribute>,
    field_name: &Ident,
) -> Result<(Option<MultiKeyOptions>, Vec<Attribute>), Error> {
    let (mut multikey_attributes, normal_attributes): (Vec<_>, _) = attributes
        .into_iter()
        .partition(|attr| attr.path().is_ident("serde_multikey"));

    if multikey_attributes.len() > 1 {
        let last = multikey_attributes.last().unwrap();
        return Err(Error::new(
            last.pound_token.spans[0],
            "`serde_multikey` was applied multiple time to the same field",
        ));
    }
    let options = if let Some(multikey_attribute) = multikey_attributes.pop() {
        let Meta::List(meta_list) = multikey_attribute.meta else {
            return Err(Error::new(
                multikey_attribute.pound_token.spans[0],
                "`serde_multikey` require list-style arguments",
            ));
        };
        let mut options: MultiKeyOptions = syn::parse2(meta_list.tokens)?;
        options.main_field_name = field_name.clone();
        Some(options)
    } else {
        None
    };

    Ok((options, normal_attributes))
}
