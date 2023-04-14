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

use heck::ToSnakeCase;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse2, Data, DeriveInput, Index};

pub fn derive_prometheus_labels_impl(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, .. } = parse2(input).unwrap();

    let snake_case_ident = ident
        .to_string()
        .trim_end_matches("Request")
        .to_snake_case();

    let struct_label = quote! {
        std::borrow::Cow::Borrowed(#snake_case_ident)
    };

    let Data::Struct(data_struct) = data else {
        panic!("`PrometheusLabels` can only be derived for structs.")
    };
    let mut field_labels = Vec::new();

    for (i, field) in data_struct.fields.iter().enumerate() {
        if field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("prometheus_label"))
        {
            let field_label = if let Some(field_ident) = &field.ident {
                quote! {
                    std::borrow::Cow::Owned(self.#field_ident.to_string())
                }
            } else {
                let field_index = Index::from(i);
                quote! {
                    std::borrow::Cow::Owned(self.#field_index.to_string())
                }
            };
            field_labels.push(field_label);
        }
    }

    let num_labels = field_labels.len() + 1;

    let codegen = quote! {
        impl PrometheusLabels<#num_labels> for #ident {
            fn labels(&self) -> OwnedPrometheusLabels<#num_labels> {
                OwnedPrometheusLabels::new([#struct_label, #( #field_labels ),*])
            }
        }
    };
    codegen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_struct() {
        let unit_struct = quote! {
            struct MyUnitRequest;
        };
        let codegen = derive_prometheus_labels_impl(unit_struct);

        let expected_codegen = quote! {
            impl PrometheusLabels<1usize> for MyUnitRequest {
                fn labels(&self) -> OwnedPrometheusLabels<1usize> {
                    OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("my_unit"),])
                }
            }
        };
        assert_eq!(codegen.to_string(), expected_codegen.to_string());
    }

    #[test]
    fn test_unamed_fields_struct() {
        let unamed_fields_struct = quote! {
            struct MyUnamedFieldsRequest(#[prometheus_label] String, String);
        };
        let codegen = derive_prometheus_labels_impl(unamed_fields_struct);

        let expected_codegen = quote! {
            impl PrometheusLabels<2usize> for MyUnamedFieldsRequest {
                fn labels(&self) -> OwnedPrometheusLabels<2usize> {
                    OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("my_unamed_fields"), std::borrow::Cow::Owned(self.0.to_string())])
                }
            }
        };
        assert_eq!(codegen.to_string(), expected_codegen.to_string());
    }

    #[test]
    fn test_named_fields_struct() {
        let named_fields_struct = quote! {
            struct MyNamedFieldsRequest {
                #[prometheus_label]
                foo: String,
                bar: String,
            }
        };
        let codegen = derive_prometheus_labels_impl(named_fields_struct);

        let expected_codegen = quote! {
            impl PrometheusLabels<2usize> for MyNamedFieldsRequest {
                fn labels(&self) -> OwnedPrometheusLabels<2usize> {
                    OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("my_named_fields"), std::borrow::Cow::Owned(self.foo.to_string())])
                }
            }
        };
        assert_eq!(codegen.to_string(), expected_codegen.to_string());
    }
}
