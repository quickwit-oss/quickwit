use inflector::Inflector;
use quote::Tokens;
use syn::Ident;

use crate::api_generator::code_gen::*;
use crate::api_generator::*;

pub fn generate(api: &Api) -> anyhow::Result<String> {
    let mut tokens = Tokens::new();
    for (endpoint_name, endpoint) in api.root.endpoints() {
        for path in &endpoint.url.paths {
            for method in &path.methods {
                let endpoint_name = endpoint_name.to_pascal_case();
                let filter_fn_name = ident(extract_filter_fn_name(&path.path, method.to_str()));
                let (warp_path, part_types) = extract_parts(path, Some(endpoint_name.clone()));
                let (warp_method, doc_method) = match method {
                    HttpMethod::Get => (quote!(warp::get()), quote!(get)),
                    HttpMethod::Post => (quote!(warp::post()), quote!(post)),
                    _ => (quote!(), quote!()),
                };

                let doc_path = &path.path.0;
                tokens.append(quote!(
                    #[utoipa::path(
                        #doc_method,
                        tag = #endpoint_name,
                        path = #doc_path,
                    )]
                    pub(crate) fn #filter_fn_name() -> impl Filter<Extract = (#(#part_types,)*), Error = Rejection> + Clone {
                        warp::path!("_elastic" / #(#warp_path)*)
                            .and(#warp_method)
                            .and(serde_qs::warp::query(serde_qs::Config::default()))
                    }
                ))
            }
        }
    }

    let generated = tokens.to_string();
    Ok(generated)
}

fn extract_filter_fn_name(path: &PathString, method: &str) -> String {
    let endpoint_path = path
        .0
        .trim_matches('/')
        .replace("/", "_")
        .replace("{", "")
        .replace("}", "");
    format!("elastic_{}_{}_filter", method, endpoint_path).replace("__", "_")
}

fn extract_parts(path: &Path, endpoint_name_opt: Option<String>) -> (Vec<Tokens>, Vec<Ident>) {
    let mut path_parts = vec![];
    let mut part_types = vec![];

    for part in path.path.0.split("/") {
        if part.is_empty() {
            continue;
        }
        if part.starts_with("{") {
            let part_name = part.trim_matches(|c| c == '{' || c == '}');
            let part_type = path.parts.get(part_name).unwrap();
            let part_type_name = get_rust_type(&part_type.ty);
            let part_ident = ident(part_type_name.to_string());
            path_parts.push(quote!(#part_ident));
            part_types.push(part_ident);
        } else {
            path_parts.push(quote!(#part));
        }
        path_parts.push(quote!( / ));
    }

    if let Some(endpoint_name) = endpoint_name_opt {
        part_types.push(ident(format!("{endpoint_name}QueryParams")));
    }

    // remove trailing slash
    if !path_parts.is_empty() {
        path_parts.remove(path_parts.len() - 1);
    }

    (path_parts, part_types)
}

fn get_rust_type(type_kind: &TypeKind) -> String {
    match type_kind {
        TypeKind::Boolean => "bool".to_string(),
        TypeKind::Number | TypeKind::Float | TypeKind::Double => "f64".to_string(),
        TypeKind::Integer | TypeKind::Long => "i64".to_string(),
        TypeKind::String | TypeKind::Text | TypeKind::Date | TypeKind::Time => "String".to_string(),
        TypeKind::List => "SimpleList".to_string(),
        TypeKind::Union(union_type) => format!(
            "({}, {})",
            get_rust_type(&union_type.0),
            get_rust_type(&union_type.1)
        ),
        TypeKind::Unknown(str) => str.clone(),
        TypeKind::Enum => "".to_string(), // TODO find way to return the enum name.
    }
}
