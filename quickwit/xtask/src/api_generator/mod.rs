// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;

#[cfg(test)]
use quote::ToTokens;
use quote::Tokens;
use semver::Version;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use void::Void;

use crate::api_generator::code_gen::url::url_builder::PathString;
use crate::{
    ES_ARTIFACTS_DIR_NAME, ES_STACK_VERSION, GENERATED_FILE_NAME, ROOT_DIR, SELECTED_SPEC_FILES,
};

pub mod code_gen;
pub mod output;

use std::cmp::Ordering;

use itertools::Itertools;
use output::{merge_file, write_file};

lazy_static! {
    static ref VERSION: Version = semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
}

/// Record of generated files
#[derive(Deserialize, Serialize, Default)]
pub struct GeneratedFiles {
    pub written: BTreeSet<String>,
    pub merged: BTreeSet<String>,
}

/// Location of the record of generated files in the `src` directory.
pub const GENERATED_TOML: &str = ".generated.toml";

/// A complete API specification parsed from the REST API specs
#[derive(Debug)]
pub struct Api {
    /// parameters that are common to all API methods
    pub common_params: BTreeMap<String, Type>,
    /// root API methods e.g. Search, Index
    pub root: ApiNamespace,
    /// namespace client methods e.g. Indices.Create, Ml.PutJob
    pub namespaces: BTreeMap<String, ApiNamespace>,
    /// enums in parameters
    pub enums: Vec<ApiEnum>,
}

impl Api {
    /// Find the right ApiEndpoint from the REST API specs for the API call
    /// defined in the YAML test.
    ///
    /// The REST API specs model only the stable APIs
    /// currently, so no endpoint will be found for experimental or beta APIs
    pub fn endpoint_for_api_call(&self, api_call: &str) -> Option<&ApiEndpoint> {
        let api_call_path: Vec<&str> = api_call.split('.').collect();
        match api_call_path.len() {
            1 => self.root.endpoints().get(api_call_path[0]),
            _ => match self.namespaces.get(api_call_path[0]) {
                Some(namespace) => namespace.endpoints().get(api_call_path[1]),
                None => None,
            },
        }
    }
}

/// A HTTP method in the REST API spec
#[derive(Debug, Eq, PartialEq, Deserialize, Clone, Copy, Ord, PartialOrd)]
pub enum HttpMethod {
    #[serde(rename = "HEAD")]
    Head,
    #[serde(rename = "GET")]
    Get,
    #[serde(rename = "POST")]
    Post,
    #[serde(rename = "PUT")]
    Put,
    #[serde(rename = "PATCH")]
    Patch,
    #[serde(rename = "DELETE")]
    Delete,
}

/// Converts a `HttpMethod` in the REST spec, into the AST for
/// a `Method` in the elasticsearch client
impl quote::ToTokens for HttpMethod {
    fn to_tokens(&self, tokens: &mut quote::Tokens) {
        tokens.append("Method");
        tokens.append("::");
        match *self {
            HttpMethod::Head => tokens.append("Head"),
            HttpMethod::Get => tokens.append("Get"),
            HttpMethod::Post => tokens.append("Post"),
            HttpMethod::Put => tokens.append("Put"),
            HttpMethod::Patch => tokens.append("Patch"),
            HttpMethod::Delete => tokens.append("Delete"),
        }
    }
}

/// A type defined in the REST API spec
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Type {
    #[serde(rename = "type", default)]
    pub ty: TypeKind,
    pub description: Option<String>,
    #[serde(default = "Vec::new")]
    pub options: Vec<Value>,
    #[serde(default)]
    pub default: Option<Value>,
}

/// The type of the param or part
#[derive(Debug, PartialEq, Clone)]
pub enum TypeKind {
    Unknown(String),
    List,
    Enum,
    String,
    Text,
    Boolean,
    Number,
    Float,
    Double,
    Integer,
    Long,
    Date,
    Time,
    Union(Box<(TypeKind, TypeKind)>),
}

impl<'de> Deserialize<'de> for TypeKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        Ok(TypeKind::from(value.as_str()))
    }
}

impl From<&str> for TypeKind {
    fn from(s: &str) -> Self {
        match s {
            "list" => TypeKind::List,
            "enum" => TypeKind::Enum,
            "string" => TypeKind::String,
            "text" => TypeKind::Text,
            "boolean" => TypeKind::Boolean,
            "number" => TypeKind::Number,
            "float" => TypeKind::Float,
            "double" => TypeKind::Double,
            "int" => TypeKind::Integer,
            "long" => TypeKind::Long,
            "date" => TypeKind::Date,
            "time" => TypeKind::Time,
            n => {
                let values: Vec<&str> = n.split('|').collect();
                if values.len() != 2 {
                    TypeKind::Unknown(n.to_string())
                } else {
                    let union = Box::new((TypeKind::from(values[0]), TypeKind::from(values[1])));
                    TypeKind::Union(union)
                }
            }
        }
    }
}

impl Default for TypeKind {
    fn default() -> Self {
        TypeKind::Unknown("".to_string())
    }
}

/// Details about a deprecated API url path
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Deprecated {
    pub version: String,
    pub description: String,
}

impl PartialOrd for Deprecated {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (
            Version::parse(&self.version),
            Version::parse(&other.version),
        ) {
            (Err(_), _) => None,
            (_, Err(_)) => None,
            (Ok(self_version), Ok(other_version)) => self_version.partial_cmp(&other_version),
        }
    }
}

impl Deprecated {
    /// Combine optional deprecations, keeping either lack of deprecation or the highest version
    pub fn combine<'a>(
        left: &'a Option<Deprecated>,
        right: &'a Option<Deprecated>,
    ) -> &'a Option<Deprecated> {
        if let (Some(leftd), Some(rightd)) = (left, right) {
            if leftd > rightd {
                left
            } else {
                right
            }
        } else {
            &None
        }
    }
}

/// An API url path
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Path {
    pub path: PathString,
    pub methods: Vec<HttpMethod>,
    #[serde(default = "BTreeMap::new")]
    pub parts: BTreeMap<String, Type>,
    pub deprecated: Option<Deprecated>,
}

/// The URL components of an API endpoint
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Url {
    pub paths: Vec<Path>,
}

/// Body of an API endpoint
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Body {
    pub description: Option<String>,
    pub required: Option<bool>,
    pub serialize: Option<String>,
}

/// Wraps the URL string to replace master or current in URL path with the
/// major.minor version of the api_generator.
fn documentation_url_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    Ok(DocumentationUrlString::replace_version_in_url(s))
}

/// A Documentation URL string
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct DocumentationUrlString(
    #[serde(deserialize_with = "documentation_url_string")] pub String,
);

impl DocumentationUrlString {
    fn from_url(s: String) -> Self {
        let s = Self::replace_version_in_url(s);
        Self(s)
    }

    fn replace_version_in_url(s: String) -> String {
        match url::Url::parse(&s) {
            Ok(u) => {
                let mut u = u;
                if u.path().contains("/master") {
                    u.set_path(
                        u.path()
                            .replace(
                                "/master",
                                format!("/{}.{}", VERSION.major, VERSION.minor).as_str(),
                            )
                            .as_str(),
                    );
                } else if u.path().contains("/current") {
                    u.set_path(
                        u.path()
                            .replace(
                                "/current",
                                format!("/{}.{}", VERSION.major, VERSION.minor).as_str(),
                            )
                            .as_str(),
                    );
                }
                u.into()
            }
            Err(_) => s,
        }
    }
}

impl core::ops::Deref for DocumentationUrlString {
    type Target = String;

    fn deref(self: &'_ Self) -> &'_ Self::Target {
        &self.0
    }
}

impl fmt::Display for DocumentationUrlString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Documentation for an API endpoint
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Documentation {
    pub url: Option<DocumentationUrlString>,
    pub description: Option<String>,
}

impl FromStr for Documentation {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Documentation {
            url: Some(DocumentationUrlString::from_url(s.to_owned())),
            description: None,
        })
    }
}

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Void>,
    D: Deserializer<'de>,
{
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where T: Deserialize<'de> + FromStr<Err = Void>
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where E: serde::de::Error {
            Ok(FromStr::from_str(value).unwrap())
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where M: MapAccess<'de> {
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

/// Stability level of an API endpoint. Ordering defines increasing stability level, i.e.
/// `beta` is "more stable" than `experimental`.
#[derive(Debug, Eq, PartialEq, Deserialize, Clone, Copy, Ord, PartialOrd)]
pub enum Stability {
    #[serde(rename = "experimental")]
    Experimental,
    #[serde(rename = "beta")]
    Beta,
    #[serde(rename = "stable")]
    Stable,
}

impl Stability {
    pub fn feature_name(self) -> Option<&'static str> {
        match self {
            Stability::Experimental => Some("experimental-apis"),
            Stability::Beta => Some("beta-apis"),
            Stability::Stable => None,
        }
    }

    /// Returns the (optional) feature configuration for this stability level as an outer
    /// attribute, for use e.g. on function definitions.
    pub fn outer_cfg_attr(self) -> Option<Tokens> {
        let feature_name = self.feature_name();
        feature_name.map(|name| quote!(#[cfg(feature = #name)]))
    }

    /// Returns the (optional) feature configuration for this stability level as an inner
    /// attribute, for use e.g. at the top of a module source file
    pub fn inner_cfg_attr(self) -> Option<Tokens> {
        let feature_name = self.feature_name();
        feature_name.map(|name| quote!(#![cfg(feature = #name)]))
    }
}

/// An API endpoint defined in the REST API specs
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct ApiEndpoint {
    pub full_name: Option<String>,
    #[serde(deserialize_with = "string_or_struct")]
    documentation: Documentation,
    pub stability: Stability,
    pub url: Url,
    pub deprecated: Option<Deprecated>,
    #[serde(default = "BTreeMap::new")]
    pub params: BTreeMap<String, Type>,
    pub body: Option<Body>,
}

impl ApiEndpoint {
    /// Whether the endpoint supports sending a body
    pub fn supports_body(&self) -> bool {
        self.body.is_some()
            || self.url.paths.iter().any(|p| {
                p.methods.contains(&HttpMethod::Post) || p.methods.contains(&HttpMethod::Put)
            })
    }

    /// Whether the endpoint supports sending a newline delimited body
    pub fn supports_nd_body(&self) -> bool {
        self.supports_body()
            && match &self.body {
                Some(b) => match &b.serialize {
                    Some(s) => s == "bulk",
                    _ => false,
                },
                None => false,
            }
    }
}

#[derive(Debug)]
pub struct ApiNamespace {
    stability: Stability,
    endpoints: BTreeMap<String, ApiEndpoint>,
}

impl ApiNamespace {
    pub fn new() -> Self {
        ApiNamespace {
            stability: Stability::Experimental, // will grow in stability as we add endpoints
            endpoints: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, name: String, endpoint: ApiEndpoint) {
        // Stability of a namespace is that of the most stable of its endpoints
        self.stability = Stability::max(self.stability, endpoint.stability);
        self.endpoints.insert(name, endpoint);
    }

    pub fn stability(&self) -> Stability {
        self.stability
    }

    pub fn endpoints(&self) -> &BTreeMap<String, ApiEndpoint> {
        &self.endpoints
    }
}

/// Common parameters accepted by all API endpoints
#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct Common {
    description: Option<String>,
    #[serde(deserialize_with = "string_or_struct")]
    documentation: Documentation,
    params: BTreeMap<String, Type>,
}

/// An enum defined in the REST API specs
#[derive(Debug)]
pub struct ApiEnum {
    pub name: String,
    pub description: Option<String>,
    pub values: Vec<String>,
    pub stability: Stability, // inherited from the declaring API
}

impl Hash for ApiEnum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for ApiEnum {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for ApiEnum {}

pub fn generate_api() -> anyhow::Result<()> {
    let download_dir = ROOT_DIR
        .join(ES_ARTIFACTS_DIR_NAME)
        .join(&*ES_STACK_VERSION)
        .join("rest-api-spec")
        .join("api");

    let target_file = ROOT_DIR
        .join("quickwit-serve")
        .join("src")
        .join("elastic_search_api")
        .join(GENERATED_FILE_NAME);

    if target_file.exists() {
        let _ = fs::remove_file(&target_file).expect(&format!(
            "Error removing existing target file: {:?}.",
            target_file
        ));
    }

    // TODO
    // think of versioning

    // read the Api from files
    let api = read_api(&download_dir, SELECTED_SPEC_FILES)?;
    // println!("Generating at: {:?}", api);

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&target_file)?;
    let mut writer = BufWriter::new(file);

    // Endpoints Params types
    writer
        .write(code_gen::root::generate(&api)?.as_bytes())
        .unwrap();

    // Helper Types
    writer
        .write(code_gen::params::generate(&api)?.as_bytes())
        .unwrap();

    //  merge_file(
    //      |section| sections.remove(section),
    //      generated_dir.as_path(),
    //      GENERATED_FILE_NAME,
    //      &mut tracker,
    //  )?;

    Ok(())
}

/// Reads Api from a directory of REST Api specs
pub fn read_api(download_dir: &PathBuf, files: &[&str]) -> anyhow::Result<Api> {
    let paths = fs::read_dir(download_dir)?;
    let mut namespaces = BTreeMap::<String, ApiNamespace>::new();
    let mut enums: HashSet<ApiEnum> = HashSet::new();
    let mut common_params = BTreeMap::new();
    let root_key = "root";

    for path in paths {
        let path = path?.path();
        let name = path.file_name().map(|path| path.to_str());
        let display = path.to_string_lossy().into_owned();

        if name
            .unwrap()
            .map(|name| name.ends_with(".json") && !name.starts_with('_') && files.contains(&name))
            .unwrap_or(true)
        {
            let mut file = File::open(&path)?;
            let (name, api_endpoint) = endpoint_from_file(display, &mut file)?;

            if api_endpoint.stability != Stability::Stable && api_endpoint.deprecated.is_some() {
                // Do not generate deprecated unstable endpoints
                continue;
            }

            let name_parts: Vec<&str> = name.splitn(2, '.').collect();
            let (namespace, method_name) = match name_parts.len() {
                len if len > 1 => (name_parts[0].to_string(), name_parts[1].to_string()),
                _ => (root_key.to_string(), name),
            };

            // collect unique enum values
            for param in api_endpoint
                .params
                .iter()
                .filter(|p| p.1.ty == TypeKind::Enum)
            {
                let options: Vec<String> = param
                    .1
                    .options
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();

                enums.insert(ApiEnum {
                    name: param.0.to_string(),
                    description: param.1.description.clone(),
                    values: options,
                    stability: api_endpoint.stability,
                });
            }

            // collect api endpoints into namespaces
            if !namespaces.contains_key(&namespace) {
                let mut api_namespace = ApiNamespace::new();
                api_namespace.add(method_name, api_endpoint);
                namespaces.insert(namespace.to_string(), api_namespace);
            } else {
                namespaces
                    .get_mut(&namespace)
                    .unwrap()
                    .add(method_name, api_endpoint);
            }
        } else if name
            .map(|name| name == Some("_common.json"))
            .unwrap_or(true)
        {
            let mut file = File::open(&path)?;
            let common = common_params_from_file(display, &mut file)?;
            common_params = common.params;
        }
    }

    // extract the root methods
    let root = namespaces.remove(root_key).unwrap();

    let mut sorted_enums = enums.into_iter().collect::<Vec<_>>();
    sorted_enums.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Api {
        common_params,
        root,
        namespaces,
        enums: sorted_enums,
    })
}

/// deserializes an ApiEndpoint from a file
fn endpoint_from_file<R>(name: String, reader: &mut R) -> anyhow::Result<(String, ApiEndpoint)>
where R: Read {
    // deserialize the map from the reader
    let endpoints: BTreeMap<String, ApiEndpoint> =
        serde_json::from_reader(reader).map_err(|e| super::error::ParseError {
            message: format!("Failed to parse {} because: {}", name, e),
        })?;

    // get the first (and only) endpoint name and endpoint body
    let (name, mut endpoint) = endpoints.into_iter().next().unwrap();
    endpoint.full_name = Some(name.clone());

    // sort the HTTP methods so that we can easily pattern match on them later
    for path in endpoint.url.paths.iter_mut() {
        path.methods.sort();
    }

    // endpoint deprecation is the "least deprecated" of its paths
    let deprecation = endpoint
        .url
        .paths
        .iter()
        .map(|p| &p.deprecated)
        .fold1(|d1, d2| Deprecated::combine(d1, d2))
        .unwrap_or(&None);

    if let Some(deprecated) = deprecation {
        endpoint.deprecated = Some(Deprecated {
            version: deprecated.version.clone(),
            description: "Deprecated via one of the child items".to_string(),
        })
    }

    Ok((name, endpoint))
}

/// deserializes Common from a file
fn common_params_from_file<R>(name: String, reader: &mut R) -> anyhow::Result<Common>
where R: Read {
    let common: Common = serde_json::from_reader(reader).map_err(|e| super::error::ParseError {
        message: format!("Failed to parse {} because: {}", name, e),
    })?;

    Ok(common)
}

/// Asserts that the expected generated AST matches the actual generated AST
#[cfg(test)]
pub fn ast_eq<T: ToTokens>(expected: Tokens, actual: T) {
    assert_eq!(expected, quote!(#actual));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stability_ordering() {
        assert!(Stability::Beta > Stability::Experimental);
        assert!(Stability::Stable > Stability::Beta);
    }

    #[test]
    fn combine_deprecations() {
        let d1 = Some(Deprecated {
            version: "7.5.0".to_string(),
            description: "foo".to_string(),
        });

        let d2 = Some(Deprecated {
            version: "7.6.0".to_string(),
            description: "foo".to_string(),
        });

        assert_eq!(&d2, Deprecated::combine(&d1, &d2));
        assert_eq!(&None, Deprecated::combine(&d1, &None));
    }
}
