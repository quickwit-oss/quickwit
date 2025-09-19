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

use std::borrow::Cow;
use std::env;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, bail};
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::de::Error;
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Protocol {
    Actor = 1,
    Azure = 2,
    File = 3,
    Grpc = 4,
    PostgreSQL = 5,
    Ram = 6,
    S3 = 7,
    Google = 8,
}

impl Protocol {
    pub fn as_str(&self) -> &str {
        match &self {
            Protocol::Actor => "actor",
            Protocol::Azure => "azure",
            Protocol::File => "file",
            Protocol::Grpc => "grpc",
            Protocol::PostgreSQL => "postgresql",
            Protocol::Ram => "ram",
            Protocol::S3 => "s3",
            Protocol::Google => "gs",
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(&self, Protocol::File)
    }

    pub fn is_file_storage(&self) -> bool {
        matches!(&self, Protocol::File | Protocol::Ram)
    }

    pub fn is_object_storage(&self) -> bool {
        matches!(&self, Protocol::Azure | Protocol::S3 | Protocol::Google)
    }

    pub fn is_database(&self) -> bool {
        matches!(&self, Protocol::PostgreSQL)
    }
}

impl Display for Protocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}", self.as_str())
    }
}

impl FromStr for Protocol {
    type Err = anyhow::Error;

    fn from_str(protocol: &str) -> anyhow::Result<Self> {
        match protocol {
            "azure" => Ok(Protocol::Azure),
            "file" => Ok(Protocol::File),
            "grpc" => Ok(Protocol::Grpc),
            "actor" => Ok(Protocol::Actor),
            "pg" | "postgres" | "postgresql" => Ok(Protocol::PostgreSQL),
            "ram" => Ok(Protocol::Ram),
            "s3" => Ok(Protocol::S3),
            "gs" => Ok(Protocol::Google),
            _ => bail!("unknown URI protocol `{protocol}`"),
        }
    }
}

const PROTOCOL_SEPARATOR: &str = "://";

/// Encapsulates the URI type.
///
/// URI's string representation are guaranteed to start
/// by the protocol `str()` representation.
///
/// # Disclaimer
///
/// Uri has to be built using `Uri::from_str`.
/// This function has some normalization behavior.
/// Some protocol have several acceptable string representation (`pg`, `postgres`, `postgresql`).
///
/// If the representation in the input string is not canonical, it will get normalized.
/// In other words, a parsed URI may not have the exact string representation as the original
/// string.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Uri {
    uri: String,
    protocol: Protocol,
}

impl Uri {
    /// This is only used for test. We artificially restrict the lifetime to 'static
    /// to avoid misuses.
    pub fn for_test(uri: &'static str) -> Self {
        Uri::from_str(uri).unwrap()
    }

    /// Returns the extension of the URI.
    pub fn extension(&self) -> Option<&str> {
        Path::new(&self.uri).extension()?.to_str()
    }

    /// Returns the URI as a string slice.
    pub fn as_str(&self) -> &str {
        &self.uri
    }

    /// Returns the protocol of the URI.
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// Strips sensitive information such as credentials from URI.
    fn as_redacted_str(&self) -> Cow<'_, str> {
        if self.protocol().is_database() {
            static DATABASE_URI_PATTERN: OnceCell<Regex> = OnceCell::new();
            DATABASE_URI_PATTERN
                .get_or_init(|| {
                    Regex::new("(?P<before>^.*://.*)(?P<password>:.*@)(?P<after>.*)")
                        .expect("regular expression should compile")
                })
                .replace(&self.uri, "$before:***redacted***@$after")
        } else {
            Cow::Borrowed(&self.uri)
        }
    }

    pub fn redact(&mut self) {
        self.uri = self.as_redacted_str().into_owned();
    }

    /// Returns the file path of the URI.
    /// Applies only to `file://` and `ram://` URIs.
    pub fn filepath(&self) -> Option<&Path> {
        if self.protocol().is_file_storage() {
            Some(self.path())
        } else {
            None
        }
    }

    /// Returns the parent URI.
    /// Does not apply to PostgreSQL URIs.
    pub fn parent(&self) -> Option<Uri> {
        if self.protocol().is_database() {
            return None;
        }
        let path = self.path();
        let protocol = self.protocol();

        if protocol == Protocol::S3 && path.components().count() < 2 {
            return None;
        }
        if protocol == Protocol::Azure && path.components().count() < 3 {
            return None;
        }
        if protocol == Protocol::Google && path.components().count() < 2 {
            return None;
        }
        let parent_path = path.parent()?;

        Some(Self {
            uri: format!("{protocol}{PROTOCOL_SEPARATOR}{}", parent_path.display()),
            protocol,
        })
    }

    fn path(&self) -> &Path {
        Path::new(&self.uri[self.protocol.as_str().len() + PROTOCOL_SEPARATOR.len()..])
    }

    /// Returns the last component of the URI.
    pub fn file_name(&self) -> Option<&Path> {
        if self.protocol() == Protocol::PostgreSQL {
            return None;
        }
        let path = self.path();

        if self.protocol() == Protocol::S3 && path.components().count() < 2 {
            return None;
        }
        if self.protocol() == Protocol::Azure && path.components().count() < 3 {
            return None;
        }
        if self.protocol() == Protocol::Google && path.components().count() < 2 {
            return None;
        }
        path.file_name().map(Path::new)
    }

    /// Consumes the [`Uri`] struct and returns the normalized URI as a string.
    pub fn into_string(self) -> String {
        self.uri
    }

    /// Creates a new [`Uri`] with `path` adjoined to `self`.
    /// Fails if `path` is absolute.
    pub fn join<P: AsRef<Path> + std::fmt::Debug>(&self, path: P) -> anyhow::Result<Self> {
        if path.as_ref().is_absolute() {
            bail!(
                "cannot join URI `{}` with absolute path `{:?}`",
                self.uri,
                path
            );
        }
        let joined = match self.protocol() {
            Protocol::File => Path::new(&self.uri)
                .join(path)
                .to_string_lossy()
                .to_string(),
            Protocol::PostgreSQL => bail!(
                "cannot join PostgreSQL URI `{}` with path `{:?}`",
                self.uri,
                path
            ),
            _ => format!(
                "{}{}{}",
                self.uri,
                if self.uri.ends_with('/') { "" } else { "/" },
                path.as_ref().display(),
            ),
        };
        Ok(Self {
            uri: joined,
            protocol: self.protocol,
        })
    }

    /// Attempts to construct a [`Uri`] from a string.
    /// A `file://` protocol is assumed if not specified.
    /// File URIs are resolved (normalized) relative to the current working directory
    /// unless an absolute path is specified.
    /// Handles special characters such as `~`, `.`, `..`.
    fn parse_str(uri_str: &str) -> anyhow::Result<Self> {
        // CAUTION: Do not display the URI in error messages to avoid leaking credentials.
        if uri_str.is_empty() {
            bail!("failed to parse empty URI");
        }
        let (protocol, mut path) = match uri_str.split_once(PROTOCOL_SEPARATOR) {
            None => (Protocol::File, uri_str.to_string()),
            Some((protocol, path)) => (Protocol::from_str(protocol)?, path.to_string()),
        };
        if protocol == Protocol::File {
            if path.starts_with('~') {
                // We only accept `~` (alias to the home directory) and `~/path/to/something`.
                // If there is something following the `~` that is not `/`, we bail.
                if path.len() > 1 && !path.starts_with("~/") {
                    bail!("failed to normalize URI: tilde expansion is only partially supported");
                }

                let home_dir_path = home::home_dir()
                    .context("failed to normalize URI: could not resolve home directory")?
                    .to_string_lossy()
                    .to_string();

                path.replace_range(0..1, &home_dir_path);
            }
            if Path::new(&path).is_relative() {
                let current_dir = env::current_dir().context(
                    "failed to normalize URI: could not resolve current working directory. the \
                     directory does not exist or user has insufficient permissions",
                )?;
                path = current_dir.join(path).to_string_lossy().to_string();
            }
            path = normalize_path(Path::new(&path))
                .to_string_lossy()
                .to_string();
        }
        Ok(Self {
            uri: format!("{protocol}{PROTOCOL_SEPARATOR}{path}"),
            protocol,
        })
    }
}

impl AsRef<str> for Uri {
    fn as_ref(&self) -> &str {
        &self.uri
    }
}

impl Debug for Uri {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("Uri")
            .field("uri", &self.as_redacted_str())
            .finish()
    }
}

impl Display for Uri {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}", self.as_redacted_str())
    }
}

impl FromStr for Uri {
    type Err = anyhow::Error;

    fn from_str(uri_str: &str) -> anyhow::Result<Self> {
        Uri::parse_str(uri_str)
    }
}

impl PartialEq<&str> for Uri {
    fn eq(&self, other: &&str) -> bool {
        &self.uri == other
    }
}

impl PartialEq<String> for Uri {
    fn eq(&self, other: &String) -> bool {
        &self.uri == other
    }
}

impl<'de> Deserialize<'de> for Uri {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let uri_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let uri = Uri::from_str(&uri_str).map_err(D::Error::custom)?;
        Ok(uri)
    }
}

impl Serialize for Uri {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.uri)
    }
}

/// Normalizes a path by resolving the components like (., ..).
/// This helper does the same thing as `Path::canonicalize`.
/// It only differs from `Path::canonicalize` by not checking file existence
/// during resolution.
/// <https://github.com/rust-lang/cargo/blob/fede83ccf973457de319ba6fa0e36ead454d2e20/src/cargo/util/paths.rs#L61>
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut resulting_path_buf =
        if let Some(component @ Component::Prefix(..)) = components.peek().cloned() {
            components.next();
            PathBuf::from(component.as_os_str())
        } else {
            PathBuf::new()
        };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                resulting_path_buf.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                resulting_path_buf.pop();
            }
            Component::Normal(inner_component) => {
                resulting_path_buf.push(inner_component);
            }
        }
    }
    resulting_path_buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_new_uri() {
        Uri::from_str("").unwrap_err();

        let home_dir = home::home_dir().unwrap();
        let current_dir = env::current_dir().unwrap();

        let uri = Uri::from_str("file:///home/foo/bar").unwrap();
        assert_eq!(uri.protocol(), Protocol::File);
        assert_eq!(uri.filepath(), Some(Path::new("/home/foo/bar")));
        assert_eq!(uri, "file:///home/foo/bar");
        assert_eq!(uri, "file:///home/foo/bar".to_string());
        assert_eq!(
            Uri::from_str("file:///foo./bar..").unwrap(),
            "file:///foo./bar.."
        );
        assert_eq!(
            Uri::from_str("home/homer/docs/dognuts").unwrap(),
            format!("file://{}/home/homer/docs/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::from_str("home/homer/docs/../dognuts").unwrap(),
            format!("file://{}/home/homer/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::from_str("home/homer/docs/../../dognuts").unwrap(),
            format!("file://{}/home/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::from_str("/home/homer/docs/dognuts").unwrap(),
            "file:///home/homer/docs/dognuts"
        );
        assert_eq!(
            Uri::from_str("~").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::from_str("~/").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::from_str("~anything/bar").unwrap_err().to_string(),
            "failed to normalize URI: tilde expansion is only partially supported"
        );
        assert_eq!(
            Uri::from_str("~/.").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::from_str("~/..").unwrap(),
            format!("file://{}", home_dir.parent().unwrap().display())
        );
        assert_eq!(
            Uri::from_str("file://").unwrap(),
            format!("file://{}", current_dir.display())
        );
        assert_eq!(Uri::from_str("file:///").unwrap(), "file:///");
        assert_eq!(
            Uri::from_str("file://.").unwrap(),
            format!("file://{}", current_dir.display())
        );
        assert_eq!(
            Uri::from_str("file://..").unwrap(),
            format!("file://{}", current_dir.parent().unwrap().display())
        );
        assert_eq!(
            Uri::from_str("s3://home/homer/docs/dognuts").unwrap(),
            "s3://home/homer/docs/dognuts"
        );
        assert_eq!(
            Uri::from_str("s3://home/homer/docs/../dognuts").unwrap(),
            "s3://home/homer/docs/../dognuts"
        );
        assert_eq!(
            Uri::from_str("azure://account/container/docs/dognuts").unwrap(),
            "azure://account/container/docs/dognuts"
        );
        assert_eq!(
            Uri::from_str("azure://account/container/homer/docs/../dognuts").unwrap(),
            "azure://account/container/homer/docs/../dognuts"
        );
        assert_eq!(
            Uri::from_str("gs://bucket/docs/dognuts").unwrap(),
            "gs://bucket/docs/dognuts"
        );
        assert_eq!(
            Uri::from_str("gs://bucket/homer/docs/../dognuts").unwrap(),
            "gs://bucket/homer/docs/../dognuts"
        );
        assert_eq!(
            Uri::from_str("actor://localhost:7281/an-actor-id").unwrap(),
            "actor://localhost:7281/an-actor-id"
        );

        assert_eq!(
            Uri::from_str("http://localhost:9000/quickwit")
                .unwrap_err()
                .to_string(),
            "unknown URI protocol `http`"
        );
    }

    #[test]
    fn test_uri_protocol() {
        assert_eq!(Uri::for_test("file:///home").protocol(), Protocol::File);
        assert_eq!(Uri::for_test("ram:///in-memory").protocol(), Protocol::Ram);
        assert_eq!(Uri::for_test("s3://bucket/key").protocol(), Protocol::S3);
        assert_eq!(
            Uri::for_test("azure://account/bucket/key").protocol(),
            Protocol::Azure
        );
        assert_eq!(
            Uri::for_test("gs://bucket/key").protocol(),
            Protocol::Google
        );
        assert_eq!(
            Uri::for_test("postgres://localhost:5432/metastore").protocol(),
            Protocol::PostgreSQL
        );
        assert_eq!(
            Uri::for_test("postgresql://localhost:5432/metastore").protocol(),
            Protocol::PostgreSQL
        );
    }

    #[test]
    fn test_uri_extension() {
        assert!(Uri::for_test("s3://").extension().is_none());

        assert_eq!(
            Uri::for_test("s3://config.json").extension().unwrap(),
            "json"
        );
        assert_eq!(
            Uri::for_test("azure://config.foo").extension().unwrap(),
            "foo"
        );
    }

    #[test]
    fn test_uri_join() {
        assert_eq!(
            Uri::for_test("file:///").join("foo").unwrap(),
            "file:///foo"
        );
        assert_eq!(
            Uri::for_test("file:///foo").join("bar").unwrap(),
            "file:///foo/bar"
        );
        assert_eq!(
            Uri::for_test("file:///foo/").join("bar").unwrap(),
            "file:///foo/bar"
        );
        assert_eq!(
            Uri::for_test("ram://foo").join("bar").unwrap(),
            "ram://foo/bar"
        );
        assert_eq!(
            Uri::for_test("s3://bucket/").join("key").unwrap(),
            "s3://bucket/key"
        );
        assert_eq!(
            Uri::for_test("azure://account/container")
                .join("key")
                .unwrap(),
            "azure://account/container/key"
        );
        assert_eq!(
            Uri::for_test("gs://bucket").join("key").unwrap(),
            "gs://bucket/key"
        );
        Uri::for_test("s3://bucket/").join("/key").unwrap_err();
        Uri::for_test("azure://account/container/")
            .join("/key")
            .unwrap_err();
        Uri::for_test("postgres://username:password@localhost:5432/metastore")
            .join("table")
            .unwrap_err();
    }

    #[test]
    fn test_uri_parent() {
        assert!(Uri::for_test("file:///").parent().is_none());
        assert_eq!(Uri::for_test("file:///foo").parent().unwrap(), "file:///");
        assert_eq!(Uri::for_test("file:///foo/").parent().unwrap(), "file:///");
        assert_eq!(
            Uri::for_test("file:///foo/bar").parent().unwrap(),
            "file:///foo"
        );
        assert!(
            Uri::for_test("postgres://localhost:5432/db")
                .parent()
                .is_none()
        );

        assert!(Uri::for_test("ram:///").parent().is_none());
        assert_eq!(Uri::for_test("ram:///foo").parent().unwrap(), "ram:///");
        assert_eq!(Uri::for_test("ram:///foo/").parent().unwrap(), "ram:///");
        assert_eq!(
            Uri::for_test("ram:///foo/bar").parent().unwrap(),
            "ram:///foo"
        );
        assert!(Uri::for_test("s3://bucket").parent().is_none());
        assert!(Uri::for_test("s3://bucket/").parent().is_none());
        assert_eq!(
            Uri::for_test("s3://bucket/foo").parent().unwrap(),
            "s3://bucket"
        );
        assert_eq!(
            Uri::for_test("s3://bucket/foo/").parent().unwrap(),
            "s3://bucket"
        );
        assert_eq!(
            Uri::for_test("s3://bucket/foo/bar").parent().unwrap(),
            "s3://bucket/foo"
        );
        assert_eq!(
            Uri::for_test("s3://bucket/foo/bar/").parent().unwrap(),
            "s3://bucket/foo"
        );
        assert!(Uri::for_test("azure://account/").parent().is_none());
        assert!(Uri::for_test("azure://account").parent().is_none());
        assert!(
            Uri::for_test("azure://account/container/")
                .parent()
                .is_none()
        );
        assert!(
            Uri::for_test("azure://account/container")
                .parent()
                .is_none()
        );
        assert_eq!(
            Uri::for_test("azure://account/container/foo")
                .parent()
                .unwrap(),
            "azure://account/container"
        );
        assert_eq!(
            Uri::for_test("azure://account/container/foo/")
                .parent()
                .unwrap(),
            "azure://account/container"
        );
        assert_eq!(
            Uri::for_test("azure://account/container/foo/bar")
                .parent()
                .unwrap(),
            "azure://account/container/foo"
        );
        assert!(Uri::for_test("gs://bucket").parent().is_none());
        assert!(Uri::for_test("gs://bucket/").parent().is_none());
        assert_eq!(
            Uri::for_test("gs://bucket/foo").parent().unwrap(),
            "gs://bucket"
        );
        assert_eq!(
            Uri::for_test("gs://bucket/foo/").parent().unwrap(),
            "gs://bucket"
        );
        assert_eq!(
            Uri::for_test("gs://bucket/foo/bar").parent().unwrap(),
            "gs://bucket/foo"
        );
        assert_eq!(
            Uri::for_test("gs://bucket/foo/bar/").parent().unwrap(),
            "gs://bucket/foo"
        );
    }

    #[test]
    fn test_uri_file_name() {
        assert!(Uri::for_test("file:///").file_name().is_none());
        assert_eq!(
            Uri::for_test("file:///foo").file_name().unwrap(),
            Path::new("foo")
        );
        assert_eq!(
            Uri::for_test("file:///foo/").file_name().unwrap(),
            Path::new("foo")
        );
        assert!(
            Uri::for_test("postgres://localhost:5432/db")
                .file_name()
                .is_none()
        );

        assert!(Uri::for_test("ram:///").file_name().is_none());
        assert_eq!(
            Uri::for_test("ram:///foo").file_name().unwrap(),
            Path::new("foo")
        );
        assert_eq!(
            Uri::for_test("ram:///foo/").file_name().unwrap(),
            Path::new("foo")
        );
        assert!(Uri::for_test("s3://bucket").file_name().is_none());
        assert!(Uri::for_test("s3://bucket/").file_name().is_none());
        assert_eq!(
            Uri::for_test("s3://bucket/foo").file_name().unwrap(),
            Path::new("foo"),
        );
        assert_eq!(
            Uri::for_test("s3://bucket/foo/").file_name().unwrap(),
            Path::new("foo"),
        );
        assert!(Uri::for_test("azure://account").file_name().is_none());
        assert!(Uri::for_test("azure://account/").file_name().is_none());
        assert!(
            Uri::for_test("azure://account/container")
                .file_name()
                .is_none()
        );
        assert!(
            Uri::for_test("azure://account/container/")
                .file_name()
                .is_none()
        );
        assert_eq!(
            Uri::for_test("azure://account/container/foo")
                .file_name()
                .unwrap(),
            Path::new("foo"),
        );
        assert_eq!(
            Uri::for_test("azure://account/container/foo/")
                .file_name()
                .unwrap(),
            Path::new("foo"),
        );
        assert!(Uri::for_test("gs://bucket").file_name().is_none());
        assert!(Uri::for_test("gs://bucket/").file_name().is_none());
        assert_eq!(
            Uri::for_test("gs://bucket/foo").file_name().unwrap(),
            Path::new("foo"),
        );
        assert_eq!(
            Uri::for_test("gs://bucket/foo/").file_name().unwrap(),
            Path::new("foo"),
        );
    }

    #[test]
    fn test_uri_filepath() {
        assert_eq!(
            Uri::for_test("file:///").filepath().unwrap(),
            Path::new("/")
        );
        assert_eq!(
            Uri::for_test("file:///foo").filepath().unwrap(),
            Path::new("/foo")
        );
        assert_eq!(Uri::for_test("ram:///").filepath().unwrap(), Path::new("/"));
        assert_eq!(
            Uri::for_test("ram:///foo").filepath().unwrap(),
            Path::new("/foo")
        );
        assert!(Uri::for_test("s3://bucket/").filepath().is_none());
        assert!(
            Uri::for_test("azure://account/container/")
                .filepath()
                .is_none()
        );
        assert!(
            Uri::for_test("azure://account/container/foo.json")
                .filepath()
                .is_none()
        );
        assert!(Uri::for_test("gs://bucket/").filepath().is_none());
    }

    #[test]
    fn test_uri_as_redacted_str() {
        assert_eq!(
            Uri::for_test("s3://bucket/key").as_redacted_str(),
            "s3://bucket/key"
        );
        assert_eq!(
            Uri::for_test("azure://account/container/key").as_redacted_str(),
            "azure://account/container/key"
        );
        assert_eq!(
            Uri::for_test("gs://bucket/key").as_redacted_str(),
            "gs://bucket/key"
        );
        assert_eq!(
            Uri::for_test("postgres://localhost:5432/metastore").as_redacted_str(),
            "postgresql://localhost:5432/metastore"
        );
        assert_eq!(
            Uri::for_test("pg://username@localhost:5432/metastore").as_redacted_str(),
            "postgresql://username@localhost:5432/metastore"
        );
        {
            for protocol in ["postgres", "postgresql"] {
                let uri = Uri::from_str(&format!(
                    "{protocol}://username:password@localhost:5432/metastore"
                ))
                .unwrap();
                let expected_uri =
                    "postgresql://username:***redacted***@localhost:5432/metastore".to_string();
                assert_eq!(uri.as_redacted_str(), expected_uri);
                assert_eq!(format!("{uri}"), expected_uri);
                assert_eq!(
                    format!("{uri:?}"),
                    format!("Uri {{ uri: \"{expected_uri}\" }}")
                );
            }
        }
    }

    #[test]
    fn test_uri_serialize() {
        let uri = Uri::for_test("s3://bucket/key");
        assert_eq!(
            serde_json::to_value(uri).unwrap(),
            serde_json::Value::String("s3://bucket/key".to_string())
        );
    }
}
