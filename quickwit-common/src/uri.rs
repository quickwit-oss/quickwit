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

use std::borrow::Cow;
use std::env;
use std::ffi::OsStr;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Context};
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Protocol {
    File,
    PostgreSQL,
    Ram,
    S3,
    Azure,
}

impl Protocol {
    pub fn as_str(&self) -> &str {
        match &self {
            Protocol::File => "file",
            Protocol::PostgreSQL => "postgresql",
            Protocol::Ram => "ram",
            Protocol::S3 => "s3",
            Protocol::Azure => "azure",
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(&self, Protocol::File)
    }

    pub fn is_postgresql(&self) -> bool {
        matches!(&self, Protocol::PostgreSQL)
    }

    pub fn is_ram(&self) -> bool {
        matches!(&self, Protocol::Ram)
    }

    pub fn is_s3(&self) -> bool {
        matches!(&self, Protocol::S3)
    }

    pub fn is_azure(&self) -> bool {
        matches!(&self, Protocol::Azure)
    }

    pub fn is_file_storage(&self) -> bool {
        matches!(&self, Protocol::File | Protocol::Ram)
    }

    pub fn is_object_storage(&self) -> bool {
        matches!(&self, Protocol::S3 | Protocol::Azure)
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
            "file" => Ok(Protocol::File),
            "postgres" | "postgresql" => Ok(Protocol::PostgreSQL),
            "ram" => Ok(Protocol::Ram),
            "s3" => Ok(Protocol::S3),
            "azure" => Ok(Protocol::Azure),
            _ => bail!("Unknown URI protocol `{}`.", protocol),
        }
    }
}

const PROTOCOL_SEPARATOR: &str = "://";

#[derive(Debug, PartialEq)]
pub enum Extension {
    Json,
    Toml,
    Unknown(String),
    Yaml,
}

impl Extension {
    fn maybe_new(extension: &str) -> Option<Self> {
        match extension {
            "json" => Some(Self::Json),
            "toml" => Some(Self::Toml),
            "yaml" | "yml" => Some(Self::Yaml),
            "" => None,
            unknown => Some(Self::Unknown(unknown.to_string())),
        }
    }
}

/// Encapsulates the URI type.
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Uri {
    uri: String,
    protocol_idx: usize,
}

impl Uri {
    /// Attempts to construct a [`Uri`] from a raw string slice.
    /// A `file://` protocol is assumed if not specified.
    /// File URIs are resolved (normalized) relative to the current working directory
    /// unless an absolute path is specified.
    /// Handles special characters like `~`, `.`, `..`.
    pub fn try_new(uri: &str) -> anyhow::Result<Self> {
        if uri.is_empty() {
            bail!("URI is empty.");
        }
        let (protocol, mut path) = match uri.split_once(PROTOCOL_SEPARATOR) {
            None => (Protocol::File.as_str(), uri.to_string()),
            Some((protocol, path)) => (protocol, path.to_string()),
        };
        if protocol == Protocol::File.as_str() {
            if path.starts_with('~') {
                // We only accept `~` (alias to the home directory) and `~/path/to/something`.
                // If there is something following the `~` that is not `/`, we bail out.
                if path.len() > 1 && !path.starts_with("~/") {
                    bail!("Path syntax `{}` is not supported.", uri);
                }

                let home_dir_path = home::home_dir()
                    .context("Failed to resolve home directory.")?
                    .to_string_lossy()
                    .to_string();

                path.replace_range(0..1, &home_dir_path);
            }
            if Path::new(&path).is_relative() {
                let current_dir = env::current_dir().context(
                    "Failed to resolve current working directory: dir does not exist or \
                     insufficient permissions.",
                )?;
                path = current_dir.join(path).to_string_lossy().to_string();
            }
            path = normalize_path(Path::new(&path))
                .to_string_lossy()
                .to_string();
        }
        Ok(Self {
            uri: format!("{}{}{}", protocol, PROTOCOL_SEPARATOR, path),
            protocol_idx: protocol.len(),
        })
    }

    /// Constructs a [`Uri`] from a properly formatted string `<protocol>://<path>` where `path` is
    /// normalized. Use this method exclusively for trusted input.
    pub fn new(uri: String) -> Self {
        let protocol_idx = uri
            .find(PROTOCOL_SEPARATOR)
            .expect("URI lacks protocol separator. Use `Uri::new` exclusively for trusted input.");
        let protocol_str = &uri[..protocol_idx];
        protocol_str
            .parse::<Protocol>()
            .expect("URI protocol is invalid. Use `Uri::new` exclusively for trusted input.`");
        Self { uri, protocol_idx }
    }

    #[cfg(test)]
    fn for_test(uri: &str) -> Self {
        Uri::new(uri.to_string())
    }

    /// Returns the extension of the URI.
    pub fn extension(&self) -> Option<Extension> {
        Path::new(&self.uri)
            .extension()
            .and_then(OsStr::to_str)
            .and_then(Extension::maybe_new)
    }

    /// Returns the URI as a string slice.
    pub fn as_str(&self) -> &str {
        &self.uri
    }

    /// Returns the protocol of the URI.
    pub fn protocol(&self) -> Protocol {
        Protocol::from_str(&self.uri[..self.protocol_idx]).expect("Failed to parse URI protocol. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
    }

    /// Strips sensitive information such as credentials from URI.
    pub fn as_redacted_str(&self) -> Cow<str> {
        if self.protocol().is_database() {
            static DATABASE_URI_PATTERN: OnceCell<Regex> = OnceCell::new();
            DATABASE_URI_PATTERN
                .get_or_init(||
                    Regex::new("(?P<before>^.*://.*)(?P<password>:.*@)(?P<after>.*)")
                        .expect("Failed to compile regular expression. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
                )
                .replace(&self.uri, "$before:***redacted***@$after")
        } else {
            Cow::Borrowed(&self.uri)
        }
    }

    /// Returns the file path of the URI.
    /// Applies only to `file://` URIs.
    pub fn filepath(&self) -> Option<&Path> {
        if self.protocol().is_file_storage() {
            Some(Path::new(
                &self.uri[self.protocol_idx + PROTOCOL_SEPARATOR.len()..],
            ))
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
        let protocol = &self.uri[..self.protocol_idx];
        let path = Path::new(&self.uri[self.protocol_idx + PROTOCOL_SEPARATOR.len()..]);
        if self.protocol().is_s3() && path.components().count() < 2 {
            return None;
        }
        if self.protocol().is_azure() && path.components().count() < 3 {
            return None;
        }
        path.parent().map(|parent| {
            Uri::new(format!(
                "{protocol}{PROTOCOL_SEPARATOR}{}",
                parent.display()
            ))
        })
    }

    /// Returns the last component of the URI.
    pub fn file_name(&self) -> Option<&Path> {
        if self.protocol().is_postgresql() {
            return None;
        }
        let path = Path::new(&self.uri[self.protocol_idx + PROTOCOL_SEPARATOR.len()..]);
        if self.protocol().is_s3() && path.components().count() < 2 {
            return None;
        }
        if self.protocol().is_azure() && path.components().count() < 3 {
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
                "Cannot join URI `{}` with absolute path `{:?}`.",
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
                "Cannot join PostgreSQL URI `{}` with path `{:?}`.",
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
            protocol_idx: self.protocol_idx,
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
        Uri::try_new("").unwrap_err();

        let home_dir = home::home_dir().unwrap();
        let current_dir = env::current_dir().unwrap();

        let uri = Uri::try_new("file:///home/foo/bar").unwrap();
        assert_eq!(uri.protocol(), Protocol::File);
        assert_eq!(uri.filepath(), Some(Path::new("/home/foo/bar")));
        assert_eq!(uri, "file:///home/foo/bar");
        assert_eq!(uri, "file:///home/foo/bar".to_string());
        assert_eq!(
            Uri::try_new("file:///foo./bar..").unwrap(),
            "file:///foo./bar.."
        );
        assert_eq!(
            Uri::try_new("home/homer/docs/dognuts").unwrap(),
            format!("file://{}/home/homer/docs/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::try_new("home/homer/docs/../dognuts").unwrap(),
            format!("file://{}/home/homer/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::try_new("home/homer/docs/../../dognuts").unwrap(),
            format!("file://{}/home/dognuts", current_dir.display())
        );
        assert_eq!(
            Uri::try_new("/home/homer/docs/dognuts").unwrap(),
            "file:///home/homer/docs/dognuts"
        );
        assert_eq!(
            Uri::try_new("~").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::try_new("~/").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::try_new("~anything/bar").unwrap_err().to_string(),
            "Path syntax `~anything/bar` is not supported."
        );
        assert_eq!(
            Uri::try_new("~/.").unwrap(),
            format!("file://{}", home_dir.display())
        );
        assert_eq!(
            Uri::try_new("~/..").unwrap(),
            format!("file://{}", home_dir.parent().unwrap().display())
        );
        assert_eq!(
            Uri::try_new("file://").unwrap(),
            format!("file://{}", current_dir.display())
        );
        assert_eq!(Uri::try_new("file:///").unwrap(), "file:///");
        assert_eq!(
            Uri::try_new("file://.").unwrap(),
            format!("file://{}", current_dir.display())
        );
        assert_eq!(
            Uri::try_new("file://..").unwrap(),
            format!("file://{}", current_dir.parent().unwrap().display())
        );
        assert_eq!(
            Uri::try_new("s3://home/homer/docs/dognuts").unwrap(),
            "s3://home/homer/docs/dognuts"
        );
        assert_eq!(
            Uri::try_new("s3://home/homer/docs/../dognuts").unwrap(),
            "s3://home/homer/docs/../dognuts"
        );
        assert_eq!(
            Uri::try_new("azure://account/container/docs/dognuts").unwrap(),
            "azure://account/container/docs/dognuts"
        );
        assert_eq!(
            Uri::try_new("azure://account/container/homer/docs/../dognuts").unwrap(),
            "azure://account/container/homer/docs/../dognuts"
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
            Extension::Json
        );
        assert_eq!(
            Uri::for_test("azure://config.foo").extension().unwrap(),
            Extension::Unknown("foo".to_string())
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
        assert!(Uri::for_test("postgres://localhost:5432/db")
            .parent()
            .is_none());

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
        assert!(Uri::for_test("azure://account/container/")
            .parent()
            .is_none());
        assert!(Uri::for_test("azure://account/container")
            .parent()
            .is_none());
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
        assert!(Uri::for_test("postgres://localhost:5432/db")
            .file_name()
            .is_none());

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
        assert!(Uri::for_test("azure://account/container")
            .file_name()
            .is_none());
        assert!(Uri::for_test("azure://account/container/")
            .file_name()
            .is_none());
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
        assert!(Uri::for_test("azure://account/container/")
            .filepath()
            .is_none());
        assert!(Uri::for_test("azure://account/container/foo.json")
            .filepath()
            .is_none());
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
            Uri::for_test("postgres://localhost:5432/metastore").as_redacted_str(),
            "postgres://localhost:5432/metastore"
        );
        assert_eq!(
            Uri::for_test("postgres://username@localhost:5432/metastore").as_redacted_str(),
            "postgres://username@localhost:5432/metastore"
        );
        {
            for protocol in ["postgres", "postgresql"] {
                let uri = Uri::new(format!(
                    "{}://username:password@localhost:5432/metastore",
                    protocol
                ));
                let expected_uri = format!(
                    "{}://username:***redacted***@localhost:5432/metastore",
                    protocol
                );
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
            serde_json::to_value(&uri).unwrap(),
            serde_json::Value::String("s3://bucket/key".to_string())
        );
    }
}
