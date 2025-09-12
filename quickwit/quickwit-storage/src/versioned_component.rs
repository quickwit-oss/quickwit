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

use std::io::Read;

use anyhow::Context;
use tantivy::directory::OwnedBytes;

/// Helper trait for versioning.
///
/// See the unit test for an example.
pub trait VersionedComponent: Default + Copy + Clone {
    /// This number is used to identify that the type.
    const MAGIC_NUMBER: u32;
    /// Type of the component we are versioning.
    type Component;
    /// Component name, used to make explicit error messages.
    fn component_name() -> &'static str {
        std::any::type_name::<Self::Component>()
    }
    /// Return the version code.
    fn to_version_code(self) -> u32;

    /// Serialize the header.
    /// Only the current version is meant to be serialized.
    fn header() -> [u8; 8] {
        let mut header = [0u8; 8];
        header[0..4].copy_from_slice(&Self::MAGIC_NUMBER.to_le_bytes());
        header[4..8].copy_from_slice(&Self::default().to_version_code().to_le_bytes());
        header
    }

    /// Deserialize the header from a `Read` trait.
    /// This method will check for the magic number and version code.
    fn try_read_component(bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component> {
        let version = try_read_version::<Self>(bytes)?;
        version.deserialize_impl(bytes)
    }

    /// Parse the version code.
    /// This version is meant to be implemented but only to be called
    /// from `try_deserialize_from_bytes`.
    ///
    /// If the version is unknown, this method should return `None`.
    fn try_from_version_code_impl(version_code: u32) -> Option<Self>;

    /// Function to serialize a given component with the current codec.
    fn serialize(component: &Self::Component) -> Vec<u8> {
        let mut output = Vec::with_capacity(8);
        output.extend_from_slice(&Self::header());
        Self::serialize_impl(component, &mut output);
        output
    }

    /// Serialize the component using the current format.
    ///
    /// This function should NOT serialize the header.
    /// It should only append content to the `output` buffer.
    ///
    /// This function is meant to be implemented but should not be called directly.
    /// Instead, client should use `.serialize(..)`.
    fn serialize_impl(component: &Self::Component, output: &mut Vec<u8>);

    /// This method is meant to be implemented but not called, except by `try_read_component`.
    ///
    /// This method should consume the bytes from the `OwnedBytes`.
    fn deserialize_impl(&self, bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component>;
}

/// Deserialize the header from a `Read` trait.
///
/// (This function is not part of the trait to make it private.)
fn try_read_version<V: VersionedComponent>(bytes: &mut OwnedBytes) -> anyhow::Result<V> {
    let mut header_bytes: [u8; 8] = [0u8; 8];
    bytes
        .read_exact(&mut header_bytes[..])
        .with_context(|| format!("failed to read header for {}", V::component_name()))?;
    try_deserialize_from_bytes::<V>(header_bytes)
}

/// Deserialize the header from 8 bytes.
/// An error is returned if the magic number does not match,
/// or if the version is unsupported.
///
/// (This function is not part of the trait to make it private.)
fn try_deserialize_from_bytes<V: VersionedComponent>(header_bytes: [u8; 8]) -> anyhow::Result<V> {
    let magic_number = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
    if magic_number != V::MAGIC_NUMBER {
        anyhow::bail!("hot directory metadata's magic number does not match");
    }
    let version_code: u32 = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());
    V::try_from_version_code_impl(version_code).with_context(|| {
        format!(
            "version code {} is not supported for {}",
            version_code,
            V::component_name()
        )
    })
}

#[cfg(test)]
mod tests {
    use tantivy::directory::OwnedBytes;

    use crate::VersionedComponent;

    #[derive(Copy, Clone, Default)]
    #[repr(u32)]
    enum FakeComponentCodec {
        V1,
        #[default]
        V2 = 2,
    }

    #[derive(Debug)]
    struct FakeComponent {
        value: u32,
    }

    impl VersionedComponent for FakeComponentCodec {
        const MAGIC_NUMBER: u32 = 332_221_734u32;

        type Component = FakeComponent;

        fn to_version_code(self) -> u32 {
            self as u32
        }

        fn try_from_version_code_impl(version_code: u32) -> Option<Self> {
            match version_code {
                1u32 => Some(Self::V1),
                2u32 => Some(Self::V2),
                _ => None,
            }
        }

        fn serialize_impl(component: &Self::Component, output: &mut Vec<u8>) {
            output.extend_from_slice(&component.value.to_le_bytes());
        }

        fn deserialize_impl(&self, bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component> {
            match self {
                FakeComponentCodec::V1 => {
                    if bytes.len() < 8 {
                        anyhow::bail!("not enough bytes to deserialize");
                    }
                    let value_bytes: [u8; 8] = bytes[0..8].try_into().unwrap();
                    let value: u32 = u64::from_le_bytes(value_bytes) as u32;
                    Ok(FakeComponent { value })
                }
                FakeComponentCodec::V2 => {
                    if bytes.len() < 4 {
                        anyhow::bail!("not enough bytes to deserialize");
                    }
                    let value_bytes: [u8; 4] = bytes[0..4].try_into().unwrap();
                    bytes.advance(4);
                    let value: u32 = u32::from_le_bytes(value_bytes);
                    Ok(FakeComponent { value })
                }
            }
        }
    }

    #[test]
    fn test_versioned_component() {
        let component = FakeComponent { value: 42 };
        let buf = FakeComponentCodec::serialize(&component);
        {
            let mut payload = OwnedBytes::new(buf.clone());
            let fake_component = FakeComponentCodec::try_read_component(&mut payload).unwrap();
            assert_eq!(fake_component.value, 42u32);
        }
        {
            let mut buf_clone = buf.clone();
            buf_clone[0] = 0u8;
            let mut payload = OwnedBytes::new(buf_clone);
            let fake_component_err =
                FakeComponentCodec::try_read_component(&mut payload).unwrap_err();
            assert!(
                fake_component_err
                    .to_string()
                    .to_lowercase()
                    .contains("magic number")
            );
        }
        {
            let mut buf_clone = buf;
            buf_clone.truncate(4);
            buf_clone.extend_from_slice(&1u32.to_le_bytes());
            buf_clone.extend_from_slice(&32u64.to_le_bytes());
            let mut payload = OwnedBytes::new(buf_clone);
            let fake_component = FakeComponentCodec::try_read_component(&mut payload).unwrap();
            assert_eq!(fake_component.value, 32u32);
        }
    }
}
