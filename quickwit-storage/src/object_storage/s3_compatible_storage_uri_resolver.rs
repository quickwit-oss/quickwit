/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use ec2_instance_metadata::{InstanceMetadata, InstanceMetadataClient};
pub use rusoto_core::Region;
use std::{str::FromStr, sync::Arc};

use crate::{S3CompatibleObjectStorage, StorageFactory};

/// S3 Object storage Uri Resolver
///
/// The default implementation uses s3 as a protocol, and detects the region trying to us
/// sequencially `AWS_DEFAULT_REGION` environment variable, `AWS_REGION` environment variable,
/// region from ec2 instance metadata and lastly to default value `Region::UsEast1`.
pub struct S3CompatibleObjectStorageFactory {
    region: Region,
    protocol: &'static str,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3CompatibleObjetStorageFactory with the given AWS region.
    pub fn new(region: Region, protocol: &'static str) -> Self {
        S3CompatibleObjectStorageFactory { region, protocol }
    }
}

fn region_from_env_variable() -> Option<Region> {
    let region_str_from_env = std::env::var("AWS_DEFAULT_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .ok()?;
    Region::from_str(&region_str_from_env).ok()
}

// Sniffes the EC2 region from the EC2 instance API.
//
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
fn region_from_ec2_instance() -> Option<Region> {
    let instance_metadata_client: InstanceMetadataClient =
        ec2_instance_metadata::InstanceMetadataClient::new();
    let instance_metadata: InstanceMetadata = instance_metadata_client.get().ok()?;
    Region::from_str(instance_metadata.region).ok()
}

/// Sniff default region using different means in sequence to identify the one that should be used.
/// - `AWS_DEFAULT_REGION` environment variable
/// - `AWS_REGION` environment variable
/// - region from ec2 instance metadata
/// - fallback to us-east-1
fn sniff_default_region() -> Region {
    region_from_env_variable()
        .or_else(region_from_ec2_instance)
        .unwrap_or_default()
}

impl Default for S3CompatibleObjectStorageFactory {
    fn default() -> Self {
        S3CompatibleObjectStorageFactory::new(sniff_default_region(), "s3")
    }
}

impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn protocol(&self) -> String {
        self.protocol.to_string()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<std::sync::Arc<dyn crate::Storage>> {
        let storage = S3CompatibleObjectStorage::from_uri(self.region.clone(), uri)?;
        Ok(Arc::new(storage))
    }
}
