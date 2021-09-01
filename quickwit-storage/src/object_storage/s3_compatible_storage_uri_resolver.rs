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
use once_cell::sync::OnceCell;
use quickwit_common::{get_quickwit_env, QuickwitEnv};
pub use rusoto_core::Region;
use std::str::FromStr;
use std::sync::Arc;

use crate::{S3CompatibleObjectStorage, StorageFactory};

/// The region provider lazily returns a region.
///
/// The "lazy" part was introduced following #478.
/// Indeed, sniffing the Amazon S3 region that should be used
/// requires performing a request on a URI with a timeout of 2s.
///
/// This call was making everything use of quickwit slower.
/// By using this region provider, we only perform this call if
/// the s3 protocol is actually used.
#[derive(Debug)]
pub enum RegionProvider {
    /// Quickwit will try to sniff the right region for Amazon S3.
    ///
    /// It will try the following method in sequence to identify the one that should be used,
    /// and stop as soon as a region is found.
    /// - `AWS_DEFAULT_REGION` environment variable
    /// - `AWS_REGION` environment variable
    /// - region from ec2 instance metadata
    /// - default to us-east-1
    S3,
    /// Target a local localstack instance, mocking Amazon S3.
    /// This is mostly useful for integration tests.
    ///
    /// If the QUICKWIT_ENV environment variable is set to LOCAL,
    /// quickwit will try to connect to `localhost:4566`
    /// otherwise, it will try to connect to `localstack:4566`.
    Localstack,
}

/// Returns a localstack region (used for testing).
fn localstack_region() -> Region {
    let endpoint = if get_quickwit_env() == QuickwitEnv::LOCAL {
        "http://localhost:4566".to_string()
    } else {
        "http://localstack:4566".to_string()
    };
    Region::Custom {
        name: "localstack".to_string(),
        endpoint,
    }
}

fn sniff_s3_default_region() -> Region {
    static CACHED_S3_DEFAULT_REGION: OnceCell<Region> = OnceCell::new();
    CACHED_S3_DEFAULT_REGION
        .get_or_init(|| {
            region_from_env_variable()
                .or_else(region_from_ec2_instance)
                .unwrap_or_default()
        })
        .clone()
}

impl RegionProvider {
    /// Returns the region to use for the S3 object storage.
    pub fn get_region(&self) -> Region {
        match self {
            RegionProvider::S3 => sniff_s3_default_region(),
            RegionProvider::Localstack => localstack_region(),
        }
    }
}

/// S3 Object storage Uri Resolver
///
/// The default implementation uses s3 as a protocol, and detects the region trying to us
/// sequencially `AWS_DEFAULT_REGION` environment variable, `AWS_REGION` environment variable,
/// region from ec2 instance metadata and lastly to default value `Region::UsEast1`.
pub struct S3CompatibleObjectStorageFactory {
    region_provider: RegionProvider,
    protocol: &'static str,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3CompatibleObjetStorageFactory with the given AWS region.
    pub fn new(region_provider: RegionProvider, protocol: &'static str) -> Self {
        S3CompatibleObjectStorageFactory {
            region_provider,
            protocol,
        }
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

impl Default for S3CompatibleObjectStorageFactory {
    fn default() -> Self {
        S3CompatibleObjectStorageFactory::new(RegionProvider::S3, "s3")
    }
}

impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn protocol(&self) -> String {
        self.protocol.to_string()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<std::sync::Arc<dyn crate::Storage>> {
        let storage = S3CompatibleObjectStorage::from_uri(self.region_provider.get_region(), uri)?;
        Ok(Arc::new(storage))
    }
}
