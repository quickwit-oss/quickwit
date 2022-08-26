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

use std::str::FromStr;

use anyhow::Context;
use ec2_instance_metadata::InstanceMetadataClient;
use once_cell::sync::OnceCell;
use rusoto_core::credential::ProfileProvider;
use rusoto_core::Region;
use tracing::{debug, error, info, instrument, warn};

/// Default region to use, if none has been configured.
const QUICKWIT_DEFAULT_REGION: Region = Region::UsEast1;

#[instrument]
fn sniff_s3_region() -> anyhow::Result<Region> {
    // Attempt to read region from environment variable and return an error if malformed.
    if let Some(region) = region_from_env_variables()? {
        info!(region=?region, from="env-variable", "set-aws-region");
        return Ok(region);
    }
    // Attempt to read region from config file and return an error if malformed.
    if let Some(region) = region_from_config_file()? {
        info!(region=?region, from="config-file", "set-aws-region");
        return Ok(region);
    }
    // Attempt to read region from EC2 instance metadata service and return an error if service
    // unavailable or region malformed.
    if let Some(region) = region_from_ec2_instance()? {
        info!(region=?region, from="ec2-instance-metadata-service", "set-aws-region");
        return Ok(region);
    }
    info!(region=?QUICKWIT_DEFAULT_REGION, from="default", "set-aws-region");
    Ok(QUICKWIT_DEFAULT_REGION)
}

/// Returns the region to use for the S3 object storage.
///
/// This function tests different methods in turn to get the region.
/// One of this method runs a GET request on an EC2 API Endpoint.
/// If this endpoint is inaccessible, this can block for a few seconds.
///
/// For this reason, this function needs to be called in a lazy manner.
/// We cannot call it once when we create the
/// `S3CompatibleObjectStorageFactory` for instance, as it would
/// impact the start of quickwit in a context where S3 is not used.
///
/// This function caches its results.
pub fn sniff_s3_region_and_cache() -> anyhow::Result<Region> {
    static CACHED_S3_DEFAULT_REGION: OnceCell<Region> = OnceCell::new();
    CACHED_S3_DEFAULT_REGION
        .get_or_try_init(sniff_s3_region)
        .map(|region| region.clone())
}

pub fn region_from_str(region_str: &str) -> anyhow::Result<Region> {
    // Try to interpret the string as a regular AWS S3 Region like `us-east-1`.
    if let Ok(region) = Region::from_str(region_str) {
        return Ok(region);
    }
    // Maybe this is a custom endpoint (for MinIO for instance).
    // We require custom endpoints to explicitely state the http/https protocol`.
    if !region_str.starts_with("http") {
        anyhow::bail!(
            "Invalid AWS region. Quickwit expects an AWS region code like `us-east-1` or a \
             http:// endpoint"
        );
    }

    // For some storage provider like Cloudflare R2, the user has to set a custom region's name.
    // We use `AWS_REGION` env variable for that.
    let region_name =
        std::env::var("AWS_REGION").unwrap_or_else(|_| "qw-custom-endpoint".to_string());

    Ok(Region::Custom {
        name: region_name,
        endpoint: region_str.trim_end_matches('/').to_string(),
    })
}

fn s3_region_env_var() -> Option<String> {
    for env_var_key in ["QW_S3_ENDPOINT", "AWS_REGION", "AWS_DEFAULT_REGION"] {
        if let Ok(env_var) = std::env::var(env_var_key) {
            info!(
                env_var_key = env_var_key,
                env_var = env_var.as_str(),
                "Setting AWS Region from environment variable."
            );
            return Some(env_var);
        }
    }
    None
}

#[instrument]
fn region_from_env_variables() -> anyhow::Result<Option<Region>> {
    if let Some(region_str) = s3_region_env_var() {
        match region_from_str(&region_str) {
            Ok(region) => Ok(Some(region)),
            Err(err) => {
                error!(err=?err, "Failed to parse region set from environment variable.");
                Err(err)
            }
        }
    } else {
        Ok(None)
    }
}

#[instrument]
fn region_from_config_file() -> anyhow::Result<Option<Region>> {
    ProfileProvider::region()
        .context("Failed to parse AWS config file.")?
        .map(|region| Region::from_str(&region))
        .transpose()
        .context("Failed to parse region from config file.")
}

// Sniffes the region from the EC2 instance metadata service.
//
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
#[instrument]
fn region_from_ec2_instance() -> anyhow::Result<Option<Region>> {
    let instance_metadata_client: InstanceMetadataClient =
        ec2_instance_metadata::InstanceMetadataClient::new();
    match instance_metadata_client.get() {
        Ok(instance_metadata) => {
            let region = Region::from_str(instance_metadata.region)
                .context("Failed to parse region fetched from instance metadata service.")?;
            Ok(Some(region))
        }
        Err(ec2_instance_metadata::Error::NotFound(_)) => {
            debug!(
                "Failed to sniff region from AWS instance metadata service. Service is not \
                 available."
            );
            Ok(None)
        }
        Err(err) => {
            warn!(err=?err, "Failed to sniff region from the AWS instance metadata service.");
            anyhow::bail!(err)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_region_from_str() {
        assert_eq!(region_from_str("us-east-1").unwrap(), Region::UsEast1);
        assert_eq!(
            region_from_str("http://localhost:4566").unwrap(),
            Region::Custom {
                name: "qw-custom-endpoint".to_string(),
                endpoint: "http://localhost:4566".to_string()
            }
        );
        assert_eq!(
            region_from_str("http://localhost:4566/").unwrap(),
            Region::Custom {
                name: "qw-custom-endpoint".to_string(),
                endpoint: "http://localhost:4566".to_string()
            }
        );
        std::env::set_var("AWS_REGION", "my-custom-region");
        assert_eq!(
            region_from_str("http://localhost:4566/").unwrap(),
            Region::Custom {
                name: "my-custom-region".to_string(),
                endpoint: "http://localhost:4566".to_string()
            }
        );
        assert!(region_from_str("us-eat-1").is_err());
    }
}
