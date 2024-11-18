// The Quickwit Enterprise Edition (EE) license
// Copyright (c) 2024-present Quickwit Inc.
//
// With regard to the Quickwit Software:
//
// This software and associated documentation files (the "Software") may only be
// used in production, if you (and any entity that you represent) hold a valid
// Quickwit Enterprise license corresponding to your usage.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// For all third party components incorporated into the Quickwit Software, those
// components are licensed under the original license provided by the owner of the
// applicable component.

use std::str::FromStr;
use std::sync::OnceLock;
use std::time::SystemTime;

use biscuit_auth::builder::{Predicate, Rule, Term};
use biscuit_auth::builder_ext::AuthorizerExt;
use biscuit_auth::macros::biscuit;
use biscuit_auth::{Authorizer, Biscuit, KeyPair, PrivateKey, PublicKey};
use tracing::{error, info};

// For tests, we use a different public key, for which the private
// key is shipped in the code, so we can emit licenses.
#[cfg(any(test, feature = "testsuite"))]
const QUICKWIT_PUBLIC_KEY_HEX: &str =
    "5a81acfc945c7c64c3e95a0c8ce2caafec7c496e76db9ff528755eca7fc16c25";

// This is our actual quickwit license public key.
// The private key is secret.
#[cfg(not(any(test, feature = "testsuite")))]
const QUICKWIT_PUBLIC_KEY_HEX: &str =
    "32d02f39dbfcf6e70d0eceae3e4055c82fad8df7c9a062b89b21e3efd05c1d3f";

const QUICKWIT_PRIVATE_KEY_HEX_TEST: &str =
    "43fec56169112de681ab5935fdda7d589b79ba430bd128bc85d18a592813d0b6";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid biscuit")]
    Unverified,
    #[error("invalid parameter {0}")]
    InvalidParameter(String),
    #[error("license expired")]
    LicenseExpired,
}

#[derive(Debug)]
pub struct License {
    pub license_level: LicenseLevel,
    pub licensee: String,
}

fn extract_single_key_value(authorizer: &mut Authorizer, key: &str) -> Result<String, Error> {
    let head_predicate = Predicate::new(
        "data".to_string(),
        vec![Term::Variable("placeholder".to_string())],
    );
    let body_predicate = Predicate::new(
        key.to_string(),
        vec![Term::Variable("placeholder".to_string())],
    );
    let rule = Rule::new(head_predicate, vec![body_predicate], Vec::new(), Vec::new());
    let values: Vec<(String,)> = authorizer
        .query(rule)
        .map_err(|_| Error::InvalidParameter(key.to_string()))?; //r#"data($license_level) <- license($license_level)"#)
    if values.len() != 1 {
        return Err(Error::InvalidParameter(key.to_string()));
    }
    let value_str = values.into_iter().next().unwrap().0;
    Ok(value_str)
}

fn extract_license(license_str: &str) -> Result<License, Error> {
    let quickwit_public_key =
        PublicKey::from_bytes_hex(QUICKWIT_PUBLIC_KEY_HEX).expect("Invalid public key");
    let biscuit =
        Biscuit::from_base64(license_str, quickwit_public_key).map_err(|_| Error::Unverified)?;
    let mut authorizer = Authorizer::new();
    authorizer.set_time();
    authorizer
        .add_token(&biscuit)
        .map_err(|_| Error::Unverified)?;
    authorizer.add_allow_all();
    authorizer.authorize().map_err(|_| Error::LicenseExpired)?;
    let license_level_str: String = extract_single_key_value(&mut authorizer, "license_level")?;
    let licensee: String = extract_single_key_value(&mut authorizer, "licensee")?;
    let license_level = LicenseLevel::from_str(&license_level_str)
        .map_err(|_| Error::InvalidParameter("license_level".to_string()))?;
    Ok(License {
        license_level,
        licensee,
    })
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum LicenseLevel {
    Community,
    Gold,
}

impl LicenseLevel {
    pub fn name(&self) -> &'static str {
        match self {
            LicenseLevel::Community => "community",
            LicenseLevel::Gold => "gold",
        }
    }
}

impl FromStr for LicenseLevel {
    type Err = anyhow::Error;

    fn from_str(license_str: &str) -> anyhow::Result<Self> {
        match license_str {
            "gold" => Ok(Self::Gold),
            _ => Err(anyhow::anyhow!("Unknown license level: {}", license_str)),
        }
    }
}

static LICENSE: OnceLock<License> = OnceLock::new();

pub fn set_license(license_hex_str: &str) -> Result<(), Error> {
    let license = extract_license(license_hex_str)?;
    if let Err(_license) = LICENSE.set(license) {
        // Someone already set the license.
        error!("license already set, this should not happen");
    }
    let license = LICENSE.get().unwrap();
    info!(
        "enabling {:?} license for {}",
        license.license_level,
        license.licensee.as_str()
    );
    Ok(())
}

pub fn get_license_level() -> LicenseLevel {
    #[cfg(not(test))]
    const DEFAULT_LICENSE_LEVEL: LicenseLevel = LicenseLevel::Community;
    #[cfg(test)]
    const DEFAULT_LICENSE_LEVEL: LicenseLevel = LicenseLevel::Gold;
    LICENSE
        .get()
        .map(|license| license.license_level)
        .unwrap_or(DEFAULT_LICENSE_LEVEL)
}

use biscuit_auth::builder_ext::BuilderExt;

pub fn create_license(
    license_level: LicenseLevel,
    licensee: &str,
    expiration_date: SystemTime,
) -> biscuit_auth::builder::BiscuitBuilder {
    let license_level_str = license_level.name();
    let mut biscuit_builder = biscuit!(
        r#"license_level({license_level_str});
        licensee({licensee});
        "#,
    );
    biscuit_builder.check_expiration_date(expiration_date);
    biscuit_builder
}

pub fn get_fake_license_for_tests() -> String {
    let private_key: PrivateKey =
        PrivateKey::from_bytes_hex(QUICKWIT_PRIVATE_KEY_HEX_TEST).unwrap();
    let key_pair = KeyPair::from(&private_key);
    let license_biscuit = create_license(
        LicenseLevel::Gold,
        "QuickwitTestOnly",
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_077_439_515),
    )
    .build(&key_pair)
    .unwrap();
    license_biscuit.to_base64().unwrap()
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use biscuit_auth::macros::biscuit;
    use biscuit_auth::{KeyPair, PrivateKey};

    use super::*;

    const QUICKWIT_INVALID_PRIVATE_KEY_HEX: &str =
        "42fec56169112de681ab5935fdda7d589b79ba430bd128bc85d18a592813d0b6";

    #[test]
    fn test_license_success() {
        let private_key: PrivateKey =
            PrivateKey::from_bytes_hex(QUICKWIT_PRIVATE_KEY_HEX_TEST).unwrap();
        let key_pair = KeyPair::from(&private_key);
        let biscuit = super::create_license(
            LicenseLevel::Gold,
            "LittleBlue Inc.",
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_077_439_515),
        )
        .build(&key_pair)
        .unwrap();
        let license_str = biscuit.to_base64().unwrap();

        let license = super::extract_license(&license_str).unwrap();
        assert_eq!(license.license_level, LicenseLevel::Gold);
        assert_eq!(&license.licensee, "LittleBlue Inc.");
    }

    #[test]
    fn test_license_invalid_private_key() {
        let private_key: PrivateKey =
            PrivateKey::from_bytes_hex(QUICKWIT_INVALID_PRIVATE_KEY_HEX).unwrap();
        let key_pair = KeyPair::from(&private_key);
        let biscuit = super::create_license(
            LicenseLevel::Gold,
            "LittleBlue Inc.",
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_077_439_515),
        )
        .build(&key_pair)
        .unwrap();
        let license_str = biscuit.to_base64().unwrap();
        let err = super::extract_license(&license_str).unwrap_err();
        assert!(matches!(err, super::Error::Unverified));
    }

    #[test]
    fn test_license_expired() {
        let private_key: PrivateKey =
            PrivateKey::from_bytes_hex(QUICKWIT_PRIVATE_KEY_HEX_TEST).unwrap();
        let key_pair = KeyPair::from(&private_key);
        let biscuit = super::create_license(
            LicenseLevel::Gold,
            "LittleBlue Inc.",
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_730_370_989),
        )
        .build(&key_pair)
        .unwrap();
        let license_str = biscuit.to_base64().unwrap();
        let err = super::extract_license(&license_str).unwrap_err();
        assert!(matches!(err, super::Error::LicenseExpired));
    }

    #[test]
    fn test_missing_licensee() {
        let private_key: PrivateKey =
            PrivateKey::from_bytes_hex(QUICKWIT_PRIVATE_KEY_HEX_TEST).unwrap();
        let key_pair = KeyPair::from(&private_key);
        let biscuit = biscuit!(
            r#"license_level("gold");
            check if time($time), $time <= 2035-05-30T20:00:00Z;
            "#,
        )
        .build(&key_pair)
        .unwrap();
        let license_str = biscuit.to_base64().unwrap();
        let err = super::extract_license(&license_str).unwrap_err();
        assert!(matches!(err, super::Error::InvalidParameter(_)));
    }
}
