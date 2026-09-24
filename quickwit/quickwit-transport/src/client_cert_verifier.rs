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

use std::sync::Arc;

use quickwit_config::ClientIdentityMatcher;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, Error, SignatureScheme};
use x509_parser::extensions::GeneralName;

/// Adds leaf identity authorization to standard mTLS verification. Every cryptographic check,
/// including proof of possession in TLS 1.2 and TLS 1.3, remains delegated to the inner verifier.
/// The policy is immutable and belongs to one server configuration, as does its session cache.
#[derive(Debug)]
pub(crate) struct IdentityClientCertVerifier {
    pub cert_verifier: Arc<dyn ClientCertVerifier>,
    pub identity_matcher: ClientIdentityMatcher,
}

impl IdentityClientCertVerifier {
    fn verify_identity(&self, leaf_certificate: &CertificateDer<'_>) -> Result<(), Error> {
        let (remaining, certificate) =
            x509_parser::parse_x509_certificate(leaf_certificate.as_ref())
                .map_err(|_| CertificateError::BadEncoding)?;
        if !remaining.is_empty() {
            return Err(CertificateError::BadEncoding.into());
        }
        // Parses the whole SAN extension before accepting any rule. A malformed or duplicate SAN
        // is a certificate error, even when a common name would otherwise match.
        let subject_alt_name_opt = certificate
            .subject_alternative_name()
            .map_err(|_| CertificateError::BadEncoding)?;
        // Rejects invalid entries that the accessor can return without an error.
        if let Some(subject_alt_name) = &subject_alt_name_opt {
            let has_invalid_name = subject_alt_name
                .value
                .general_names
                .iter()
                .any(|name| matches!(name, GeneralName::Invalid(..)));
            if has_invalid_name {
                return Err(CertificateError::BadEncoding.into());
            }
        }
        // Decodes every CN before matching so an early match cannot hide a decoding error.
        let mut common_names = Vec::new();
        if self.identity_matcher.has_common_name_rules() {
            for common_name in certificate.subject().iter_common_name() {
                let name = common_name
                    .as_str()
                    .map_err(|_| CertificateError::BadEncoding)?;
                common_names.push(name);
            }
        }
        let common_name_matches = common_names
            .iter()
            .any(|name| self.identity_matcher.matches_common_name(name));
        if common_name_matches {
            return Ok(());
        }
        let Some(subject_alt_name_extension) = subject_alt_name_opt else {
            return Err(CertificateError::ApplicationVerificationFailure.into());
        };
        for name in &subject_alt_name_extension.value.general_names {
            match name {
                GeneralName::DNSName(dns_name)
                    if self.identity_matcher.matches_dns_san(dns_name) =>
                {
                    return Ok(());
                }
                GeneralName::URI(uri) if self.identity_matcher.matches_uri_san(uri) => {
                    return Ok(());
                }
                _ => {}
            }
        }
        Err(CertificateError::ApplicationVerificationFailure.into())
    }
}

impl ClientCertVerifier for IdentityClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        self.cert_verifier.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.cert_verifier.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.cert_verifier.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        leaf_certificate: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        let verified =
            self.cert_verifier
                .verify_client_cert(leaf_certificate, intermediates, now)?;
        self.verify_identity(leaf_certificate)?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.cert_verifier
            .verify_tls12_signature(message, cert, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.cert_verifier
            .verify_tls13_signature(message, cert, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.cert_verifier.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.cert_verifier.requires_raw_public_keys()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::AllowedClientIdentities;
    use rcgen::{
        BasicConstraints, CertificateParams, CustomExtension, DnType, DnValue,
        ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
    };
    use rustls::RootCertStore;
    use rustls::crypto::CryptoProvider;
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;

    use super::*;

    struct TestCa {
        cert: CertificateDer<'static>,
        issuer: Issuer<'static, KeyPair>,
    }

    struct TestIdentity {
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    }

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(rustls::crypto::aws_lc_rs::default_provider())
    }

    impl TestCa {
        fn new() -> Self {
            static NEXT_CA: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let mut params = CertificateParams::default();
            let ca_id = NEXT_CA.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            params
                .distinguished_name
                .push(DnType::CommonName, format!("test CA {ca_id}"));
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            let key = KeyPair::generate().unwrap();
            let cert = params.self_signed(&key).unwrap().der().clone();
            Self {
                cert,
                issuer: Issuer::new(params, key),
            }
        }

        fn issue(&self, params: CertificateParams) -> TestIdentity {
            let key = KeyPair::generate().unwrap();
            TestIdentity {
                cert: params.signed_by(&key, &self.issuer).unwrap().der().clone(),
                key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            }
        }

        fn roots(&self) -> RootCertStore {
            let mut roots = RootCertStore::empty();
            roots.add(self.cert.clone()).unwrap();
            roots
        }

        fn verifier(
            &self,
            allowed_identities: AllowedClientIdentities,
        ) -> Arc<IdentityClientCertVerifier> {
            let cert_verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(self.roots()), provider())
                    .build()
                    .unwrap();
            let identity_matcher = allowed_identities.compile().unwrap();
            Arc::new(IdentityClientCertVerifier {
                cert_verifier,
                identity_matcher,
            })
        }
    }

    fn leaf_params(
        common_name: Option<&str>,
        dns_sans: &[&str],
        uri_names: &[&str],
    ) -> CertificateParams {
        let mut params = CertificateParams::new(
            dns_sans
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        if let Some(name) = common_name {
            params.distinguished_name.push(DnType::CommonName, name);
        }
        for uri in uri_names {
            params
                .subject_alt_names
                .push(SanType::URI((*uri).try_into().unwrap()));
        }
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        params
    }

    fn dns_policy(name: &str) -> AllowedClientIdentities {
        AllowedClientIdentities {
            dns_sans: vec![name.to_string()],
            ..Default::default()
        }
    }

    fn spiffe_policy() -> AllowedClientIdentities {
        AllowedClientIdentities {
            uri_sans: vec!["spiffe://example.com/ns/logging/**".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_trusted_client_identity_allowlist() {
        let ca = TestCa::new();
        let verifier = ca.verifier(dns_policy("*.example.com"));
        for (names, accepted) in [
            (vec![], false),
            (vec!["*.example.com"], false),
            (vec!["another.example.org", "collector.example.com"], true),
            (vec!["collector.example.com"], true),
            (vec!["collector.example.com.evil"], false),
            (vec!["collector.example.org"], false),
            (vec!["nested.collector.example.com"], false),
        ] {
            let identity = ca.issue(leaf_params(Some("collector.example.com"), &names, &[]));
            assert_eq!(
                verifier
                    .verify_client_cert(&identity.cert, &[], UnixTime::now())
                    .is_ok(),
                accepted,
                "{names:?}"
            );
        }
    }

    #[test]
    fn test_common_name_dns_and_uri_are_alternatives() {
        let ca = TestCa::new();
        let verifier = ca.verifier(AllowedClientIdentities {
            common_names: vec!["forward*".to_string()],
            dns_sans: vec!["collector.example.com".to_string()],
            uri_sans: vec!["spiffe://example.com/ns/logging/sa/*".to_string()],
        });
        for params in [
            leaf_params(Some("forwarder"), &["unrelated.example.org"], &[]),
            leaf_params(Some("other"), &["collector.example.com"], &[]),
            leaf_params(None, &[], &["spiffe://example.com/ns/logging/sa/collector"]),
        ] {
            let identity = ca.issue(params);
            verifier
                .verify_client_cert(&identity.cert, &[], UnixTime::now())
                .unwrap();
        }
        let identity = ca.issue(leaf_params(
            Some("other"),
            &["other.example.com"],
            &["spiffe://other.com/ns/logging/sa/collector"],
        ));
        assert!(
            verifier
                .verify_client_cert(&identity.cert, &[], UnixTime::now())
                .is_err()
        );
    }

    #[test]
    fn test_multiple_common_names_match_any_configured_identity() {
        let ca = TestCa::new();
        let verifier = ca.verifier(AllowedClientIdentities {
            common_names: vec!["second".to_string()],
            ..Default::default()
        });
        let mut params = leaf_params(Some("first"), &[], &[]);
        // A custom DN attribute with the CN OID permits two CNs in this generated subject.
        params
            .distinguished_name
            .push(DnType::CustomDnType(vec![2, 5, 4, 3]), "second");
        let identity = ca.issue(params);
        verifier
            .verify_client_cert(&identity.cert, &[], UnixTime::now())
            .unwrap();
    }

    #[test]
    fn test_matching_common_name_does_not_bypass_decoding_errors() {
        let ca = TestCa::new();
        let verifier = ca.verifier(AllowedClientIdentities {
            common_names: vec!["allowed".to_string()],
            ..Default::default()
        });
        let matching_name = DnValue::Utf8String("allowed".to_string());
        // Uses a BMPString CN, which the attribute's as_str decoder does not support.
        let undecodable_name = DnValue::BmpString("other".try_into().unwrap());
        for (first_name, second_name) in [
            (matching_name.clone(), undecodable_name.clone()),
            (undecodable_name, matching_name),
        ] {
            let mut params = leaf_params(None, &[], &[]);
            params
                .distinguished_name
                .push(DnType::CommonName, first_name);
            params
                .distinguished_name
                .push(DnType::CustomDnType(vec![2, 5, 4, 3]), second_name);
            let identity = ca.issue(params);
            verifier
                .cert_verifier
                .verify_client_cert(&identity.cert, &[], UnixTime::now())
                .unwrap();
            assert!(matches!(
                verifier.verify_client_cert(&identity.cert, &[], UnixTime::now()),
                Err(Error::InvalidCertificate(CertificateError::BadEncoding))
            ));
        }
    }

    #[test]
    fn test_identity_match_never_bypasses_certificate_validation() {
        let ca = TestCa::new();
        for policy in [dns_policy("collector.example.com"), spiffe_policy()] {
            let verifier = ca.verifier(policy);
            let params = leaf_params(
                None,
                &["collector.example.com"],
                &["spiffe://example.com/ns/logging/collector"],
            );
            let other_ca = TestCa::new();
            let untrusted = other_ca.issue(params.clone());
            assert!(matches!(
                verifier.verify_client_cert(&untrusted.cert, &[], UnixTime::now()),
                Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
            ));

            let mut expired_params = params.clone();
            expired_params.not_after = time::OffsetDateTime::now_utc() - time::Duration::days(1);
            let expired = ca.issue(expired_params);
            assert!(
                verifier
                    .verify_client_cert(&expired.cert, &[], UnixTime::now())
                    .is_err()
            );

            let mut wrong_usage_params = params;
            wrong_usage_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let wrong_usage = ca.issue(wrong_usage_params);
            assert!(
                verifier
                    .verify_client_cert(&wrong_usage.cert, &[], UnixTime::now())
                    .is_err()
            );
        }
    }

    #[test]
    fn test_uri_sans_match_any_uri_without_requiring_spiffe_profile() {
        let ca = TestCa::new();
        let verifier = ca.verifier(AllowedClientIdentities {
            uri_sans: vec![
                "https://example.com/identities/*".to_string(),
                "spiffe://example.com/ns/logging/**".to_string(),
                "urn:example:collector".to_string(),
            ],
            ..Default::default()
        });
        for uris in [
            vec!["https://example.com/identities/collector"],
            vec![
                "https://other.com/unrelated",
                "spiffe://example.com/ns/logging/collector",
            ],
            vec!["spiffe://example.com/ns/logging/%2F"],
            vec!["spiffe://example.com/ns/logging/collector"],
            vec!["urn:example:collector", "urn:example:other"],
        ] {
            // An ordinary TLS client certificate need not have the additional X.509-SVID
            // extensions or server-auth EKU, even when its URI SAN uses the spiffe scheme.
            let mut params = leaf_params(Some("collector"), &[], &uris);
            params.is_ca = IsCa::NoCa;
            params.key_usages.clear();
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            let identity = ca.issue(params);
            verifier
                .verify_client_cert(&identity.cert, &[], UnixTime::now())
                .unwrap();
        }
        for uris in [
            vec![],
            vec!["https://example.com/identities/a/b"],
            vec!["spiffe://example.com/ns/logging/%invalid"],
            vec![
                "spiffe://other.com/ns/logging/collector",
                "urn:example:other",
            ],
        ] {
            let identity = ca.issue(leaf_params(None, &[], &uris));
            assert!(
                verifier
                    .verify_client_cert(&identity.cert, &[], UnixTime::now())
                    .is_err(),
                "{uris:?}"
            );
        }
    }

    #[test]
    fn test_invalid_san_entries_cannot_be_bypassed_by_matching_identities() {
        let ca = TestCa::new();
        let verifiers = [
            (
                "CN",
                ca.verifier(AllowedClientIdentities {
                    common_names: vec!["forwarder".to_string()],
                    ..Default::default()
                }),
            ),
            ("DNS", ca.verifier(dns_policy("collector.example.com"))),
            (
                "URI",
                ca.verifier(AllowedClientIdentities {
                    uri_sans: vec!["urn:example:collector".to_string()],
                    ..Default::default()
                }),
            ),
        ];
        // Encodes DNS ([2]) and URI ([6]) SAN entries using short-form DER lengths.
        let mut valid_entries = Vec::new();
        for (tag, name) in [
            (0x82, "collector.example.com"),
            (0x86, "urn:example:collector"),
        ] {
            valid_entries.push(tag);
            valid_entries.push(u8::try_from(name.len()).unwrap());
            valid_entries.extend_from_slice(name.as_bytes());
        }
        for (invalid_tag, invalid_first) in
            [(0x82, false), (0x82, true), (0x86, false), (0x86, true)]
        {
            let invalid_entry = [invalid_tag, 0x01, 0xff];
            let entries = if invalid_first {
                [invalid_entry.as_slice(), valid_entries.as_slice()].concat()
            } else {
                [valid_entries.as_slice(), invalid_entry.as_slice()].concat()
            };
            let mut extension_der = vec![0x30, u8::try_from(entries.len()).unwrap()];
            extension_der.extend_from_slice(&entries);
            let mut params = leaf_params(Some("forwarder"), &[], &[]);
            params
                .custom_extensions
                .push(CustomExtension::from_oid_content(
                    &[2, 5, 29, 17],
                    extension_der,
                ));
            let identity = ca.issue(params);
            for (identity_kind, verifier) in &verifiers {
                // WebPKI accepts the certificate; identity validation must reject its malformed
                // SAN.
                verifier
                    .cert_verifier
                    .verify_client_cert(&identity.cert, &[], UnixTime::now())
                    .unwrap();
                let result = verifier.verify_client_cert(&identity.cert, &[], UnixTime::now());
                assert!(
                    matches!(
                        &result,
                        Err(Error::InvalidCertificate(CertificateError::BadEncoding))
                    ),
                    "{identity_kind}, SAN tag {invalid_tag:#x}, invalid_first={invalid_first}: \
                     {result:?}"
                );
            }
        }
    }

    #[test]
    fn test_malformed_or_duplicate_san_cannot_be_bypassed_by_cn() {
        let ca = TestCa::new();
        let verifier = ca.verifier(AllowedClientIdentities {
            common_names: vec!["forwarder".to_string()],
            ..Default::default()
        });
        for dns_sans in [vec![], vec!["example.com"]] {
            let mut params = leaf_params(Some("forwarder"), &dns_sans, &[]);
            // A malformed SAN sequence, signed by the trusted CA. With DNS names present this also
            // introduces a duplicate extension. Neither should be hidden by the matching CN.
            params
                .custom_extensions
                .push(CustomExtension::from_oid_content(
                    &[2, 5, 29, 17],
                    vec![0x30, 0x01, 0xff],
                ));
            let identity = ca.issue(params);
            assert!(
                verifier
                    .verify_client_cert(&identity.cert, &[], UnixTime::now())
                    .is_err()
            );
        }
        assert!(
            verifier
                .verify_client_cert(
                    &CertificateDer::from(vec![0x30, 0x00]),
                    &[],
                    UnixTime::now()
                )
                .is_err()
        );
    }

    #[test]
    fn test_intermediate_identities_cannot_authorize_leaf() {
        let ca = TestCa::new();
        let verifier = ca.verifier(dns_policy("allowed.example.com"));
        let mut intermediate_params =
            leaf_params(Some("intermediate"), &["allowed.example.com"], &[]);
        intermediate_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        intermediate_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        intermediate_params.extended_key_usages.clear();
        let intermediate_key = KeyPair::generate().unwrap();
        let intermediate_cert = intermediate_params
            .signed_by(&intermediate_key, &ca.issuer)
            .unwrap();
        let issuer = Issuer::new(intermediate_params, intermediate_key);
        let params = leaf_params(Some("leaf"), &["denied.example.com"], &[]);
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        let result = verifier.verify_client_cert(
            cert.der(),
            &[intermediate_cert.der().clone()],
            UnixTime::now(),
        );
        assert!(matches!(
            result,
            Err(Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure
            ))
        ));
    }

    mod handshakes {
        use std::io;
        use std::time::Duration;

        use rustls::client::ResolvesClientCert;
        use rustls::pki_types::ServerName;
        use rustls::sign::CertifiedKey;
        use rustls::{ClientConfig, ServerConfig, SupportedProtocolVersion};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::{TlsAcceptor, TlsConnector};

        use super::*;

        fn server_config(
            ca: &TestCa,
            identity: &TestIdentity,
            allowed_identities: AllowedClientIdentities,
            version: &'static SupportedProtocolVersion,
        ) -> Arc<ServerConfig> {
            Arc::new(
                ServerConfig::builder_with_provider(provider())
                    .with_protocol_versions(&[version])
                    .unwrap()
                    .with_client_cert_verifier(ca.verifier(allowed_identities))
                    .with_single_cert(vec![identity.cert.clone()], identity.key.clone_key())
                    .unwrap(),
            )
        }

        fn client_config(
            ca: &TestCa,
            identity: Option<&TestIdentity>,
            version: &'static SupportedProtocolVersion,
        ) -> Arc<ClientConfig> {
            let builder = ClientConfig::builder_with_provider(provider())
                .with_protocol_versions(&[version])
                .unwrap()
                .with_root_certificates(ca.roots());
            Arc::new(match identity {
                Some(client_identity) => builder
                    .with_client_auth_cert(
                        vec![client_identity.cert.clone()],
                        client_identity.key.clone_key(),
                    )
                    .unwrap(),
                None => builder.with_no_client_auth(),
            })
        }

        /// Checks both the server's acceptance and application data exchange. A TLS 1.3 client can
        /// finish its side of the handshake before it receives the server's rejection of
        /// its certificate.
        async fn exchange(
            server_config: Arc<ServerConfig>,
            client_config: Arc<ClientConfig>,
        ) -> bool {
            let (server_io, client_io) = tokio::io::duplex(32_768);
            let server = async move {
                let mut stream = TlsAcceptor::from(server_config).accept(server_io).await?;
                assert_eq!(stream.read_u8().await?, 41);
                stream.write_u8(42).await?;
                stream.flush().await?;
                Ok::<_, io::Error>(())
            };
            let client = async move {
                let server_name = ServerName::try_from("server.example.com").unwrap();
                let mut stream = TlsConnector::from(client_config)
                    .connect(server_name, client_io)
                    .await?;
                stream.write_u8(41).await?;
                stream.flush().await?;
                assert_eq!(stream.read_u8().await?, 42);
                Ok::<_, io::Error>(())
            };
            let (server_result, client_result) =
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(server, client)
                })
                .await
                .expect("TLS exchange must complete");
            server_result.is_ok() && client_result.is_ok()
        }

        #[tokio::test]
        async fn test_identity_authorization_handshakes_tls12_and_tls13() {
            let ca = TestCa::new();
            let server_identity = ca.issue(leaf_params(None, &["server.example.com"], &[]));
            let allowed = ca.issue(leaf_params(None, &["collector.example.com"], &[]));
            let denied = ca.issue(leaf_params(None, &["payroll.example.com"], &[]));
            let untrusted = TestCa::new().issue(leaf_params(None, &["collector.example.com"], &[]));
            for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
                let server = server_config(
                    &ca,
                    &server_identity,
                    dns_policy("collector.example.com"),
                    version,
                );
                assert!(
                    exchange(server.clone(), client_config(&ca, Some(&allowed), version)).await
                );
                assert!(
                    !exchange(server.clone(), client_config(&ca, Some(&denied), version)).await
                );
                assert!(
                    !exchange(
                        server.clone(),
                        client_config(&ca, Some(&untrusted), version)
                    )
                    .await
                );
                assert!(!exchange(server, client_config(&ca, None, version)).await);
            }
        }

        #[derive(Debug)]
        struct FixedClientIdentity(Arc<CertifiedKey>);

        impl ResolvesClientCert for FixedClientIdentity {
            fn resolve(
                &self,
                _root_hints: &[&[u8]],
                _schemes: &[SignatureScheme],
            ) -> Option<Arc<CertifiedKey>> {
                Some(self.0.clone())
            }

            fn has_certs(&self) -> bool {
                true
            }
        }

        #[tokio::test]
        async fn test_matching_identity_still_requires_proof_of_private_key() {
            let ca = TestCa::new();
            let server_identity = ca.issue(leaf_params(None, &["server.example.com"], &[]));
            let client = ca.issue(leaf_params(None, &["collector.example.com"], &[]));
            let wrong_key = ca.issue(leaf_params(None, &["other.example.com"], &[]));
            let signing_key = provider()
                .key_provider
                .load_private_key(wrong_key.key)
                .unwrap();
            let certified_key = Arc::new(CertifiedKey::new(vec![client.cert.clone()], signing_key));
            for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
                let client_config = ClientConfig::builder_with_provider(provider())
                    .with_protocol_versions(&[version])
                    .unwrap()
                    .with_root_certificates(ca.roots())
                    .with_client_cert_resolver(Arc::new(FixedClientIdentity(
                        certified_key.clone(),
                    )));
                let server = server_config(
                    &ca,
                    &server_identity,
                    dns_policy("collector.example.com"),
                    version,
                );
                assert!(!exchange(server, Arc::new(client_config)).await);
            }
        }

        #[tokio::test]
        async fn test_restarted_server_does_not_resume_sessions_from_old_policy() {
            let ca = TestCa::new();
            let server_identity = ca.issue(leaf_params(None, &["server.example.com"], &[]));
            let allowed = ca.issue(leaf_params(None, &["collector.example.com"], &[]));
            let version = &rustls::version::TLS13;
            let client = client_config(&ca, Some(&allowed), version);
            let old_server = server_config(
                &ca,
                &server_identity,
                dns_policy("collector.example.com"),
                version,
            );
            assert!(exchange(old_server, client.clone()).await);
            let new_server = server_config(
                &ca,
                &server_identity,
                dns_policy("other.example.com"),
                version,
            );
            assert!(!exchange(new_server, client).await);
        }

        #[tokio::test]
        async fn test_renewed_spiffe_identity_is_authorized() {
            let ca = TestCa::new();
            let server_identity = ca.issue(leaf_params(None, &["server.example.com"], &[]));
            let server = server_config(
                &ca,
                &server_identity,
                spiffe_policy(),
                &rustls::version::TLS13,
            );
            let params = leaf_params(None, &[], &["spiffe://example.com/ns/logging/collector"]);
            for _ in 0..2 {
                // Renew the certificate with a new private key while retaining the allowed
                // identity.
                let client = ca.issue(params.clone());
                assert!(
                    exchange(
                        server.clone(),
                        client_config(&ca, Some(&client), &rustls::version::TLS13)
                    )
                    .await
                );
            }
        }
    }
}
