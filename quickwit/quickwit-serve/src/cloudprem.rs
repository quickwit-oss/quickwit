use std::sync::LazyLock;

use anyhow::{bail, Context};
use openssl::pkey::{PKey, PKeyRef, Public};
use openssl::x509::X509;
use quickwit_proto::tonic::{Request, Status};

/// CA certificate used by the CloudPrem bridge.
const CA_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIFgTCCA2mgAwIBAgIUYuETt9thznTL6Ut6YYdtkE2FiuwwDQYJKoZIhvcNAQEL
BQAwUDEiMCAGA1UEAwwZRGF0YWRvZyBQb0MgQ1AtQnJpZGdlIENBMTEWMBQGA1UE
CgwNRGF0YWRvZywgSW5jLjESMBAGA1UECwwJQ2xvdWRwcmVtMB4XDTI1MDMwNDEz
NDYwNloXDTI2MDMwNDEzNDYwNlowUDEiMCAGA1UEAwwZRGF0YWRvZyBQb0MgQ1At
QnJpZGdlIENBMTEWMBQGA1UECgwNRGF0YWRvZywgSW5jLjESMBAGA1UECwwJQ2xv
dWRwcmVtMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAnO/eoS7JS2T6
CGemCu43r1+CE37qMNlbhSYZjgFYnemMEwSPdE8QJyhVP0lOfONZoRAgbm+OHN7D
JGxCGuURiKZwa1Lp88v4dDY0KqM7Cf/7d4RRTm+x3AsIRraKfjFlA7Rlz9NRzrR8
F03a1lNn2bmaJcVnx6RW7fXlRLzL7vZ5LbihngfZCvAN0kuL0YDzHmfrZVA86QAl
w07oOfot4KZyQlQbfYVD837OCxcGVdU/bzCEIlM0VQA76dlthHF9VVJOn+Tb/KPw
oO+VHNFGQnBnnVNA6LlATdrX+C+b/tjWDtsNdPHZQ5kQSDNu9/enqpYF6YRHhtLQ
Qs9BXtUXbsONaNbCNhqtuW4b6YV9Klxl8+Fox7kDtLkKNO6luXGTCwCSy4tSkR7Z
Mgcp1nFDmp3CEvQRqwNt/on9HAmDs7BQ6GsunW4kpw4i8kBCFCilnPhOvFyYI1mF
e+dOTXj8t+xBvKEg30R7qGTuRUz6cMhU/cKqe7RvhYyFFSaUdXzskKb+GtyzPcGW
HShcHq5rX/qxOd3QI2tIA/M5ouno3PyI+SzMO6OUhbECQnjXCru6m++q2Py4Kq3Y
sGonPYCdCQhjCbjvMcZ2ic7e2Z/qWCKBEpkWgnwUrW/YbvcoibCfzIdIKiEIKtaH
XvffovEMOn3AqYyZ/v+nB+vIjzlPf/8CAwEAAaNTMFEwHQYDVR0OBBYEFAwyYj4Y
XVoxYeftPXJhmwHRXHELMB8GA1UdIwQYMBaAFAwyYj4YXVoxYeftPXJhmwHRXHEL
MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIBAGIg3xg42hcf0KFx
vN4eWHDeukq6QLdCfhPsS81fCRdAlCMYbEAmUSU0FqzNoyi32NfVDX5X6vaY3s0U
eFZIGbC0xCkpCEuBYlGDbAVi1TRIjrl0yQbtOP0LjNrquOgNBozE5+T3U88FPXaO
XCYbCLX7H4Ef3lBLD5buojKptf7y+N62aStcBYsY4z7sb23qHvyz4hT5pXDQzkg8
dtWmxdRQQx1A8WcolhfCyhmEHatEvRE4TdzUngEJTgIoSW/7yNPjOKDeuLmPNaVw
ObbRK2RWT4SUS62MWwKO96101kG+G/GNMzNpktNejEXm7IdC2hB1MVQf7iO3tx16
pxnzSA+ClqHnsfcPyfqC2ltUr0wxlDDEWYBUdeQra78xTz3Tc+xZMGkueIBZFaSi
Joj1DTZRKazY6SM/J7KEgzySa27MC/BVI2YXI/wyir0Qc2bO+tsNuzAOSHHBbKlS
BuuTTnETDpeSLKR9N3he2zPi0IVPLoJf3FLrMAOUbb+xueB2fF924nQpN/1zBlTl
sy8tmib1+j2gkdmZMSNupsapVmS4WuGpn9nE13Kt0kmYwi7VXI7KMoTS8DopTEDV
f+AdHt+id6szY1xc9nHU0WtWHs604UgLxp/a2+rQqkPIYMLouFtckBG7/ccxDZVP
Dn8SYLNTdzyEDglFposs32DKuLQi
-----END CERTIFICATE-----
";

static CA_CERT_PUBLIC_KEY: LazyLock<PKey<Public>> = LazyLock::new(|| {
    X509::from_pem(CA_CERT.as_bytes())
        .expect("CA certificate should be valid x509 certificate")
        .public_key()
        .expect("CA certificate should have a public key")
});

/// AWS mTLS interceptor.
///
/// This interceptor parses and verifies the CloudPrem bridge client certificate.
///
/// The client certificate is forwarded by the AWS ALB to the backend via the
/// urlencoded `X-Amzn-Mtls-Clientcert` header, which also carries the intermediate certificates
/// but in the context of CloudPrem, there is always only one certificate.
///
/// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html
pub(crate) fn aws_mtls_interceptor(request: Request<()>) -> Result<Request<()>, Status> {
    aws_mtls_interceptor_impl(request, &CA_CERT_PUBLIC_KEY)
}

fn aws_mtls_interceptor_impl(
    request: Request<()>,
    ca_cert_public_key: &PKeyRef<Public>,
) -> Result<Request<()>, Status> {
    let is_internal_traffic = request.metadata().get("X-Forwarded-For").is_none();

    if is_internal_traffic {
        return Ok(request);
    }
    let Some(encoded_client_cert) = request.metadata().get("X-Amzn-Mtls-Clientcert") else {
        return Err(Status::unauthenticated("could not find client certificate"));
    };
    let client_cert = urlencoding::decode_binary(encoded_client_cert.as_bytes());
    let verify_result = verify_client_cert(&client_cert, ca_cert_public_key);

    match verify_result {
        Ok(true) => Ok(request),
        Ok(false) => Err(Status::unauthenticated(
            "failed to verify client certificate",
        )),
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    }
}

fn verify_client_cert(
    client_cert: &[u8],
    ca_cert_public_key: &PKeyRef<Public>,
) -> anyhow::Result<bool> {
    let mut client_cert_pems =
        pem::parse_many(client_cert).context("failed to parse client certificate")?;

    if client_cert_pems.is_empty() {
        bail!("could not find client certificate");
    } else if client_cert_pems.len() > 1 {
        bail!(
            "expected only one client certificate, got {}",
            client_cert_pems.len()
        );
    }
    let client_cert_pem = client_cert_pems
        .pop()
        .expect("`client_cert_pems` should not be empty");

    X509::from_der(client_cert_pem.contents())
        .context("failed to parse X.509 client certificate")?
        .verify(ca_cert_public_key)
        .context("failed to verify client certificate")
}

#[cfg(test)]
mod tests {
    use quickwit_proto::tonic::Code;

    use super::*;

    const TEST_CA_CERT: &[u8] = include_bytes!("../../quickwit-integration-tests/test_data/ca.crt");
    const TEST_CLIENT_CERT: &[u8] =
        include_bytes!("../../quickwit-integration-tests/test_data/server.crt");

    #[test]
    fn test_aws_mtls_interceptor() {
        // Internal traffic should be allowed
        let request = Request::new(());
        aws_mtls_interceptor(request).unwrap();

        // External traffic without client certificate should be rejected
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-forwarded-for", "127.0.0.1".parse().unwrap());

        let status = aws_mtls_interceptor(request).unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with invalid client certificate should be allowed
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-forwarded-for", "127.0.0.1".parse().unwrap());

        let encoded_client_cert = urlencoding::encode_binary(TEST_CLIENT_CERT);
        request.metadata_mut().insert(
            "x-amzn-mtls-clientcert",
            encoded_client_cert.parse().unwrap(),
        );

        let status = aws_mtls_interceptor_impl(request, &CA_CERT_PUBLIC_KEY).unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with valid client certificate should be allowed
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-forwarded-for", "127.0.0.1".parse().unwrap());

        let encoded_client_cert = urlencoding::encode_binary(TEST_CLIENT_CERT);
        request.metadata_mut().insert(
            "x-amzn-mtls-clientcert",
            encoded_client_cert.parse().unwrap(),
        );

        let ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        aws_mtls_interceptor_impl(request, &ca_cert_public_key).unwrap();
    }

    #[test]
    fn test_verify_client_cert() {
        let verified = verify_client_cert(TEST_CLIENT_CERT, &CA_CERT_PUBLIC_KEY).unwrap();
        assert!(!verified);

        let ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        let verified = verify_client_cert(TEST_CLIENT_CERT, &ca_cert_public_key).unwrap();

        assert!(verified);
    }
}
