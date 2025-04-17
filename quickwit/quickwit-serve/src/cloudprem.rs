use std::sync::LazyLock;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, bail};
use futures::future::{Either, Ready, ready};
use openssl::pkey::{PKey, PKeyRef, Public};
use openssl::x509::X509;
use quickwit_proto::tonic::Status;
use quickwit_proto::tonic::body::BoxBody;
use quickwit_proto::tonic::codegen::http::{Request, Response};
use quickwit_proto::tonic::transport::Body;
use tower::{Layer, Service};
use tracing::info;

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
/// The AWS ALB forwards the client certificate to the backend via the URL-encoded
/// `X-Amzn-Mtls-Clientcert` header. It can also carry intermediate certificates, but in the context
/// of CloudPrem, we always expect a single certificate.
///
/// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html
fn aws_mtls_interceptor_impl<T>(
    request: Request<T>,
    ca_cert_public_key: &PKeyRef<Public>,
) -> Result<Request<T>, Status> {
    let path = request.uri().path();
    let is_external_traffic = path.starts_with("/cloudprem");

    if !is_external_traffic {
        return Ok(request);
    }
    let Some(encoded_client_cert) = request.headers().get("X-Amzn-Mtls-Clientcert") else {
        return Err(Status::unauthenticated("could not find client certificate"));
    };
    let client_cert = urlencoding::decode_binary(encoded_client_cert.as_bytes());
    let verify_result = verify_client_cert(&client_cert, ca_cert_public_key);

    match verify_result {
        Ok(Some(cert)) => {
            let Ok(subject) = cert
                .subject_name()
                .entries()
                .map(|entry| entry.data().as_utf8().map(|entry| entry.to_string()))
                .collect::<Result<Vec<_>, _>>()
            else {
                // this shouldn't happen, but if it does, it seems better to reject the query than
                // accept an unauditable one.
                return Err(Status::invalid_argument("unparseable subject".to_string()));
            };
            let subject = subject.join(", ");
            info!(target: "audit_log", path, subject, "received request");
            Ok(request)
        }
        Ok(None) => Err(Status::unauthenticated(
            "failed to verify client certificate",
        )),
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    }
}

fn verify_client_cert(
    client_cert: &[u8],
    ca_cert_public_key: &PKeyRef<Public>,
) -> anyhow::Result<Option<X509>> {
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

    let x509 = X509::from_der(client_cert_pem.contents())
        .context("failed to parse X.509 client certificate")?;
    if x509
        .verify(ca_cert_public_key)
        .context("failed to verify client certificate")?
    {
        Ok(Some(x509))
    } else {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct AwsMtlsInterceptor<'a, S> {
    inner: S,
    ca_cert_public_key: &'a PKeyRef<Public>,
}

impl<S> Service<Request<Body>> for AwsMtlsInterceptor<'_, S>
where S: Service<Request<Body>, Response = Response<BoxBody>>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        match aws_mtls_interceptor_impl(request, self.ca_cert_public_key) {
            Ok(request) => Either::Left(self.inner.call(request)),
            Err(status) => Either::Right(ready(Ok(status.to_http()))),
        }
    }
}

#[derive(Clone)]
pub(crate) struct AwsMtlsInterceptorLayer<'a> {
    ca_cert_public_key: &'a PKeyRef<Public>,
}

impl AwsMtlsInterceptorLayer<'static> {
    pub fn for_cloudprem_bridge() -> Self {
        Self {
            ca_cert_public_key: &CA_CERT_PUBLIC_KEY,
        }
    }
}

impl<'a, S> Layer<S> for AwsMtlsInterceptorLayer<'a> {
    type Service = AwsMtlsInterceptor<'a, S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            ca_cert_public_key: self.ca_cert_public_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;
    use quickwit_proto::tonic::{Code, Status};
    use tonic::body::empty_body;
    use tower::service_fn;

    use super::*;

    const TEST_CA_CERT: &[u8] = include_bytes!("../../quickwit-integration-tests/test_data/ca.crt");
    const TEST_CLIENT_CERT: &[u8] =
        include_bytes!("../../quickwit-integration-tests/test_data/server.crt");

    #[test]
    fn test_aws_mtls_interceptor_impl() {
        // Internal traffic should be allowed
        let request = Request::builder().uri("/api/v1/indexes").body(()).unwrap();
        aws_mtls_interceptor_impl(request, &CA_CERT_PUBLIC_KEY).unwrap();

        // External traffic without client certificate should be rejected
        let request = Request::builder().uri("/cloudprem").body(()).unwrap();
        let status = aws_mtls_interceptor_impl(request, &CA_CERT_PUBLIC_KEY).unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with invalid client certificate should be rejected
        let encoded_client_cert = urlencoding::encode_binary(TEST_CLIENT_CERT).to_string();

        let request = Request::builder()
            .uri("/cloudprem")
            .header("x-amzn-mtls-clientcert", &encoded_client_cert)
            .body(())
            .unwrap();
        let status = aws_mtls_interceptor_impl(request, &CA_CERT_PUBLIC_KEY).unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with valid client certificate should be allowed
        let request = Request::builder()
            .uri("/cloudprem")
            .header("x-amzn-mtls-clientcert", &encoded_client_cert)
            .body(())
            .unwrap();
        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        aws_mtls_interceptor_impl(request, &test_ca_cert_public_key).unwrap();
    }

    #[tokio::test]
    async fn test_aws_mtls_interceptor_layer() {
        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        let interceptor = AwsMtlsInterceptorLayer {
            ca_cert_public_key: &test_ca_cert_public_key,
        };
        let service = service_fn(|_request: Request<Body>| async {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("grpc-status", "0")
                .body(empty_body())
                .unwrap();
            Ok::<_, Status>(response)
        });
        let mut intercepted_service = interceptor.layer(service);

        let request = Request::builder()
            .uri("/cloudprem")
            .body(Body::empty())
            .unwrap();
        let response = intercepted_service.call(request).await.unwrap();
        assert_eq!(
            response.headers().get("grpc-status"),
            Some(&"16".parse().unwrap())
        );

        let encoded_client_cert = urlencoding::encode_binary(TEST_CLIENT_CERT).to_string();
        let request = Request::builder()
            .uri("/cloudprem")
            .header("x-amzn-mtls-clientcert", &encoded_client_cert)
            .body(Body::empty())
            .unwrap();
        let response = intercepted_service.call(request).await.unwrap();
        assert_eq!(
            response.headers().get("grpc-status"),
            Some(&"0".parse().unwrap())
        );
    }

    #[test]
    fn test_verify_client_cert() {
        let verified = verify_client_cert(TEST_CLIENT_CERT, &CA_CERT_PUBLIC_KEY).unwrap();
        assert!(verified.is_none());

        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        let verified = verify_client_cert(TEST_CLIENT_CERT, &test_ca_cert_public_key).unwrap();

        assert!(verified.is_some());
    }
}
