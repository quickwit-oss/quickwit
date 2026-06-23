use std::sync::{Arc, LazyLock};
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, bail};
use futures::future::{Either, Ready, ready};
use openssl::pkey::{PKey, PKeyRef, Public};
use openssl::x509::X509;
use quickwit_proto::tonic::Status;
use quickwit_proto::tonic::body::Body;
use quickwit_proto::tonic::codegen::http::{Request, Response};
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

const CLOUDPREM_PATH: &str = "/cloudprem";
pub(crate) const AWS_MTLS_HEADER: &str = "X-Amzn-Mtls-Clientcert";

/// mTLS header interceptor.
///
/// This interceptor parses and verifies the CloudPrem bridge client certificate.
///
/// Some load balancers support terminating mTLS connections and forwarding the client certificate
/// as a header. They can also carry intermediate certificates, but in the context of CloudPrem,
/// we always expect exactly one certificate.
///
/// The AWS ALB is such a load balancer, it forwards URL-encoded certs in the
/// `X-Amzn-Mtls-Clientcert` header. https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html
///
/// Traefik also supports this feature, and forwards certs stripped from newlines in the
/// `X-Forwarded-Tls-Client-Cert` header. https://doc.traefik.io/traefik/middlewares/http/passtlsclientcert/
///
/// Newlines are always ignored and decoding a document containing no `%` leaves content
/// unmodified, so we can trivially support both those cases by always URL-decoding received
/// certificates.
fn mtls_header_interceptor_impl<T>(
    request: Request<T>,
    ca_cert_public_key: &PKeyRef<Public>,
    header_name: &str,
    protected_path: &str,
) -> Result<Request<T>, Box<Status>> {
    let path = request.uri().path();
    let is_external_traffic = path.starts_with(protected_path);

    if !is_external_traffic {
        return Ok(request);
    }
    let Some(encoded_client_cert) = request.headers().get(header_name) else {
        return Err(Box::new(Status::unauthenticated(
            "could not find client certificate",
        )));
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
                return Err(Box::new(Status::invalid_argument(
                    "unparseable subject".to_string(),
                )));
            };
            let subject = subject.join(", ");
            info!(target: "audit_log", path, subject, "received request");
            Ok(request)
        }
        Ok(None) => Err(Box::new(Status::unauthenticated(
            "failed to verify client certificate",
        ))),
        Err(error) => Err(Box::new(Status::invalid_argument(error.to_string()))),
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
pub(crate) struct MtlsHeaderInterceptor<'a, S> {
    inner: S,
    ca_cert_public_key: &'a PKeyRef<Public>,
    header_name: Arc<str>,
    protected_path: &'a str,
}

impl<S> Service<Request<Body>> for MtlsHeaderInterceptor<'_, S>
where S: Service<Request<Body>, Response = Response<Body>>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        match mtls_header_interceptor_impl(
            request,
            self.ca_cert_public_key,
            &self.header_name,
            self.protected_path,
        ) {
            Ok(request) => Either::Left(self.inner.call(request)),
            Err(status) => Either::Right(ready(Ok(status.into_http()))),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MtlsHeaderInterceptorLayer<'a> {
    ca_cert_public_key: &'a PKeyRef<Public>,
    header_name: Arc<str>,
    protected_path: &'a str,
}

impl MtlsHeaderInterceptorLayer<'static> {
    pub fn for_cloudprem_port(header_name: Option<String>) -> Self {
        Self {
            ca_cert_public_key: &CA_CERT_PUBLIC_KEY,
            header_name: header_name
                .unwrap_or_else(|| AWS_MTLS_HEADER.to_string())
                .into(),
            protected_path: "", // on the CloudPrem port, we do auth for everything
        }
    }

    pub fn for_grpc_port() -> Self {
        Self {
            ca_cert_public_key: &CA_CERT_PUBLIC_KEY,
            header_name: AWS_MTLS_HEADER.to_string().into(),
            protected_path: CLOUDPREM_PATH,
        }
    }
}

impl<'a, S> Layer<S> for MtlsHeaderInterceptorLayer<'a> {
    type Service = MtlsHeaderInterceptor<'a, S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            ca_cert_public_key: self.ca_cert_public_key,
            header_name: self.header_name.clone(),
            protected_path: self.protected_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;
    use quickwit_proto::tonic::{Code, Status};
    use tower::service_fn;

    use super::*;

    const TEST_CA_CERT: &[u8] =
        include_bytes!("../../../resources/tests/tls/ca.crt");
    const TEST_CLIENT_CERT: &[u8] =
        include_bytes!("../../../resources/tests/tls/server.crt");

    #[test]
    fn test_mtls_header_interceptor_impl() {
        // Internal traffic should be allowed
        let request = Request::builder().uri("/api/v1/indexes").body(()).unwrap();
        mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            AWS_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap();

        // External traffic without client certificate should be rejected
        let request = Request::builder().uri("/cloudprem").body(()).unwrap();
        let status = mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            AWS_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with invalid client certificate should be rejected
        let encoded_client_cert = urlencoding::encode_binary(TEST_CLIENT_CERT).to_string();

        let request = Request::builder()
            .uri("/cloudprem")
            .header("x-amzn-mtls-clientcert", &encoded_client_cert)
            .body(())
            .unwrap();
        let status = mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            AWS_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with valid client certificate should be allowed
        let request = Request::builder()
            .uri("/cloudprem")
            .header("x-amzn-mtls-clientcert", &encoded_client_cert)
            .body(())
            .unwrap();
        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        mtls_header_interceptor_impl(
            request,
            &test_ca_cert_public_key,
            AWS_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap();
    }

    #[test]
    fn test_traefik_mtls_interceptor_impl() {
        const TRAEFIK_MTLS_HEADER: &str = "X-Forwarded-Tls-Client-Cert";
        // Internal traffic should be allowed
        let request = Request::builder().uri("/api/v1/indexes").body(()).unwrap();
        mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            TRAEFIK_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap();

        // External traffic without client certificate should be rejected
        let request = Request::builder().uri("/cloudprem").body(()).unwrap();
        let status = mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            TRAEFIK_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with invalid client certificate should be rejected
        let encoded_client_cert = std::str::from_utf8(TEST_CLIENT_CERT)
            .unwrap()
            .replace('\n', "");

        let request = Request::builder()
            .uri("/cloudprem")
            .header(TRAEFIK_MTLS_HEADER, &encoded_client_cert)
            .body(())
            .unwrap();
        let status = mtls_header_interceptor_impl(
            request,
            &CA_CERT_PUBLIC_KEY,
            TRAEFIK_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);

        // External traffic with valid client certificate should be allowed
        let request = Request::builder()
            .uri("/cloudprem")
            .header(TRAEFIK_MTLS_HEADER, &encoded_client_cert)
            .body(())
            .unwrap();
        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        mtls_header_interceptor_impl(
            request,
            &test_ca_cert_public_key,
            TRAEFIK_MTLS_HEADER,
            CLOUDPREM_PATH,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_mtls_header_interceptor_layer() {
        let test_ca_cert_public_key = X509::from_pem(TEST_CA_CERT).unwrap().public_key().unwrap();
        let interceptor = MtlsHeaderInterceptorLayer {
            ca_cert_public_key: &test_ca_cert_public_key,
            header_name: AWS_MTLS_HEADER.to_string().into(),
            protected_path: CLOUDPREM_PATH,
        };
        let service = service_fn(|_request: Request<Body>| async {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("grpc-status", "0")
                .body(Body::empty())
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
