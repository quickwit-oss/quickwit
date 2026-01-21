//! This module implements reverse CloudPrem connection
//!
//! In it, we create a websocket connection to Datadog's edge, and
//! wait for pseudo-gRPC calls, we then

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http::{HeaderValue, StatusCode};
use prost::Message as ProstMessage;
use quickwit_common::retry::RetryParams;
use quickwit_proto::GrpcServiceError;
use quickwit_proto::cloudprem::*;
use quickwit_proto::metastore::{
    GetClusterIdentityRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::tonic::Code;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::JoinSet;
use tokio_tungstenite::client_async_tls;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{
    Error as TungsteniteError, ProtocolError as TungsteniteProtocolError, TlsError,
};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

// Duration below which we consider the connection closed in an unhealty manner independantly of how
// it was closed, and for which we log an error no matter what.
const MIN_HEALTHY_CONN_DURATION: Duration = Duration::from_secs(60);

// TODO we need to add trace id propagation, maybe some form of cancelation too?

async fn handle_request(server: CloudPremServiceClient, full_request: AnyRequest) -> AnyResponse {
    use any_request::Request;
    use any_response::Response;

    macro_rules! handle_err {
        ($e:expr) => {
            match $e {
                Ok(r) => r,
                Err(e) => {
                    let status = e.into_grpc_status();
                    return AnyResponse {
                        req_id: full_request.req_id,
                        grpc_code: status.code() as u32,
                        response: Some(Response::GrpcMessage(status.message().to_string())),
                    };
                }
            }
        };
    }

    let unimplemented = |msg: &str| AnyResponse {
        req_id: full_request.req_id,
        grpc_code: Code::Unimplemented as u32,
        response: Some(Response::GrpcMessage(msg.to_string())),
    };
    let Some(request) = full_request.request else {
        return unimplemented("Missing or unknown request kind");
    };
    let response = match request {
        Request::Ping(ping) => Response::Ping(handle_err!(server.ping(ping).await)),
        Request::List(list) => Response::List(handle_err!(server.list(list).await)),
        Request::FetchOne(fetch_one) => {
            Response::FetchOne(handle_err!(server.fetch_one(fetch_one).await))
        }
        Request::Aggregation(aggregate) => {
            Response::Aggregation(handle_err!(server.aggregate(aggregate).await))
        }
        Request::PullClusterMetrics(pull_cluster_metrics) => Response::PullClusterMetrics(
            handle_err!(server.pull_cluster_metrics(pull_cluster_metrics).await),
        ),
        Request::RootSearch(root_search) => {
            Response::RootSearch(handle_err!(server.root_search(root_search).await))
        }
        Request::RootListTerms(root_list_terms) => {
            Response::RootListTerms(handle_err!(server.root_list_terms(root_list_terms).await))
        }
        Request::RootListFields(root_list_fields) => {
            Response::RootListFields(handle_err!(server.root_list_fields(root_list_fields).await))
        }
        Request::GetIndexes(get_indexes) => {
            Response::GetIndexes(handle_err!(server.get_indexes(get_indexes).await))
        }
        Request::DeleteIndex(delete_index) => {
            Response::DeleteIndex(handle_err!(server.delete_index(delete_index).await))
        }
        Request::UpdateIndex(update_index) => {
            Response::UpdateIndex(handle_err!(server.update_index(update_index).await))
        }
        Request::CreateIndex(create_index) => {
            Response::CreateIndex(handle_err!(server.create_index(create_index).await))
        }

        _ => return unimplemented("Unimplemented request"),
    };
    AnyResponse {
        req_id: full_request.req_id,
        grpc_code: Code::Ok as u32,
        response: Some(response),
    }
}

async fn handle_single_ws_message_and_reply(
    server: CloudPremServiceClient,
    buffer: Bytes,
    response_channel: Sender<Message>,
) {
    let req = match AnyRequest::decode(buffer) {
        Ok(req) => req,
        Err(e) => {
            warn!("received undecodable protobuf frame: {e:?}");
            // we can't even reply with an error for that request: we don't know the req_id
            return;
        }
    };
    let response = handle_request(server, req).await;
    let message = Message::binary(response.encode_to_vec());
    let _ = response_channel.send(message).await;
}

enum Never {}

async fn single_websocket(
    target_domain: &str,
    proxy_url: Option<&http::uri::Authority>,
    dd_api_key: &str,
    service: CloudPremServiceClient,
    cluster_remote_uid: String,
    cluster_name: String,
) -> Result<Never, TungsteniteError> {
    let mut pending_requests = JoinSet::new();
    let (sender, mut receiver) = channel(5);

    let target_url =
        format!("wss://{target_domain}/api/unstable/cloudprem-connection-gateway/connect");
    let mut request = target_url.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("DD-API-KEY", HeaderValue::from_str(dd_api_key)?);

    let stream = proxy::get_proxied_stream(target_domain, proxy_url).await?;
    let (mut ws, _) = client_async_tls(request, stream).await?;

    let cluster_identify = AnyResponse {
        req_id: 0,
        grpc_code: Code::Ok as u32,
        response: Some(any_response::Response::ClusterIdentify(ClusterIdentify {
            org_id: 0,
            cluster_remote_uid,
            name: cluster_name,
        })),
    };
    let cluster_identify_ws = Message::binary(cluster_identify.encode_to_vec());
    ws.send(cluster_identify_ws).await?;

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut unreplied_count = 0;

    loop {
        tokio::select! {
                reply = receiver.recv() => {
                        if let Some(reply) = reply {
                            ws.send(reply).await?;
                        }
                },
                message = ws.next() => {
                        let message = match message {
                            Some(res) => res?,
                            None => return Err(TungsteniteError::ConnectionClosed), // remote isn't supposed to close the connection
                        };
                        match message {
                                Message::Binary(buffer) => {
                                        pending_requests.spawn(handle_single_ws_message_and_reply(service.clone(), buffer, sender.clone()));
                                },
                                Message::Text(payload) => {
                                        unreplied_count = 0;
                                        if payload == "ping" {
                                            ws.send(Message::Text("pong".into())).await?;
                                        }
                                },
                                Message::Close(_) => return Err(TungsteniteError::ConnectionClosed),
                                Message::Ping(payload) => {
                                    // this is likely dead-code, we reply just in case the go library decides to emit websocket ping on its own
                                    ws.send(Message::Pong(payload)).await?;
                                },
                                Message::Pong(_) => (),
                                _ => warn!("received unsupported frame"),
                        }
                },
                _ = interval.tick() => {
                        // This is on main liveliness detection mechanism. We don't use standard websocket ping/pong because they get interpreted by the Go library
                        // which makes it very hard to check for liveliness manually.
                        if unreplied_count > 2 {
                                warn!("other side unresponsive, retrying connection");
                                return Err(TungsteniteError::ConnectionClosed)
                        }
                        unreplied_count += 1;
                        ws.send(Message::Text("ping".into())).await?;
                }
        }
    }
}

// transform errors to success if they are errors we expect to run into in normal condition (such as
// connection reset) for these non-errors, we don't log anything
fn report_err(e: &TungsteniteError) -> bool {
    match e {
        TungsteniteError::ConnectionClosed => false,
        TungsteniteError::AlreadyClosed => false,
        TungsteniteError::Protocol(TungsteniteProtocolError::ResetWithoutClosingHandshake) => false,
        // native tls doesn't less us match on errors
        // TODO verify we get the same error on linux as on macos
        TungsteniteError::Tls(TlsError::Native(e))
            if e.to_string().contains("connection closed via error") =>
        {
            // this is just a "we lost the connection" error, in trials, it happened about once
            // every day in normal operation
            false
        }
        _ => true,
    }
}

fn format_err(err: &TungsteniteError) -> String {
    let prefix = "error in reverse conn:";
    match err {
        TungsteniteError::Http(resp) if resp.status() == StatusCode::FORBIDDEN => {
            format!("{prefix} invalid authentication parameters")
        }
        _ => format!("{prefix} {err:?}"),
    }
}

pub(crate) async fn maintain_websocket(
    target_domain: String,
    dd_api_key: String,
    cluster_name: String,
    service: CloudPremServiceClient,
    metastore: MetastoreServiceClient,
) {
    let mut metastore_cool_down = tokio::time::interval(Duration::from_secs(3));

    // we try to get the cluster id in a loop: in case we start before a metastore is available,
    // we don't want to disable reverse connection, or crashloop for so little
    let cluster_remote_uid = loop {
        metastore_cool_down.tick().await;
        match metastore
            .get_cluster_identity(GetClusterIdentityRequest {})
            .await
        {
            Ok(response) => break response.uuid,
            Err(e) => warn!("failed to get cluster identity from metastore: {e:?}"),
        }
    };

    info!(cluster_remote_uid=%cluster_remote_uid, "fetched cluster remote uid");

    let backoff = RetryParams {
        // if this error comes from an incident at DD, we don't want to hammer the api so much that
        // it never can recover
        max_delay: Duration::from_secs(150),
        // we never stop retrying
        max_attempts: usize::MAX,
        ..RetryParams::aggressive()
    };

    let env_vars = std::env::vars().collect::<Vec<_>>();
    let proxy_url = if !proxy::ignore_proxy(&target_domain, &env_vars) {
        match proxy::get_https_proxy_url(&env_vars)
            .map(proxy::validate_uri)
            .transpose()
        {
            Ok(proxy) => proxy,
            Err(e) => {
                error!("got invalid proxy url: {e:?}");
                return;
            }
        }
    } else {
        None
    };

    if let Some(proxy_url) = proxy_url.as_ref() {
        info!("using proxy: `{proxy_url}`")
    }

    let mut retry_count = 0;

    loop {
        info!("initiating new reverse connection");
        let before_conn = Instant::now();
        let Err(err) = single_websocket(
            &target_domain,
            proxy_url.as_ref(),
            &dd_api_key,
            service.clone(),
            cluster_remote_uid.clone(),
            cluster_name.clone(),
        )
        .await;

        let msg = format_err(&err);
        let too_short = before_conn.elapsed() < MIN_HEALTHY_CONN_DURATION;
        if too_short {
            error!("{msg}")
        } else if report_err(&err) {
            warn!("{msg}")
        }

        if too_short {
            retry_count += 1;
            tokio::time::sleep(backoff.compute_delay(retry_count)).await;
        } else {
            retry_count = 0;
            // we don't need to backoff, the loop already took enough time in itself
        }
    }
}

mod proxy {
    use anyhow::bail;
    use http::uri::Authority;
    use httparse::{Response, Status};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::error::Error as TungsteniteError;

    use crate::cloudprem::InstrumentedStream;

    pub fn ignore_proxy<'a>(
        target: &str,
        env: impl IntoIterator<Item = &'a (String, String)>,
    ) -> bool {
        for (key, value) in env {
            if key.eq_ignore_ascii_case("no_proxy") {
                for rule_fqdn in value.split(',') {
                    // final dot indicate a fully qualified domain name, we always consider the
                    // provided domains as FQDN so we can ignore them
                    let rule = rule_fqdn.trim_end_matches('.');
                    // TODO we don't support IP rules, but it's fine: we only ever use read domain
                    // names, not IP addresses
                    if rule.starts_with('.') {
                        // initial dot indicate a suffix rule
                        if let Some(start_offset) = target.len().checked_sub(rule.len())
                            && rule.eq_ignore_ascii_case(&target[start_offset..])
                        {
                            return true;
                        }
                    } else if rule.eq_ignore_ascii_case(target) {
                        return true;
                    }
                }
                break;
            }
        }
        false
    }

    pub fn get_https_proxy_url<'a>(
        env: impl IntoIterator<Item = &'a (String, String)>,
    ) -> Option<String> {
        let mut all_proxy = None;
        let mut https_proxy = None;
        for (key, value) in env {
            if key.eq_ignore_ascii_case("all_proxy") {
                all_proxy = Some(value);
            } else if key.eq_ignore_ascii_case("https_proxy") {
                https_proxy = Some(value);
            }
        }
        // prefer more specific proxy
        https_proxy.or(all_proxy).cloned()
    }

    pub fn validate_uri(proxy_url: String) -> anyhow::Result<Authority> {
        let parsed_uri = proxy_url.parse::<http::uri::Uri>()?;

        // validation that we support that kind of uri
        if parsed_uri.path() != "/" || parsed_uri.query().is_some() {
            bail!("proxy url should have no path");
        }
        let Some(scheme) = parsed_uri.scheme() else {
            bail!("proxy url should have a scheme");
        };
        if scheme != &http::uri::Scheme::HTTP {
            // TODO we could support other schemes if requested (https or socks5 for instance, but
            // they are less common than plain http proxy)
            bail!("unsupported proxy url scheme: {scheme}");
        }

        let Some(authority) = parsed_uri.authority() else {
            bail!("missing domain name in proxy url")
        };

        Ok(authority.clone())
    }

    // we use the explicit lifetime syntax otherwise our stream captures original_target and proxy,
    // despite not needing them
    pub async fn get_proxied_stream<'a>(
        original_target: &str,
        proxy_opt: Option<&Authority>,
    ) -> Result<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + use<'a>, TungsteniteError> {
        let Some(proxy) = proxy_opt else {
            let stream = tokio::net::TcpStream::connect((original_target, 443)).await?;
            let stream = InstrumentedStream::new(stream);
            return Ok(stream);
        };

        let mut stream =
            tokio::net::TcpStream::connect((proxy.host(), proxy.port_u16().unwrap_or(80))).await?;
        let payload = format!(
            "CONNECT {host}:443 HTTP/1.1\r\nhost: {host}:443\r\nuser-agent: \
             datadog-cloudprem\r\n\r\n",
            host = original_target,
        );
        stream.write_all(payload.as_bytes()).await?;
        stream.flush().await?;

        read_handshake(&mut stream).await?;

        let stream = InstrumentedStream::new(stream);
        Ok(stream)
    }

    async fn read_handshake(stream: &mut tokio::net::TcpStream) -> Result<(), TungsteniteError> {
        let mut buf = Vec::with_capacity(1024);
        let mut read = 0;
        loop {
            buf.resize(buf.len() + 1024, 0);
            let current_start_offset = read;
            let peek_len = stream.peek(&mut buf).await?;
            read += peek_len;

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut response = Response::new(&mut headers);
            match response.parse(&buf[..read]) {
                Err(e) => return Err(std::io::Error::other(e).into()),
                Ok(Status::Partial) => {
                    // we expect an answer in less than 1KiB most of the time, so 16KiB ought to be
                    // plenty
                    if read > 16384 {
                        return Err(std::io::Error::other(
                            "proxy handshaked unfinished after 16kiB",
                        )
                        .into());
                    }
                    if let Some(code) = response.code
                        && code != 200
                    {
                        return Err(std::io::Error::other(format!(
                            "proxy connection failed with http code {code}"
                        ))
                        .into());
                    }
                    // actually comsume so we always make progress
                    stream
                        .read_exact(&mut buf[current_start_offset..read])
                        .await?;
                }
                Ok(Status::Complete(consummed_total)) => {
                    if response.code != Some(200) {
                        return Err(std::io::Error::other(format!(
                            "proxy connection failed with http code {}",
                            response.code.unwrap_or(0)
                        ))
                        .into());
                    }
                    stream
                        .read_exact(&mut buf[current_start_offset..consummed_total])
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_ignore_proxy() {
            // prefix, but not a prefix search
            assert!(!ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,datadoghq.com,abcdef".to_string()
                )]
            ));
            assert!(!ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,datadoghq.com.,abcdef".to_string()
                )]
            ));

            // exact match
            assert!(ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,app.datadoghq.com,abcdef".to_string()
                )]
            ));
            assert!(ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,app.datadoghq.com.,abcdef".to_string()
                )]
            ));

            // prefix search
            assert!(ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,.datadoghq.com,abcdef".to_string()
                )]
            ));
            assert!(ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "No_PrOxY".to_string(),
                    "10.0.0.0/8,.datadoghq.com.,abcdef".to_string()
                )]
            ));

            // wrong variable
            assert!(!ignore_proxy(
                "app.datadoghq.com",
                [&(
                    "something_else".to_string(),
                    "10.0.0.0/8,.datadoghq.com.,abcdef".to_string()
                )]
            ));
            assert!(!ignore_proxy("app.datadoghq.com", []));

            // multiple env var
            assert!(ignore_proxy(
                "app.datadoghq.com",
                [
                    &("abc".to_string(), "def".to_string()),
                    &(
                        "No_PrOxY".to_string(),
                        "10.0.0.0/8,.datadoghq.com.,abcdef".to_string()
                    )
                ]
            ));
        }

        #[test]
        fn test_get_https_proxy_url() {
            assert_eq!(get_https_proxy_url([]), None);
            assert_eq!(
                get_https_proxy_url([&("abc".to_string(), "def".to_string())]),
                None
            );
            assert_eq!(
                get_https_proxy_url([&("http_proxy".to_string(), "url1".to_string())]),
                None
            );

            assert_eq!(
                get_https_proxy_url([&("https_proxy".to_string(), "url1".to_string())]),
                Some("url1".to_string())
            );
            assert_eq!(
                get_https_proxy_url([&("all_proxy".to_string(), "url1".to_string())]),
                Some("url1".to_string())
            );

            assert_eq!(
                get_https_proxy_url([
                    &("all_proxy".to_string(), "url1".to_string()),
                    &("https_proxy".to_string(), "url2".to_string())
                ]),
                Some("url2".to_string())
            );
            assert_eq!(
                get_https_proxy_url([
                    &("https_proxy".to_string(), "url2".to_string()),
                    &("all_proxy".to_string(), "url1".to_string())
                ]),
                Some("url2".to_string())
            );
        }
    }
}
