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
use quickwit_proto::tonic::Code;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::JoinSet;
use tokio_tungstenite::connect_async;
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
    target: &str,
    dd_api_key: &str,
    dd_application_key: &str,
    service: CloudPremServiceClient,
) -> Result<Never, TungsteniteError> {
    let mut pending_requests = JoinSet::new();
    let (sender, mut receiver) = channel(5);

    let mut request = target.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("DD-API-KEY", HeaderValue::from_str(dd_api_key)?);
    headers.insert("DD-APPLICATION-KEY", dd_application_key.parse()?);

    let (mut ws, _) = connect_async(request).await?;

    let cluster_identify = AnyResponse {
        req_id: 0,
        grpc_code: Code::Ok as u32,
        response: Some(any_response::Response::ClusterIdentify(ClusterIdentify {
            org_id: 0,
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
    target: String,
    dd_api_key: String,
    dd_application_key: String,
    service: CloudPremServiceClient,
) {
    let backoff = RetryParams {
        // if this error comes from an incident at DD, we don't want to hammer the api so much that
        // it never can recover
        max_delay: Duration::from_secs(150),
        // we never stop retrying
        max_attempts: usize::MAX,
        ..RetryParams::aggressive()
    };
    let url = format!("wss://{target}/api/unstable/cloudprem-connection-gateway/connect");

    let mut retry_count = 0;

    loop {
        info!("new connection");
        let before_conn = Instant::now();
        let Err(err) =
            single_websocket(&url, &dd_api_key, &dd_application_key, service.clone()).await;

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
