//! This module implements reverse CloudPrem connection
//!
//! In it, we create a websocket connection to Datadog's edge, and
//! wait for pseudo-gRPC calls, we then

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use quickwit_proto::GrpcServiceError;
use quickwit_proto::cloudprem::*;
use quickwit_proto::tonic::Code;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::JoinSet;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

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
        Err(_) => todo!(),
    };
    let response = handle_request(server, req).await;
    let message = Message::binary(response.encode_to_vec());
    let _ = response_channel.send(message).await;
}

async fn single_websocket(
    target: &str,
    dd_token: &str,
    service: CloudPremServiceClient,
) -> anyhow::Result<()> {
    let mut pending_requests = JoinSet::new();
    let (sender, mut receiver) = channel(5);

    let mut request = target.into_client_request()?;
    request
        .headers_mut()
        .insert("DD-API-KEY", dd_token.parse()?);

    let (mut ws, _) = connect_async(target).await?;

    let cluster_identify = AnyResponse {
        req_id: 0,
        grpc_code: Code::Ok as u32,
        response: Some(any_response::Response::ClusterIdentify(ClusterIdentify {
            org_id: 0,
        })),
    };
    let cluster_identify_ws = Message::binary(cluster_identify.encode_to_vec());
    ws.send(cluster_identify_ws).await?;

    loop {
        tokio::select! {
                reply = receiver.recv() => {
                        if let Some(reply) = reply {
                            if let Err(e) = ws.send(reply).await {
                                todo!("some errors should gracefull exit, other should return an err {e:?}")
                            }
                        }
                },
                message = ws.next() => {
                        let message = match message {
                            Some(Ok(message)) => message,
                            Some(Err(e)) => todo!("some errors should gracefull exit, other should return an err {e:?}"),
                            None => return Ok(()),
                        };
                        match message {
                                Message::Binary(buffer) => {
                                        pending_requests.spawn(handle_single_ws_message_and_reply(service.clone(), buffer, sender.clone()));
                                },
                                Message::Ping(ping) => {
                                        sender.send(Message::Pong(ping)).await?;
                                },
                                Message::Pong(_pong) => (),// TODO reset some clock that tells us if our peer is alive
                                Message::Close(_close) => todo!(),
                                _ => todo!(),
                        }
                }
                // TODO send pings regularly
        }
    }
}

pub(crate) async fn maintain_websocket(
    target: String,
    dd_token: String,
    service: CloudPremServiceClient,
) {
    // TODO maybe add endpoint as a suffix?
    loop {
        // TODO some errors should be fatal (invalid token)
        if let Err(e) = single_websocket(&target, &dd_token, service.clone()).await {
            tracing::error!("error in reverse conn: {e:?}")
        }
    }
}
