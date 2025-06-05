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

use std::net::SocketAddr;
use std::time::Duration;

use futures::Future;
use hyper::Uri;
use tokio::time::error::Elapsed;
use tower::Service as _;

pub async fn wait_until_predicate<Fut>(
    predicate: impl Fn() -> Fut,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<(), Elapsed>
where
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(timeout, async move {
        loop {
            if predicate().await {
                break;
            }
            tokio::time::sleep(retry_interval).await
        }
    })
    .await
}

/// Tries to connect at most 3 times to `SocketAddr`.
/// If not successful, returns an error.
/// This is a convenient function to wait before sending gRPC requests
/// to this `SocketAddr`.
pub async fn wait_for_server_ready(socket_addr: SocketAddr) -> anyhow::Result<()> {
    let mut num_attempts = 0;
    let max_num_attempts = 10;
    let uri = Uri::builder()
        .scheme("http")
        .authority(socket_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;

    while num_attempts < max_num_attempts {
        tokio::time::sleep(Duration::from_millis(50 * (num_attempts + 1))).await;
        let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
        match http.call(uri.clone()).await {
            Ok(_) => break,
            Err(_) => {
                println!(
                    "Failed to connect to `{}` failed, retrying {}/{}",
                    socket_addr,
                    num_attempts + 1,
                    max_num_attempts
                );
                num_attempts += 1;
            }
        }
    }
    if num_attempts == max_num_attempts {
        anyhow::bail!("too many attempts to connect to `{}`", socket_addr);
    }
    Ok(())
}
