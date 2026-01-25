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

use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use futures::StreamExt;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_proto::ingest::ingester::{
    DecommissionRequest, IngesterService, IngesterStatus, OpenObservationStreamRequest,
};
use tracing::info;

/// Tries to get the current status of an ingester by opening an observation stream
/// and reading the first message.
///
/// # Errors
///
/// Returns an error if:
/// - The observation stream fails to open
/// - The stream ends without producing a message
/// - The stream ends after returning an error
pub async fn try_get_ingester_status(
    ingester: &impl IngesterService,
) -> anyhow::Result<IngesterStatus> {
    let mut observation_stream = ingester
        .open_observation_stream(OpenObservationStreamRequest {})
        .await
        .context("failed to open observation stream")?;

    let next_observation_message = observation_stream
        .next()
        .await
        .context("observation stream ended")?
        .context("observation stream failed")?;

    Ok(next_observation_message.status())
}

/// Waits for an ingester to reach a specific status by monitoring its observation stream.
///
/// This function continuously polls the observation stream until the ingester reaches
/// the desired status.
///
/// # Errors
///
/// Returns an error if:
/// - The observation stream fails to open
/// - The stream ends without producing a message
/// - The stream ends after returning an error
/// - The timeout is exceeded
pub async fn wait_for_ingester_status(
    ingester: &impl IngesterService,
    status: IngesterStatus,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        timeout_after,
        wait_for_ingester_status_inner(ingester, status),
    )
    .await
    .with_context(|| {
        format!(
            "timed out waiting for ingester to transition to status {status} after {} seconds",
            timeout_after.as_secs(),
        )
    })?
}

async fn wait_for_ingester_status_inner(
    ingester: &impl IngesterService,
    status: IngesterStatus,
) -> anyhow::Result<()> {
    let mut observation_stream = ingester
        .open_observation_stream(OpenObservationStreamRequest {})
        .await
        .context("failed to open observation stream")?;

    loop {
        match observation_stream.next().await {
            Some(Ok(observation_message)) => {
                if observation_message.status() == status {
                    return Ok(());
                }
            }
            Some(Err(error)) => {
                return Err(anyhow!(error).context("observation stream failed"));
            }
            None => {
                bail!("observation stream ended");
            }
        }
    }
}

/// Initiates decommission of an ingester and waits for it to complete.
///
/// This function sends a decommission request to the ingester and then waits
/// for it to reach the `Decommissioned` status.
///
/// # Errors
///
/// Returns an error if:
/// - The decommission request fails
/// - The observation stream fails to open
/// - The stream ends without producing a message
/// - The stream ends after returning an error
/// - The timeout is exceeded
pub async fn wait_for_ingester_decommission(
    ingester: &impl IngesterService,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let now = Instant::now();

    ingester
        .decommission(DecommissionRequest {})
        .await
        .context("failed to initiate ingester decommission")?;

    wait_for_ingester_status(
        ingester,
        IngesterStatus::Decommissioned,
        now.elapsed().saturating_sub(timeout_after),
    )
    .await?;

    info!(
        "successfully decommissioned ingester in {}",
        now.elapsed().pretty_display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use quickwit_common::ServiceStream;
    use quickwit_proto::ingest::ingester::{
        DecommissionResponse, IngesterServiceClient, MockIngesterService, ObservationMessage,
    };

    use super::*;

    #[tokio::test]
    async fn test_try_get_ingester_status() {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .once()
            .returning(|_| {
                let (service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Initializing as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();
                Ok(service_stream)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        let status = try_get_ingester_status(&ingester).await.unwrap();
        assert_eq!(status, IngesterStatus::Initializing);
    }

    #[tokio::test]
    async fn test_wait_for_ingester_status() {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .once()
            .returning(|_| {
                let (service_stream_tx, service_stream) = ServiceStream::new_bounded(2);
                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Initializing as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Ready as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();
                Ok(service_stream)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        wait_for_ingester_status(&ingester, IngesterStatus::Ready, Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_ingester_decommission() {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .once()
            .returning(|_| {
                let (service_stream_tx, service_stream) = ServiceStream::new_bounded(3);
                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Ready as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Decommissioning as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Decommissioned as i32,
                };
                service_stream_tx.try_send(Ok(message)).unwrap();
                Ok(service_stream)
            });
        mock_ingester
            .expect_decommission()
            .once()
            .returning(|_| Ok(DecommissionResponse {}));
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        wait_for_ingester_decommission(&ingester, Duration::from_secs(1))
            .await
            .unwrap();
    }
}
