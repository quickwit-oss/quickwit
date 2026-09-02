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

use anyhow::{Context, anyhow};
use bytesize::ByteSize;
use futures::StreamExt;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_proto::ingest::ingester::{
    DecommissionRequest, IngesterService, IngesterStatus, ObservationMessage,
    OpenObservationStreamRequest,
};
use tracing::{error, info};

use super::metrics::{DECOMMISSION_FAILED, DECOMMISSION_SUCCEEDED};

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
    wait_for_ingester_status_inner(ingester, status, timeout_after)
        .await
        .map_err(|(error, _last_observation)| error)
}

/// Initiates decommission of an ingester, if one is present. Returns an error if the decommission
/// request fails.
pub async fn notify_ingester_decommission(
    ingester_opt: Option<&impl IngesterService>,
) -> anyhow::Result<()> {
    let Some(ingester) = ingester_opt else {
        return Ok(());
    };
    ingester
        .decommission(DecommissionRequest {})
        .await
        .context("failed to initiate ingester decommission")?;
    Ok(())
}

/// Waits for an ingester to reach the `Decommissioned` status, if one is present.
pub async fn wait_for_ingester_decommission(
    ingester_opt: Option<&impl IngesterService>,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let Some(ingester) = ingester_opt else {
        return Ok(());
    };
    let now = Instant::now();

    match wait_for_ingester_status_inner(ingester, IngesterStatus::Decommissioned, timeout_after)
        .await
    {
        Ok(()) => {
            DECOMMISSION_SUCCEEDED.inc();
            info!(
                "successfully decommissioned ingester in {}",
                now.elapsed().pretty_display()
            );
            Ok(())
        }
        Err((error, last_observation)) => {
            DECOMMISSION_FAILED.inc();
            let error = error.context(format!(
                "failed to decommission ingester after {}",
                timeout_after.pretty_display()
            ));
            log_ingester_decommission_failure(&error, &last_observation);
            Err(error)
        }
    }
}

async fn wait_for_ingester_status_inner(
    ingester: &impl IngesterService,
    status: IngesterStatus,
    timeout_after: Duration,
) -> Result<(), (anyhow::Error, Option<ObservationMessage>)> {
    debug_assert!(
        timeout_after > Duration::ZERO,
        "timeout_after should be greater than zero"
    );
    let mut last_observation: Option<ObservationMessage> = None;

    let sleep = tokio::time::sleep(timeout_after);
    tokio::pin!(sleep);

    let mut observation_stream = tokio::select! {
        result = ingester.open_observation_stream(OpenObservationStreamRequest {}) => {
            result
                .context("failed to open observation stream")
                .map_err(|error| (error, None))?
        }
        _ = &mut sleep => {
            let error = anyhow!(
                "timed out while waiting for ingester to transition to status {status} after {}",
                timeout_after.pretty_display(),
            );
            return Err((error, None));
        }
    };
    loop {
        tokio::select! {
            observation = observation_stream.next() => {
                match observation {
                    Some(Ok(observation_message)) => {
                        if observation_message.status() == status {
                            return Ok(());
                        }
                        last_observation = Some(observation_message);
                    }
                    Some(Err(error)) => {
                        let error = anyhow!(error).context("observation stream failed");
                        return Err((error, last_observation));
                    }
                    None => {
                        return Err((anyhow!("observation stream ended"), last_observation));
                    }
                }
            }
            _ = &mut sleep => {
                let error = anyhow!(
                    "timed out while waiting for ingester to transition to status {status} after {}",
                    timeout_after.pretty_display(),
                );
                return Err((error, last_observation));
            }
        }
    }
}

fn log_ingester_decommission_failure(
    error: &anyhow::Error,
    last_observation: &Option<ObservationMessage>,
) {
    match last_observation {
        Some(observation_message) => error!(
            %error,
            "{} record(s) remaining in WAL, using {} of memory and {} of disk",
            observation_message.wal_num_records,
            ByteSize(observation_message.wal_memory_used_bytes),
            ByteSize(observation_message.wal_disk_used_bytes),
        ),
        None => error!(%error, "failed to decommission ingester"),
    }
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
                    ..Default::default()
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
                    ..Default::default()
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Ready as i32,
                    ..Default::default()
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
    async fn test_wait_for_ingester_decommission_elapsed_timeout_not_zero() {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .once()
            .returning(|_| {
                let (service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                // Simulate the ingester transitioning to Decommissioned after 50ms.
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let message = ObservationMessage {
                        node_id: "test-ingester".to_string(),
                        status: IngesterStatus::Decommissioned as i32,
                        ..Default::default()
                    };
                    service_stream_tx.try_send(Ok(message)).unwrap();
                });
                Ok(service_stream)
            });
        mock_ingester
            .expect_decommission()
            .once()
            .returning(|_| Ok(DecommissionResponse {}));
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        notify_ingester_decommission(Some(&ingester)).await.unwrap();
        wait_for_ingester_decommission(Some(&ingester), Duration::from_secs(1))
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
                    ..Default::default()
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Decommissioning as i32,
                    ..Default::default()
                };
                service_stream_tx.try_send(Ok(message)).unwrap();

                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: IngesterStatus::Decommissioned as i32,
                    ..Default::default()
                };
                service_stream_tx.try_send(Ok(message)).unwrap();
                Ok(service_stream)
            });
        mock_ingester
            .expect_decommission()
            .once()
            .returning(|_| Ok(DecommissionResponse {}));
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        notify_ingester_decommission(Some(&ingester)).await.unwrap();
        wait_for_ingester_decommission(Some(&ingester), Duration::from_secs(1))
            .await
            .unwrap();
    }
}
