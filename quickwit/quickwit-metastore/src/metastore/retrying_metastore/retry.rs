// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::fmt::Debug;

use futures::Future;
use quickwit_common::retry::{RetryParams, Retryable};
use tracing::{debug, warn};

/// Retry with exponential backoff and full jitter. Implementation and default values originate from
/// the Java SDK. See also: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>.
pub async fn retry<F, U, E, Fut>(retry_params: &RetryParams, f: F) -> Result<U, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<U, E>>,
    E: Retryable + Debug + 'static,
{
    let mut num_attempts = 0;

    loop {
        let response = f().await;

        num_attempts += 1;

        match response {
            Ok(response) => {
                return Ok(response);
            }
            Err(error) => {
                if !error.is_retryable() {
                    return Err(error);
                }
                if num_attempts >= retry_params.max_attempts {
                    warn!(
                        num_attempts=%num_attempts,
                        "Request failed"
                    );
                    return Err(error);
                }
                let delay = retry_params.compute_delay(num_attempts);
                debug!(
                    num_attempts=%num_attempts,
                    delay_millis=%delay.as_millis(),
                    error=?error,
                    "Request failed, retrying"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use futures::future::ready;

    use super::{retry, RetryParams, Retryable};

    #[derive(Debug, Eq, PartialEq)]
    pub enum Retry<E> {
        Transient(E),
        Permanent(E),
    }

    impl<E> Retryable for Retry<E> {
        fn is_retryable(&self) -> bool {
            match self {
                Retry::Transient(_) => true,
                Retry::Permanent(_) => false,
            }
        }
    }

    async fn simulate_retries<T>(values: Vec<Result<T, Retry<usize>>>) -> Result<T, Retry<usize>> {
        let values_it = RwLock::new(values.into_iter());
        retry(&RetryParams::for_test(), || {
            ready(values_it.write().unwrap().next().unwrap())
        })
        .await
    }

    #[tokio::test]
    async fn test_retry_accepts_ok() {
        assert_eq!(simulate_retries(vec![Ok(())]).await, Ok(()));
    }

    #[tokio::test]
    async fn test_retry_does_retry() {
        assert_eq!(
            simulate_retries(vec![Err(Retry::Transient(1)), Ok(())]).await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn test_retry_stops_retrying_on_non_retryable_error() {
        assert_eq!(
            simulate_retries(vec![Err(Retry::Permanent(1)), Ok(())]).await,
            Err(Retry::Permanent(1))
        );
    }

    #[tokio::test]
    async fn test_retry_retries_up_at_most_attempts_times() {
        let retry_sequence: Vec<_> = (0..30)
            .map(|retry_id| Err(Retry::Transient(retry_id)))
            .chain(Some(Ok(())))
            .collect();
        assert_eq!(
            simulate_retries(retry_sequence).await,
            Err(Retry::Transient(29))
        );
    }

    #[tokio::test]
    async fn test_retry_retries_up_to_max_attempts_times() {
        let retry_sequence: Vec<_> = (0..29)
            .map(|retry_id| Err(Retry::Transient(retry_id)))
            .chain(Some(Ok(())))
            .collect();
        assert_eq!(simulate_retries(retry_sequence).await, Ok(()));
    }
}
