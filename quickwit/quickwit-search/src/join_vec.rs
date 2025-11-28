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

use tokio::task::{Id, JoinError, JoinSet};

use crate::SearchError;

/// Ordered equivalent of [`tokio::task::JoinSet`] that cancels all tasks on
/// first error encountered.
///
/// When the `TryJoinVec` is dropped, all associated tasks are immediately
/// aborted.
pub struct TryJoinVec<T, E> {
    /// Tasks submitted to the underlying JoinSet wrap their output with their
    /// spawn order.
    join_set: JoinSet<(usize, Result<T, E>)>,
}

#[derive(Debug)]
pub enum TryJoinVecError<E> {
    JoinError(JoinError),
    Error(E),
}

impl<E: Into<SearchError>> From<TryJoinVecError<E>> for SearchError {
    fn from(err: TryJoinVecError<E>) -> Self {
        match err {
            TryJoinVecError::JoinError(join_err) => join_err.into(),
            TryJoinVecError::Error(e) => e.into(),
        }
    }
}

impl<E> From<JoinError> for TryJoinVecError<E> {
    fn from(err: JoinError) -> Self {
        TryJoinVecError::JoinError(err)
    }
}

impl<T, E> TryJoinVec<T, E> {
    pub fn new() -> Self {
        Self {
            join_set: JoinSet::new(),
        }
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let next_index = self.join_set.len();
        self.join_set
            .spawn(async move { (next_index, future.await) });
    }
}

impl<T, E> TryJoinVec<T, E> {
    /// Joins all tasks, returning their results in the order they were spawned.
    ///
    /// If any task returns an error or joining fails, the first failure
    /// encountered is returned and all other tasks are aborted.
    pub async fn try_join_all(self) -> Result<Vec<T>, TryJoinVecError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let task_count = self.join_set.len();
        let mut staging_results: Vec<Option<T>> = (0..task_count).map(|_| None).collect();
        let mut join_set = self.join_set;

        while let Some(join_result) = join_set.join_next().await {
            // when returning early, the owned JoinSet is dropped,
            // aborting all other tasks
            let (index, result) = join_result?;
            match result {
                Ok(value) => staging_results[index] = Some(value),
                Err(e) => return Err(TryJoinVecError::Error(e)),
            }
        }

        let results = staging_results
            .into_iter()
            .map(|opt| opt.unwrap())
            .collect();
        Ok(results)
    }
}

/// Ordered equivalent of [`tokio::task::JoinSet`] which returns all task results.
///
/// When the `JoinVec` is dropped, all associated tasks are immediately aborted.
pub struct JoinVec<T, V> {
    /// Tasks submitted to the underlying JoinSet wrap their output with their
    /// spawn order.
    join_set: JoinSet<(usize, T)>,
    /// Keeps track of the spawn order using task IDs in case of panics.
    task_ids: Vec<(Id, V)>,
}

impl<T, V> JoinVec<T, V> {
    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            join_set: JoinSet::new(),
            task_ids: Vec::with_capacity(capacity),
        }
    }

    pub fn spawn<F>(&mut self, value: V, future: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let next_index = self.task_ids.len();
        let handle = self
            .join_set
            .spawn(async move { (next_index, future.await) });
        self.task_ids.push((handle.id(), value));
    }

    pub async fn join_all(self) -> Vec<(V, Result<T, JoinError>)>
    where T: Send + 'static {
        let mut staging_results: Vec<Option<Result<T, JoinError>>> =
            (0..self.task_ids.len()).map(|_| None).collect();
        let mut join_set = self.join_set;

        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok((idx, val)) => {
                    staging_results[idx] = Some(Ok(val));
                }
                Err(e) => {
                    let idx = self
                        .task_ids
                        .iter()
                        .position(|&(task_id, _)| task_id == e.id())
                        .unwrap();
                    staging_results[idx] = Some(Err(e));
                }
            };
        }

        let results = staging_results
            .into_iter()
            .zip(self.task_ids)
            .map(|(result_opt, (_, value))| (value, result_opt.unwrap()))
            .collect();
        results
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, sleep};

    use super::*;

    #[tokio::test]
    async fn test_try_join_all_maintains_insertion_order() {
        let mut join_vec = TryJoinVec::new();

        for i in 0..5 {
            let sleep_ms = 5 - i;
            join_vec.spawn(async move {
                sleep(Duration::from_millis(sleep_ms)).await;
                Ok::<_, String>(i)
            });
        }

        let results = join_vec.try_join_all().await.unwrap();

        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_try_join_all_aborts_remaining_tasks_on_panic() {
        let mut join_vec = TryJoinVec::new();
        let (tx_1, rx_1) = tokio::sync::oneshot::channel::<()>();
        let (tx_2, rx_2) = tokio::sync::oneshot::channel::<()>();

        join_vec.spawn(async move {
            let _ = tx_1.send(());
            Ok::<_, String>(1)
        });

        join_vec.spawn(async move {
            sleep(Duration::from_millis(5)).await;
            panic!("intentional panic");
        });

        join_vec.spawn(async move {
            sleep(Duration::from_secs(1)).await;
            // we expect tx to be dropped before send() is called
            let _ = tx_2.send(());
            Ok::<_, String>(1)
        });

        let result = join_vec.try_join_all().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TryJoinVecError::JoinError(_)));
        // wait to make sure the task is aborted and dropped
        sleep(Duration::from_millis(1)).await;
        assert!(rx_1.await.is_ok());
        assert!(rx_2.await.is_err());
    }

    #[tokio::test]
    async fn test_try_join_all_aborts_remaining_tasks_on_error() {
        let mut join_vec = TryJoinVec::new();
        let (tx_1, rx_1) = tokio::sync::oneshot::channel::<()>();
        let (tx_2, rx_2) = tokio::sync::oneshot::channel::<()>();

        join_vec.spawn(async move {
            sleep(Duration::from_secs(1)).await;
            // we expect tx to be dropped before send() is called
            let _ = tx_1.send(());
            Ok::<_, String>(())
        });

        join_vec.spawn(async move { Err::<(), _>("intentional error".to_string()) });

        join_vec.spawn(async move {
            sleep(Duration::from_secs(1)).await;
            // we expect tx to be dropped before send() is called
            let _ = tx_2.send(());
            Ok::<_, String>(())
        });

        let result = join_vec.try_join_all().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TryJoinVecError::Error(_)));
        // wait to make sure the task is aborted and dropped
        sleep(Duration::from_millis(1)).await;
        assert!(rx_1.await.is_err());
        assert!(rx_2.await.is_err());
    }

    #[tokio::test]
    async fn test_join_all_maintains_insertion_order() {
        let mut join_vec = JoinVec::new_with_capacity(5);

        for i in 0..5 {
            let sleep_ms = 5 - i;
            join_vec.spawn(10 + i, async move {
                sleep(Duration::from_millis(sleep_ms)).await;
                i as usize
            });
        }

        let results = join_vec.join_all().await;
        assert_eq!(results.len(), 5);
        for (idx, (val, result)) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), idx);
            assert_eq!(val, 10 + idx as u64);
        }
    }

    #[tokio::test]
    async fn test_join_all_panic() {
        let mut join_vec = JoinVec::new_with_capacity(1);

        join_vec.spawn("spawn_1", async move { 1 });

        join_vec.spawn("spawn_2", async move {
            sleep(Duration::from_millis(5)).await;
            panic!("intentional panic");
        });

        join_vec.spawn("spawn_3", async move {
            sleep(Duration::from_millis(10)).await;
            2
        });

        let result = join_vec.join_all().await;
        assert!(result.len() == 3);
        assert!(matches!(result[0], ("spawn_1", Ok(1))));
        assert!(matches!(result[1], ("spawn_2", Err(_))));
        assert!(matches!(result[2], ("spawn_3", Ok(2))));
    }
}
